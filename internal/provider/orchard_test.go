package provider

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/ysya/runscaler/internal/config"
)

type fakeOrchard struct {
	mu         sync.Mutex
	vms        map[string]orchardVM
	creates    int
	deletes    []string
	requests   []orchardStarterRequest
	state      string
	log        string
	createHook func(orchardVM) error
}

func (f *fakeOrchard) Check(context.Context) error { return nil }

func (f *fakeOrchard) Get(_ context.Context, name string) (orchardVM, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	vm, ok := f.vms[name]
	if !ok {
		return vm, &orchardHTTPError{Status: 404}
	}
	return vm, nil
}
func (f *fakeOrchard) Create(_ context.Context, vm orchardVM) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.createHook != nil {
		if err := f.createHook(vm); err != nil {
			return err
		}
	}
	f.creates++
	vm.UID = "uid-" + vm.Name
	vm.Status = "pending"
	f.vms[vm.Name] = vm
	return nil
}
func (f *fakeOrchard) Delete(_ context.Context, name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deletes = append(f.deletes, name)
	delete(f.vms, name)
	return nil
}
func (f *fakeOrchard) Exec(_ context.Context, _ string, _ string, input orchardStarterRequest) (orchardStarterStatus, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.requests = append(f.requests, input)
	if input.Action == "probe" {
		return orchardStarterStatus{State: "ready"}, nil
	}
	state := f.state
	if state == "" {
		state = "starting"
	}
	return orchardStarterStatus{State: state, Log: f.log}, nil
}

func orchardTestProvider(t *testing.T) (*OrchardProvider, *fakeOrchard) {
	t.Helper()
	ss := config.ScaleSetConfig{RegistrationURL: "https://github.com/example/project", ScaleSetName: "macos", Provider: "orchard", RunnerImage: "registry.example/macos:runner", Orchard: config.OrchardConfig{URL: "http://127.0.0.1:6120", StateDir: t.TempDir(), CPU: 4, Memory: 8192, RunnerDir: "/opt/actions-runner", StarterCommand: "/usr/local/bin/test-starter", PollInterval: time.Millisecond}}
	ledger, err := openOrchardLedger(ss.Orchard.StateDir, ss.RegistrationURL+"\x00"+ss.ScaleSetName, ss.Orchard.URL)
	if err != nil {
		t.Fatal(err)
	}
	fake := &fakeOrchard{vms: map[string]orchardVM{}}
	p := &OrchardProvider{config: ss, api: fake, ledger: ledger, logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	t.Cleanup(func() { p.Shutdown(context.Background()) })
	return p, fake
}

func TestOrchardRequestPersistsIntentAndBudgetsBeforeCreate(t *testing.T) {
	p, fake := orchardTestProvider(t)
	p.config.Orchard.WorkerLabels = map[string]string{"platform": "macos"}
	t.Setenv("ORCHARD_TEST_PASSWORD", "guest-password-secret")
	p.config.Orchard.PasswordEnv = "ORCHARD_TEST_PASSWORD"
	fake.createHook = func(vm orchardVM) error {
		raw, err := os.ReadFile(p.ledger.path)
		if err != nil {
			return err
		}
		if !strings.Contains(string(raw), vm.Name) {
			t.Error("request was not durable before create")
		}
		if strings.Contains(string(raw), "guest-password-secret") {
			t.Error("guest credential persisted")
		}
		if vm.Resources["org.cirruslabs.logical-cores"] != 4 || vm.Resources["org.cirruslabs.memory-mib"] != 8192 || vm.Resources["org.cirruslabs.tart-vms"] != 1 {
			t.Errorf("wrong admission budget: %v", vm.Resources)
		}
		if len(vm.Labels) != 1 || vm.Labels["platform"] != "macos" {
			t.Error("ownership polluted worker selectors")
		}
		if vm.Password != "guest-password-secret" {
			t.Error("guest credential was not passed to Orchard")
		}
		return nil
	}
	id, err := p.RequestInstance(context.Background(), "runner-a")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = p.RequestInstance(context.Background(), "runner-a"); err != nil {
		t.Fatal(err)
	}
	if fake.creates != 1 {
		t.Fatalf("stable demand requeued the request %d times", fake.creates)
	}
	r, ok := p.ledger.get(id)
	if !ok || r.UID == "" {
		t.Fatal("controller UID was not saved")
	}
	stat, err := os.Stat(p.ledger.path)
	if err != nil {
		t.Fatal(err)
	}
	if stat.Mode().Perm() != 0600 {
		t.Fatalf("ledger mode %o", stat.Mode().Perm())
	}
}

func TestOrchardPendingAdmissionDoesNotRunGuestAndCanCancel(t *testing.T) {
	p, fake := orchardTestProvider(t)
	id, err := p.RequestInstance(context.Background(), "runner-wait")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Millisecond)
	defer cancel()
	if err = p.WaitPrepared(ctx, id); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("waiting result: %v", err)
	}
	if len(fake.requests) != 0 {
		t.Fatal("pending VM received guest commands")
	}
	if err = p.RemoveInstance(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	if len(fake.vms) != 0 {
		t.Fatal("canceled pending request remains")
	}
}

func TestOrchardStartAndRecoveryKeepJITOutOfLedger(t *testing.T) {
	p, fake := orchardTestProvider(t)
	id, err := p.RequestInstance(context.Background(), "runner-launch")
	if err != nil {
		t.Fatal(err)
	}
	vm := fake.vms[id]
	vm.Status = "running"
	fake.vms[id] = vm
	ctx := context.Background()
	if err = p.WaitPrepared(ctx, id); err != nil {
		t.Fatal(err)
	}
	if err = p.RecordJITRequested(ctx, "runner-launch", id); err != nil {
		t.Fatal(err)
	}
	if err = p.RecordJITRunner(ctx, "runner-launch", id, 42); err != nil {
		t.Fatal(err)
	}
	const jit = "secret-jit-material"
	if _, err = p.StartPreparedInstance(ctx, "runner-launch", id, jit); err != nil {
		t.Fatal(err)
	}
	raw, _ := os.ReadFile(p.ledger.path)
	if strings.Contains(string(raw), jit) {
		t.Fatal("JIT was persisted in host ledger")
	}
	ledger, err := openOrchardLedger(p.config.Orchard.StateDir, p.config.RegistrationURL+"\x00"+p.config.ScaleSetName, p.config.Orchard.URL)
	if err != nil {
		t.Fatal(err)
	}
	restarted := &OrchardProvider{config: p.config, api: p.api, ledger: ledger, logger: p.logger}
	t.Cleanup(func() { restarted.Shutdown(context.Background()) })
	recovered, err := restarted.RecoverInstances(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(recovered) != 1 || recovered[0].RunnerID != 42 || !recovered[0].LaunchAttempted || !recovered[0].JITRequested {
		t.Fatalf("lost durable launch intent: %+v", recovered)
	}
	if err = p.RemoveInstance(ctx, id); err != nil {
		t.Fatal(err)
	}
	if _, ok := p.ledger.get(id); !ok {
		t.Fatal("registration tombstone removed before deregistration")
	}
	if err = p.RecordJITRemoved(ctx, "runner-launch", id); err != nil {
		t.Fatal(err)
	}
	if _, ok := p.ledger.get(id); ok {
		t.Fatal("resolved tombstone remains")
	}
	if err = p.RecordJITRemoved(ctx, "runner-launch", id); err != nil {
		t.Fatal("idempotent registration cleanup failed")
	}
}

func TestOrchardRefusesChangedVMIdentity(t *testing.T) {
	p, fake := orchardTestProvider(t)
	id, err := p.RequestInstance(context.Background(), "runner-owned")
	if err != nil {
		t.Fatal(err)
	}
	vm := fake.vms[id]
	vm.UID = "replacement-owned-by-someone-else"
	fake.vms[id] = vm
	if err = p.RemoveInstance(context.Background(), id); err == nil {
		t.Fatal("replaced VM accepted")
	}
	if len(fake.deletes) != 0 {
		t.Fatal("replaced VM was deleted")
	}
	if _, err = p.RecoverInstances(context.Background()); err == nil {
		t.Fatal("recovery accepted mismatched UID")
	}
	if err = p.RemoveInstance(context.Background(), "agent-guest"); err != nil {
		t.Fatal(err)
	}
	if len(fake.deletes) != 0 {
		t.Fatal("unowned guest was deleted")
	}
}

func TestOrchardAmbiguousCreateReturnsOwnedHandle(t *testing.T) {
	p, fake := orchardTestProvider(t)
	fake.createHook = func(orchardVM) error { return errors.New("connection dropped") }
	id, err := p.RequestInstance(context.Background(), "runner-partial")
	if err == nil || id == "" {
		t.Fatalf("ambiguous create lost handle: %q %v", id, err)
	}
	if _, ok := p.ledger.get(id); !ok {
		t.Fatal("ambiguous create lost durable cleanup intent")
	}
}

func TestOrchardLateCreateIsCleanedAfterFirstMissingResponse(t *testing.T) {
	p, fake := orchardTestProvider(t)
	fake.createHook = func(orchardVM) error { return errors.New("create response lost") }
	id, err := p.RequestInstance(context.Background(), "runner-late")
	if err == nil {
		t.Fatal("expected an ambiguous create error")
	}
	if err = p.RemoveInstance(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	record, ok := p.ledger.get(id)
	if !ok || !record.CreateUncertain || !record.InstanceRemoved {
		t.Fatal("first missing read discarded uncertain create intent")
	}
	fake.mu.Lock()
	late := record.VM
	late.UID = "late-controller-uid"
	late.Status = "running"
	fake.vms[id] = late
	fake.vms["agent-owned"] = orchardVM{Name: "agent-owned", UID: "agent-uid", Status: "running"}
	fake.mu.Unlock()
	deadline := time.After(time.Second)
	for {
		if _, exists := p.ledger.get(id); !exists {
			break
		}
		select {
		case <-deadline:
			t.Fatal("late VM was not reconciled")
		case <-time.After(time.Millisecond):
		}
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if _, exists := fake.vms[id]; exists {
		t.Fatal("late VM remains after its tombstone was cleared")
	}
	if _, exists := fake.vms["agent-owned"]; !exists {
		t.Fatal("late-create cleanup deleted another client's VM")
	}
}

func TestOrchardFullRunnerIdentityFitsResourceNameLimit(t *testing.T) {
	p, _ := orchardTestProvider(t)
	id, err := p.RequestInstance(context.Background(), "runner-11111111-2222-4333-8444-555555555555")
	if err != nil {
		t.Fatal(err)
	}
	if len(id) > 63 {
		t.Fatalf("VM name exceeds Orchard's resource limit: %d", len(id))
	}
}

func TestOrchardLedgerCorruptionStopsRecovery(t *testing.T) {
	p, _ := orchardTestProvider(t)
	if err := os.WriteFile(p.ledger.path, []byte("broken"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := openOrchardLedger(p.config.Orchard.StateDir, p.config.RegistrationURL+"\x00"+p.config.ScaleSetName, p.config.Orchard.URL); err == nil {
		t.Fatal("corrupt ledger accepted")
	}
}

func TestOrchardActiveLedgerCannotMoveToAnotherController(t *testing.T) {
	p, _ := orchardTestProvider(t)
	id, err := p.RequestInstance(context.Background(), "runner-controller-binding")
	if err != nil {
		t.Fatal(err)
	}
	identity := p.config.RegistrationURL + "\x00" + p.config.ScaleSetName
	if _, err = openOrchardLedger(p.config.Orchard.StateDir, identity, "https://another-controller.example"); err == nil {
		t.Fatal("active ledger silently moved away from the controller owning its VM")
	}
	if _, err = openOrchardLedger(p.config.Orchard.StateDir, identity, p.config.Orchard.URL+"/"); err != nil {
		t.Fatal("equivalent controller URL rejected", err)
	}
	if err = p.RemoveInstance(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	if _, err = openOrchardLedger(p.config.Orchard.StateDir, identity, "https://another-controller.example"); err != nil {
		t.Fatal("empty ledger could not change controllers", err)
	}
}

func TestOrchardParallelRequestsAndLedgerTransactions(t *testing.T) {
	p, _ := orchardTestProvider(t)
	var workers sync.WaitGroup
	for index := range 12 {
		workers.Add(1)
		go func(index int) {
			defer workers.Done()
			name := "runner-parallel-" + strconv.Itoa(index)
			id, err := p.RequestInstance(context.Background(), name)
			if err != nil {
				t.Error(err)
				return
			}
			if err = p.RecordJITRequested(context.Background(), name, id); err != nil {
				t.Error(err)
				return
			}
			if err = p.RecordJITRunner(context.Background(), name, id, index+1); err != nil {
				t.Error(err)
			}
		}(index)
	}
	workers.Wait()
	ledger, err := openOrchardLedger(p.config.Orchard.StateDir, p.config.RegistrationURL+"\x00"+p.config.ScaleSetName, p.config.Orchard.URL)
	if err != nil {
		t.Fatal(err)
	}
	if len(ledger.records()) != 12 {
		t.Fatal("parallel writes lost durable ownership records")
	}
}

func TestOrchardReadOnlyPreflightChecksAuthenticationWithoutStateOrVMCreation(t *testing.T) {
	for _, code := range []int{http.StatusOK, http.StatusUnauthorized, http.StatusNotFound} {
		t.Run(strconv.Itoa(code), func(t *testing.T) {
			requests := make(chan string, 2)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests <- r.Method
				if strings.HasSuffix(r.URL.Path, "/controller/info") {
					w.WriteHeader(code)
					if code == http.StatusOK {
						_, _ = io.WriteString(w, `{"version":"0.58.0"}`)
					}
				} else {
					w.WriteHeader(http.StatusNotFound)
				}
			}))
			defer server.Close()
			t.Setenv("ORCHARD_PREFLIGHT_TOKEN", "preflight-secret")
			ss := config.ScaleSetConfig{Orchard: config.OrchardConfig{URL: server.URL, ServiceAccount: "runner", TokenEnv: "ORCHARD_PREFLIGHT_TOKEN", StateDir: filepath.Join(t.TempDir(), "not-created")}}
			err := CheckOrchardConnection(context.Background(), ss)
			if (err == nil) != (code == http.StatusOK) {
				t.Fatalf("preflight status %d: %v", code, err)
			}
			if <-requests != http.MethodGet {
				t.Fatal("preflight mutated Orchard")
			}
			if _, err := os.Stat(ss.Orchard.StateDir); !os.IsNotExist(err) {
				t.Fatal("preflight created local persistent state")
			}
			if err != nil && strings.Contains(err.Error(), "preflight-secret") {
				t.Fatal("preflight exposed a credential")
			}
		})
	}
}

func TestOrchardDiagnosticsAreBoundedAndCredentialsRedacted(t *testing.T) {
	p, fake := orchardTestProvider(t)
	id, err := p.RequestInstance(context.Background(), "runner-diagnostics")
	if err != nil {
		t.Fatal(err)
	}
	const secret = "orchard-credential-secret"
	p.secrets = []string{secret}
	fake.log = strings.Repeat("x", 64<<10) + secret + " tail"
	diagnostics, err := p.CollectDiagnostics(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if len(diagnostics.RawLog) > 64<<10 || strings.Contains(diagnostics.RawLog, secret) || !strings.Contains(diagnostics.RawLog, "[REDACTED]") {
		t.Fatal("diagnostics exceeded its bound or exposed a configured credential")
	}
}

func TestOrchardExecUsesFixedCommandAndSecretStdin(t *testing.T) {
	const jit = "secret-jit-payload"
	const token = "secret-api-token"
	var input orchardStarterRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.RequestURI, jit) || strings.Contains(r.RequestURI, token) {
			t.Error("secret appeared in URL")
		}
		if r.URL.Query().Get("command") != "/usr/local/bin/test-starter" || r.URL.Query().Get("interactive") != "true" {
			t.Error("exec did not use fixed interactive command")
		}
		user, password, ok := r.BasicAuth()
		if !ok || user != "controller" || password != token {
			t.Error("wrong Orchard authentication")
		}
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			t.Error(err)
			return
		}
		defer func() { _ = conn.CloseNow() }()
		_, raw, err := conn.Read(r.Context())
		if err != nil {
			t.Error(err)
			return
		}
		var frame orchardExecFrame
		if json.Unmarshal(raw, &frame) != nil || frame.Type != "stdin" || json.Unmarshal(frame.Data, &input) != nil {
			t.Error("invalid stdin payload")
			return
		}
		_, raw, err = conn.Read(r.Context())
		if err != nil {
			t.Error(err)
			return
		}
		frame = orchardExecFrame{}
		if json.Unmarshal(raw, &frame) != nil || frame.Type != "stdin" || len(frame.Data) != 0 {
			t.Error("stdin was not closed")
		}
		stdout, _ := json.Marshal(orchardExecFrame{Type: "stdout", Data: []byte(`{"state":"starting"}`)})
		if err = conn.Write(r.Context(), websocket.MessageText, stdout); err != nil {
			return
		}
		exit := []byte(`{"type":"exit","exit":{"code":0}}`)
		_ = conn.Write(r.Context(), websocket.MessageText, exit)
	}))
	defer server.Close()
	client, err := newOrchardHTTPClient(server.URL, "controller", token)
	if err != nil {
		t.Fatal(err)
	}
	status, err := client.Exec(context.Background(), "owned-vm", "/usr/local/bin/test-starter", orchardStarterRequest{Action: "start", Name: "runner-a", JITConfig: jit})
	if err != nil {
		t.Fatal(err)
	}
	if status.State != "starting" || input.JITConfig != jit {
		t.Fatalf("wrong exec exchange: %+v %+v", status, input)
	}
}
