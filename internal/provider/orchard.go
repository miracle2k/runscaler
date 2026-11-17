package provider

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/ysya/runscaler/internal/config"
)

// OrchardProvider submits fresh guests to Orchard's common queue. There is
// no local Tart coordinator, warm pool, image prune, or agent-VM sweep here.
type OrchardProvider struct {
	config        config.ScaleSetConfig
	api           orchardAPI
	ledger        *orchardLedger
	initErr       error
	logger        *slog.Logger
	secrets       []string
	cleanupMu     sync.Mutex
	cleanupCancel context.CancelFunc
	cleanupWake   chan struct{}
	cleanupWG     sync.WaitGroup
	closed        bool
}

func NewOrchardProvider(ss config.ScaleSetConfig, logger *slog.Logger) *OrchardProvider {
	p := &OrchardProvider{config: ss, logger: logger}
	token := os.Getenv(ss.Orchard.TokenEnv)
	password := os.Getenv(ss.Orchard.PasswordEnv)
	p.secrets = []string{token, password}
	if token == "" {
		p.initErr = errors.New("orchard token environment variable is empty")
		return p
	}
	if ss.Orchard.PasswordEnv != "" && password == "" {
		p.initErr = errors.New("orchard guest password environment variable is empty")
		return p
	}
	p.api, p.initErr = newOrchardHTTPClient(ss.Orchard.URL, ss.Orchard.ServiceAccount, token)
	if p.initErr == nil {
		p.ledger, p.initErr = openOrchardLedger(ss.Orchard.StateDir, ss.RegistrationURL+"\x00"+ss.ScaleSetName, ss.Orchard.URL)
	}
	return p
}

func (p *OrchardProvider) CheckStart(ctx context.Context) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return p.initErr
}

func (p *OrchardProvider) RequestInstance(ctx context.Context, name string) (string, error) {
	if err := p.CheckStart(ctx); err != nil {
		return "", err
	}
	if name == "" || strings.ContainsAny(name, "/\\\r\n") {
		return "", errors.New("invalid Orchard runner name")
	}
	id := p.ledger.prefix() + name
	if _, exists := p.ledger.get(id); !exists {
		vm := orchardVM{Name: id, Image: p.config.RunnerImage, CPU: p.config.Orchard.CPU, Memory: p.config.Orchard.Memory, Headless: true, Username: p.config.Orchard.Username, RestartPolicy: "Never", Labels: p.config.Orchard.WorkerLabels, Resources: map[string]uint64{
			"org.cirruslabs.logical-cores": uint64(p.config.Orchard.CPU),
			"org.cirruslabs.memory-mib":    uint64(p.config.Orchard.Memory),
			"org.cirruslabs.tart-vms":      1,
		}}
		if err := p.ledger.change(id, func(r *orchardRecord) error {
			*r = orchardRecord{Name: name, VMName: id, VM: vm, CreatedAt: time.Now().UTC()}
			return nil
		}); err != nil {
			return "", err
		}
	}
	r, _ := p.ledger.get(id)
	if r.Name != name || r.InstanceRemoved {
		return id, errors.New("orchard request identity was already retired")
	}
	vm, err := p.api.Get(ctx, id)
	if orchardNotFound(err) && r.UID == "" {
		if err = p.ledger.change(id, func(r *orchardRecord) error { r.CreateUncertain = true; return nil }); err != nil {
			return id, err
		}
		request := r.VM
		request.Password = os.Getenv(p.config.Orchard.PasswordEnv)
		if err = p.api.Create(ctx, request); err != nil && !orchardConflict(err) {
			var status *orchardHTTPError
			if errors.As(err, &status) && status.Status >= 400 && status.Status < 500 && status.Status != 408 && status.Status != 429 {
				// A completed rejection cannot create a late VM. Transport,
				// timeout and server failures retain the durable uncertainty.
				_ = p.ledger.change(id, func(r *orchardRecord) error { r.CreateUncertain = false; return nil })
			}
			return id, err
		}
		vm, err = p.api.Get(ctx, id)
	}
	if err != nil {
		return id, err
	}
	if err = p.acceptVM(id, vm); err != nil {
		return id, err
	}
	return id, nil
}

func (p *OrchardProvider) acceptVM(id string, vm orchardVM) error {
	r, ok := p.ledger.get(id)
	if !ok || vm.Name != id || vm.UID == "" || r.UID != "" && r.UID != vm.UID {
		return errors.New("orchard VM ownership identity mismatch; no action taken")
	}
	if r.UID == "" && (vm.Image != r.VM.Image || vm.CPU != r.VM.CPU || vm.Memory != r.VM.Memory || !reflect.DeepEqual(vm.Resources, r.VM.Resources) || !sameOrchardLabels(vm.Labels, r.VM.Labels)) {
		return errors.New("orchard VM specification differs from saved request; no action taken")
	}
	if r.UID == "" || r.CreateUncertain && !r.InstanceRemoved {
		return p.ledger.change(id, func(r *orchardRecord) error {
			r.UID = vm.UID
			if !r.InstanceRemoved {
				r.CreateUncertain = false
			}
			return nil
		})
	}
	return nil
}

func sameOrchardLabels(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

func (p *OrchardProvider) ownedVM(ctx context.Context, id string) (orchardVM, error) {
	if err := p.CheckStart(ctx); err != nil {
		return orchardVM{}, err
	}
	if _, ok := p.ledger.get(id); !ok {
		return orchardVM{}, errors.New("orchard instance is not in this manager's ledger")
	}
	vm, err := p.api.Get(ctx, id)
	if err == nil {
		err = p.acceptVM(id, vm)
	}
	return vm, err
}

func (p *OrchardProvider) WaitPrepared(ctx context.Context, id string) error {
	var bootDeadline time.Time
	for {
		vm, err := p.ownedVM(ctx, id)
		if err != nil {
			return err
		}
		switch vm.Status {
		case "failed":
			return errors.New("orchard could not prepare the VM")
		case "running":
			if bootDeadline.IsZero() {
				bootDeadline = time.Now().Add(p.config.Orchard.EffectiveReadyTimeout())
			}
			probeCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
			status, probeErr := p.api.Exec(probeCtx, id, p.config.Orchard.EffectiveStarterCommand(), orchardStarterRequest{Action: "probe"})
			cancel()
			if probeErr == nil && status.State == "ready" {
				return nil
			}
			if time.Now().After(bootDeadline) {
				return fmt.Errorf("guest starter is unavailable: %w", ErrReadinessTimeout)
			}
		}
		if err := p.pause(ctx); err != nil {
			return err
		}
	}
}

func (p *OrchardProvider) StartPreparedInstance(ctx context.Context, name, id, jit string) (string, error) {
	if _, err := p.ownedVM(ctx, id); err != nil {
		return id, err
	}
	if err := p.ledger.change(id, func(r *orchardRecord) error {
		if r.Name != name || !r.JITRequested || r.JITRemoved || jit == "" {
			return errors.New("orchard runner has no matching JIT intent")
		}
		r.LaunchAttempted = true
		return nil
	}); err != nil {
		return id, err
	}
	status, err := p.api.Exec(ctx, id, p.config.Orchard.EffectiveStarterCommand(), orchardStarterRequest{Action: "start", Name: name, RunnerDir: p.config.Orchard.RunnerDir, JITConfig: jit})
	if err != nil {
		return id, err
	}
	if status.State != "starting" && status.State != "ready" && status.State != "busy" {
		return id, errors.New("guest runner did not start")
	}
	return id, nil
}

// StartInstance is a compatible fallback. The controller uses the prepared
// interface so JIT is never generated before Orchard admission.
func (p *OrchardProvider) StartInstance(ctx context.Context, name, jit string) (string, error) {
	id, err := p.RequestInstance(ctx, name)
	if err != nil {
		return id, err
	}
	if err = p.WaitPrepared(ctx, id); err != nil {
		return id, err
	}
	if err = p.RecordJITRequested(ctx, name, id); err != nil {
		return id, err
	}
	return p.StartPreparedInstance(ctx, name, id, jit)
}

func (p *OrchardProvider) RecordJITRequested(ctx context.Context, name, id string) error {
	if err := p.CheckStart(ctx); err != nil {
		return err
	}
	return p.ledger.change(id, func(r *orchardRecord) error {
		if r.Name != name || r.VMName != id {
			return errors.New("orchard JIT intent has no owned request")
		}
		r.JITRequested = true
		r.JITRemoved = false
		return nil
	})
}

func (p *OrchardProvider) RecordJITRunner(ctx context.Context, name, id string, runnerID int) error {
	if err := p.CheckStart(ctx); err != nil {
		return err
	}
	return p.ledger.change(id, func(r *orchardRecord) error {
		if r.Name != name || !r.JITRequested || runnerID <= 0 {
			return errors.New("orchard JIT registration identity mismatch")
		}
		r.RunnerID = runnerID
		return nil
	})
}

func (p *OrchardProvider) RecordJITRemoved(ctx context.Context, name, id string) error {
	if err := p.CheckStart(ctx); err != nil {
		return err
	}
	if _, ok := p.ledger.get(id); !ok {
		return nil
	}
	return p.ledger.change(id, func(r *orchardRecord) error {
		if r.Name != name {
			return errors.New("orchard removed registration identity mismatch")
		}
		r.JITRemoved = true
		return nil
	})
}

func (p *OrchardProvider) RemoveInstance(ctx context.Context, id string) error {
	if err := p.CheckStart(ctx); err != nil {
		return err
	}
	if _, ok := p.ledger.get(id); !ok {
		return nil
	}
	_, err := p.ownedVM(ctx, id)
	if err != nil && !orchardNotFound(err) {
		return err
	}
	if err == nil {
		err = p.api.Delete(ctx, id)
	}
	if err != nil && !orchardNotFound(err) {
		return err
	}
	err = p.ledger.change(id, func(r *orchardRecord) error {
		r.InstanceRemoved = true
		// Observing the VM before a successful delete resolves a late
		// create. Missing after an ambiguous POST is not sufficient.
		if r.UID != "" {
			r.CreateUncertain = false
		}
		return nil
	})
	if err == nil {
		p.startUncertainCleanup()
	}
	return err
}

func (p *OrchardProvider) RecoverInstances(ctx context.Context) ([]RecoveredInstance, error) {
	if err := p.CheckStart(ctx); err != nil {
		return nil, err
	}
	if err := checkOrchardAPI(ctx, p.api); err != nil {
		return nil, err
	}
	var result []RecoveredInstance
	for _, r := range p.ledger.records() {
		_, err := p.ownedVM(ctx, r.VMName)
		if err != nil && !orchardNotFound(err) {
			return nil, err
		}
		removed := orchardNotFound(err)
		if removed != r.InstanceRemoved {
			if err := p.ledger.change(r.VMName, func(record *orchardRecord) error { record.InstanceRemoved = removed; return nil }); err != nil {
				return nil, err
			}
		}
		result = append(result, RecoveredInstance{Name: r.Name, InstanceID: r.VMName, RunnerID: r.RunnerID, JITRequested: r.JITRequested && !r.JITRemoved, LaunchAttempted: r.LaunchAttempted, InstanceRemoved: removed})
	}
	p.startUncertainCleanup()
	return result, nil
}

// CheckOrchardConnection is an authenticated read-only probe. It creates no
// ledger, VM request, or guest. Orchard authorizes VM reads before looking up
// the exact name, so a missing result confirms usable read access.
func CheckOrchardConnection(ctx context.Context, ss config.ScaleSetConfig) error {
	token := os.Getenv(ss.Orchard.TokenEnv)
	if token == "" {
		return errors.New("orchard token environment variable is empty")
	}
	if ss.Orchard.PasswordEnv != "" && os.Getenv(ss.Orchard.PasswordEnv) == "" {
		return errors.New("orchard guest password environment variable is empty")
	}
	api, err := newOrchardHTTPClient(ss.Orchard.URL, ss.Orchard.ServiceAccount, token)
	if err != nil {
		return err
	}
	return checkOrchardAPI(ctx, api)
}

func checkOrchardAPI(ctx context.Context, api orchardAPI) error {
	probeCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := api.Check(probeCtx); err != nil {
		return err
	}
	_, err := api.Get(probeCtx, "runscaler-readonly-connectivity-probe")
	if orchardNotFound(err) {
		return nil
	}
	return err
}

func (p *OrchardProvider) starterStatus(ctx context.Context, id string) (orchardStarterStatus, error) {
	_, err := p.ownedVM(ctx, id)
	if err != nil {
		return orchardStarterStatus{}, err
	}
	r, _ := p.ledger.get(id)
	return p.api.Exec(ctx, id, p.config.Orchard.EffectiveStarterCommand(), orchardStarterRequest{Action: "status", Name: r.Name})
}

func (p *OrchardProvider) WaitReady(ctx context.Context, id string) error {
	readyCtx, cancel := context.WithTimeout(ctx, p.config.Orchard.EffectiveReadyTimeout())
	defer cancel()
	for {
		status, err := p.starterStatus(readyCtx, id)
		if err == nil {
			switch status.State {
			case "ready", "busy":
				return nil
			case "exited", "missing":
				return ErrExitedBeforeReady
			}
		} else if orchardNotFound(err) {
			return ErrExitedBeforeReady
		}
		if err := p.pause(readyCtx); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return ErrReadinessTimeout
		}
	}
}

func (p *OrchardProvider) WaitInstance(ctx context.Context, id string) error {
	for {
		if err := p.CheckStart(ctx); err != nil {
			return err
		}
		if record, exists := p.ledger.get(id); !exists || record.InstanceRemoved {
			return nil
		}
		status, err := p.starterStatus(ctx, id)
		if orchardNotFound(err) || err == nil && (status.State == "exited" || status.State == "missing") {
			return nil
		}
		if err != nil {
			return err
		}
		if err := p.pause(ctx); err != nil {
			return err
		}
	}
}

func (p *OrchardProvider) CollectDiagnostics(ctx context.Context, id string) (ExitDiagnostics, error) {
	if _, err := p.ownedVM(ctx, id); err != nil {
		return ExitDiagnostics{}, err
	}
	r, _ := p.ledger.get(id)
	status, err := p.api.Exec(ctx, id, p.config.Orchard.EffectiveStarterCommand(), orchardStarterRequest{Action: "diagnostics", Name: r.Name, MaxBytes: 64 << 10})
	if err != nil {
		return ExitDiagnostics{}, err
	}
	log := status.Log
	for _, secret := range p.secrets {
		if secret != "" {
			log = strings.ReplaceAll(log, secret, "[REDACTED]")
		}
	}
	if len(log) > 64<<10 {
		log = log[len(log)-(64<<10):]
	}
	return ExitDiagnostics{ExitCode: status.ExitCode, RawLog: log}, nil
}

func (p *OrchardProvider) pause(ctx context.Context) error {
	timer := time.NewTimer(p.config.Orchard.EffectivePollInterval())
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// Shutdown stops background cleanup. Resource cleanup stays explicit and
// ledger-backed; stopping the provider never sweeps another client's VM or
// terminates a detached busy guest.
func (p *OrchardProvider) Shutdown(ctx context.Context) {
	p.cleanupMu.Lock()
	p.closed = true
	if p.cleanupCancel != nil {
		p.cleanupCancel()
	}
	p.cleanupMu.Unlock()
	done := make(chan struct{})
	go func() { p.cleanupWG.Wait(); close(done) }()
	select {
	case <-done:
	case <-ctx.Done():
	}
}

// A timed-out create can commit after cleanup first sees 404. Release local
// controller accounting, but retain that exact name durably and keep looking
// for a late VM until it can be positively identified and deleted. There is
// no age-based assumption that absence proves the POST never committed.
func (p *OrchardProvider) startUncertainCleanup() {
	if p.ledger == nil {
		return
	}
	hasWork := false
	for _, r := range p.ledger.records() {
		if r.CreateUncertain && r.InstanceRemoved {
			hasWork = true
			break
		}
	}
	if !hasWork {
		return
	}
	p.cleanupMu.Lock()
	if p.closed {
		p.cleanupMu.Unlock()
		return
	}
	if p.cleanupCancel == nil {
		ctx, cancel := context.WithCancel(context.Background())
		p.cleanupCancel = cancel
		p.cleanupWake = make(chan struct{}, 1)
		p.cleanupWG.Add(1)
		go p.uncertainCleanupLoop(ctx, p.cleanupWake)
	}
	wake := p.cleanupWake
	p.cleanupMu.Unlock()
	select {
	case wake <- struct{}{}:
	default:
	}
}

func (p *OrchardProvider) uncertainCleanupLoop(ctx context.Context, wake <-chan struct{}) {
	defer p.cleanupWG.Done()
	ticker := time.NewTicker(p.config.Orchard.EffectivePollInterval())
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-wake:
		case <-ticker.C:
		}
		for _, r := range p.ledger.records() {
			if !r.CreateUncertain || !r.InstanceRemoved {
				continue
			}
			attemptCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
			vm, err := p.api.Get(attemptCtx, r.VMName)
			if err == nil {
				err = p.acceptVM(r.VMName, vm)
			}
			if err == nil {
				err = p.api.Delete(attemptCtx, r.VMName)
				if orchardNotFound(err) {
					err = nil
				}
			}
			cancel()
			if err == nil {
				_ = p.ledger.change(r.VMName, func(record *orchardRecord) error {
					record.CreateUncertain = false
					record.InstanceRemoved = true
					return nil
				})
			}
		}
	}
}

var _ InstanceProvider = (*OrchardProvider)(nil)
var _ PreparedInstanceProvider = (*OrchardProvider)(nil)
var _ InstanceRegistrationRecorder = (*OrchardProvider)(nil)
var _ InstanceRecovery = (*OrchardProvider)(nil)
