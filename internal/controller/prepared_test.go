package controller

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/actions/scaleset"

	"github.com/ysya/runscaler/internal/capacity"
	"github.com/ysya/runscaler/internal/provider"
)

type preparedRequest struct{ name, id string }

type preparedTestProvider struct {
	mu          sync.Mutex
	requests    []preparedRequest
	waits       map[string]chan error
	started     []string
	removed     []string
	events      []string
	cleared     []string
	startErr    error
	intentErr   error
	recovered   []provider.RecoveredInstance
	removeError error
	requestErr  error
	noRequestID bool
	recoveryErr error
}

func (p *preparedTestProvider) StartInstance(context.Context, string, string) (string, error) {
	panic("prepared providers must not use legacy startup")
}

func (p *preparedTestProvider) RequestInstance(_ context.Context, name string) (string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.waits == nil {
		p.waits = make(map[string]chan error)
	}
	id := "request-" + name
	p.waits[id] = make(chan error, 1)
	p.requests = append(p.requests, preparedRequest{name, id})
	p.events = append(p.events, "request")
	if p.noRequestID {
		return "", p.requestErr
	}
	return id, p.requestErr
}

func (p *preparedTestProvider) WaitPrepared(ctx context.Context, id string) error {
	p.mu.Lock()
	wait := p.waits[id]
	p.mu.Unlock()
	select {
	case err := <-wait:
		p.mu.Lock()
		p.events = append(p.events, "prepared")
		p.mu.Unlock()
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *preparedTestProvider) StartPreparedInstance(_ context.Context, name, id, _ string) (string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.started = append(p.started, name)
	p.events = append(p.events, "start")
	return id, p.startErr
}

func (p *preparedTestProvider) RemoveInstance(_ context.Context, id string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.removeError != nil {
		return p.removeError
	}
	p.removed = append(p.removed, id)
	return nil
}

func (p *preparedTestProvider) Shutdown(context.Context) {}

func (p *preparedTestProvider) RecordJITRequested(context.Context, string, string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.events = append(p.events, "jit-requested")
	return p.intentErr
}

func (p *preparedTestProvider) RecordJITRunner(context.Context, string, string, int) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.events = append(p.events, "jit-recorded")
	return nil
}

func (p *preparedTestProvider) RecordJITRemoved(_ context.Context, name, _ string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.cleared = append(p.cleared, name)
	return nil
}

func (p *preparedTestProvider) RecoverInstances(context.Context) ([]provider.RecoveredInstance, error) {
	return slices.Clone(p.recovered), p.recoveryErr
}

type watchedPreparedTestProvider struct {
	*preparedTestProvider
	exited chan struct{}
}

func (p *watchedPreparedTestProvider) WaitInstance(ctx context.Context, _ string) error {
	select {
	case <-p.exited:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *preparedTestProvider) snapshot() (requests []preparedRequest, started, removed, events, cleared []string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return slices.Clone(p.requests), slices.Clone(p.started), slices.Clone(p.removed), slices.Clone(p.events), slices.Clone(p.cleared)
}

func (p *preparedTestProvider) ready(id string, err error) {
	p.mu.Lock()
	wait := p.waits[id]
	p.mu.Unlock()
	wait <- err
}

type preparedTestClient struct {
	mockScaleset
	jitErr      error
	jitWait     chan string
	lookup      map[string]*scaleset.RunnerReference
	lookupCalls int
}

func (c *preparedTestClient) GenerateJitRunnerConfig(ctx context.Context, setting *scaleset.RunnerScaleSetJitRunnerSetting, id int) (*scaleset.RunnerScaleSetJitRunnerConfig, error) {
	if c.jitWait != nil {
		c.jitWait <- setting.Name
		<-ctx.Done()
		return nil, ctx.Err()
	}
	if c.jitErr != nil {
		return nil, c.jitErr
	}
	return c.mockScaleset.GenerateJitRunnerConfig(ctx, setting, id)
}

func (c *preparedTestClient) GetRunnerByName(_ context.Context, name string) (*scaleset.RunnerReference, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lookupCalls++
	return c.lookup[name], nil
}

func newPreparedController(t *testing.T, max int, p provider.InstanceProvider, c *preparedTestClient) *ScaleSetController {
	t.Helper()
	s := NewScaleSetController(1, 0, max, p, c, slog.New(slog.DiscardHandler))
	t.Cleanup(func() { s.Shutdown(context.Background()) })
	return s
}

func requested(t *testing.T, p *preparedTestProvider, count int) []preparedRequest {
	t.Helper()
	waitFor(t, func() bool { requests, _, _, _, _ := p.snapshot(); return len(requests) == count }, "request count did not converge")
	requests, _, _, _, _ := p.snapshot()
	return requests
}

func generated(c *preparedTestClient) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.generated
}

func TestPreparedJITWaitsForAdmissionAndPreservesQueueAge(t *testing.T) {
	p, c := &preparedTestProvider{}, &preparedTestClient{}
	s := newPreparedController(t, 1, p, c)
	if _, err := s.HandleDesiredRunnerCount(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	request := requested(t, p, 1)[0]
	for range 5 {
		if _, err := s.HandleDesiredRunnerCount(context.Background(), 1); err != nil {
			t.Fatal(err)
		}
	}
	if generated(c) != 0 {
		t.Fatal("JIT was generated before shared capacity became available")
	}
	if got := requested(t, p, 1)[0]; got != request {
		t.Fatal("stable demand replaced the queued request")
	}
	p.ready(request.id, nil)
	waitFor(t, func() bool { _, started, _, _, _ := p.snapshot(); return len(started) == 1 }, "admitted runner did not start")
	_, _, _, events, _ := p.snapshot()
	if !slices.Equal(events, []string{"request", "prepared", "jit-requested", "jit-recorded", "start"}) {
		t.Fatalf("registration order = %v", events)
	}
	if generated(c) != 1 {
		t.Fatal("admitted runner did not receive one fresh JIT configuration")
	}
}

func TestPreparedCancellationCleansQueuedRequestWithoutJIT(t *testing.T) {
	p, c := &preparedTestProvider{}, &preparedTestClient{}
	s := newPreparedController(t, 1, p, c)
	if _, err := s.HandleDesiredRunnerCount(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	request := requested(t, p, 1)[0]
	if _, err := s.HandleDesiredRunnerCount(context.Background(), 0); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { _, _, removed, _, _ := p.snapshot(); return slices.Contains(removed, request.id) }, "canceled request was not deleted")
	if generated(c) != 0 || s.instances.count() != 0 || s.RuntimeStatus().StartFailures != 0 {
		t.Fatal("normal cancellation registered a runner, retained capacity or counted as failure")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	lease, err := s.capacity.Acquire(ctx)
	if err != nil {
		t.Fatal("canceled preparation retained its count lease")
	}
	lease.Release()
}

func TestPreparedCleanupContinuesWhileAnotherRequestWaits(t *testing.T) {
	p, c := &preparedTestProvider{}, &preparedTestClient{}
	s := newPreparedController(t, 2, p, c)
	if _, err := s.HandleDesiredRunnerCount(context.Background(), 2); err != nil {
		t.Fatal(err)
	}
	requests := requested(t, p, 2)
	p.ready(requests[0].id, nil)
	waitFor(t, func() bool { _, started, _, _, _ := p.snapshot(); return len(started) == 1 }, "first runner did not start")
	if err := s.HandleJobStarted(context.Background(), &scaleset.JobStarted{RunnerName: requests[0].name}); err != nil {
		t.Fatal(err)
	}
	if err := s.HandleJobCompleted(context.Background(), &scaleset.JobCompleted{RunnerName: requests[0].name}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.HandleDesiredRunnerCount(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { _, _, removed, _, _ := p.snapshot(); return slices.Contains(removed, requests[0].id) }, "active-job cleanup was blocked by queued preparation")
	if generated(c) != 1 || !s.RuntimeStatus().WaitingForCapacity {
		t.Fatal("waiting request was started or lost its capacity-wait status")
	}
}

func TestPreparedDemandDecreaseCancelsNewestRequests(t *testing.T) {
	p, c := &preparedTestProvider{}, &preparedTestClient{}
	s := newPreparedController(t, 3, p, c)
	if _, err := s.HandleDesiredRunnerCount(context.Background(), 3); err != nil {
		t.Fatal(err)
	}
	requests := requested(t, p, 3)
	slices.SortFunc(requests, func(a, b preparedRequest) int {
		if s.instances.attempt(a.name).id < s.instances.attempt(b.name).id {
			return -1
		}
		return 1
	})
	if _, err := s.HandleDesiredRunnerCount(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { _, _, removed, _, _ := p.snapshot(); return len(removed) == 2 }, "excess queued requests were not canceled")
	_, _, removed, _, _ := p.snapshot()
	if slices.Contains(removed, requests[0].id) || generated(c) != 0 {
		t.Fatal("demand decrease lost the oldest request or generated JIT while queued")
	}
	p.ready(requests[0].id, nil)
	waitFor(t, func() bool {
		_, started, _, _, _ := p.snapshot()
		return slices.Equal(started, []string{requests[0].name})
	}, "oldest queued request did not start")
}

func TestPreparedAmbiguousStartRetainsGitHubAssignedGuest(t *testing.T) {
	p := &preparedTestProvider{startErr: errors.New("start response lost")}
	c := &preparedTestClient{mockScaleset: mockScaleset{removeErr: map[int]error{1: scaleset.JobStillRunningError}}}
	s := newPreparedController(t, 1, p, c)
	if _, err := s.HandleDesiredRunnerCount(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	request := requested(t, p, 1)[0]
	p.ready(request.id, nil)
	waitFor(t, func() bool { phase, _ := s.instances.phase(request.name); return phase == instanceBusy }, "assigned runner was not retained after lost start response")
	_, _, removed, _, _ := p.snapshot()
	if len(removed) != 0 {
		t.Fatal("a possibly assigned guest was deleted")
	}
	if err := s.HandleJobCompleted(context.Background(), &scaleset.JobCompleted{RunnerName: request.name}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.HandleDesiredRunnerCount(context.Background(), 0); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { _, _, removed, _, _ := p.snapshot(); return len(removed) == 1 }, "completed ambiguous start was not cleaned")
}

func TestPreparedLostJITResponseRetainsLateRegistrationTombstone(t *testing.T) {
	p := &preparedTestProvider{}
	c := &preparedTestClient{jitErr: errors.New("JIT response lost")}
	s := newPreparedController(t, 1, p, c)
	if _, err := s.HandleDesiredRunnerCount(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	request := requested(t, p, 1)[0]
	p.ready(request.id, nil)
	waitFor(t, func() bool { _, _, removed, _, _ := p.snapshot(); return len(removed) == 1 }, "never-launched guest was not cleaned")
	if _, err := s.HandleDesiredRunnerCount(context.Background(), 0); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		c.mu.Lock()
		lookedUp := c.lookupCalls > 0
		c.mu.Unlock()
		s.registrationMu.Lock()
		defer s.registrationMu.Unlock()
		r := s.registrationLookups[request.name]
		return lookedUp && !s.registrationInFlight && r != nil && !r.retryAt.IsZero()
	}, "lost JIT response was not looked up")
	if !s.HasRunner(request.name) || s.registrationCount() != 1 {
		t.Fatal("a first not-found lookup discarded the ambiguous registration")
	}
	c.mu.Lock()
	c.lookup = map[string]*scaleset.RunnerReference{request.name: {ID: 99, Name: request.name, RunnerScaleSetID: 1}}
	c.mu.Unlock()
	s.registrationMu.Lock()
	s.registrationLookups[request.name].retryAt = time.Time{}
	s.registrationMu.Unlock()
	s.signalReconcile()
	waitFor(t, func() bool { return s.registrationCount() == 0 }, "late registration was not reconciled")
	if !slices.Contains(c.deregisteredIDs(), 99) {
		t.Fatal("late registration was not deregistered")
	}
	_, _, _, _, cleared := p.snapshot()
	if !slices.Contains(cleared, request.name) {
		t.Fatal("durable registration tombstone was not cleared")
	}
}

func TestPreparedIntentFailureNeverRegistersRunner(t *testing.T) {
	p := &preparedTestProvider{intentErr: errors.New("intent was not durably saved")}
	c := &preparedTestClient{}
	s := newPreparedController(t, 1, p, c)
	if _, err := s.HandleDesiredRunnerCount(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	request := requested(t, p, 1)[0]
	p.ready(request.id, nil)
	waitFor(t, func() bool { _, _, removed, _, _ := p.snapshot(); return len(removed) == 1 }, "failed intent did not clean the guest")
	_, _, _, _, cleared := p.snapshot()
	if generated(c) != 0 || !slices.Contains(cleared, request.name) {
		t.Fatal("failed durable intent made a GitHub call or retained a false JIT intent")
	}
}

func TestPreparedCancellationDuringJITNeverLaunchesRunner(t *testing.T) {
	p := &preparedTestProvider{}
	c := &preparedTestClient{jitWait: make(chan string, 1)}
	s := newPreparedController(t, 1, p, c)
	if _, err := s.HandleDesiredRunnerCount(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	request := requested(t, p, 1)[0]
	p.ready(request.id, nil)
	select {
	case <-c.jitWait:
	case <-time.After(time.Second):
		t.Fatal("JIT request did not start")
	}
	if _, err := s.HandleDesiredRunnerCount(context.Background(), 0); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { _, _, removed, _, _ := p.snapshot(); return len(removed) == 1 }, "JIT cancellation did not clean the guest")
	_, started, _, _, _ := p.snapshot()
	if len(started) != 0 || s.RuntimeStatus().StartFailures != 0 {
		t.Fatal("canceled JIT launched a runner or counted as startup failure")
	}
}

func TestPreparedRecoveryKeepsBusyGuestAndCleansUnlaunchedRequest(t *testing.T) {
	p := &preparedTestProvider{recovered: []provider.RecoveredInstance{
		{Name: "runner-active", InstanceID: "active", RunnerID: 7, JITRequested: true, LaunchAttempted: true},
		{Name: "runner-queued", InstanceID: "queued"},
	}}
	c := &preparedTestClient{mockScaleset: mockScaleset{removeErr: map[int]error{7: scaleset.JobStillRunningError}}}
	s := newPreparedController(t, 2, p, c)
	if err := s.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { phase, _ := s.instances.phase("runner-active"); return phase == instanceBusy }, "busy recovered runner was not adopted")
	waitFor(t, func() bool { _, _, removed, _, _ := p.snapshot(); return slices.Contains(removed, "queued") }, "unlaunched recovered request was not cleaned")
	_, _, removed, _, _ := p.snapshot()
	if slices.Contains(removed, "active") {
		t.Fatal("busy recovered runner was removed")
	}
	if _, err := s.HandleDesiredRunnerCount(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	if err := s.HandleJobCompleted(context.Background(), &scaleset.JobCompleted{RunnerName: "runner-active"}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.HandleDesiredRunnerCount(context.Background(), 0); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { _, _, removed, _, _ := p.snapshot(); return slices.Contains(removed, "active") }, "recovered completed job was not cleaned")
}

func TestPreparedRecoveryRejectsForeignRunnerIdentity(t *testing.T) {
	p := &preparedTestProvider{recovered: []provider.RecoveredInstance{{Name: "runner-active", InstanceID: "active", JITRequested: true, LaunchAttempted: true}}}
	c := &preparedTestClient{lookup: map[string]*scaleset.RunnerReference{"runner-active": {ID: 7, Name: "runner-active", RunnerScaleSetID: 2}}}
	s := newPreparedController(t, 1, p, c)
	if err := s.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		s.instances.mu.Lock()
		defer s.instances.mu.Unlock()
		i := s.instances.instances["runner-active"]
		return i != nil && i.cleanupAttempts > 0
	}, "recovered identity was not checked")
	_, _, removed, _, _ := p.snapshot()
	if len(c.deregisteredIDs()) != 0 || len(removed) != 0 {
		t.Fatal("foreign registration or its guest was deleted")
	}
}

func TestPreparedFailedBootCleansRequestWithoutJIT(t *testing.T) {
	p, c := &preparedTestProvider{}, &preparedTestClient{}
	s := newPreparedController(t, 1, p, c)
	if _, err := s.HandleDesiredRunnerCount(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	request := requested(t, p, 1)[0]
	p.ready(request.id, errors.New("guest boot failed"))
	waitFor(t, func() bool { _, _, removed, _, _ := p.snapshot(); return len(removed) == 1 }, "failed boot retained its request")
	if generated(c) != 0 || s.RuntimeStatus().StartFailures != 1 {
		t.Fatal("failed boot registered a runner or lost its failure status")
	}
}

func TestPreparedPartialRequestFailureCleansOwnedHandle(t *testing.T) {
	p := &preparedTestProvider{requestErr: errors.New("create response lost")}
	c := &preparedTestClient{}
	s := newPreparedController(t, 1, p, c)
	if _, err := s.HandleDesiredRunnerCount(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	request := requested(t, p, 1)[0]
	waitFor(t, func() bool { _, _, removed, _, _ := p.snapshot(); return slices.Contains(removed, request.id) }, "partial request error discarded its cleanup handle")
	if generated(c) != 0 {
		t.Fatal("partial resource request registered a runner")
	}
}

func TestPreparedRequestFailureBeforeOwnershipReleasesLease(t *testing.T) {
	p := &preparedTestProvider{requestErr: errors.New("request not sent"), noRequestID: true}
	c := &preparedTestClient{}
	s := newPreparedController(t, 1, p, c)
	if _, err := s.HandleDesiredRunnerCount(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	requested(t, p, 1)
	waitFor(t, func() bool { return s.instances.count() == 0 }, "unowned request failure retained local state")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	lease, err := s.capacity.Acquire(ctx)
	if err != nil {
		t.Fatal("unowned request failure retained its count lease")
	}
	lease.Release()
}

func TestPreparedDrainCancelsQueuedAdmission(t *testing.T) {
	p, c := &preparedTestProvider{}, &preparedTestClient{}
	s := newPreparedController(t, 1, p, c)
	if _, err := s.HandleDesiredRunnerCount(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	request := requested(t, p, 1)[0]
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := s.Drain(ctx); err != nil {
		t.Fatal(err)
	}
	_, _, removed, _, _ := p.snapshot()
	if !slices.Contains(removed, request.id) || generated(c) != 0 || s.RuntimeStatus().StartFailures != 0 {
		t.Fatal("drain failed to clean queued admission without registering or counting a failure")
	}
}

func TestPreparedAdmissionWaitsForFreshDemandAfterCompletion(t *testing.T) {
	p, c := &preparedTestProvider{}, &preparedTestClient{}
	s := newPreparedController(t, 2, p, c)
	if _, err := s.HandleDesiredRunnerCount(context.Background(), 2); err != nil {
		t.Fatal(err)
	}
	requests := requested(t, p, 2)
	p.ready(requests[0].id, nil)
	waitFor(t, func() bool { _, started, _, _, _ := p.snapshot(); return len(started) == 1 }, "first runner did not start")
	if err := s.HandleJobCompleted(context.Background(), &scaleset.JobCompleted{RunnerName: requests[0].name}); err != nil {
		t.Fatal(err)
	}
	p.ready(requests[1].id, nil)
	waitFor(t, func() bool {
		_, _, _, events, _ := p.snapshot()
		prepared := 0
		for _, event := range events {
			if event == "prepared" {
				prepared++
			}
		}
		return prepared == 2
	}, "second admission did not complete")
	// The handler has not delivered the statistics following JobCompleted.
	// Give an incorrectly unguarded worker time to expose its extra JIT call.
	time.Sleep(20 * time.Millisecond)
	if generated(c) != 1 {
		t.Fatal("admission generated JIT from stale pre-completion demand")
	}
	if _, err := s.HandleDesiredRunnerCount(context.Background(), 0); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { _, _, removed, _, _ := p.snapshot(); return len(removed) == 2 }, "fresh lower demand did not clean the admitted request")
}

func TestPreparedRecoveryCapacityFailureDoesNotAdoptPartialJobs(t *testing.T) {
	p := &preparedTestProvider{recovered: []provider.RecoveredInstance{
		{Name: "runner-first", InstanceID: "first", RunnerID: 1, JITRequested: true, LaunchAttempted: true},
		{Name: "runner-second", InstanceID: "second", RunnerID: 2, JITRequested: true, LaunchAttempted: true},
	}}
	c := &preparedTestClient{}
	broker := capacity.NewBroker(1)
	s := NewScaleSetController(1, 0, 2, p, c, slog.New(slog.DiscardHandler), WithCapacityAllocator(broker.Register("test", 0)))
	t.Cleanup(func() { s.Shutdown(context.Background()) })
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := s.Recover(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("reduced-capacity recovery error = %v", err)
	}
	_, _, removed, _, _ := p.snapshot()
	if s.instances.count() != 0 || broker.InUse() != 0 || len(removed) != 0 || len(c.deregisteredIDs()) != 0 {
		t.Fatal("failed recovery partially adopted or removed existing jobs")
	}
}

func TestPreparedRecoveryCleansGuestWhoseRegistrationWasAlreadyRemoved(t *testing.T) {
	p := &preparedTestProvider{recovered: []provider.RecoveredInstance{{Name: "runner-cleared", InstanceID: "cleared", LaunchAttempted: true}}}
	c := &preparedTestClient{}
	s := newPreparedController(t, 1, p, c)
	if err := s.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { _, _, removed, _, _ := p.snapshot(); return slices.Contains(removed, "cleared") }, "guest survived a crash between deregistration and removal")
	c.mu.Lock()
	lookups := c.lookupCalls
	c.mu.Unlock()
	if lookups != 0 || len(c.deregisteredIDs()) != 0 {
		t.Fatal("already-cleared registration was treated as an ambiguous active runner")
	}
}

func TestPreparedAmbiguousExitCannotBypassBusyDeregistration(t *testing.T) {
	p := &preparedTestProvider{startErr: errors.New("launch response lost")}
	watch := &watchedPreparedTestProvider{preparedTestProvider: p, exited: make(chan struct{})}
	c := &preparedTestClient{mockScaleset: mockScaleset{removeErr: map[int]error{1: scaleset.JobStillRunningError}}}
	s := newPreparedController(t, 1, watch, c)
	if _, err := s.HandleDesiredRunnerCount(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	request := requested(t, p, 1)[0]
	p.ready(request.id, nil)
	waitFor(t, func() bool { phase, _ := s.instances.phase(request.name); return phase == instanceBusy }, "ambiguous launch did not retain assigned work")
	// A missing process can precede a delayed launch command committing.
	// It must not bypass the GitHub busy guard for a prepared provider.
	close(watch.exited)
	waitFor(t, func() bool { return len(c.deregisteredIDs()) >= 2 }, "exit report bypassed GitHub deregistration")
	waitFor(t, func() bool { phase, _ := s.instances.phase(request.name); return phase == instanceBusy }, "busy job was not retained after an ambiguous exit report")
	_, _, removed, _, _ := p.snapshot()
	if len(removed) != 0 {
		t.Fatal("ambiguous process-missing evidence deleted a guest that GitHub still holds")
	}
	if err := s.HandleJobCompleted(context.Background(), &scaleset.JobCompleted{RunnerName: request.name}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.HandleDesiredRunnerCount(context.Background(), 0); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { _, _, removed, _, _ := p.snapshot(); return len(removed) == 1 }, "completed retained job was not cleaned")
}

func TestPreparedRecoveredExitCannotBypassBusyDeregistration(t *testing.T) {
	p := &preparedTestProvider{recovered: []provider.RecoveredInstance{{Name: "runner-delayed", InstanceID: "delayed", RunnerID: 7, JITRequested: true, LaunchAttempted: true}}}
	watch := &watchedPreparedTestProvider{preparedTestProvider: p, exited: make(chan struct{})}
	c := &preparedTestClient{mockScaleset: mockScaleset{removeErr: map[int]error{7: scaleset.JobStillRunningError}}}
	s := newPreparedController(t, 1, watch, c)
	if err := s.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { phase, _ := s.instances.phase("runner-delayed"); return phase == instanceBusy }, "recovered job was not adopted")
	close(watch.exited)
	waitFor(t, func() bool { return len(c.deregisteredIDs()) >= 2 }, "recovered exit bypassed GitHub deregistration")
	_, _, removed, _, _ := p.snapshot()
	if len(removed) != 0 {
		t.Fatal("recovery deleted a guest after uncertain process-exit evidence")
	}
}

func TestPreparedAbortAfterAnotherControllerFailsRetainsRecoveredBusyGuest(t *testing.T) {
	p := &preparedTestProvider{recovered: []provider.RecoveredInstance{{Name: "runner-active", InstanceID: "active", RunnerID: 7, JITRequested: true, LaunchAttempted: true}}}
	c := &preparedTestClient{mockScaleset: mockScaleset{removeErr: map[int]error{7: scaleset.JobStillRunningError}}}
	s := newPreparedController(t, 1, p, c)
	if err := s.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { phase, _ := s.instances.phase("runner-active"); return phase == instanceBusy }, "busy recovered guest was not adopted")
	failed := newPreparedController(t, 1, &preparedTestProvider{recoveryErr: errors.New("another ledger is invalid")}, &preparedTestClient{})
	if err := failed.Recover(context.Background()); err == nil {
		t.Fatal("second controller should have failed startup")
	}
	s.Abort(context.Background())
	_, _, removed, _, _ := p.snapshot()
	if len(removed) != 0 || s.instances.count() != 0 {
		t.Fatal("exceptional cross-controller teardown destroyed an existing job or retained local accounting")
	}
	// Abort detaches local ownership but leaves durable state for the next
	// controller instead of treating startup failure as a force deadline.
	restarted := newPreparedController(t, 1, p, c)
	if err := restarted.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { phase, _ := restarted.instances.phase("runner-active"); return phase == instanceBusy }, "retained job was not recoverable after abort")
}
