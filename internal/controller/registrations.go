package controller

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/actions/scaleset"

	"github.com/ysya/runscaler/internal/provider"
)

type pendingRegistration struct {
	name       string
	instanceID string
	attempts   int
	retryAt    time.Time
}

type pendingRegistrationLookup struct {
	instanceID string
	attempts   int
	retryAt    time.Time
}

type runnerByNameClient interface {
	GetRunnerByName(context.Context, string) (*scaleset.RunnerReference, error)
}

// Failed provider starts own no instance capacity, but retain their known
// GitHub identity until deregistration succeeds. Only one request per
// controller is in flight; unrelated instance reconciliation can continue.
func (s *ScaleSetController) queueRegistration(name string, id int) {
	s.queueRegistrationForInstance(name, "", id)
}

func (s *ScaleSetController) queueRegistrationForInstance(name, instanceID string, id int) {
	s.registrationMu.Lock()
	defer s.registrationMu.Unlock()
	if s.registrations == nil {
		s.registrations = make(map[int]*pendingRegistration)
	}
	s.registrations[id] = &pendingRegistration{name: name, instanceID: instanceID}
}

func (s *ScaleSetController) queuePreparedRegistration(name, instanceID string, runnerID int) {
	if _, ok := s.provider.(provider.InstanceRegistrationRecorder); !ok {
		if runnerID > 0 {
			s.queueRegistration(name, runnerID)
		}
		return
	}
	if runnerID > 0 {
		s.queueRegistrationForInstance(name, instanceID, runnerID)
	} else {
		s.registrationMu.Lock()
		if s.registrationLookups == nil {
			s.registrationLookups = make(map[string]*pendingRegistrationLookup)
		}
		if _, exists := s.registrationLookups[name]; !exists {
			s.registrationLookups[name] = &pendingRegistrationLookup{instanceID: instanceID}
		}
		s.registrationMu.Unlock()
	}
	s.signalReconcile()
}

func (s *ScaleSetController) hasPendingRegistration(name string) bool {
	s.registrationMu.Lock()
	defer s.registrationMu.Unlock()
	for _, r := range s.registrations {
		if r.name == name {
			return true
		}
	}
	_, ok := s.registrationLookups[name]
	return ok
}

func (s *ScaleSetController) registrationCount() int {
	s.registrationMu.Lock()
	defer s.registrationMu.Unlock()
	return len(s.registrations) + len(s.registrationLookups)
}

func (s *ScaleSetController) claimRegistration() (int, bool) {
	s.registrationMu.Lock()
	defer s.registrationMu.Unlock()
	if s.registrationInFlight {
		return 0, false
	}
	for id, r := range s.registrations {
		if !time.Now().Before(r.retryAt) {
			s.registrationInFlight = true
			return id, true
		}
	}
	return 0, false
}

func (s *ScaleSetController) removeRegistration(parent context.Context, id int) {
	ctx, cancel := context.WithTimeout(parent, 30*time.Second)
	s.registrationMu.Lock()
	r := s.registrations[id]
	instanceID, name := "", ""
	if r != nil {
		instanceID, name = r.instanceID, r.name
	}
	s.registrationMu.Unlock()
	err := s.scalesetClient.RemoveRunner(ctx, int64(id))
	if err == nil || errors.Is(err, scaleset.RunnerNotFoundError) || errors.Is(err, scaleset.NotFoundError) {
		if instanceID != "" {
			err = s.recordRegistrationRemoved(ctx, name, instanceID)
		} else {
			err = nil
		}
	}
	cancel()
	s.registrationMu.Lock()
	s.registrationInFlight = false
	r = s.registrations[id]
	if r == nil {
		s.registrationMu.Unlock()
		return
	}
	if err == nil || errors.Is(err, scaleset.RunnerNotFoundError) || errors.Is(err, scaleset.NotFoundError) {
		delete(s.registrations, id)
		s.registrationMu.Unlock()
		s.logger.Debug("Cleaned failed-start runner registration", slog.Int("runnerID", id))
		s.signalReconcile()
		return
	}
	r.attempts++
	delay := removalRetryDelay(r.attempts)
	r.retryAt = time.Now().Add(delay)
	s.registrationMu.Unlock()
	// JobStillRunningError also retains the registration. Never force removal
	// or destroy provider resources in response to a registration-only failure.
	s.logger.Warn("Failed-start runner registration retained for retry", slog.Int("runnerID", id), slog.Duration("retryIn", delay), slog.Any("error", err))
	if !s.draining.Load() && parent.Err() == nil {
		time.AfterFunc(delay, s.signalReconcile)
	}
	s.signalReconcile()
}

func (s *ScaleSetController) claimRegistrationLookup() (string, bool) {
	s.registrationMu.Lock()
	defer s.registrationMu.Unlock()
	if s.registrationInFlight {
		return "", false
	}
	for name, r := range s.registrationLookups {
		if !time.Now().Before(r.retryAt) {
			s.registrationInFlight = true
			return name, true
		}
	}
	return "", false
}

// A first not-found result cannot prove that a timed-out JIT request will not
// finish later. Keep the durable tombstone and retry until a registration can
// be identified and removed. No JIT configuration is saved in this queue.
func (s *ScaleSetController) reconcileRegistrationLookup(parent context.Context, name string) {
	s.registrationMu.Lock()
	r := s.registrationLookups[name]
	if r == nil {
		s.registrationInFlight = false
		s.registrationMu.Unlock()
		return
	}
	instanceID := r.instanceID
	s.registrationMu.Unlock()
	ctx, cancel := context.WithTimeout(parent, 30*time.Second)
	id, err := s.lookupRunnerID(ctx, name)
	resolved := err == nil && id > 0
	if resolved {
		err = s.scalesetClient.RemoveRunner(ctx, int64(id))
		if err == nil || errors.Is(err, scaleset.RunnerNotFoundError) || errors.Is(err, scaleset.NotFoundError) {
			err = s.recordRegistrationRemoved(ctx, name, instanceID)
		}
	}
	cancel()
	s.registrationMu.Lock()
	s.registrationInFlight = false
	r = s.registrationLookups[name]
	if r == nil {
		s.registrationMu.Unlock()
		return
	}
	if resolved && err == nil {
		delete(s.registrationLookups, name)
		s.registrationMu.Unlock()
		s.signalReconcile()
		return
	}
	r.attempts++
	delay := removalRetryDelay(r.attempts)
	r.retryAt = time.Now().Add(delay)
	s.registrationMu.Unlock()
	if err != nil && parent.Err() == nil {
		s.logger.Warn("Registration identity recovery will retry", slog.String("name", name), slog.Duration("retryIn", delay))
	}
	if !s.draining.Load() && parent.Err() == nil {
		time.AfterFunc(delay, s.signalReconcile)
	}
	s.signalReconcile()
}

func (s *ScaleSetController) lookupRunnerID(ctx context.Context, name string) (int, error) {
	client, ok := s.scalesetClient.(runnerByNameClient)
	if !ok {
		return 0, errors.New("scale set client cannot recover a runner by name")
	}
	runner, err := client.GetRunnerByName(ctx, name)
	if errors.Is(err, scaleset.RunnerNotFoundError) || errors.Is(err, scaleset.NotFoundError) {
		return 0, nil
	}
	if err != nil || runner == nil {
		return 0, err
	}
	if runner.ID <= 0 || runner.Name != name || runner.RunnerScaleSetID != s.scaleSetID {
		return 0, errors.New("recovered runner identity or scale set does not match")
	}
	return runner.ID, nil
}

// Shutdown has stopped reconciliation before this final bounded attempt.
// Failure leaves the remote registration for the opt-in recovery sweep.
func (s *ScaleSetController) flushRegistrations(ctx context.Context) {
	s.registrationMu.Lock()
	ids := make([]int, 0, len(s.registrations))
	for id := range s.registrations {
		ids = append(ids, id)
	}
	s.registrationMu.Unlock()
	for _, id := range ids {
		if ctx.Err() != nil {
			return
		}
		s.removeRegistration(ctx, id)
	}
	s.registrationMu.Lock()
	names := make([]string, 0, len(s.registrationLookups))
	for name := range s.registrationLookups {
		names = append(names, name)
	}
	s.registrationMu.Unlock()
	for _, name := range names {
		if ctx.Err() != nil {
			return
		}
		s.reconcileRegistrationLookup(ctx, name)
	}
}
