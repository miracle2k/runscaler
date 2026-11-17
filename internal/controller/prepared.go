package controller

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/actions/scaleset"
	"github.com/google/uuid"

	"github.com/ysya/runscaler/internal/capacity"
	"github.com/ysya/runscaler/internal/provider"
)

// Only prepared providers run startup in the background. Legacy Docker/Tart
// providers retain their existing startup ordering and capacity behavior.
func (s *ScaleSetController) startPreparedWithLease(p provider.PreparedInstanceProvider, lease *capacity.Lease) {
	ctx, cancel := context.WithCancel(s.workCtx)
	name := "runner-" + uuid.NewString()
	attempt := s.recordStartAttempt(time.Now())
	if !s.instances.reservePrepared(name, lease, cancel, attempt) {
		cancel()
		lease.Release()
		return
	}
	s.provisioningWG.Add(1)
	go func() {
		defer s.provisioningWG.Done()
		defer cancel()
		defer func() {
			s.instances.finishPreparation(name)
			s.signalReconcile()
		}()
		s.runPrepared(ctx, p, name, lease, attempt)
	}()
}

func (s *ScaleSetController) runPrepared(ctx context.Context, p provider.PreparedInstanceProvider, name string, lease *capacity.Lease, attempt startAttempt) {
	started := time.Now()
	id, err := p.RequestInstance(ctx, name)
	if id != "" && !s.instances.setPreparedResource(name, id) {
		// A bounded shutdown can detach state before a misbehaving request
		// returns. Its durable provider record remains available for recovery.
		cleanCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		_ = s.provider.RemoveInstance(cleanCtx, id)
		cancel()
		lease.Release()
		return
	}
	endPreparation := func(reason string, failure error) {
		if failure != nil {
			s.recordStartFailureFor(ctx, reason, attempt)
			if ctx.Err() == nil {
				s.logger.Error("Prepared runner startup failed; scheduling cleanup", slog.String("name", name), slog.String("reason", reason))
			}
		}
		if id == "" {
			s.instances.failProvision(name)
			lease.Release()
		} else {
			_, _, _ = s.instances.markDone(name, reasonPreparation, "")
		}
	}
	if err != nil || id == "" {
		if err == nil {
			err = fmt.Errorf("prepared provider returned an empty resource ID")
		}
		endPreparation("provider_request_failed", err)
		return
	}

	s.setCapacityWaiting(true)
	err = p.WaitPrepared(ctx, id)
	s.setCapacityWaiting(false)
	if err != nil {
		endPreparation("provider_prepare_failed", err)
		return
	}
	if !s.preparationNeeded(ctx, name, false) {
		endPreparation("", nil)
		return
	}

	if recorder, ok := s.provider.(provider.InstanceRegistrationRecorder); ok {
		if err := recorder.RecordJITRequested(ctx, name, id); err != nil {
			// GenerateJitRunnerConfig was never called, even if the recorder
			// partially saved its intent before returning an error.
			s.instances.markRegistrationUnrequested(name)
			endPreparation("registration_intent_failed", err)
			return
		}
	}
	if !s.preparationNeeded(ctx, name, false) {
		s.instances.markRegistrationUnrequested(name)
		endPreparation("", nil)
		return
	}

	jit, err := s.scalesetClient.GenerateJitRunnerConfig(ctx, &scaleset.RunnerScaleSetJitRunnerSetting{Name: name}, s.scaleSetID)
	runnerID := 0
	if jit != nil && jit.Runner != nil {
		runnerID = jit.Runner.ID
	}
	if err != nil || jit == nil || runnerID <= 0 {
		if err == nil {
			err = fmt.Errorf("JIT response did not identify its runner")
		}
		s.queuePreparedRegistration(name, id, runnerID)
		endPreparation("jit_config_failed", err)
		return
	}
	s.instances.setJIT(name, jit.EncodedJITConfig, runnerID)
	if recorder, ok := s.provider.(provider.InstanceRegistrationRecorder); ok {
		if err := recorder.RecordJITRunner(ctx, name, id, runnerID); err != nil {
			s.queuePreparedRegistration(name, id, runnerID)
			endPreparation("registration_record_failed", err)
			return
		}
	}
	if !s.preparationNeeded(ctx, name, true) {
		s.queuePreparedRegistration(name, id, runnerID)
		endPreparation("", nil)
		return
	}

	startedID, err := p.StartPreparedInstance(ctx, name, id, jit.EncodedJITConfig)
	if startedID != "" && startedID != id && err == nil {
		err = fmt.Errorf("prepared provider changed its resource ID during startup")
	}
	if err != nil {
		// The start response can be lost after the runner connects. Keep the
		// resource, and let GitHub refuse deregistration if it holds a job.
		s.recordStartFailureFor(ctx, "provider_start_failed", attempt)
		_, _, _ = s.instances.markDone(name, reasonReadiness, "")
		s.observePrepared(name, id, nil)
		return
	}

	ready, needsReadiness := s.provider.(provider.InstanceReadiness)
	readyCtx, cancelReady := context.WithCancel(s.workCtx)
	var removeNow, tracked bool
	if needsReadiness {
		removeNow, tracked = s.instances.markProvisioned(name, id, runnerID, cancelReady)
	} else {
		cancelReady()
		removeNow, tracked = s.instances.markReady(name, id, runnerID)
	}
	if !tracked {
		cancelReady()
		// Shutdown owns the resource after detaching it. Do not race that
		// cleanup or delete a potentially assigned runner from this worker.
		return
	}
	if !needsReadiness {
		s.recordStartSuccessFor(attempt)
	}
	s.logger.Info("Runner started", slog.String("name", name), slog.String("instanceID", id), slog.Duration("startupTime", time.Since(started)))
	if removeNow || s.draining.Load() {
		cancelReady()
		return
	}
	var waitReady func()
	if needsReadiness {
		waitReady = func() { defer cancelReady(); s.waitReady(readyCtx, ready, name, id, attempt) }
	}
	s.observePrepared(name, id, waitReady)
}

// The desired lock orders cancellation and the final launch boundary. Stable
// demand does not cancel/recreate an Orchard request, preserving its queue age.
func (s *ScaleSetController) preparationNeeded(ctx context.Context, name string, launch bool) bool {
	for {
		s.desiredMu.Lock()
		s.instances.cancelPreparingAboveTarget(min(s.maxRunners, s.minRunners+s.desiredRunners))
		if ctx.Err() != nil || s.draining.Load() || !s.instances.preparationCurrent(name) {
			s.desiredMu.Unlock()
			return false
		}
		if s.pendingCompletions == 0 && !s.demandStale {
			needed := !launch || s.instances.beginPreparedLaunch(name)
			s.desiredMu.Unlock()
			return needed
		}
		// Completion and unexpected-exit barriers also apply to preparations
		// admitted while listener statistics are stale. Keep the same request
		// until the next statistics callback instead of creating surplus JITs.
		changed := s.demandChanged
		s.desiredMu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
			return false
		}
	}
}

func (s *ScaleSetController) observePrepared(name, id string, waitReady func()) {
	if watcher, ok := s.provider.(provider.InstanceWatcher); ok {
		s.observerWG.Add(1)
		go func() { defer s.observerWG.Done(); s.watchInstance(s.workCtx, watcher, name, id) }()
	}
	if waitReady != nil {
		s.observerWG.Add(1)
		go waitReady()
	}
}

func (s *ScaleSetController) recordRegistrationRemoved(ctx context.Context, name, id string) error {
	if recorder, ok := s.provider.(provider.InstanceRegistrationRecorder); ok {
		if err := recorder.RecordJITRemoved(ctx, name, id); err != nil {
			return err
		}
		s.instances.markRegistrationCleared(name, id)
	}
	return nil
}
