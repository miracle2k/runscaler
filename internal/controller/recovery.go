package controller

import (
	"context"
	"fmt"
	"time"

	"github.com/ysya/runscaler/internal/capacity"
	"github.com/ysya/runscaler/internal/provider"
)

// Recover restores provider-owned durable intents before the first listener
// demand callback. Possibly launched runners are deregistered before removal;
// GitHub's busy refusal adopts them until their completion arrives. A provider
// without durable recovery keeps its previous startup behavior.
func (s *ScaleSetController) Recover(ctx context.Context) error {
	p, ok := s.provider.(provider.InstanceRecovery)
	if !ok {
		return nil
	}
	s.recoveryMu.Lock()
	defer s.recoveryMu.Unlock()
	s.desiredMu.Lock()
	started := s.desiredRevision != 0
	s.desiredMu.Unlock()
	if started {
		return fmt.Errorf("instance recovery must finish before listening for demand")
	}
	records, err := p.RecoverInstances(ctx)
	if err != nil {
		return fmt.Errorf("recover provider instances: %w", err)
	}
	seen := make(map[string]bool, len(records))
	possiblyActive := 0
	for _, record := range records {
		if record.Name == "" || record.InstanceID == "" || seen[record.Name] {
			return fmt.Errorf("provider returned an invalid or duplicate recovery identity")
		}
		seen[record.Name] = true
		if record.JITRequested && record.LaunchAttempted && !record.InstanceRemoved {
			possiblyActive++
		}
	}
	if possiblyActive > s.maxRunners {
		return fmt.Errorf("recovery owns %d possibly active runners; max-runners must be at least that count during recovery", possiblyActive)
	}
	// Complete fallible preparation before taking ownership of any possibly
	// busy resource. A startup error must not leave partially adopted jobs for
	// the caller's deferred force-shutdown to destroy.
	leases := make(map[string]*capacity.Lease)
	adopted := false
	defer func() {
		if !adopted {
			for _, lease := range leases {
				lease.Release()
			}
		}
	}()
	for _, record := range records {
		if s.instances.hasName(record.Name) {
			continue
		}
		if record.InstanceRemoved && !record.JITRequested {
			if err := s.recordRegistrationRemoved(ctx, record.Name, record.InstanceID); err != nil {
				return err
			}
		}
		if !record.JITRequested || !record.LaunchAttempted || record.InstanceRemoved {
			continue
		}
		acquireCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		lease, err := s.capacity.Acquire(acquireCtx)
		cancel()
		if err != nil {
			return fmt.Errorf("reserve recovered runner capacity: %w", err)
		}
		leases[record.Name] = lease
	}
	for _, record := range records {
		if s.instances.hasName(record.Name) {
			continue
		}
		if !record.JITRequested || !record.LaunchAttempted || record.InstanceRemoved {
			if record.JITRequested {
				s.queuePreparedRegistration(record.Name, record.InstanceID, record.RunnerID)
			}
			if record.InstanceRemoved {
				continue
			}
			// This request never reached runner launch and cannot hold work.
			// Orchard retains its actual resource budget until guest cleanup;
			// no new local runner lease is needed merely to retry that cleanup.
			s.instances.reserve(record.Name, &capacity.Lease{})
			s.instances.markReady(record.Name, record.InstanceID, record.RunnerID)
			s.instances.markDone(record.Name, reasonPreparation, "")
			continue
		}
		// Recoveries retain count leases just like normally launched runners.
		// Bound the wait if capacity settings were reduced across a restart.
		lease := leases[record.Name]
		if !s.instances.reserve(record.Name, lease) {
			lease.Release()
			continue
		}
		s.instances.markReady(record.Name, record.InstanceID, record.RunnerID)
		s.instances.markLaunched(record.Name, record.InstanceID)
		s.instances.markDone(record.Name, reasonReadiness, "")
		s.observePrepared(record.Name, record.InstanceID, nil)
	}
	adopted = true
	s.signalReconcile()
	return nil
}
