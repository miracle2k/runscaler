package controller

import (
	"context"
	"log/slog"
	"time"

	"github.com/ysya/runscaler/internal/provider"
)

// Abort ends an exceptional controller/startup failure without force-destroying
// durable guests that may still hold jobs. Other scale sets can fail after this
// controller successfully recovered a busy guest; that is not an explicit drain
// deadline. A future controller recovers anything retained in the provider ledger.
// Legacy providers keep their existing shutdown behavior.
func (s *ScaleSetController) Abort(ctx context.Context) {
	if _, durable := s.provider.(provider.InstanceRecovery); !durable {
		s.Shutdown(ctx)
		return
	}
	s.draining.Store(true)
	s.stopWork()
	abortCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := s.waitWorker(abortCtx); err == nil {
		for name := range s.instances.identities() {
			s.instances.markDone(name, reasonDrain, "")
		}
		for abortCtx.Err() == nil {
			name, id, ok := s.instances.claimRemoval(time.Now())
			if !ok {
				break
			}
			s.removeClaimedInstance(abortCtx, name, id)
		}
	}
	retained := 0
	for _, i := range s.instances.detachAll() {
		if i.id != "" {
			retained++
		}
		i.lease.Release()
	}
	s.flushRegistrations(abortCtx)
	s.provider.Shutdown(abortCtx)
	s.logger.Info("Controller aborted; durable guests retained for recovery", slog.Int("retained", retained))
}
