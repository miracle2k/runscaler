package controller

import (
	"context"
	"sync"
	"time"

	"github.com/ysya/runscaler/internal/backoff"
	"github.com/ysya/runscaler/internal/capacity"
)

type instancePhase uint8

const (
	instanceProvisioning instancePhase = iota
	instanceIdle
	instanceBusy
	instanceRemoving
)

// Why a runner left the controller's state. Reported on the "Runner removed"
// log line so an operator can tell a finished job from a scale-down.
const (
	reasonJobCompleted = "job-completed"
	reasonScaledDown   = "scaled-down"
	reasonDrain        = "drain"
	reasonExited       = "exited"
	reasonShutdown     = "shutdown"
	reasonReadiness    = "readiness-failed"
	reasonPreparation  = "preparation-ended"
)

// jobRef identifies the job a runner picked up, for the removal log line.
type jobRef struct {
	id     string
	name   string // JobDisplayName
	repo   string // owner/name
	runID  int64
	result string // set once the job completes
}

type managedInstance struct {
	id                   string
	runnerID             int // GitHub runner ID from the JIT config; 0 when unknown
	phase                instancePhase
	lease                *capacity.Lease
	removeReason         string
	job                  jobRef
	cleanupInFlight      bool
	cleanupAttempts      int
	cleanupRetryAt       time.Time
	attempt              startAttempt
	worked               bool
	readinessCancel      context.CancelFunc
	jit                  string
	diagnosticsCollected bool
	diagnosticReason     string
	preparationCancel    context.CancelFunc
	preparationInFlight  bool
	launchAttempted      bool
	registrationClear    bool
}

// removedInstance is what finishRemoval hands back: the lease to
// release and the context the removal log line reports.
type removedInstance struct {
	lease  *capacity.Lease
	reason string
	job    jobRef
}

// instanceState is the controller's source of truth. Provisioning instances
// count toward desired and host capacity before slow provider I/O begins;
// removing instances keep their lease until cleanup succeeds.
type instanceState struct {
	mu        sync.Mutex
	instances map[string]*managedInstance
}

func newInstanceState() instanceState {
	return instanceState{instances: make(map[string]*managedInstance)}
}

func (r *instanceState) reserve(name string, lease *capacity.Lease) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.instances[name]; exists {
		return false
	}
	r.instances[name] = &managedInstance{phase: instanceProvisioning, lease: lease}
	return true
}

func (r *instanceState) reservePrepared(name string, lease *capacity.Lease, cancel context.CancelFunc, attempt startAttempt) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.instances[name]; exists {
		return false
	}
	r.instances[name] = &managedInstance{
		phase: instanceProvisioning, lease: lease, attempt: attempt,
		preparationCancel: cancel, preparationInFlight: true,
	}
	return true
}

// cancelPreparingAboveTarget keeps the oldest preparations in the queue. A
// launch that may already hold a GitHub job is left to normal deregistration.
func (r *instanceState) cancelPreparingAboveTarget(target int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	current := 0
	for _, i := range r.instances {
		if i.phase != instanceRemoving {
			current++
		}
	}
	for current > target {
		var newest *managedInstance
		for _, i := range r.instances {
			if i.phase == instanceProvisioning && i.preparationCancel != nil &&
				(newest == nil || i.attempt.id > newest.attempt.id) {
				newest = i
			}
		}
		if newest == nil {
			return
		}
		newest.phase = instanceRemoving
		newest.removeReason = reasonPreparation
		newest.preparationCancel()
		current--
	}
}

func (r *instanceState) setPreparedResource(name, id string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	i := r.instances[name]
	if i == nil {
		return false
	}
	i.id = id
	return true
}

func (r *instanceState) preparationCurrent(name string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	i := r.instances[name]
	return i != nil && i.phase == instanceProvisioning && i.preparationCancel != nil
}

func (r *instanceState) beginPreparedLaunch(name string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	i := r.instances[name]
	if i == nil || i.phase != instanceProvisioning || i.preparationCancel == nil {
		return false
	}
	i.preparationCancel = nil
	i.launchAttempted = true
	return true
}

func (r *instanceState) finishPreparation(name string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if i := r.instances[name]; i != nil {
		i.preparationInFlight = false
		i.preparationCancel = nil
	}
}

// hasName reports whether name is tracked in any phase.
func (r *instanceState) hasName(name string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, ok := r.instances[name]
	return ok
}

func (r *instanceState) failProvision(name string) {
	r.mu.Lock()
	if instance, ok := r.instances[name]; ok && instance.id == "" {
		delete(r.instances, name)
	}
	r.mu.Unlock()
}

// markReady records the provider ID. removeNow is true when a completion or
// drain raced provider startup and cleanup must happen immediately.
func (r *instanceState) markReady(name, instanceID string, runnerID int) (removeNow, tracked bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	instance, ok := r.instances[name]
	if !ok {
		return false, false
	}
	instance.id = instanceID
	instance.runnerID = runnerID
	if instance.phase == instanceProvisioning {
		instance.phase = instanceIdle
	}
	return instance.phase == instanceRemoving, true
}

func (r *instanceState) markProvisioned(name, id string, runnerID int, cancel context.CancelFunc) (removing, tracked bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	i, ok := r.instances[name]
	if !ok {
		return false, false
	}
	i.id, i.runnerID = id, runnerID
	if i.phase == instanceProvisioning {
		i.readinessCancel = cancel
	} else {
		cancel()
	}
	return i.phase == instanceRemoving, true
}

func (r *instanceState) readinessResult(name, id string, failureReason string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	i, ok := r.instances[name]
	if !ok || i.id != id || i.phase != instanceProvisioning {
		return false
	}
	if failureReason != "" {
		i.phase = instanceRemoving
		i.removeReason = reasonReadiness
		i.diagnosticReason = failureReason
	} else {
		i.phase = instanceIdle
	}
	return true
}

func (r *instanceState) attempt(name string) startAttempt {
	r.mu.Lock()
	defer r.mu.Unlock()
	if i := r.instances[name]; i != nil {
		return i.attempt
	}
	return startAttempt{}
}

func (r *instanceState) setAttempt(name string, attempt startAttempt) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if i := r.instances[name]; i != nil {
		i.attempt = attempt
	}
}

func (r *instanceState) setJIT(name, jit string, runnerID int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if i := r.instances[name]; i != nil {
		i.jit = jit
		i.runnerID = runnerID
	}
}
func (r *instanceState) claimDiagnostics(name, id string) (string, string, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	i := r.instances[name]
	if i == nil || i.id != id || i.diagnosticsCollected || (i.removeReason != reasonReadiness && i.removeReason != reasonExited) {
		return "", "", false
	}
	i.diagnosticsCollected = true
	return i.diagnosticReason, i.jit, true
}

func (r *instanceState) markExited(name, id string) (startAttempt, bool, bool, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	i, ok := r.instances[name]
	if !ok || i.id != id || (i.phase == instanceRemoving && i.removeReason != reasonReadiness) {
		return startAttempt{}, false, false, false
	}
	unassigned := !i.worked && i.removeReason != reasonReadiness
	i.phase = instanceRemoving
	i.removeReason = reasonExited
	i.diagnosticReason = "runner_exited_unexpectedly"
	if !i.worked {
		i.diagnosticReason = "runner_exited_before_ready"
	}
	if i.readinessCancel != nil {
		i.readinessCancel()
	}
	return i.attempt, unassigned, i.worked, true
}

func (r *instanceState) hasWork(attempt startAttempt) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, i := range r.instances {
		if i.attempt == attempt && i.worked && i.removeReason != reasonExited {
			return true
		}
	}
	return false
}

// count returns instances that may still serve work. Idle removals retain
// local capacity because GitHub can restore them to busy; replacing them early
// could exceed max-runners. Completed removals cannot be restored and are
// excluded so their capacity can be replaced when the global lease is released.
func (r *instanceState) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	count := 0
	for _, instance := range r.instances {
		if instance.phase != instanceRemoving || isUnassignedRemoval(instance) {
			count++
		}
	}
	return count
}

func (r *instanceState) counts() (idle, busy int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, instance := range r.instances {
		switch instance.phase {
		case instanceIdle:
			idle++
		case instanceBusy:
			busy++
		}
	}
	return idle, busy
}

func (r *instanceState) lifecycleCounts() (provisioning, idle, busy, removing int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, instance := range r.instances {
		switch instance.phase {
		case instanceProvisioning:
			provisioning++
		case instanceIdle:
			idle++
		case instanceBusy:
			busy++
		case instanceRemoving:
			removing++
		}
	}
	return provisioning, idle, busy, removing
}

func (r *instanceState) contains(name, instanceID string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	instance, ok := r.instances[name]
	return ok && instance.id == instanceID && (instance.phase != instanceRemoving || instance.removeReason == reasonReadiness)
}

func (r *instanceState) markBusy(name string, job jobRef) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	instance, ok := r.instances[name]
	if !ok {
		return false
	}
	switch {
	case instance.phase == instanceIdle, instance.phase == instanceProvisioning:
		instance.phase = instanceBusy
	case instance.phase == instanceBusy && instance.job.id == "":
		// GitHub refused to deregister this runner because it already held a
		// job, and restoreBusy kept it before this JobStarted arrived.
	case isUnassignedRemoval(instance):
		// GitHub assigned a job while removal of this "idle" runner was in
		// progress. Record the job; deregistration decides whether it stays.
	default:
		return false
	}
	instance.job = job
	instance.worked = true
	if instance.readinessCancel != nil {
		instance.readinessCancel()
	}
	return true
}

// isUnassignedRemoval reports a runner removed because it looked idle
// (scale-down or drain), as opposed to one whose job finished.
func isUnassignedRemoval(instance *managedInstance) bool {
	return instance.phase == instanceRemoving &&
		(instance.removeReason == reasonScaledDown || instance.removeReason == reasonDrain || instance.removeReason == reasonReadiness ||
			(instance.removeReason == reasonExited && instance.launchAttempted && !instance.registrationClear))
}

// deregistration returns the GitHub runner ID to deregister before the
// instance is destroyed. Only runners removed as idle need it: a runner whose
// job completed is already gone from GitHub, being ephemeral.
func (r *instanceState) deregistration(name, instanceID string) (runnerID int, needed bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	instance, ok := r.instances[name]
	if !ok || instance.id != instanceID || instance.registrationClear || !isUnassignedRemoval(instance) {
		return 0, false
	}
	return instance.runnerID, true
}

func (r *instanceState) markLaunched(name, id string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if i := r.instances[name]; i != nil && i.id == id {
		i.launchAttempted = true
	}
}

func (r *instanceState) registrationCleared(name, id string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	i := r.instances[name]
	return i != nil && i.id == id && i.registrationClear
}

func (r *instanceState) markRegistrationCleared(name, id string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if i := r.instances[name]; i != nil && i.id == id {
		i.registrationClear = true
	}
}

func (r *instanceState) runnerID(name, instanceID string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	if i := r.instances[name]; i != nil && i.id == instanceID {
		return i.runnerID
	}
	return 0
}

func (r *instanceState) setRunnerID(name, instanceID string, runnerID int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if i := r.instances[name]; i != nil && i.id == instanceID {
		i.runnerID = runnerID
	}
}

func (r *instanceState) removalReason(name, instanceID string) string {
	r.mu.Lock()
	defer r.mu.Unlock()
	if i := r.instances[name]; i != nil && i.id == instanceID {
		return i.removeReason
	}
	return ""
}

func (r *instanceState) clearRegistrationOnRemoval(name, instanceID string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	i := r.instances[name]
	return i != nil && i.id == instanceID && (i.registrationClear || i.removeReason == reasonJobCompleted)
}

func (r *instanceState) markRegistrationUnrequested(name string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if i := r.instances[name]; i != nil {
		i.registrationClear = true
	}
}

// restoreBusy cancels the removal of a runner GitHub refused to deregister
// because it holds a job. It returns false when the job completed while the
// deregistration call was in flight, in which case removal should proceed.
func (r *instanceState) restoreBusy(name, instanceID string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	instance, ok := r.instances[name]
	if !ok || instance.id != instanceID || !isUnassignedRemoval(instance) {
		return false
	}
	instance.phase = instanceBusy
	instance.worked = true
	instance.removeReason = ""
	instance.cleanupInFlight = false
	instance.cleanupAttempts = 0
	instance.cleanupRetryAt = time.Time{}
	return true
}

// markDone transitions a runner to removing and records why, plus the job's
// result when one finished, for the "Runner removed" line. ready is false only
// for the narrow race where GitHub reports completion before StartInstance
// returns. A completion can also take over an idle removal; doing both under
// this lock prevents restoreBusy from racing between two completion checks.
func (r *instanceState) markDone(name, reason, result string) (instanceID string, ready, ok bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	instance, ok := r.instances[name]
	if !ok || (instance.phase == instanceRemoving && (reason != reasonJobCompleted || !isUnassignedRemoval(instance))) {
		return "", false, false
	}
	instance.phase = instanceRemoving
	instance.removeReason = reason
	instance.job.result = result
	if reason == reasonJobCompleted {
		instance.worked = true
	}
	if instance.readinessCancel != nil {
		instance.readinessCancel()
	}
	return instance.id, instance.id != "", true
}

// markIdleAboveTarget transitions one surplus idle instance to removing. Busy
// runners are never preempted, and the count/transition share the same lock as
// markBusy so job assignment wins or loses the boundary atomically. Unlike
// count, this planned count excludes every removal to retain the desired idle
// runners while earlier removals are still in flight.
func (r *instanceState) markIdleAboveTarget(target int) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	current := 0
	for _, instance := range r.instances {
		if instance.phase != instanceRemoving {
			current++
		}
	}
	if current <= target {
		return false
	}
	for _, instance := range r.instances {
		if instance.phase == instanceIdle {
			instance.phase = instanceRemoving
			instance.removeReason = reasonScaledDown
			return true
		}
	}
	return false
}

// idleForRemoval atomically establishes the drain boundary by transitioning
// every idle runner to removing before returning provider IDs.
func (r *instanceState) idleForRemoval() map[string]string {
	r.mu.Lock()
	defer r.mu.Unlock()
	result := make(map[string]string)
	for name, instance := range r.instances {
		if instance.phase == instanceIdle || instance.phase == instanceProvisioning {
			instance.phase = instanceRemoving
			instance.removeReason = reasonDrain
			result[name] = instance.id
			if instance.preparationCancel != nil {
				instance.preparationCancel()
			}
		}
	}
	return result
}

// claimRemoval atomically assigns one eligible cleanup operation to a worker.
// A failed cleanup is given its own retry deadline, so it cannot block cleanup
// or scale-up work for unrelated instances.
func (r *instanceState) claimRemoval(now time.Time) (name, instanceID string, ok bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for name, instance := range r.instances {
		if instance.phase == instanceRemoving && instance.id != "" && !instance.preparationInFlight && !instance.cleanupInFlight && !now.Before(instance.cleanupRetryAt) {
			instance.cleanupInFlight = true
			return name, instance.id, true
		}
	}
	return "", "", false
}

// Retries of a runner whose removal failed: 5s doubling to 80s, four
// doublings. No jitter: each runner retries on its own schedule.
const (
	removalRetryInitial = 5 * time.Second
	removalRetryMax     = 80 * time.Second
)

func removalRetryDelay(attempts int) time.Duration {
	return backoff.Exponential(attempts, removalRetryInitial, removalRetryMax)
}

func (r *instanceState) retryRemoval(name, instanceID string, now time.Time) (time.Duration, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	instance, ok := r.instances[name]
	if !ok || instance.phase != instanceRemoving || instance.id != instanceID || !instance.cleanupInFlight {
		return 0, false
	}
	instance.cleanupInFlight = false
	instance.cleanupAttempts++
	delay := removalRetryDelay(instance.cleanupAttempts)
	instance.cleanupRetryAt = now.Add(delay)
	return delay, true
}

func (r *instanceState) finishRemoval(name, instanceID string) (removedInstance, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	instance, ok := r.instances[name]
	if !ok || instance.phase != instanceRemoving || instance.id != instanceID {
		return removedInstance{}, false
	}
	delete(r.instances, name)
	return removedInstance{lease: instance.lease, reason: instance.removeReason, job: instance.job}, true
}

func (r *instanceState) detachAll() map[string]managedInstance {
	r.mu.Lock()
	defer r.mu.Unlock()
	result := make(map[string]managedInstance, len(r.instances))
	for name, instance := range r.instances {
		result[name] = *instance
	}
	clear(r.instances)
	return result
}

func (r *instanceState) identities() map[string]string {
	r.mu.Lock()
	defer r.mu.Unlock()
	result := make(map[string]string, len(r.instances))
	for name, i := range r.instances {
		result[name] = i.id
	}
	return result
}
