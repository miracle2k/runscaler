package provider

import (
	"context"
	"errors"
	"time"
)

// InstanceProvider provisions and removes the isolated instance that hosts
// one ephemeral GitHub Actions runner (a Docker container or Tart VM).
type InstanceProvider interface {
	// StartInstance creates and starts a new ephemeral runner with the given
	// name and JIT configuration. Returns an instance ID used for cleanup.
	StartInstance(ctx context.Context, name string, jitConfig string) (instanceID string, err error)

	// RemoveInstance stops and removes a runner by its instance ID. It must be
	// idempotent (an already-absent instance is success) and must honor ctx so
	// controller drain and shutdown deadlines remain bounded.
	RemoveInstance(ctx context.Context, instanceID string) error

	// Shutdown performs provider-specific cleanup.
	Shutdown(ctx context.Context)
}

// PreparedInstanceProvider separates a queued resource request from runner
// registration. RequestInstance returns promptly after recording/requesting the
// resource; WaitPrepared may wait for shared capacity and guest readiness. The
// controller generates a fresh JIT configuration only after that wait succeeds.
//
// An owned resource ID must also be returned on a partial RequestInstance error
// so cleanup can retry. StartPreparedInstance may have started a runner even when
// it returns an error; the controller deregisters before destroying that resource.
// A non-empty startup ID must equal the requested ID.
// All IDs use the same namespace as InstanceProvider.RemoveInstance.
type PreparedInstanceProvider interface {
	RequestInstance(ctx context.Context, name string) (instanceID string, err error)
	WaitPrepared(ctx context.Context, instanceID string) error
	StartPreparedInstance(ctx context.Context, name, instanceID, jitConfig string) (string, error)
}

// InstanceRegistrationRecorder durably records registration ownership without
// storing the JIT secret. The requested intent is saved before the GitHub call,
// including calls whose response might be lost. Removed is idempotent and is
// recorded only once GitHub confirms deletion, completion proves it is gone,
// or the controller proves that registration was never requested.
type InstanceRegistrationRecorder interface {
	RecordJITRequested(ctx context.Context, name, instanceID string) error
	RecordJITRunner(ctx context.Context, name, instanceID string, runnerID int) error
	RecordJITRemoved(ctx context.Context, name, instanceID string) error
}

// RecoveredInstance describes one durable resource/registration intent. A
// removed guest may remain as a tombstone until its registration is reconciled.
// LaunchAttempted means that a runner might hold work and must be deregistered
// before the guest is removed. A provider validates its resource ownership and
// remote identity before returning these records.
type RecoveredInstance struct {
	Name            string
	InstanceID      string
	RunnerID        int
	JITRequested    bool
	LaunchAttempted bool
	InstanceRemoved bool
}

type InstanceRecovery interface {
	RecoverInstances(ctx context.Context) ([]RecoveredInstance, error)
}

// InstancePreflight optionally rejects provisioning before GitHub creates a
// JIT registration. Providers must recheck allocation constraints when used.
type InstancePreflight interface {
	CheckStart(context.Context) error
}

// InstanceWatcher is optionally implemented by providers that can observe an
// instance exiting independently of GitHub job messages. A nil return means
// the instance stopped; a non-nil error means observation failed or the
// context ended.
type InstanceWatcher interface {
	WaitInstance(ctx context.Context, instanceID string) error
}

// InstanceReadiness is optional. Resource creation and readiness are distinct:
// a returned ID stays tracked until readiness, work evidence or safe cleanup.
type InstanceReadiness interface {
	WaitReady(context.Context, string) error
}

var (
	ErrReadinessDisabled    = errors.New("readiness_disabled")
	ErrReadinessTimeout     = errors.New("readiness_timeout")
	ErrReadinessUnavailable = errors.New("readiness_unavailable")
	ErrExitedBeforeReady    = errors.New("runner_exited_before_ready")
)

type ExitDiagnostics struct {
	ExitCode   *int       `json:"exit_code,omitempty"`
	OOMKilled  *bool      `json:"oom_killed,omitempty"`
	FinishedAt *time.Time `json:"finished_at,omitempty"`
	// RawLog is private collection input. It is never serialized directly.
	RawLog string `json:"-"`
}

type InstanceDiagnostics interface {
	CollectDiagnostics(context.Context, string) (ExitDiagnostics, error)
}
