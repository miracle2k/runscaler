# Shared Orchard admission

The `orchard` provider submits fresh macOS VM requests to an unchanged
[Orchard](https://github.com/openai/orchard) controller. Orchard starts the
guests through Tart. CI and interactive clients share Orchard's resource
queue; `max-runners` bounds CI demand and does not reserve worker slots.
Native host builds remain outside this queue.

Start from [config.orchard.example.toml](../config.orchard.example.toml).
Supply GitHub App and Orchard credentials outside the repository. Repository
scale sets need the App's repository **Administration: read and write**
permission. The Orchard service account needs compute read/write/connect
access. Ordinary trusted HTTPS works; HTTP is accepted only on loopback,
including a local SSH tunnel endpoint.

For the bundled Linux system service, place state under
`/var/lib/runner/orchard`, which is inside its prepared writable state tree.
A custom state directory needs operator-provided ownership and systemd write
allowances. `runner validate` checks controller authentication and read access
without creating a VM or persistent provider state.

Until Orchard support is included in an upstream release, build this fork from
a pinned revision. The startup update check is advisory, but `runner update`
and the upstream installer download upstream binaries and can remove this
provider. `disable-update` controls the guest's official GitHub runner update,
not controller updates.

Set `min-runners = 0`. The provider creates a request only for assigned job
demand and always removes that CI guest after the job. It has no idle or warm
CI pool and does not reuse CI disks. Interactive clients can keep and reuse
their own guests and are responsible for deleting them. All clients must
submit through Orchard to participate in admission; unmanaged Tart guests
are not accounted for by Orchard.

## Resource budgets

Each request declares `cpu` and `memory` as guest hardware settings and also
claims these Orchard scheduling resources:

| Resource | Request value |
| --- | --- |
| `org.cirruslabs.logical-cores` | `orchard.cpu` |
| `org.cirruslabs.memory-mib` | `orchard.memory` in MiB |
| `org.cirruslabs.tart-vms` | 1 |

Configure matching capacities on the worker after reserving CPU/RAM for the
host and native tools. Other clients must declare the same resource keys.
Orchard schedules older fitting requests first and can backfill a smaller
request when an older request cannot fit. Worker labels select image/runtime
compatibility; they do not establish resource ownership or reserve slots.

An admission wait has no provider timeout while demand remains. Guest startup
and runner readiness have the configurable `ready-timeout`. GitHub's own job
queue/assignment deadlines still apply. Cancellation removes excess pending
requests without waiting for admission.

## Guest starter contract

Prepare an image with current official GitHub runner binaries at `runner-dir`,
the required build tools, and SSH enabled for Orchard exec. No runner is
registered in the image. Orchard's exec endpoint currently uses SSH, including
when the image also has the Tart guest agent. The provider never invokes a
local Tart binary or connects directly to a worker/guest address.

Install a starter at the externally configured `starter-command`. This is a
fixed command, not a template. The provider invokes it through Orchard exec
with one JSON request on stdin and expects one JSON result on stdout plus a
zero command exit status. Do not echo requests or credentials. The supported
requests are:

```json
{"action":"probe"}
{"action":"start","name":"runner-unique","runner_dir":"/opt/actions-runner","jit_config":"<secret>"}
{"action":"status","name":"runner-unique"}
{"action":"diagnostics","name":"runner-unique","max_bytes":65536}
```

Results contain `state`: `starting`, `ready`, `busy`, `exited`, or `missing`.
`probe` returns `ready` only when the starter and runner tools are available.
`start` must be idempotent for a unique name and return promptly after durable
launch intent. `ready` means the runner is listening for jobs, not merely that
a process exists. `status` may include an integer `exit_code`; diagnostics may
also include `log`, limited to the requested byte count. Never return secrets.

Run the runner as an unprivileged guest user in a detached supervisor so it
survives an exec connection/controller failure. For macOS UI/XCTest workflows,
the supervisor must run in the logged-in user's Aqua launchd domain, with
`SessionCreate=false`; a plain SSH session is insufficient for some UI tests.
Keep launch payload/status files private. Consume and remove JIT payloads,
then pass JIT only through the runner's internal
`ACTIONS_RUNNER_INPUT_JITCONFIG` environment variable. Start one ephemeral
official runner per guest. The guest shim is an operator-supplied image
component, so its paths, GUI account, and installation remain external.

Orchard serializes exec command and environment parameters into request URLs.
The provider sends all JIT data through stdin WebSocket frames. Do not add JIT
to `starter-command`, Orchard exec environment parameters, or `startup_script`.

## Ownership and recovery

Keep `state-dir` on persistent private storage. The provider writes durable
intents before creating VMs, generating registrations, or launching runners.
Its ledger contains an owner UUID, exact VM names/UIDs, runner names/IDs, and
lifecycle state; it contains no JIT payload or guest/API credential.

Restart recovery checks saved exact names and controller-issued VM UIDs.
It resolves ambiguous JIT requests through GitHub's exact runner-name lookup.
It retains registration tombstones until cleanup is confirmed, and it never
deletes another client's guest. A corrupt ledger stops recovery; restore the
saved state instead of deleting files or running a broad resource sweep.

After an abrupt controller exit, GitHub can temporarily retain the previous
message session. The listener retries with bounded backoff until the server
accepts a new session. Existing jobs keep running during this delay; do not
delete or recreate a scale set to force a takeover.

An ambiguous VM-create response also retains an exact-name tombstone. A
missing first read does not discard that intent: the provider checks for a
late-created VM and deletes it when observed. This background cleanup does
not hold a local runner count lease and resumes after restart. Active ledgers
are bound to the configured Orchard URL; recover existing resources through
that endpoint before changing it.

Before destroying a possibly started runner, the controller deregisters it
with GitHub. If GitHub reports a running job, the guest is retained. Cleanup
retries after transient failures. Guest status and bounded diagnostics remain
available through the starter after controller reconnection.

The provider does not use local Tart orphan sweeps, warm pools, cache pruning,
or Docker maintenance. `runner doctor --fix` does not sweep Orchard resources;
owned-resource recovery runs when the scale-set controller starts.
