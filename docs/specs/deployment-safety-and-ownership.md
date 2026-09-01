# Deployment safety and ownership

## Status

Required specification. Safety requirements override backward compatibility
with the current orphan-deletion behavior.

## Invariants

1. A live resource is never deleted solely because it is absent from the
   desired project.
2. A resource can be mutated only when its ownership mode permits it.
3. Deletion is computed from previously applied streamt state, not by subtracting
   desired resources from the entire cluster.
4. A partial or selected plan cannot alter resources outside its selection.
5. `apply` executes the exact reviewed plan and rejects stale or modified plans.
6. Destructive behavior defaults to disabled in every environment mode.

## Ownership modes

Every declared runtime resource has one of three modes:

| Mode | Meaning | Observe | Create/update | Delete |
|---|---|---:|---:|---:|
| `external` | Exists outside streamt ownership | yes | no | never |
| `managed` | Created and lifecycle-managed by this project | yes | yes | explicit only |
| `adopted` | Existing resource explicitly claimed by this project | yes | yes | explicit only |

Sources default to `external`. Output resources default to `managed` only after
the project successfully creates them. Existing output resources require
explicit adoption before mutation unless the user selects a backend-specific
create-if-absent policy.

## Stable resource identity

Compiled artifacts carry a stable identity independent of display names:

```text
streamt://<project>/<environment>/<kind>/<logical-name>
```

They also carry:

- Project and environment.
- Artifact kind and physical target name.
- Owning source/model/test/exposure.
- Ownership mode.
- Source file and logical configuration path when available.
- Content checksum.

Backend APIs may not support storing this metadata. The state backend is the
authority for ownership; discovery heuristics are never sufficient to delete.

## State model

The last applied state records only resources streamt owns or has adopted:

```json
{
  "state_version": 1,
  "project": "payments",
  "environment": "prod",
  "serial": 12,
  "resources": {
    "streamt://payments/prod/topic/payments_clean": {
      "physical_name": "payments.clean.v1",
      "ownership": "managed",
      "artifact_checksum": "sha256:...",
      "backend": "direct-kafka"
    }
  }
}
```

Local state is acceptable for development but must warn that it is unsuitable
for shared CI. A production direct-apply backend requires remote state and
locking. External deployment backends may use their own state authority.
Local snapshots are isolated by environment at
`.streamt/state/<environment>.json`.

## Planning algorithm

For each desired resource:

- Desired + no live resource: propose create when ownership permits.
- Desired + live + prior ownership: propose an update or no-op.
- Desired + live + no prior ownership: report `requires_adoption`; do not mutate.

For each resource in prior owned state but absent from the full desired project:

- Report a potential removal.
- Propose deletion only when destructive changes are enabled and explicitly
  requested.

For every other live resource:

- Ignore it for lifecycle planning.
- It may still be used as read-only evidence for impact analysis.

When the plan is selected or targeted, removal detection is disabled outside
the selected closure.

## Plan/apply protocol

A saved plan contains the desired manifest checksum, prior-state serial,
environment fingerprint, live-state observations used by the plan, proposed
actions, policy decisions, and plan checksum.

`apply` must reject a plan when:

- Project content changed after planning.
- State serial changed.
- The environment differs.
- The plan is expired under configured policy.
- Required approval or destructive confirmation is absent.

## Adoption

`streamt import` discovers resources and emits `external` declarations.

`streamt adopt`:

1. Reads the live resource.
2. Shows the exact attributes streamt will begin managing.
3. Requires explicit resource and environment confirmation.
4. Writes an ownership entry without changing the resource.
5. Produces a new plan before any later mutation.

Bulk adoption requires a saved selection and non-interactive confirmation token
suitable for CI review.

## Destructive operations

Topic deletion, subject deletion, connector deletion, state reset, partition
reduction, and state-incompatible Flink replacement are destructive.

They require:

- A full, non-targeted plan.
- Previous streamt ownership.
- Environment policy permitting destructive changes.
- An explicit destructive flag or dedicated destroy command.
- A reviewed plan checksum.

Topic and stateful-job deletion should support an environment-level policy that
forbids them entirely.

## Immediate compatibility behavior

Local ownership state is persisted after successful direct applies. Automatic
orphan deletion remains disabled until removal is an explicit, reviewed
workflow backed by state appropriate to the deployment environment. This
intentionally trades cleanup convenience for safety.
