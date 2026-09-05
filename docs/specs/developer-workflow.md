# Developer workflow

## Status

Planned requirements, revised on 2026-09-04. The
[product direction](product-direction.md) defines the purpose; the
[execution plan](../plans/2026-09-04-developer-experience-execution.md) records
confirmed choices, open questions, work order, and autonomy limits.

These requirements do not add supported commands, ownership modes, or execution
backends. Current behavior remains documented in the
[support matrix](../reference/support-matrix.md).

## First complete result

An engineer can describe an existing stream and application, add a managed SQL
transformation, see its output, then change and redeploy that transformation.
The project explains how the external and managed parts connect. SQL is the
first authoring interface; custom applications remain part of the model.

Use a deterministic orders example. Start with Kafka input and output so the
first result does not depend on a sink connector or external account. The exact
execution route is pending: existing Flink SQL remains a baseline, while a
Kafka-without-Flink route is a required design decision. A PostgreSQL sink via
Connect is an optional extension, not a prerequisite.

## Existing systems and ownership

Discovery produces a preview before writing project files. The user selects a
bounded set of resources and can rerun import without clobbering local edits.
Each discovered value records its origin and whether it is complete. Credentials
use environment references; discovery does not persist secret values.

Import supports only provider data that can be verified. Kafka topic metadata
does not reveal an application's SQL. Flink job status does not prove its source
definition. Missing behavior remains an external boundary with a useful message
about what the user would need to supply before management is possible.

The desired external-resource policy is:

- Include external nodes in the project graph and documentation.
- Validate local names, references, and the structure of declared contracts.
- Do not automatically refresh, enforce drift, mutate, or recover external
  resources as part of ordinary project work.
- Run live inspection only when explicitly requested, subject to the pending
  observation decision. Report it as an observation, not a new desired state.
- Keep declared information labelled as such; an old imported schema is not
  proof of the live schema's compatibility.

Ownership adoption is a separate action with existing safety requirements.
Changing a label or importing a file cannot authorize mutation of a resource
already present on a cluster. Extend the adoption surface only when that kind's
definition, identity, observation, and recovery contracts are complete.

Managed resources retain live planning and stale-plan checks. Opting out of
external drift detection must not opt out of checks on managed operations.
If an operation needs external evidence for safety, report that requirement and
request explicit inspection rather than fabricate evidence or access it silently.

## Custom applications

Represent a custom application by its declared inputs, outputs, owner, repository,
and optional consumer-group identity. Reuse existing exposure fields where they
fit; investigate producer and mixed-role graph behavior before adding a parallel
application schema. Do not claim to infer dependencies from arbitrary code.

A later managed application may reference an immutable image or JAR. That needs
a chosen execution target and an explicit lifecycle contract. Compiling Java or
Python source, building images, scheduling processes, and managing rolling
upgrades are separate work, pending the application-scope decision.

## Kafka without Flink

The existing SQL compilation path requires a supported executor; Kafka itself
does not execute the generated Flink SQL. Missing Flink must produce a clear
explanation, not a deployment that creates output topics without processing data.

A possible Kafka Streams executor needs a written SQL subset, type and null
semantics, serialization and key handling, deterministic topology identities,
and bounded runtime configuration. Source offsets and processing guarantees must
be explicit even for filters and projections. Unsupported statements fail before
resource creation. Do not substitute Python expression evaluation, a different
SQL dialect, or an empty topic for actual execution evidence.

Keep the compiler/backend distinction small. A runtime prototype must not
introduce an HTTP control plane, arbitrary code execution, hot reload, joins,
windows, or distributed deployment until those are separately selected.
Whether the first experiment generates an application or runs a versioned plan
in a fixed runner is a recorded architecture decision.

## Scaffold and local environment

The default scaffold must pass strict validation and compile offline without
requiring production credentials. It must use explicit columns, show the
selected runtime, and identify the next command that can actually succeed.
Do not silently replace unresolved secret configuration with production defaults.

The example must work from an installed distribution in a fresh directory. Ship
or explicitly fetch the versioned example assets; do not assume the user cloned
the repository. Use isolated resource names, bounded readiness checks, pinned
runtime dependencies, and useful errors for missing prerequisites or occupied
ports. Avoid starting Console, catalogs, Gateway, or Connect unless the chosen
example needs them.

The demo setup owns its disposable input fixtures. A `source` declaration alone
does not create its external topic. Seed known records and inspect results by
record identity rather than relying on a fixed sleep or a topic-exists check.
State cleanup must target only resources created by that demo run. Do not reset
a shared Docker installation or reuse production credentials.

## Development checks and visible results

Distinguish offline validation, tests over controlled inputs, and checks against
live data. Current schema/sample/continuous tests are not an implemented
given-input/expected-output transformation test framework.

First test the reference transformation on the real selected executor. Include
valid input, deliberately invalid input, and records that expose the effect of
the transformation. Show exact expected output for the supported behavior.
After the runtime decision, extract reusable fixture-based test support where
the example proves its semantics.

Static preview remains useful without infrastructure. It must identify what it
has not executed and cannot certify runtime windows, state, delivery, or timing.

## Review and update

Git review compares declared versions. Live deployment planning checks the
selected environment. The current offline plan assumes a fresh deployment; it
is not a Git base/head comparison and cannot authorize `apply`.

A semantic Git comparison and the existing GitHub Action can share a readable
report once implemented. Neither replaces the real create/change/redeploy test.
Protected or shared environments keep their reviewed-plan and state requirements.

The first update must change output behavior on the same application and
resource identities. Define exactly how the selected backend handles shutdown,
source offsets, emitted records, partial failure, and recovery. A no-op repeat
must not create an extra job or duplicate owned resources. Recreating the whole
environment, renaming the application, resetting offsets, or discarding state
does not satisfy the update criterion.

Flink updates remain blocked until a separate lifecycle contract proves the
supported transition. Apply the same standard to a Kafka Streams runner. The
choice between an initial projection/filter update and a stateful aggregation
upgrade remains open. Do not promise zero downtime or exactly-once upgrades.

## Acceptance evidence

| Check | Required evidence |
| --- | --- |
| First use | Installed package, fresh directory, strict scaffold validation, no manual edits to generated artifacts |
| Discovery | Selected resources only; repeat import preserves edits; unknown implementation fields stay unknown |
| External resources | Local graph validation; no automatic drift or provider mutation; opt-in observations labelled separately |
| Mixed project | One external custom application and one managed SQL model have explicit input/output relationships |
| Execution | Seeded records produce the expected output on the chosen real executor |
| Repeatability | Repeat plan/apply creates no extra processor and changes no unrelated resource |
| Update | Changed behavior on stable identities, with documented offset/delivery behavior and a tested failure path |
| Unsupported behavior | Unsupported SQL and unsafe lifecycle transitions stop before mutation |
| Delivery | The documented commands run in CI with the same package assets and retain shared-environment safety |
| Handoff | Commands, versions, result evidence, outstanding limits, and the next bounded task are recorded |

Record elapsed steps and intervention points without claiming a time target was
met before measurement. Track failures on cold setup separately from warm runs.
