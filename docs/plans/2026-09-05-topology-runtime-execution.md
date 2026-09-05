# Topology development and Kafka Streams execution

## Authority and product objective

On 2026-09-05 the owner approved the next implementation cycle: productize the
bounded Kafka Streams runner with Docker as its first execution target. This
supersedes the prototype-only stop in the previous execution plan. It does not
authorize publication, a general SQL engine, stateful processing, Kubernetes
scheduling, or deployment of arbitrary application images.

The owner restated the product's purpose: absorb an existing streaming topology
or start a new one, then maintain its coherence throughout development and
infrastructure changes. A runner is one component of that workflow. Success
requires dependencies, contracts, ownership, planning and execution to agree.

## Acceptance paths

1. Import a fixture-owned existing Kafka topic as external. Declare its known
   schema and an external custom application without inventing their code.
   Add a managed filter/projection model and a downstream application contract.
   Validate and review the topology, deploy only managed resources, and prove
   output. Re-import cannot clobber edited declarations or transfer ownership.
2. Define a new managed topology. Create its topics and processing through the
   same compiler, reviewed plan and state workflow. Fixture event injection is
   separate from resource ownership. Prove the supported filter change against
   the same application identity and offsets, then prove a no-op repeat.

Both paths run from an installed package outside the checkout. A missing
reference, cycle, incompatible declared consumer contract, unsupported SQL or
unsafe lifecycle transition must fail before any provider mutation. An output
topic's unchanged configuration does not mean its consumers are unaffected by
a changed transformation.

## Bounded backend contract

- Explicit Kafka Streams executor selection; existing Flink behavior stays intact.
- One typed JSON input, direct projection, optional AND-only filter, one output.
  Keep the proof's STRING/BOOLEAN/BIGINT and null/key/tombstone semantics.
- Fixed Java runner with a versioned plan. SQL changes do not rebuild the JAR.
- One Docker container per managed model, immutable image identity, stable
  project/environment/model application identity, no automatic image pulls.
- Local Docker execution only initially. The CLI does not operate a scheduler
  or install a central control plane. Docker's daemon remains a trusted boundary.
- Kafka credentials are runtime-only inputs, never compiled plans, labels or
  command output. Authentication/TLS choices fail explicitly when unsupported.
- No silent offset reset. Starting position for a fresh application is explicit;
  existing progress cannot be replaced with a fresh group or a renamed application.
- First replacement strategy is bounded clean stop then start. Crash recovery,
  concurrent changes, retention loss and stale state require tested behavior,
  not an assumed exactly-once lifecycle guarantee.

## Ordered work

- [x] Record the approval and topology-centered acceptance contract.
- [ ] Enforce declared custom-application column/type compatibility locally.
- [ ] Add the explicit executor, typed compiled artifact and strict SQL compiler;
      reject unsupported topology edges and ambiguous resource identities.
- [ ] Build the maintained runner, deterministic image context, validation and
      safe lifecycle signals. Inventory dependencies and distribution limits.
- [ ] Integrate planning, selection, status, reviewed-plan identity, ownership,
      durable operation state and whole-plan preflight. Start with create/no-op;
      block replacement until its evidence and recovery contract are complete.
- [ ] Implement the supported same-identity replacement and recovery protocol;
      test process failure and interruption without weakening existing blockers.
- [ ] Package the minimal two-path example and runner build assets, prove the
      installed creation/change loop, and reuse it in CI.
- [ ] Complete declared Git base/head comparison and dependency impact reporting
      through the existing workflow. A declared comparison does not authorize apply.
- [ ] Review all changes, run unit/scenario/type/docs/package and real-target
      gates, commit logical tested chunks and push normally.

Work remains in this order unless a dependency requires an earlier correction.
Incomplete lifecycle transitions stay blocked; do not mark the full cycle done
after compiler tests or a standalone runner pass.

## Delegation and boundaries

The primary agent owns CLI/deployment/state integration and commits. A compiler
worker owns model/runtime schema and compilation. A runner worker owns the
maintained Java/Docker subtree. An acceptance worker owns application-contract
checks, then independent installed-package verification. File ownership is
explicit to prevent concurrent changes to planner and state contracts.

Preserve unrelated user files, including the March compile-reference plan,
`prompts/` and `uv.lock`. Historical prototype evidence remains unchanged.
Use disposable uniquely named test resources and validate exact ownership
before cleanup. Do not prune shared Docker resources or reset offsets to make
a test pass. External catalog writes, release tags and uploads are not authorized.

Stop for a new product decision only if the work needs broader SQL semantics,
stateful processing, a new scheduler, arbitrary custom artifact deployment,
production access, or a guarantee that requires data/state loss. Ordinary bugs
and missing tests are implementation work within this approved cycle.

## Evidence

Baseline: `f3f7b89`, all 24 CI jobs passed, including real DataHub and Strimzi.
The prior Kafka Streams proof remains evidence for its narrow contract, not for
the productized backend. Record new commands and observed results here as each
milestone is verified.
