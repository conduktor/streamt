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
- [x] Enforce declared custom-application column/type compatibility locally.
- [x] Add the explicit executor, typed compiled artifact and strict SQL compiler;
      reject unsupported topology edges and ambiguous resource identities.
- [x] Build the maintained runner, deterministic image context, validation and
      safe lifecycle signals. Inventory dependencies and distribution limits.
- [x] Integrate planning, selection, status, reviewed-plan identity, ownership,
      durable operation state and whole-plan preflight. Start with create/no-op;
      block replacement until its evidence and recovery contract are complete.
- [ ] Implement the supported same-identity replacement and recovery protocol;
      follow the [predicate replacement contract](../specs/kafka-streams-replacement.md)
      and test interruption without weakening existing blockers.
- [x] Package the minimal two-path example and runner build assets, prove the
      installed creation/no-op loop, and add the same journey to CI.
- [ ] Extend installed acceptance and CI to the supported change/recovery loop.
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

Creation checkpoint, 2026-09-05:

- The maintained runner 0.1.1 passed 115 JVM tests and real plaintext Kafka acceptance:
  initial position, clean predicate change on the same group, poison record,
  missing/out-of-range offsets, container-side cluster/topic identity refusal
  without consuming pending records, and local-only validation. Its source/build
  evidence is in `runtimes/kafka-streams/verification.json`; a standalone change
  does not enable product replacement.
- Source CLI acceptance `afc614ee6542` (SDK 2.13.2) and installed-wheel acceptance
  `af2322b164ee` (SDK 2.15.0) each ran both entry paths, including the real managed
  `init` starter, 36 command invocations and 12 expected failures under umask 0077.
  Both proved exact keyed output, committed offsets and unchanged repeat plans
  and applies. Each fixture used a distinct Kafka cluster ID; only its owned
  Docker resources were removed. The curated package evidence records artifact
  hashes, build context parity and cleanup in
  `tests/package/verification/kafka-streams-create.json`.
- Invalid consumer contracts fail offline before provider construction. Invalid
  internal bootstrap configurations leave providers, declarations and `.streamt`
  unchanged; rejected apply may still regenerate compiled files in `generated/`.
- The complete unit/scenario suite passed 6,133 tests with 31 skips. The isolated
  staged creation commit passed 6,044 tests with 31 skips before commit. Ruff,
  mypy (113 files), strict documentation and workflow validation passed locally.
  Remote CI for the new commits is pending.
- The real journey exposed and fixed inherited broker settings being treated as
  topic overrides, incorrect override removal, and native SDK UUID/config-source
  representation mismatches. It did not hide these with fixture configuration.
- Direct runtime boundary tests verify status freshness, identity revalidation,
  no-op progress checks, private credentials and refusal of invalid local TLS
  material before provider construction. Authenticated broker acceptance is open.
- A freshly started broker can return NOT_COORDINATOR before its group coordinator
  is ready. The fixture uses a bounded read-only readiness probe. Product creation
  does not retry uncertain offset writes; it retains a pending operation.
- Predicate-only replacement has typed immutable action evidence and ordered
  durable checkpoints. Its version-4 envelope is used only by evidenced runner
  updates; existing operations retain their version-3 format. Local and PostgreSQL
  state validators reject missing checkpoints and inconsistent prior/result
  ownership records. This foundation does not enable replacement or recovery;
  both stay blocked until the execution and resume protocol is verified.

Follow-up implementation checkpoint:

- A pure transition validator now covers execution, explicit same-operation
  resume and read-only recovery. It rejects signal-in-flight recovery, foreign
  generations, changed ownership/volume/provider identities, retention loss
  and offset movement while no candidate has started. It performs no provider
  calls and does not enable an update command.
- Docker evidence readers inspect exact IDs, hash actual mounted plan bytes,
  verify fixed mounts and the retained volume instance, and bind a candidate to
  its operation/action/fingerprint. A disposable never-started Docker fixture
  verified these readers and was removed with its owned volume afterward.
- The initial TERM cleanup observations returned 143, not the journal's then
  required zero. Cleanup alone did not establish clean-close conditions. The
  follow-up contract below addresses that mismatch without normalizing codes.
- CI for `a7ab3e2` passed both installed Kafka SDK lanes and real DataHub, but
  failed Python 3.10 timestamp parsing and all five Strimzi package lanes. The
  timestamp fix preserves nanoseconds across Python versions. The Strimzi guard
  now checks the reviewed dependency floor and CLI registration; it retains its
  provider-denial tests and passed source/wheel/direct-sdist parity locally.
- Cold coordinator acceptance now passes from source `ac0c4bffa195` (SDK 2.13.2)
  and a fresh installed wheel `c1b443338f61` (SDK 2.15.0), each with 36 commands
  and 12 expected failures. The first online plan ran before Kafka's internal
  offsets topic existed, without a fixture group warmup. All 115 Python modules
  matched source, sdist, wheel and installed bytes. The distinct evidence is in
  `tests/package/verification/kafka-streams-cold-coordinator.json`; the earlier
  creation record remains historical. This attests the exercised classic/consumer
  group APIs, not universal absence of other Kafka group types.
- The compatibility fix's isolated staged tree passed 6,166 unit/scenario tests
  on Python 3.10 with 31 skips. With the newer foundation and cold-coordinator
  changes, both Python 3.10 and 3.12 passed 6,525 tests with 32 skips. Ruff, mypy
  and strict documentation also passed. Remote CI for `8a3114d` passed all
  26 jobs, including both installed Kafka Streams SDK lanes, Python 3.10–3.14,
  real DataHub and real Strimzi acceptance (run `33976686727`).
- Reviewed format 6 now binds the full typed replacement action, without changing
  generic credential redaction or legacy format-5 checksums. Apply uses the
  reviewed tuple returned by freshness validation when writing its intent.
  Moving progress never replaces the reviewed lower bound. The planner still
  blocks replacement; no executable update can be introduced by editing a plan
  envelope. The focused plan/action and existing removal CLI checks passed
  205 tests before this checkpoint.

Read-only replacement observation checkpoint:

- The close contract now admits raw 0 or 143 only with fresh closed status,
  complete non-OOM/error-free process evidence and inactive resumable progress.
  Ordered checkpoints preserve the observed code and reject later code drift.
- The observer reconstructs the prior artifact from actual mounted bytes and
  the locked ownership checksum. It verifies fixed image environment, process
  options, mounts, volume instance and provider identities. Exact-ID process
  re-reads and a complete application-label inventory reject a restarted,
  failed or renamed generation; no read error becomes proof of absence.
- Source fixture `e35cf840ab73` (SDK 2.13.2) and installed fixture `0b92ac642e37`
  (SDK 2.15.0) passed the real fresh-init/create/no-op journey
  (17 commands, six expected refusals), then the full read-only close proof:
  raw 143, fresh closed status within process start/finish, group members 1→0,
  retained offset 5 and unchanged identities/volume/mounts. A separately created,
  never-started extra container was renamed and correctly rejected. All exact
  fixture-owned resources were cleaned up. The new evidence record is
  `tests/package/verification/kafka-streams-replacement-observer.json`. Both runs
  used the same 117-module cohort, verified against source/sdist/wheel/install.
  The included but unexercised executor module changed afterward; the observer
  record does not attest that later change. CI now repeats this observer probe
  in both installed SDK lanes, separately from the two-path creation journey.
- Independent review reproduced and verified the three observer corrections.
  The observer now has 120 tests. The complete suite before the last added
  absence case and executor tests passed 6,826 tests with 32 skips; the focused
  Python 3.10 evidence/plan/observer tests passed 631 tests.
- These readers and proofs do not activate public replacement, recovery or
  resume. The reviewed plan, runtime driver, durable resume authority and CLI
  still need their complete installed change/interruption acceptance.

Internal journaled driver checkpoint:

- The driver now verifies the full locked snapshot before each transition,
  records started/close/removal/creation/completion in order, and retains its
  last acknowledged snapshot on errors. It never retries uncertain writes,
  initializes offsets, creates a replacement volume or commits/clears state.
  `recovery_required` still needs a separate durable resume authorization.
- Source fixture `7168ba0d57cb` (SDK 2.13.2) and installed fixture `4200e1c75970`
  (SDK 2.15.0) passed a deliberately lost Docker-create acknowledgement. Both
  retained `old_removed`, found the single never-started candidate on the next
  invocation, and finished all journal boundaries without recreating it. The
  filter changed from 100 to 200; a new 150 record was rejected, 250 and 300
  records were emitted with their original binary/null keys, the prior 120
  result remained unique, and input offsets advanced from 5 to 8.
- Those probes used one held local lock and two driver invocations, not an OS
  process restart or public CLI resume. They left prior ownership state and
  the completed pending operation intact. Cleanup verified only owned fixture
  IDs and volumes, with TERM-only stops and no force escalation. Evidence is in
  `tests/package/verification/kafka-streams-replacement-executor.json`.
- The final driver cohort matched all 117 Python modules across source, sdist,
  wheel and installation, plus 159 package files and 14 unchanged runner assets.
  This is a separate cohort from the preceding observer record. Both probes now
  run in the two installed Kafka SDK CI lanes.
- Driver tests cover 89 cases, including lost journal/provider acknowledgements,
  stale snapshots, lost locks, transient startup reads and explicit continuation.
  Full Python 3.10 and 3.12 suites each passed 6,916 tests with 32 skips. Ruff,
  mypy (117 files), documentation and workflow checks passed. CI for the prior
  reviewed-action commit `5f9566d` passed all 26 jobs (run `33978991330`).
- Public update activation remains open. Follow the remaining integration order
  in the replacement contract before removing its existing planner/CLI blockers.

Durable resume checkpoint:

- Local and PostgreSQL state now authorize the same interrupted runner operation
  with an explicit resume record. Version-5 control retains the original
  version-4 intent, reviewed checksum and checkpoint bytes. Each authorization
  preserves the full incident, actor, store identity and interrupted-control
  checksum. Completed outcomes cannot be reopened.
- PostgreSQL commits resume history and control together. Local state archives
  the authorization before its control update, in the existing recovery-history
  file. A partial local write permits only the exact archived authorization's
  retry. Incidents remain available after final ownership commit and clear.
- Independent review reproduced two pre-mutation gaps: a missing local archive
  or truncated PostgreSQL history could permit a candidate start before the next
  journal write detected the inconsistency. Active runner snapshot reads now
  validate their full audit first. Tests require zero provider calls. A local
  legacy control-only delegate also now rejects a downgraded control with a
  surviving resume archive; clearing a resumed zero-progress intent is blocked.
- Full Python 3.10 and 3.12 suites each pass 7,122 tests with 32 skips. The
  PostgreSQL 14.23 and 18.4 cohorts each pass 40 real tests, including 16 new
  resume cases. Those tests cover separate connections, repeated interruptions,
  lost COMMIT acknowledgements, retained raw 143, strict history validation and
  final ownership commit. Ruff and mypy (117 modules) pass.
- Source fixture `faf26e664869` (client 2.13.2) and installed fixture
  `768d54fba5cf` (2.15.0) pass the new two-process Docker/Kafka resume probe.
  Worker one loses the create acknowledgement, records the interruption and
  exits; worker two reloads it under a new lock, resumes the existing candidate
  and commits desired ownership. The original incident survives clear. Exact
  outputs prove the threshold change and offsets 5 to 8. This uses a controlled
  worker exit and a synthetic reviewed checksum, not SIGKILL or public plan-file
  validation. The evidence is in
  `tests/package/verification/kafka-streams-durable-resume.json`; all historical
  proofs remain unchanged. Both runs use the same 117-module cohort, with exact
  source/sdist/wheel/install parity, 159 package files and 14 unchanged runtime
  assets. CI now repeats this probe in both installed SDK lanes and runs the new
  PostgreSQL resume cases against the installed wheel. Fixture-owned containers,
  volumes and networks were removed without force; evidence files remain.
- Public update, resume and recovery remain unadvertised and blocked. Their
  caller must verify the original reviewed plan and current desired project,
  retrieve a prewritten local authorization when needed, and use the locked
  executor through completion. The state backend alone does not verify SQL
  against a reviewed plan file.
