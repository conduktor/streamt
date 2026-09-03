# DataHub v1.7.0 GMS acceptance implementation plan

## Objective and status

Add a repeatable, test-only release gate that installs the built streamt wheel,
generates both canonical DataHub artifacts through the installed executable,
and proves those exact bytes are accepted by an ephemeral official DataHub
v1.7.0 GMS, survive exact aspect read-back, and produce exact direct
Dataset graph relationships.

The normative contract is
[`DataHub v1.7.0 GMS acceptance gate`](../specs/datahub-gms-v170-acceptance.md).
The existing
[`DataHub catalog export`](../specs/datahub-catalog-export.md) remains the sole
owner of production output. This plan adds no production publisher, client,
dependency, option, state, or deletion behavior.

Status on 2026-09-03: complete. The initial manual feasibility probe was
superseded by complete CI run
[`33798567142`](https://github.com/conduktor/streamt/actions/runs/33798567142),
which passed both fresh artifact variants, two ingestions per variant, exact
aspect and relationship read-back, bounded evidence, and verified teardown.

| Slice | Status | Exit evidence |
| --- | --- | --- |
| 0 — normative foundation | Complete | `9decf35`; acceptance specification and execution plan |
| 1 — locked isolated topology | Complete | `97a0fca`; pinned upstream Compose overlay, OCI locks, and static isolation tests |
| 2 — ingestion/read-back oracle | Complete | `8f3022d`, `a58c163`; both artifacts, replay, exact aspects, graph edges, and asynchronous-key handling |
| 3 — release-workflow gate | Complete | `e50018b`, `e6fc0dd`, `d84a83e`, `0f1b932`; green run `33798567142`, bounded evidence, and exact teardown |
| 4 — public truth reconciliation | Complete | public specs, guide, CLI, support matrix, release notes, navigation, and roadmap reconciled to run `33798567142` |

Tests must implement the specification rather than establish an alternative
contract. If empirical server behavior conflicts with the specification,
amend the specification explicitly before weakening an assertion.

## Frozen implementation boundary

- exact DataHub server release `v1.7.0`, commit
  `7f81ccbfe27b9acc947f5f600fcf9ddb72138a80`;
- exact official `acryl-datahub==1.7.0` CLI, SDK, and generated aspect classes;
- pinned GMS, SystemUpdate, Kafka, MySQL, and OpenSearch images;
- exactly one wheel handed off from the package job, installed in a clean
  Python 3.10 environment outside the checkout with the DataHub SDK absent;
- a reviewed representative project containing no generated MCP JSON;
- fresh `with-kafka-instance.json` and `without-kafka-instance.json` artifacts,
  each generated twice through the supplied installed `streamt` executable;
- one fresh GMS on a private data network, a loopback-only host listener, and
  fresh volumes per artifact;
- two identical official `datahub ingest mcps` invocations per artifact;
- exact read-back of every streamt-emitted aspect after each ingestion;
- exact direct Dataset `Consumes` and `Produces` relationship sets after each
  ingestion, with bounded asynchronous graph polling;
- test-only ephemeral mutation followed by container/volume destruction; and
- no production streamt code, CLI option, dependency, extra, state, publisher,
  delete, or recovery change.

The gate may prove pinned server acceptance and graph read-back. It must never
be used to claim that production streamt publishes or synchronizes metadata.

## Slice 0 — normative foundation

### Ownership

- `docs/specs/datahub-gms-v170-acceptance.md`
- `docs/plans/2026-09-03-datahub-gms-v170-acceptance.md`

Do not edit production code, tests, workflows, public references, the existing
offline specification, or the roadmap in this slice.

### Work

1. Record the empirical feasibility evidence without promoting it to release
   evidence.
2. Freeze the exact server, SDK, topology, artifact variants, ingestion,
   aspect read-back, graph relationship, replay, timeout, isolation,
   logging, and cleanup boundaries.
3. Separate primary-aspect persistence from asynchronous graph-index evidence.
4. State the narrow claims a future green gate unlocks and retain every
   publication, authentication, lifecycle, and deployment non-claim.
5. Link only immutable official v1.7.0 sources already established by the
   offline contract.

### Gate

```text
mkdocs build --strict
git diff --check -- \
  docs/specs/datahub-gms-v170-acceptance.md \
  docs/plans/2026-09-03-datahub-gms-v170-acceptance.md
```

Slice 0 is complete only after review confirms that no sentence implies a
production network path.

## Slice 1 — locked isolated topology

### Ownership

- the downloaded, checksum-verified upstream v1.7.0 Compose file;
- `tests/integration/datahub/v1.7.0/docker-compose.override.yml`
- `tests/integration/datahub/v1.7.0/images.lock.json`
- focused topology assertions in
  `tests/unit/test_datahub_gms_gate_contract.py`

Do not edit the root `docker-compose.yml`, generic integration helpers,
production packaging, or the offline artifact/oracle scripts.

### Work

1. Reduce the pinned official quickstart dependency closure to exactly Kafka
   KRaft, MySQL, OpenSearch, one-shot SystemUpdate, and GMS. The ingestion
   runner remains in its isolated host environment.
2. Layer a narrow reviewed override on the exact upstream v1.7.0 Compose file,
   retaining the environment needed for internal schema registry,
   MySQL primary storage, OpenSearch graph/search, strict URN validation, and
   the official asynchronous batch-ingestion and read-back surfaces. Do not
   invent a simplified single-container server.
3. Pin every readable image tag to its reviewed OCI index digest and record its
   Linux/amd64 manifest digest, source tag, release commit, and upstream compose
   checksum in `images.lock.json`.
4. Set `platform: linux/amd64`, `pull_policy: never`, telemetry disabled, usage
   aggregation disabled, non-debug logging, bounded Java heaps, container
   memory ceilings, and bounded health checks.
5. Put every service on one `internal: true` data network. Attach only GMS and
   SystemUpdate to the per-project bootstrap bridge; publish only GMS on
   `127.0.0.1`. Define no `container_name`, externally shared network/volume,
   host network, privileged mode, Docker-socket mount, home-directory mount, or
   cloud-directory mount.
6. Make SystemUpdate depend on healthy Kafka, MySQL, and OpenSearch. Make GMS
   depend on successful SystemUpdate. The oracle runs only after GMS health and
   `/config` identity pass.
7. Run the oracle from its isolated host virtual environment and publish only
   GMS on the dedicated `127.0.0.1` variant port. Mount no host path into the
   services.
8. Add static tests that fail if a service, non-loopback port, unexpected
   network or attachment, mutable image, host path, unbounded health check, or
   forbidden environment setting enters the topology.

### Initial reviewed image locks

```text
acryldata/datahub-gms:v1.7.0@sha256:54bc4431402846a72d1c1bdb69fae1148f74a59425144aa947fdf1c3506461f7
acryldata/datahub-upgrade:v1.7.0@sha256:21e77ad964be64b2b5a7f74c9685897ed79e8854995242eaa5e5c426395b88c0
confluentinc/cp-kafka:8.2.2@sha256:8e01c0305844d6c05bfb8e86479f5f363bb6a53497625395943a9da780de67ce
mysql:8.2@sha256:212fe73edca5df6ff14826d5eb975c914bfb91f82a2e923f9050568f99525da1
opensearchproject/opensearch:2.19.3@sha256:e96cc6ae1500a073d973c0906f30f7cf4d9c461f32f855f9242a2da933660cdd
```

The implementation re-resolves and reviews these before committing the lock;
it must not silently substitute a current quickstart or floating tag.

### Gate

```text
pytest -q tests/unit/test_datahub_gms_gate_contract.py -k topology
git diff --check -- tests/integration/datahub/v1.7.0 \
  tests/unit/test_datahub_gms_gate_contract.py
```

No service is started in the ordinary unit-test gate.

## Slice 2 — ingestion and read-back oracle

### Ownership

- `tests/integration/datahub/gms_v170_gate.py`
- `tests/integration/datahub/v1.7.0/stream_project.yml`
- focused executable/static assertions in
  `tests/unit/test_datahub_gms_gate_contract.py`

Both gate scripts are standalone and are not named `test_*.py`, so ordinary
unit collection neither imports streamt from an uninstalled checkout nor
requires the DataHub SDK. DataHub imports remain below environment sanitization
and exact version checks.

### Work

1. Make the workflow accept exactly one wheel and install it outside the
   checkout. Reject missing, multiple, editable, checkout-resolving, or
   unexpected inputs.
2. In that clean Python 3.10 environment, install the wheel,
   run `pip check`, prove the DataHub SDK is absent, and prove both the imported
   streamt module and executable resolve from that wheel environment.
3. Invoke the installed executable twice per variant with exact arguments.
   Require zero exit, canonical output, expected logical contract, and byte
   equality between repetitions. The package job remains responsible for the
   exact warning and source-baseline contract.
4. Accept only the reviewed fixture, supplied installed executables, exact
   variant, and an internal/loopback GMS URL. Reject unknown arguments,
   schemes, hosts, profiles, and tokens.
5. Remove inherited `DATAHUB_*`, proxy, Python-path, and user-site variables;
   set only reviewed local GMS, telemetry-off, casing, and temporary-home
   values before importing the official package.
6. Require installed distribution version `1.7.0`, successful `/health`, and
   exact `/config` version plus full commit. Treat missing or differently
   shaped version evidence as failure.
7. Read strict UTF-8 canonical JSON with duplicate-key rejection. Require the
   exact proposal count and revalidate every proposal with the existing deep-
   copy wrapper convention before any mutation.
8. Compute expected aspect values and direct `Consumes`/`Produces` Dataset
   relationships from the immutable, deeply validated artifact.
9. Invoke exact official `datahub ingest mcps` as a bounded child process using
   only the reviewed internal or loopback GMS address. Require exit zero,
   exactly N written, zero warnings, and zero failures from the official report.
   Do not accept a generic success string.
10. Query REST.li `entitiesV2` with a percent-encoded entity URN and the exact
    emitted aspect names. Permit only the optional server-owned key aspect in
    addition to the requested aspects and require exact JSON-value equality.
    Do not fall back to the
    v1.7.0 OpenAPI latest-entity route that fails on `SystemMetadata.__type`.
11. Query outgoing `/relationships` separately with exact `Consumes` and
    `Produces` `types` filters. Derive destinations from
    `relationships[*].entity`; require exact direct Dataset sets, the sink's
    empty `Produces`, and no unexpected Dataset edge. Poll graph state for at
    most 90 seconds; primary reads have a 30-second deadline.
12. Repeat the identical ingestion without changing bytes, repeat report,
   aspect, and relationship assertions, and require the canonical result
   summary to remain unchanged apart from an explicit attempt number.
13. Never call delete, soft-delete, patch, rollback, restore-index,
    reconciliation, stateful-ingestion, or platform-creation APIs.
14. Bound all child-process and HTTP output, redact unexpected exceptions, and
    emit one canonical evidence summary without response bodies, profiles,
    paths, environment values, or credentials.

### Failure coverage

Focused tests must prove fail-closed behavior for:

- a missing/multiple wheel, failed install or `pip check`, editable or checkout
  import, wrong executable, or an unexpectedly present DataHub SDK;
- installed-executable failure, non-canonical/nondeterministic output, or
  logical-contract mismatch;
- wrong SDK/server version or commit;
- fallback to an unapproved entity endpoint or malformed `entitiesV2` aspect
  envelope, including the observed OpenAPI `SystemMetadata.__type` failure;
- non-internal/non-loopback URL, token, or inherited profile;
- malformed/canonicality-breaking artifact, wrong count, duplicate proposal,
  or mutated bytes;
- partial ingest, warning, failure, timeout, or false success text;
- missing/wrong/extra aspect fields, including the `dataJobInfo.type`
  union and all four lineage arrays;
- a missing/wrong `types` filter or `relationships[*].entity`, and any missing,
  duplicate, transitive, self, stale, or extra Dataset relationship;
- a sink `Produces` edge;
- unbounded retry or subprocess execution; and
- secret, temporary path, raw server response, or environment leakage in
  stdout, stderr, exceptions, and evidence.

### Local executable gate

```text
python tests/integration/datahub/gms_v170_gate.py \
  --streamt-executable <INSTALLED-STREAMT> \
  --datahub-executable <EXACT-V1.7.0-DATAHUB> \
  --variant <with-kafka-instance-or-without-kafka-instance> \
  --gms-url http://127.0.0.1:<EPHEMERAL-PORT> \
```

That loopback form is for an explicitly disposable local or CI stack only.

### Gate

```text
pytest -q tests/unit/test_datahub_gms_gate_contract.py
ruff check tests/integration/datahub/gms_v170_gate.py \
  tests/unit/test_datahub_gms_gate_contract.py
```

## Slice 3 — release-workflow real-GMS gate

### Ownership

- `.github/workflows/ci.yml`
- CI-specific assertions in `tests/unit/test_datahub_gms_gate_contract.py`

Do not add the SDK to the normal unit, package, installed-wheel, or production
environments. This job MUST generate its GMS inputs through the installed
executable; it MUST NOT use checked-in or previously uploaded generated JSON.

### Job architecture

Add one `datahub-gms-v170` job after the full
`datahub-release-oracle` matrix:

```text
package
  -> DataHub Python 3.10-3.12 installed-wheel/offline oracle
  -> DataHub v1.7.0 real-GMS acceptance
```

The job uses one Linux/amd64 `ubuntu-24.04` runner, explicit read-only contents
permission, and a 45-minute timeout. It downloads the built wheel from the
package handoff; the prerequisite Python 3.10-3.12 offline matrix separately
owns source/wheel parity. The real-GMS job generates new artifacts from the
wheel as its only GMS inputs and never mounts the streamt source tree into a
service container.

### Work

1. Check out the reviewed gate, input project, and Compose overlay. Download
   exactly one built wheel, require its exact filename and nonempty bytes,
   reject extras, and record its initial SHA-256 value.
2. Preflight Docker Compose v2, Linux/amd64, free disk, and memory before image
   pulls. Fail with bounded capacity diagnostics rather than starting a stack
   that cannot complete.
3. In one clean Python 3.10 virtual environment outside the checkout, install
   the wheel, run `pip check`, and prove the DataHub SDK is absent. In a second
   isolated environment, install exact `acryl-datahub==1.7.0`. The host oracle
   invokes the installed streamt executable twice for each variant, requires
   exact contract and canonical determinism, makes each fresh output read only,
   and records its SHA-256 value before ingestion.
4. Pull every exact digest during a bounded bootstrap step. Once pulled, the
   topology's `pull_policy: never` prevents runtime registry access.
5. Create a mode-0600 temporary environment file containing fresh disposable
   token-service key/salt and database password values. Register log masks,
   never print values, and never pass repository or environment secrets.
6. For each freshly generated artifact, use a unique project name containing
   run, attempt, and variant. Start a fresh stack and volumes, enforce service
   dependency/health transitions, run the oracle twice as specified, collect
   evidence, then tear down fully before starting the other variant.
7. Run every service on the internal data network. Attach only GMS and
   SystemUpdate to the per-project bootstrap bridge, and publish only GMS on
   the dedicated loopback variant port for the host-side oracle.
8. On both success and failure, collect tail-bounded color-free service logs,
   Compose status, exact image references, only container `State`/`Health`,
   artifact hashes, and the sanitized oracle summary.
9. Never collect rendered Compose configuration or full container inspection
   because they include environment values.
10. Upload one bounded evidence bundle containing per-variant directories with
    14-day retention.
11. In an unconditional final step, run exact-project `down --volumes
    --remove-orphans` and assert no project-labeled container, network, or
    volume remains. Preserve the test failure if cleanup also fails.

### Time budgets

| Operation | Maximum |
| --- | ---: |
| all image pulls | 8 minutes |
| one stack startup including SystemUpdate | 8 minutes |
| GMS health after startup | 180 seconds |
| one `datahub ingest mcps` invocation | 120 seconds |
| one primary aspect read-back phase | 30 seconds |
| one graph relationship phase | 90 seconds |
| evidence collection | 2 minutes |
| one teardown | 2 minutes |
| complete job | 45 minutes |

Polling intervals are two seconds. Authentication, validation, and other 4xx
responses fail immediately. Only bounded connection and 5xx startup/indexing
races retry.

### CI contract tests

Static tests parse the workflow with a YAML loader that preserves the `on`
key and require:

- dependency on the complete offline DataHub oracle;
- exact single-wheel download;
- installed-wheel execution outside the checkout with the DataHub SDK absent;
- fresh generation of both variants, with checked-in or previously uploaded
  generated JSON prohibited as the GMS input;
- job and step timeouts;
- unique project names and per-variant volumes;
- digest pull before `pull_policy: never` runtime;
- no workflow secrets or public GMS URL;
- `if: always()` evidence, upload, and cleanup steps;
- bounded log collection without full inspect/config; and
- exact post-cleanup residue assertions.

### Gate

```text
actionlint .github/workflows/ci.yml
pytest -q tests/unit/test_datahub_gms_gate_contract.py
```

The first implementation run must retain full bounded diagnostics. A retry is
not acceptance evidence unless it starts from new project names and new
volumes; do not hide a deterministic first-attempt failure with an Actions job
retry.

## Slice 4 — public truth reconciliation

Starts only after Slices 1-3 pass together with the existing package and
Python 3.10-3.12 offline gates in the complete CI workflow.

### Ownership

- `docs/specs/datahub-gms-v170-acceptance.md`
- `docs/plans/2026-09-03-datahub-gms-v170-acceptance.md`
- narrowly scoped status links in `docs/specs/datahub-catalog-export.md` and
  `docs/plans/2026-09-03-datahub-catalog-export.md`
- `docs/reference/datahub-catalog.md`
- `docs/reference/support-matrix.md`
- `docs/reference/release-notes.md`
- `ROADMAP.md`

### Work

1. Change this specification from proposed to supported only for the exact
   test-only v1.7.0 quickstart conformance boundary.
2. Record the landed commit and green CI run for every implementation slice.
3. Add the narrow claims: exact artifact acceptance/persistence, exact
   read-back of the five emitted aspects, both Kafka identity variants,
   explicit Gateway identity, and exact tested graph Dataset lineage.
4. Keep the support-matrix `Observe` and `Direct plan/apply` values at `No`.
5. Preserve every statement that production `streamt docs datahub` has no URL,
   token, SDK, GMS call, publisher, read, synchronization, state, delete, or
   recovery behavior.
6. Keep authentication/TLS/cloud/HA, platform bootstrap, lifecycle semantics,
   ownership/tags/native contracts, schema/field lineage, and deployed-runtime
   behavior unsupported.

### Gate

```text
pytest -q tests/unit/test_docs_datahub_example.py \
  tests/unit/test_doc_yaml_validation.py
mkdocs build --strict
git diff --check
```

## Merge order and ownership

```text
Slice 0 specification/plan
  -> Slice 1 locked topology
  -> Slice 2 oracle and failure contracts
  -> Slice 3 CI execution
  -> Slice 4 public claims
```

Slices 1 and 2 may be developed in parallel after Slice 0, but neither lands
without their shared static contract tests. Slice 3 consumes the committed
built-wheel handoff and follows the complete offline oracle matrix, never a
shared dirty tree. Slice 4 reports only a landed green workflow.

Agents do not modify, stage, or depend on user-owned untracked plans, prompts,
or lockfiles. Each implementation slice has exclusive file ownership before
parallel work starts.

## Claims unlocked by completion

After every slice passes, public documentation may state that the shipped
streamt executable's exact supported DataHub v1.7.0 metadata files have passed
a pinned, test-only, real-GMS acceptance and read-back gate. It may state that
the five emitted aspect types persist exactly and the tested direct Dataset
lineage is visible through the pinned graph relationship surface.

Completion does not turn the offline command into a publisher. Production
publication requires a later specification for endpoint and credential input,
authentication/TLS, review and authorization, batching/partial failure,
idempotency and conflict semantics, remote reads, reconciliation/state,
delete/rollback, recovery, observability, and compatibility beyond the exact
quickstart topology.

## Known implementation risks

- Cold Docker pulls are large and may encounter registry rate limits.
- The stack requires materially more memory and disk than unit/package jobs.
- GMS graph indexing is asynchronous and may expose a deterministic incompatibility
  with the preferred `inputDatasetEdges`/`outputDatasetEdges` arrays.
- Server response defaults or union decoding may differ from the exact
  aspect expected by the offline SDK oracle.
- The custom Gateway platform URN may be accepted while the absent
  DataPlatform entity remains unsuitable for UI presentation.
- The official ingestion report needs a stable machine-readable extraction;
  success-looking text alone is not sufficient.

If aspects persist exactly but graph relationships do not converge, keep the
live-lineage claim unsupported, record the server evidence, and amend the
specification before deciding whether the artifact contract itself should
change. Never populate deprecated arrays or invent platform entities only to
make this gate pass.
