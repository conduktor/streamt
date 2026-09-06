# Resume handoff: public runner workflow and declared Git impact

## Pause and reading order

Recorded on 2026-09-06 at the owner's request. Implementation is paused.
This document preserves the work for a later session; it does not authorize
background implementation or new infrastructure acceptance work. The owner then
explicitly requested a commit and push of the existing work. Saving and validating
this checkpoint does not resume development; wait for a new request before
implementing the remaining tasks.

On resumption, read this document, the repository's `ROADMAP.md`, the
[product direction](../specs/product-direction.md), and the
[declared Git impact specification](../specs/declared-git-impact.md).
The [topology/runtime plan](2026-09-05-topology-runtime-execution.md) retains
historical milestones. This handoff takes precedence over its older status text.

## Product decisions to preserve

Streamt must absorb an existing streaming topology or start a new one, then
maintain coherence through development and infrastructure changes. The project
ties SQL, resource identities, contracts and application dependencies together.
The Kafka Streams runner is one execution path within that workflow.

- SQL is the first authoring path. Custom streaming applications also belong
  in the topology through declared inputs, outputs, owners and repositories.
  Declaring an application does not deploy its code or infer its implementation.
- Imported resources start external. Ordinary planning does not reconcile or
  mutate their configuration. Explicit observation and identity/offset checks
  needed to deploy a managed consumer remain separate.
- Adding a managed declaration does not silently adopt an existing resource.
  Ownership transfer still needs the existing explicit, evidenced workflow.
- Keep both entry paths: import an existing source and create a fresh topology.
  Both must reach actual output, a supported update and an unchanged repeat.
- The bounded fixed Kafka Streams runner uses local Docker. It is not a general
  SQL server, a scheduler or a replacement for Flink's broader SQL capabilities.
- No new Strimzi work, catalogs, cloud backends, hosted UI, stateful processing,
  custom-image/JAR deployment or source-build system in this cycle.
  Preserve existing integrations and their tested boundaries.
- No release tags, package/image publication or production access. No silent
  offset reset, forced cleanup, data-loss workaround or exactly-once/zero-downtime
  lifecycle claim.

Earlier permission allowed logical commits on main, ordinary pushes and bounded
subagent work. Implementation permission applies only when the owner resumes
that work, within these boundaries. The later commit/push request is limited
to saving this checkpoint, not starting the next lot.

## Exact repository state at the pause

Before the checkpoint, branch `main` and `origin/main` were at `0692058`
(`feat: coordinate reviewed runner replacement and durable completion`).
That baseline passed 26 CI jobs in run `34001812014`; documentation run
`34001811996` also passed. These results do not cover this newer checkpoint.

This checkpoint saves the public CLI implementation and this handoff at the
owner's request, without declaring the acceptance lot complete. Look up its exact
commit and CI status in Git history on resumption. Do not reset, clean or blindly
stage the tree. In particular, preserve
the unrelated user files `docs/plans/2026-03-15-compile-time-reference-validation.md`,
`prompts/` and `uv.lock`.

The pre-checkpoint CI draft copied and invoked
`tests/package/kafka_streams_postgres_cli_probe.py`, which does not yet exist.
That copy and step were removed before saving so CI does not call an absent
script. The existing backend PG receipt tests and new local public CLI tests
remain enabled. Implement and verify the separate coupled acceptance before
adding its CI step; do not invent a passing artifact.

The public starter and support documentation describe implemented checkpoint
behavior. They are not evidence of a released package or exact-final-commit CI.

## Implemented checkpoint changes

| Area | Files under `src/streamt/` | Behavior to retain |
| --- | --- | --- |
| Public reviewed planning | `deployer/planner.py`, `cli/commands/plan.py` | Explicitly prepare one predicate-only replacement from the exact last online full-project observation; save format 6 |
| Dedicated apply | `cli/commands/apply.py` | Keep original reviewed tuple, environment policy and state authority; enter the coordinator before generic apply/rollback/finalization |
| Diagnosis and continuation | `cli/commands/runner_state.py`, `state_cmd.py`, `deployer/kafka_streams_replacement_coordinator.py` | Original plan plus operation UUID; inspect under lock; resume the same operation; verify completed receipts without redeployment |
| Durable completion lookup | `deployer/state_backend.py`, `postgres_state_backend.py` | Read-only exact receipt/history lookup and terminal pending-snapshot gate; local postimage retry and atomic PostgreSQL completion remain distinct |
| Onboarding | `cli/commands/init.py`, `templates/kafka_streams/` | Explain predicate updates and resume; JSON support label is now `create_noop_predicate_update` |

New tests include `test_kafka_streams_public_planning.py`,
`test_kafka_streams_public_apply.py`, `test_cli_runner_state.py`,
`test_kafka_streams_completion_receipt.py`,
`tests/postgres/test_kafka_streams_receipt_real.py`, and
`tests/package/test_kafka_streams_public_cli_harness.py`.
The new local public acceptance driver is
`tests/package/kafka_streams_public_cli_probe.py`.

The scope is one predicate-only runner update, with no other provider mutations
in the same full-project apply. Projection, schema, image, identity and stateful
changes remain blocked. Direct apply and force flags cannot bypass the contract.

Read-only status cannot clear old-removed/new-absent state. Resume must preserve
the existing application ID, volume and offsets, and reuse only an exactly
verified candidate. An unacknowledged write reports `committed: null`; it must
not invent a failure or success. A completed retry validates the exact receipt,
current project, ownership and ready candidate, with no second serial increment.
This is not a general historical receipt viewer after later project changes.

## Verification already observed

Keep the distinction between a successful run, its source cohort, and a final
acceptance artifact.

- Before saving this checkpoint, the final Python 3.12 unit/scenario suite plus
  the two package-accounting tests passed 7,663 tests with 32 skips. Ruff,
  mypy (119 modules), strict documentation and actionlint also passed.
  The missing coupled-test CI step was removed, not counted as a passing gate.

- Full unit/scenario suites passed 7,661 tests with 32 skips on Python 3.10
  and 3.12. These runs started before the final JSON onboarding label edit.
  Re-run the final cohort before committing; do not infer exact-final coverage.
- After that label edit, 26 focused onboarding/acceptance-accounting tests passed;
  one installed-distribution test skipped without its distribution fixture.
  The two context-exit accounting tests also passed separately.
- Ruff and mypy passed for 119 source modules. Strict MkDocs and actionlint passed
  before the final handoff edits. Actionlint does not detect a missing copied script.
- The storage worker reported 83 real tests each on PostgreSQL 14.23 and 18.4,
  without skips: 19 receipt/gate cases plus 64 previous completion, resume,
  recovery and mutation cases. They used real writer locks and read-only DML
  guards. This is backend acceptance, not coupled Kafka/PostgreSQL CLI acceptance.
- Source local public run `e20bb4c3b647` has `accepted: true`, 13 distinct
  worker processes, three cycles, unchanged source hashes during the run and
  complete TERM-only cleanup. It includes the final init JSON label and helper.
- Earlier source run `78f6f4848f98` and installed run `c6071d4f18fe`
  passed the public local cycle before that JSON label change. The installed
  run used Kafka client 2.15.0; source used 2.13.2.
- Each local public acceptance covers existing-source import, fresh init and
  direct successful replacement. Interrupted paths lose a real Docker-create
  acknowledgement; one also loses the response after actual control clear.
  Fresh processes diagnose, resume or verify completion. Exact output and
  offsets 5 to 8, one candidate, retained incident history and final public
  no-op behavior are checked.
- These are controlled process exits, not SIGKILL tests. They use plaintext,
  one broker/partition and local state. TLS/SASL, multipartition lifecycle,
  transactional crash recovery and HA are not established.

The latest installed proof is not the final working-tree package: its init
module predates the JSON label correction and its wheel metadata predates the
root README update. Build a fresh sdist/wheel and repeat installed acceptance.
No curated public-CLI verification JSON has yet been added to the repository.

Local evidence, if still present:

- Root: `/tmp/streamt-kstreams-public-cli.SJr3IF`.
- Final source: `json-support-final-source-evidence/streamt-journey-source-e20bb4c3b647-_l9mdg9h/evidence.json`.
- Earlier installed: `final-installed-evidence/streamt-journey-installed-c6071d4f18fe-0crus2ko/evidence.json`.
- Earlier package parity: `final-parity.json` and `final-dist/`.
  Its 119-module tree checksum is `09cc1c96309f28c541d30242718b0411ca64fb43f980953dd4fa40c74434016b`;
  it is historical, not the final source checksum.

Temporary evidence is a recovery aid, not a durable repository artifact. If it
has disappeared, rerun the acceptance after authorization. Never reconstruct
a passing record from this prose.

Final checked file hashes:

```text
init.py: 9d1875937a24e5c95cd5db387c15ec334925e706e39f5d924804c22b96b2aac9
kafka_streams_journey.py: dc65f27456f94d083b833982e810f755976c97ffefa491bbd6de23c4a43e2b89
kafka_streams_public_cli_probe.py: 93baf5c01544fa13aeb7104bea64c7cc802c9eaaf3a485cb7d2e0fb5f80d1f9e
README.md: 7b04d9b32cb96d40581793f0f34c2e278dd141e50d6ab4fe2c96821c9cbd650a
```

No disposable test containers were running when this handoff was inspected.
The existing `streamt-*` development services were left running and untouched.
Do not treat their names as authorization to clean them up. Recheck actual
process/resource state at resumption; old agent/session IDs may no longer exist.

## First task after the owner resumes: finish this lot

1. Inspect `git status -sb` and targeted diffs against this handoff. Preserve
   unrelated edits; check whether the owner has changed the product direction.
2. Build a fresh sdist, build the wheel from it, and install outside the checkout.
   Prove source/sdist/wheel/installation parity for all Python modules, packaged
   templates and runner assets. Re-record hashes after any actual source change.
3. Repeat the public local CLI acceptance on the final installed package, using
   `python -I` outside the checkout. Its copied dependencies are the public
   driver, `kafka_streams_journey.py`, and
   `kafka_streams_replacement_executor_probe.py`.
   Keep source and installed evidence distinct; rerun source if its code changes.
4. Implement the separate coupled Kafka/PostgreSQL public acceptance described
   below. Do not replace it with mocked providers or the backend-only PG tests.
5. Add curated evidence under `tests/package/verification/`, with exact input
   hashes, versions, results, failure injection and cleanup scope.
   Preserve the six historical Kafka Streams proof JSON files unchanged.
   Journey's init support assertion changed; historical helper hashes remain historical.
6. Add the coupled test's CI step and script copy only after implementation.
   Install the PostgreSQL extra in that lane. Verify both installed Kafka client
   lanes (2.13.2 and 2.15.0), real PostgreSQL receipt tests and the coupled test.
   Keep the `reviewed-*.json` evidence upload and fixture-only accounting tests.
7. Finish documentation review: distinguish ordinary savable blocked plans from
   invalid runner preparation, which fails without saving a format-6 file.
   The generic recovery guidance has already been scoped away from runner resume.
8. Re-run unit/scenario/package-accounting tests, Ruff, mypy, strict docs,
   actionlint and diff checks. Re-run the relevant real PG cohorts if storage
   changes. Verify ordinary creation, external resources and existing removal
   workflows have not regressed.
9. Review the exact staged completion chunk, commit it on main,
   push normally, and inspect CI for that exact SHA. Do not publish a release.
   Record the outcome before starting the Git implementation.

Suggested local checks after authorization:

```bash
.venv/bin/pytest -q --tb=short tests/unit tests/scenarios tests/package/test_kafka_streams_public_cli_harness.py
.venv/bin/ruff check src tests
.venv/bin/mypy src/streamt
.venv/bin/mkdocs build --strict
actionlint .github/workflows/ci.yml
git diff --check
```

The prior Python 3.10 environment was
`/tmp/streamt-python310-time.qnteru/venv/bin/python`; recreate an isolated
environment if it is absent. Do not assume temporary tools or Docker images survive.

## Coupled Kafka/PostgreSQL acceptance still to implement

Proposed file: `tests/package/kafka_streams_postgres_cli_probe.py`.

Proposed options: `--checkout`, `--mode source|installed`, `--image`,
`--evidence-dir`, and required `--postgres-image`. Dependencies are
`streamt[postgres]`, the local public driver and its two helpers. Use immutable
locally available images, with no implicit pull in the test itself.

- Create a uniquely named disposable standalone PostgreSQL fixture with exact
  resource ownership. Use separate fixture administrator/owner and runtime writer
  roles. Never reuse an existing project's database or credentials.
- Generate a fresh Kafka Streams project. Initialize and migrate its state store
  through the actual public `state init` and `state migrate-postgres-v2`
  commands; deploy through the actual public reviewed create path.
- Edit the SQL, save public format 6, apply with a lost response after real Docker
  create, then exit. A fresh process must reacquire the real PostgreSQL session
  lock, diagnose without DML, resume the sole candidate and finalize atomically.
- Verify a new status and completed-resume retry without provider or audit writes.
  Prove offsets/output, retained incident/resume rows, one serial increment,
  final public no-op and no cross-project mutations.
- Keep this first coupled proof fresh-topology-only. Local public acceptance
  covers both entry paths. Do not claim coupled import acceptance or an injected
  PostgreSQL COMMIT loss unless separately executed. Backend tests already cover
  uncertain PostgreSQL commits, but are a different proof.
- Clean up only exact owned fixture resources. Preserve raw diagnostic files
  privately, and curate only secret-neutral evidence.

The removed draft CI step selected PostgreSQL 18.4's multiarchitecture image;
retain this pin when implementing the coupled test:
`postgres@sha256:96d56f7f57c6aacd1fcb908bc83b345ec5f83231ee486dd66a1baadce274db88`.
Kafka fixture image:
`apache/kafka@sha256:77e3df9054047a88b520d0cc46e16696d3b22022e1d580aeccd2632df6532837`.
The local runner used
`sha256:889f6ff67d5435e735eff85508f6f39ba73e192a76b1e2eb24e8c83c8db03df9`;
build from packaged assets if unavailable. Its 14-asset build context was
`sha256:46831266d360d989b97ce6502c845a6fca6a5eaf1cbd57688ee6a1bd0655ee18`.

## Subsequent lot: declared Git comparison and downstream impact

No Git comparison implementation has started. Follow the
[technical specification and ordered tasks](../specs/declared-git-impact.md)
only after the public workflow lot is verified and the owner has resumed work.

The order is: safe Git reader, pure declared snapshots, comparison and downstream
impact, CLI, then shared pull-request rendering and installed acceptance.
The report must explain effects on custom applications even when the output
topic configuration and declared schema do not change.

For future delegation, assign separate ownership of the Git reader, pure
comparison/graph tests, and installed/security acceptance once the snapshot
contract is agreed. The primary agent owns shared CLI integration, review and
commits. Do not depend on the previous agents remaining available, and do not
let multiple workers edit the same shared modules concurrently.

After this lot, revisit onboarding against the two reference paths. A sink
integration is a candidate only if the demonstrated source-to-destination route
needs it. Extra catalogs, clouds and runtime expansion are not an automatic queue.
