# Developer experience execution plan

## Status and objective

Planning on 2026-09-04. This is the active implementation plan after the
stabilization program. It supersedes the release-first and review-first work
order, while retaining their safety contracts and completed work.

Implement the [developer workflow](../specs/developer-workflow.md): describe an
existing streaming system, add managed processing, inspect results, and deploy
a supported change. The [product direction](../specs/product-direction.md)
includes custom applications as well as SQL models.

This planning turn changes documentation only. The multi-hour implementation
run has not started. Confirm the material decisions below before launching
runtime or lifecycle implementation. Unanswered questions are not approvals.

## Confirmed product decisions

- The product simplifies application creation, deployment, and updates. Review,
  contracts, lineage, and catalogs support that workflow.
- SQL is the first authoring path. Custom streaming applications must remain
  represented in the domain; the product is not limited to SQL data engineering.
- Existing systems can be imported as external declarations. New resources
  defined in streamt can be managed. Import does not implicitly transfer ownership.
- External declarations do not authorize mutation or drift correction. The
  requested default also excludes automatic external drift checks; clarify the
  boundary with local validation and explicit live inspection below.
- Address users who have Kafka but no Flink. Kafka Streams is a candidate for
  execution; neither that backend nor a custom SQL engine exists today.
- Strimzi is not a priority. Preserve its tested export without expansion.
- Existing permission allows logical commits on `main`; do not include unrelated
  user changes, rewrite history, or turn a normal push into a release operation.

## Decisions still required

| ID | Decision | Proposed starting scope | Status |
| --- | --- | --- | --- |
| D1 | Investment in execution without Flink | A bounded Kafka Streams proof, compared with an existing SQL runtime; no commitment to maintain a general SQL engine | Awaiting user choice |
| D2 | Custom application depth | Describe inputs/outputs and ownership now; defer deployment of images/JARs and source builds | Awaiting user choice |
| D3 | External observation | Local references validated; no automatic drift checks; live inspection only on explicit request | Awaiting confirmation of this distinction |
| D4 | First update boundary | A real filter/projection change with offset and recovery semantics; stateful aggregation upgrades remain separately gated | Awaiting user choice |
| D5 | Reference application | Orders, Kafka input/output first; optional Connect sink after the core loop | Proposed default, no external-service dependency |

The earlier Flink-to-PostgreSQL default and Kubernetes-vs-direct question are
superseded by the Kafka-without-Flink requirement and the exclusion of Strimzi
work. Do not continue with those earlier assumptions.

## Baseline evidence

Observed in the current checkout during the product discussion:

- `streamt init` creates a `SELECT *` model; its own `validate --strict` exits 1.
- The generated project compiles a Flink processor without declaring a Flink
  runtime. Offline compilation does not show that it can execute.
- The quickstart starts nine Docker services and does not seed example events.
- Source discovery/import and producer/consumer exposure fields already exist.
  Extend them where appropriate instead of creating duplicate abstractions.
- The GitHub Action defaults to offline planning; it does not compare Git base
  and head. Offline planning assumes resources are absent and cannot authorize apply.
- Existing Flink updates and resubmissions are blocked pending lifecycle evidence.
- DataHub and Strimzi have real-target acceptance tests for their narrow exports.
  Their completion does not prove the application development journey.

Reproduce relevant observations before changing code. Preserve the untracked
`docs/plans/2026-03-15-compile-time-reference-validation.md`, `prompts/`, and
`uv.lock`; inspect overlapping user work rather than assuming it belongs to this
plan. Do not rename the project or package.

## Work order and dependencies

### M0: freeze the working contract

- [ ] Record D1-D4 answers and any change to D5 in this file.
- [ ] Confirm the first execution target and update guarantee without expanding
      the support matrix ahead of tests.
- [ ] Read repository instructions and inspect git/Docker state before execution.

Exit: a future agent can identify its authorized deliverable and stop conditions
without inferring answers from earlier proposals.

### M1: model external and managed applications

Depends on M0, especially D2-D3.

- [ ] Audit discovery/import, exposure graph behavior, validation, planning,
      status, and adoption for hidden provider calls and incomplete identities.
- [ ] Specify the declaration-only external path and explicit observation entry
      points; retain every safety check needed for managed operations.
- [ ] Represent a custom producer/processor/consumer with its declared graph
      relationships; reuse the existing source/exposure model where it fits.
- [ ] Add import provenance and completeness reporting for the chosen resource
      kinds. Do not infer unavailable SQL or code from runtime names.
- [ ] Prove no-clobber repeat import, local validation without provider access,
      zero external mutations, and explicit adoption boundaries.

Exit: an imported external application and a managed model coexist in one tested
project. External changes are outside ordinary reconciliation; required missing
evidence produces an actionable message instead of implicit ownership.

### M2: choose and prove execution without Flink

Depends on M0/D1. Architecture research can run alongside M1; public schema
changes wait for the primary agent to review both contracts together.

- [ ] Record an architecture decision comparing a generated Kafka Streams app,
      a fixed runner that interprets a versioned plan, and an existing SQL runtime.
      Include deployment, distribution, maintenance, and license prerequisites.
- [ ] Name the chosen SQL subset and rejected constructs. Start with one source,
      projection, filter, and one output if D1 selects the bounded proof.
- [ ] Define types, nulls, record keys, serialization, error handling, application
      identity, offsets, processing guarantees, and runner startup/shutdown.
- [ ] Keep Flink compilation intact. Choose the executor explicitly and fail
      before mutation when its capabilities or configuration are insufficient.
- [ ] Execute deterministic records against real Kafka and the selected runner;
      test restart and unsupported SQL. Record actual output, not only compilation.

Exit for the bounded-proof option: a reproducible result and a recommendation
with specific limits. Stop for the decision to productize; do not expand the
prototype into an unbounded engine or publish it as supported.

### M3: finish the first creation journey

Depends on M1 and a productized execution route approved after M2. Scaffold
repairs and tests independent of that choice can be prepared after M0.

- [ ] Fix strict scaffold validation and missing-runtime guidance.
- [ ] Ship a complete, versioned example with installed-package access to assets.
- [ ] Start only the selected runtime's necessary services with bounded readiness
      checks; make extra UIs, catalogs, and connector services optional.
- [ ] Seed demo-owned input, run the managed transformation, and show expected
      output by record identity. Keep fixture setup separate from source ownership.
- [ ] Document both starting paths: import existing resources and add a managed
      application. Include prerequisites, errors, and scoped cleanup.

Exit: a fresh-directory walkthrough succeeds from an installed wheel, with no
provider-console workaround and no edits to generated deployment artifacts.

### M4: implement the selected update lifecycle

Depends on M3 and D4. Design review can start earlier; no new update path may
bypass the existing blockers while its contract is incomplete.

- [ ] Specify stable job/application identity, source progress, state, generated
      topics, delivery behavior, stop/start order, verification, and recovery.
- [ ] Keep the transition bound to a reviewed plan and durable operation state.
- [ ] Test a supported behavior change, no-op repeat, interruption, and an
      unsupported transition against the real selected runtime.
- [ ] Show changed output on the same identities. Never satisfy this gate by
      deleting state, renaming the job, resetting offsets, or restarting a blank demo.

Exit: the documented create/change/redeploy loop is real for the chosen scope.
If its safety cannot be proved, keep the blocker and report the missing evidence;
do not mark the cycle complete after only the creation milestone.

### M5: repeat the workflow in Git and CI

Depends on M4 for full acceptance. Static comparison work can begin after M1
when its project semantics are stable, if it does not delay runtime delivery.

- [ ] Add a declared base/head comparison and readable dependency/change report;
      preserve the difference from a live reviewed deployment plan.
- [ ] Extend the existing GitHub Action rather than introducing a second bot.
- [ ] Reuse the walkthrough in CI and a separate environment with explicit state
      isolation. Preserve protected/shared-environment remote-state requirements.
- [ ] Add a Connect sink only if the selected demonstration or user need requires it.
- [ ] Update the quickstart, README, support matrix, and release notes from the
      observed behavior. Record first-output/update timings and intervention points.

Exit: an independent reader can repeat the installed-package workflow and explain
what is external, what is managed, and which update guarantees were actually tested.

## Delegation plan for the execution run

Do not launch agents to make unresolved product decisions. Once M0 is complete,
delegate bounded tasks while the primary agent integrates and reviews:

| Assignment | Responsibility | Boundary |
| --- | --- | --- |
| Application model | M1 audit, then assigned import/exposure tests and implementation | No runtime or lifecycle edits |
| Runtime proof | M2 decision evidence and the selected isolated prototype | No production backend activation or unapproved SQL expansion |
| Journey verification | Reproduce the installed-package path, prepare fixtures, review docs | No shared Docker cleanup or changes to ownership rules |
| Primary agent | Resolve shared interfaces, integrate changes, implement/review lifecycle, own final checks and commits | One owner for core schema, planner, and CI edits |

Assign non-overlapping files per task. Workers report evidence and diffs; the
primary agent owns commits. Reassign workers to independent acceptance review
after implementation rather than having several agents edit the planner together.

## Autonomy contract

After the user confirms the execution scope, proceed through the selected
milestones without asking about routine implementation details. Use existing
helpers and tests before adding abstractions. Work on disposable local resources
with unique names; verify exact targets before any scoped cleanup.

Commit each logical, tested change on `main`, staging explicit task paths only.
Keep a short continuation entry here with completed checks, actual blockers,
and the next bounded task. Ordinary pushes must not force-update history or
create release tags. Package publication, production changes, paid resources,
new account permissions, and external catalog writes need separate authority.

Stop and request a decision when:

- A prototype would become a maintained runtime, add a daemon/control plane, or
  expand SQL beyond the selected subset.
- Managed custom applications require an unchosen scheduler or build system.
- Runtime safety requires state/offset loss, weakens ownership checks, or cannot
  meet the selected update guarantee.
- Work needs credentials, paid infrastructure, production access, or changes
  to another team's resources.
- User edits conflict with the task or a public contract needs a breaking change
  not covered by the selected milestone.

An ordinary bug or failing test is a reason to diagnose and repair within scope,
not a reason to expand the product. A release-environment blocker does not stop
local development. Do not keep adding integrations after the selected milestone
is complete just to fill an autonomous session.

## Verification and completion

For each code chunk: focused tests, relevant negative cases, and real runtime
evidence where execution or lifecycle changed. Run the parser-backed docs tests
and strict documentation build for changed examples. Runtime/public schema work
also requires Ruff, zero-error mypy, the applicable broader tests, distribution
build, and installed-wheel parity. Preserve all existing real-target gates.

Before handoff, inspect the full diff and worktree. Report the exact commands and
results, which milestones passed, what remains unsupported, and whether changes
were committed or pushed. A successful mock, parsed YAML, HTTP response, or
created topic cannot substitute for the required data and lifecycle evidence.

## Runtime research references

Kafka Streams provides application APIs; it is not an SQL server. A custom SQL
executor needs a translation and lifecycle layer. See the official
[Kafka Streams DSL](https://kafka.apache.org/43/streams/developer-guide/dsl-api/).
The [ksqlDB architecture](https://docs.confluent.io/platform/current/ksqldb/operate-and-deploy/how-it-works.html)
illustrates the parser, logical planning, and Kafka Streams topology generation
involved. These are research inputs, not a selected dependency or equivalence claim.
