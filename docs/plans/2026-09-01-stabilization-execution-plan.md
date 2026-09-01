# Stabilization execution plan

## Objective

Reach a trustworthy alpha in which configuration is strict, partial adoption is
safe, advertised CLI paths work, and the roadmap is enforced by tests and CI.

## Status — 2026-09-01

- Milestone A immediate safety stop: implemented. Environment-scoped local
  ownership state is now persisted after successful direct applies; remote
  state, locking, explicit adoption, and an explicit destroy workflow remain.
- Milestone B strict public contract: implemented with `streamt.dev/v1alpha1`
  and parser-backed documentation validation.
- Milestone C CLI reliability: the broken observe and sample-test paths are
  repaired and all top-level commands have smoke coverage.
- Milestone D CI gates: unit/scenario matrices, strict docs, wheel installation,
  Ruff, and a mypy non-regression baseline are implemented. Trusted publishing
  and the first public alpha release remain open.
- Phase 1 foundations now include live Schema Registry reference and
  compatibility validation, deterministic reviewed plan files, and a
  validate/plan-only first-party GitHub Action.

## Work order

### Milestone A: safety stop

These changes land first because current behavior can affect unrelated data.

1. Remove cluster-wide orphan deletion from ordinary plan/apply.
2. Default destructive environment policy to false.
3. Attach source/model ownership metadata to compiled artifacts.
4. Fix targeted and tag-selected compilation/filtering.
5. Add regression tests for partial estates and targeted plans.

Verification:

- A manifest with one desired topic and a live cluster containing unrelated
  topics produces no delete actions.
- `apply --target` retains the target and dependency artifacts.
- A targeted plan never performs removal detection.

### Milestone B: strict public contract

1. Introduce a shared strict Pydantic base.
2. Add the alpha `apiVersion` envelope with a legacy warning window.
3. Fix examples and unsupported fields exposed by strict parsing.
4. Replace permissive documentation-fragment checks with parser-level tests.
5. Regenerate the checked-in JSON Schema.

Verification:

- Unknown fields fail with their logical YAML path.
- The flagship README project parses without discarded fields.
- All documentation project examples validate under the production parser.

### Milestone C: CLI reliability

1. Fix `observe` to use current helper contracts and output APIs.
2. Move sample testing to the installed `confluent-kafka` client and honor
   configured authentication.
3. Add smoke tests for every top-level CLI command.
4. Remove or clearly gate options that only print "not implemented".

Verification:

- Every top-level command can be invoked through `CliRunner` without an
  unhandled exception.
- Sample tests work without undeclared dependencies.

### Milestone D: CI and release

1. Run unit and scenario tests in CI.
2. Build the wheel and install it in a clean environment for CLI smoke testing.
3. Build documentation strictly.
4. Establish and enforce a mypy baseline, then reduce it incrementally.
5. Add a tagged release workflow with trusted publishing.
6. Publish the first installable alpha and verify the documented command.

Verification:

- CI covers the same commands used during release.
- `pip install streamt` installs the published version, or documentation uses
  the actual package name if a rename is required.

## Following milestone

Continue the minimum viable change-impact plan in
`docs/specs/change-impact-plan.md`. Schema Registry resolution and reviewed
plan artifacts are implemented; explicit import/adoption and richer impact
classification are next.

## Commit strategy

Commit each milestone or independently releasable subset after its focused and
broad tests pass. Never mix generated documentation cleanup or unrelated user
workspace changes into stabilization commits.
