# Declared Git comparison and downstream impact

## Status and purpose

Planned, not implemented. Written on 2026-09-06 for later work at the owner's
request. Execution is paused; read the
[resume handoff](../plans/2026-09-06-resume-handoff.md) first. Finish and verify
the public Kafka Streams change/resume lot before starting this implementation.

This feature explains how a proposed Git change affects a streaming topology.
It compares declared versions without infrastructure access. A filter change
must show its downstream applications even when the output topic configuration
and declared schema stay the same.

The report is review evidence, not a deployment plan, a live drift check or
proof of runtime compatibility. It never transfers ownership or authorizes apply.

## Proposed public surface

Extend the existing command with an explicit mode:

```bash
streamt -o json diff --base main --head feature/orders -p path/to/project
streamt diff --base HEAD~1 --head HEAD -p path/to/project --env prod
```

These commands are proposed syntax, not available functionality.

- Both refs are required in Git mode. Resolve them to exact local commit IDs
  once and include those IDs in the report. Compare those exact trees, not an
  implicit merge base, index or dirty worktree. Do not fetch.
- Without either flag, preserve the existing live `diff` behavior. Do not
  silently change its external-observation policy as part of this feature.
- Dispatch Git mode before constructing the existing project parser, deployment
  state service or providers.
- Resolve the repository from the explicit project directory and bind one
  repository-relative project path to both snapshots. Reject paths escaping it.
  Initial scope is one project at a time, including a repository subdirectory.
- The optional environment selects committed declarations in each snapshot.
  Do not read `STREAMT_ENV`, `.env` or ambient credentials to select or resolve it.
  Omit the option for a single-environment project. Unsupported or missing
  environment declarations must be reported explicitly.
- JSON uses a separate versioned `declared_git` report inside the existing
  formatter envelope. It is never a `ReviewedPlanFile`, contains no mutation
  authorization, and must be rejected by `apply --plan`.
- A valid report returns success even when it contains changes or declared
  breakages. Invalid input/read failures return non-success. A later opt-in
  fail-on-breaking policy can use the report; it must not change default
  report semantics without explicit documentation and tests.

## Safe Git reader

Read committed trees and blobs through bounded Git subprocesses with argument
arrays, no shell interpolation and no working-tree materialization.

- Resolve refs as commits with option termination. Pass only resolved object
  IDs to subsequent object reads. Reject invalid refs and missing objects.
- Disable implicit network access, including lazy fetching in partial clones.
  Do not invoke checkout, worktree, fetch, hooks, smudge filters, textconv,
  external diff, or repository-defined commands. Sanitize Git environment and
  configuration that can redirect object lookup or execute helpers.
- Handle paths as data, using NUL-delimited listings where appropriate.
  Test spaces, Unicode, newlines and names beginning with a dash.
- Reject symlinks and submodules in consumed project inputs; never follow them
  outside a snapshot. Ignore unrelated repository files, but do not silently
  discard an unsafe required input.
- Bound subprocess duration, file count, per-blob size, total bytes, YAML
  expansion and nesting. Set and test numeric limits before merging the reader;
  an exceeded limit produces an explicit incomplete/error outcome, not a
  misleading partial no-change report.
- A missing project in one valid tree can mean project addition or removal.
  A missing commit/object or malformed project cannot be treated as an empty
  project. Missing projects in both snapshots are an error.
- Keep the dirty worktree, index, refs and deployment files unchanged.

A shallow clone may lack a requested commit. Explain how the operator can make
that commit available separately; the comparison itself remains offline.

## Pure declared snapshots

Introduce a small immutable snapshot representation, provisionally
`DeclaredProjectSnapshot`, with origin commit/path/environment, logical resource
identities, declarations, dependency edges, explicit contracts and coverage.

The existing execution parser is not a safe shortcut:

- `core/parser.py` sets up environments, loads dotenv files and resolves
  `os.environ`.
- `core/validator.py` can invoke the compiler even for validation.
- `compiler/model_resolution.py` can render project-provided Jinja macros.
- The planner's existing impact path may consult live consumer groups.

Do not run these complete paths on arbitrary commit contents. Extract or reuse
pure declaration checks, and test that forbidden entry points are never reached.
Keep strict unknown-field and identity validation where applicable without
requiring deployment credentials or inventing concrete values for placeholders.

Load committed project YAML, sources, models, referenced SQL files and selected
environment declarations. Keep environment references symbolic. Report relevant
runtime/environment changes without resolving DSNs or reading local files.

Use safe YAML parsing with explicit duplicate-key, tag, expansion and input
limits. No constructors, plugins or template execution from a commit.

Dependencies may reuse `direct_model_dependencies` where its literal parsing
contract is suitable. Audit that reuse against comments, malformed references
and opaque macros; regex matches alone are not proof of complete SQL lineage.
Static Jinja syntax inspection is permitted; rendering is not. A macro or dynamic
dependency that cannot be resolved must retain unknown coverage and potential
impact. Changing a macro must not produce an empty report simply because it
does not appear in an inline SQL field.

Declared columns and application requirements are contract evidence. Generic
compiler defaults or incomplete SQL inference are not proof of a producer's
output type. A later pure inference extension needs its own capability tests.

## Comparison model

Identify resources by type and logical name, independently of source filename.
Keep physical identities and ownership as compared fields, not matching guesses.
Treat a logical rename as removal plus addition initially.

| Change class | Expected report behavior |
| --- | --- |
| Added or removed resource/project | Preserve the relevant side's declarations and downstream paths; no provider creation/deletion implied |
| SQL content | Report transformation change even with unchanged declared output; do not claim semantic equivalence or changed outputs solely from text |
| Declared schema/contract | Compare columns, types, requiredness and application requirements using explicit evidence |
| Configuration or runtime binding | Report changed safe field paths and affected declared users; no resolved credentials |
| Physical identity | Expose the identity change as review-required, not a safe rename or offset-preserving migration |
| Ownership | Show external/managed/adopted transitions as declarations; existing adoption checks still decide authority |
| Relationships | Compare input/output/dependency edges and preserve removed paths |
| Metadata | Report owner/repository/documentation changes without turning them into deployment mutations |

Moving an unchanged declaration to a different file is not a logical resource
change; provenance may change. Canonicalize maps and unordered collections where
their semantics allow it. Preserve order where it affects behavior.

Compare validated original values internally before redaction so two different
secrets cannot both become the same placeholder and disappear from the diff.
Public text/JSON/Markdown uses an explicit safe-field allowlist. For sensitive
or arbitrary SQL/config values, expose a change marker and safe field path,
not raw values, credentials, low-entropy value hashes or snippets. Sanitize error
messages too. Escape untrusted names in terminal and Markdown output.

The report includes resolved commits, project/environment, schema version,
ordered changes, direct and transitive impacts, reason codes, evidence origins
and coverage gaps. Freeze exact field names with tests before adding consumers.

## Downstream impact

Build the base and head graphs separately. Include literal source/model inputs
and custom-application `consumes`, `produces` and `depends_on` relationships.
Retain owners and repositories already declared in the project.

Validate each graph separately, traverse each, then merge the resulting impacts.
Do not validate the union as one graph: reversing an edge across two valid
versions can make their union cyclic. Removed resources and relationships need
base-side paths so their former consumers remain visible.

Each impact records its originating change, affected logical resource, causal
path, base/head provenance, declared owner and evidence coverage. Bound path
enumeration and label truncation; do not let a dense DAG create unbounded output.

Use distinct conclusions:

- `declared_breakage`: explicit facts prove a broken declaration, such as a
  required producer column removed or a required logical reference missing.
- `potential_impact`: behavior, configuration or dependency changed, but the
  declarations do not prove runtime compatibility or incompatibility.
- `information`: descriptive changes with no demonstrated execution effect.

A changed filter propagates potential impact to downstream SQL models and
custom consumers. An unchanged output schema does not suppress that impact.
An unknown schema or macro stays unknown; absence of known consumers is not
proof that none exist. Never emit an unconditional runtime-safe verdict.

External resources remain part of declared relationships. Comparing their Git
definitions neither reads their live drift nor converts them to managed assets.

## Ordered implementation and verification

All tasks remain unchecked until the owner resumes and the preceding runner
lot passes its acceptance. File names below are suggested ownership boundaries,
not a reason to duplicate an existing pure abstraction.

1. [ ] Safe repository/object reader, for example `core/git_snapshot.py`.
   Test refs, object absence, path boundaries, shallow/partial clones, symlinks,
   submodules, executable Git configuration and resource limits without
   constructing any project runtime.
2. [ ] Pure snapshot/contract representation and loader, for example
   `core/declared_snapshot.py`. Test equivalent declarations, environment
   overlays, symbolic values, duplicate/unknown YAML fields, referenced SQL
   and opaque macros. Prove no dotenv loading or Jinja rendering.
3. [ ] Comparison and impact service, for example `core/declared_diff.py`.
   Freeze the report schema; test deterministic changes, base/head graph
   traversal, declared breakages, coverage gaps, ownership and secret-neutral
   output. This layer receives snapshots, not Git processes or provider clients.
4. [ ] Public CLI integration in `cli/commands/diff.py`.
   Preserve the old invocation; test explicit mode selection, argument errors,
   project/environment handling, JSON/text and rejection as an apply plan.
5. [ ] Shared Markdown rendering and the existing GitHub Action integration.
   Keep the reviewed deployment-plan artifact separate. Test rendering locally;
   do not post comments or change remote PRs merely to validate the renderer.
   Treat refs and content from pull requests as untrusted.
6. [ ] Installed-package acceptance outside the checkout, documentation and exact
   commit CI. Build a temporary Git repository with both entry-path examples;
   no Kafka, Docker, PostgreSQL, catalog or commercial account is required
   for this declared comparison gate.

For future parallel work, agree the immutable snapshot/report contracts first.
Then the reader tests, pure graph/comparison work and independent security/
installed acceptance can have separate owners. The primary agent integrates
shared CLI and rendering changes. Commit tested logical chunks; do not merge
unverified broad refactors into the runtime lot.

## Required end-to-end cases

- SQL-only filter change reaches a downstream model and custom application,
  with unchanged physical topic and declared schema.
- Required-column removal, incompatible declared type/requiredness, unknown
  producer schema and removed references yield distinct, justified conclusions.
- External-to-managed and managed-to-external changes never adopt, release,
  create or delete resources.
- A YAML file move preserves logical identity; a logical rename remains
  removal/addition. Project addition/removal is distinct from an unreadable ref.
- Edge reversal across two valid graphs does not create a false cycle failure.
- Dynamic references and changed macros retain uncertainty and cannot execute.
- Secrets in SQL, YAML, URLs, placeholders and failures never leak to reports.
- Malicious refs/paths, symlinks, submodules, oversized inputs, YAML expansion,
  partial clones and a dirty worktree cannot cause execution, fetches or writes.
- Identical commit inputs produce identical normalized output despite unrelated
  environment variables, credentials or working-tree changes.
- Tests forbid provider factories, deployment-state access, environment loading
  and template rendering. Installed acceptance verifies the same boundaries.

No stateful upgrade, GitOps controller, automatic reconciliation, multi-project
package resolver or additional integration is implied. After this report works,
evaluate its usefulness in the full import/create/change workflow before
choosing more features.
