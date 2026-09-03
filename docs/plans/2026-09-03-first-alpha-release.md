# First public alpha release plan

## Objective

Publish `streamt==0.1.0a1` as the first installable public alpha without
allowing an untested commit, a mismatched tag, or a locally built replacement
artifact to reach PyPI.

The upload is irreversible: Python package versions cannot be replaced after
publication. Repository preparation may land before the release, but the tag,
GitHub prerelease, trusted-publisher approval, and PyPI upload are one reviewed
operator procedure.

## Status — 2026-09-03

The source tree builds a valid wheel and source distribution, `twine check`
passes, and the wheel installs into an isolated environment. The installed CLI
reports the package version. Full CI run
[`33800184842`](https://github.com/conduktor/streamt/actions/runs/33800184842)
passed on commit `d5f48945c54daec1f7843b282115ec8bd0881ec8`, including the
Python matrix, installed-wheel checks, PostgreSQL 14/18, Conduktor Gateway
3.15, and the real DataHub v1.7.0 GMS gate.

Publication is not ready yet. There is no public `streamt` distribution, tag,
or GitHub release, and the repository has no `pypi` or `testpypi` GitHub
environment. The PyPI and TestPyPI trusted publishers must be configured by an
account authorized on those services. Do not create a release or tag until the
preparation and TestPyPI rehearsal below pass for the exact candidate commit.

## Release contract

- The first package version is the PEP 440 prerelease `0.1.0a1`; the Git tag is
  exactly `v0.1.0a1` and the GitHub release is marked as a prerelease.
- `pyproject.toml`, installed distribution metadata, `streamt.__version__`,
  `streamt --version`, the tag, and the release title all identify the same
  version.
- The release commit is on `main` and the full `CI` workflow has succeeded for
  that exact commit SHA. Success for an ancestor or a different rebuild is not
  sufficient.
- The publish job receives only distributions produced by the unprivileged
  build job. It does not check out or execute repository code while holding an
  OIDC publishing identity.
- TestPyPI and PyPI use separate protected GitHub environments and separate
  trusted-publisher registrations. No long-lived package-index token is stored
  in GitHub.
- The credential-bearing publish action is pinned to a reviewed immutable
  commit. Metadata verification and default PyPI attestations remain enabled.
- Uploads never use `skip-existing`. A duplicate version is an error, not an
  idempotent success.
- The wheel and source distribution hashes recorded by the release workflow
  must match the files exposed by the package index and GitHub release.
- Installation documentation changes to the exact PyPI command only in a
  follow-up documentation commit after production verification succeeds.
  Until then, `main` continues to describe the repository install as a preview.

## External configuration

Create pending trusted publishers for the not-yet-created projects with these
exact claims:

| Index | Owner | Repository | Workflow | GitHub environment |
|---|---|---|---|---|
| TestPyPI | `conduktor` | `streamt` | `release.yml` | `testpypi` |
| PyPI | `conduktor` | `streamt` | `release.yml` | `pypi` |

Create both GitHub environments with deployment restrictions that admit only
the exact release tag pattern. The production `pypi` environment requires a
human reviewer who did not create the tag. Branch ancestry checks supplement
these controls; they do not replace them.

The official procedures are:

- <https://docs.pypi.org/trusted-publishers/creating-a-project-through-oidc/>
- <https://docs.github.com/en/actions/how-tos/secure-your-work/security-harden-deployments/oidc-in-pypi>

Record no PyPI password, API token, OIDC token, or environment configuration in
the repository or workflow evidence artifacts.

## Implementation slices

### Slice 1: package identity

1. Set the candidate version to `0.1.0a1`.
2. Add an executable version-coherence test covering source metadata, runtime
   metadata, installed metadata, and CLI output.
3. Align Python support metadata with the tested release matrix.
4. Add stable Issues and Changelog project URLs and verify the built metadata.
5. Keep the Apache-2.0 license and packaged third-party notices in both
   distributions.

### Slice 2: exact-SHA release workflow

1. Retain isolated build and OIDC publish jobs with least privilege.
2. Resolve the release tag to its commit and require a successful completed
   `CI` workflow whose `head_sha` is exactly that commit.
3. Reject a tag/version mismatch, a commit outside `main`, missing exact-SHA CI
   success, or a regular rather than prerelease GitHub release.
4. Build wheel and sdist once, run `twine check`, install each distribution in
   a clean environment, smoke-test the CLI, and emit SHA-256 evidence.
5. Upload the verified distributions as a short-retention workflow artifact.
6. Add a manually triggered TestPyPI rehearsal that requires an existing exact
   tag and can never select the production environment or repository.
7. Pin the publish action by immutable SHA and keep production publication
   restricted to the published GitHub prerelease event.

### Slice 3: release documentation

1. Replace stale numeric test badges and make README links valid on PyPI.
2. Consolidate the accumulated unreleased notes under `0.1.0a1`.
3. After production-index verification, change installation examples to
   `python -m pip install "streamt==0.1.0a1"` in a follow-up commit and use the
   equivalent exact `postgres` extra where needed.
4. Keep the alpha limitations and unsupported deployment boundaries explicit.
5. Mark the roadmap item complete only after the production index install has
   been independently verified.

## Rehearsal and publication

1. Merge the preparation commits and wait for full CI success on the exact
   candidate SHA.
2. Create the signed or annotated `v0.1.0a1` tag locally and verify that it
   resolves to that SHA. Push only that exact tag.
3. Run the manual TestPyPI workflow. From a new environment, install with
   `--index-url https://test.pypi.org/simple/` while allowing dependencies from
   PyPI, then verify distribution metadata, `streamt --version`, `streamt
   --help`, and one offline example.
4. Compare TestPyPI hashes and attestations with the workflow evidence.
5. Publish the GitHub prerelease for the same tag and approve the protected
   `pypi` environment after reviewing the exact SHA, version, CI run, and
   artifact hashes.
6. Install `streamt==0.1.0a1` and `streamt[postgres]==0.1.0a1` from production
   PyPI in separate clean environments. Verify metadata, CLI startup, the
   packaged schema set, and one offline export.
7. Attach the verified hashes to the GitHub prerelease and update the roadmap
   only after those production checks pass.

If TestPyPI or PyPI publication fails before upload, keep the version and tag
unchanged while correcting configuration or workflow faults. If any file was
accepted by an index, never rebuild or reuse that version; diagnose the
published artifact and advance to a new PEP 440 prerelease when a code change
is required.

## Acceptance criteria

- An exact-SHA green CI run is machine-verified before either index receives a
  distribution.
- TestPyPI rehearsal and production publication use distinct protected OIDC
  identities and no stored package-index credential.
- Wheel and sdist install independently and expose the same version and CLI.
- `pip install "streamt==0.1.0a1"` succeeds from production PyPI and the
  installed artifact passes the bounded smoke checks.
- The GitHub prerelease, PyPI metadata, documentation, hashes, and attestations
  all identify the same immutable release.
