# Configuration contract

## Status

Required specification for the public YAML interface.

## Goals

- Reject misspelled, misplaced, deprecated, and unsupported fields.
- Version the public configuration independently of the Python package.
- Produce errors that identify the file and logical YAML path.
- Keep backend-specific configuration out of the portable core model.
- Ensure every documentation example is executable under the same parser.

## Configuration envelope

New projects use an explicit version:

```yaml
apiVersion: streamt.dev/v1alpha1
project:
  name: payments
  version: 1.0.0
```

During the alpha migration window, an absent `apiVersion` is interpreted as the
legacy alpha version and produces a warning. Unsupported versions are errors.

## Strict fields

All configuration models reject unknown fields. Arbitrary extension metadata is
allowed only under an explicit `extensions` mapping or namespaced `x-` fields
if the schema deliberately supports them.

Example error:

```text
models/payments.yml:12:5 [model payments_clean]
unknown field `checkpoint_interval`
did you mean `flink.checkpoint_interval_ms`?
```

Strictness applies recursively to runtime clusters, defaults, policies, tests,
assertions, connections, contracts, and failure actions.

## Core versus backend configuration

Portable fields describe intent: topic shape, processing semantics, contracts,
ownership, policy, and resource requirements.

Backend-specific fields live under an explicitly selected backend block and are
validated by a tagged/discriminated model. Setting `type: confluent`,
`type: kubernetes`, or another backend must select a complete target-specific
schema; it must not merely accept a string while discarding target fields.

## Deprecation policy

Renamed fields remain readable for at most one documented migration window.
They produce a warning containing the replacement path. Conflicting old and new
fields are errors. `streamt migrate` performs deterministic mechanical rewrites
where possible.

No unsupported field may be silently retained as an apparent feature.

## Documentation verification

Documentation tests must:

- Extract complete examples into temporary projects.
- Parse them using `ProjectParser`.
- Run project validation and compilation when the example claims compilation
  support.
- Mark non-streamt YAML explicitly.
- Avoid fragment classifiers that bypass the actual containing model.

Target-specific examples also need a backend contract test. A feature can be
labelled supported only after at least one integration or contract test reaches
the real target API boundary.

## Schema distribution

`streamt docs schema` publishes a JSON Schema containing the supported
`apiVersion`, strict additional-property behavior, aliases, discriminated
backend schemas, and descriptions. Release automation attaches it to each
release and keeps a stable URL for editor integration.

