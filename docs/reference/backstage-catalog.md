---
title: Backstage Catalog Export
description: Export deterministic Backstage core entities without publishing them
---

# Backstage Software Catalog export

`streamt docs backstage` compiles one project offline and emits deterministic
Backstage `System`, `Resource`, and `Component` entities as a canonical YAML
multi-document stream. The output is validated against the schemas pinned from
Backstage `v1.54.2` before any bytes are returned or written.

This command exports design metadata. It does not contact or publish to a
Backstage instance, read deployment state, call Kafka or Conduktor providers,
or prove that any described resource is deployed or healthy.

## Export a catalog

Identity, namespace, ownership, and lifecycle are explicit because streamt
cannot safely infer Backstage policy:

```bash
streamt docs backstage \
  --catalog-id payments-prod \
  --catalog-namespace payments \
  --default-owner-ref group:platform/payments \
  --lifecycle production \
  --owner-map backstage-owners.json \
  --kafka-cluster-ref resource:platform/kafka-prod \
  --gateway-cluster-ref resource:platform/gateway-prod \
  --domain-ref domain:platform/commerce \
  --output-file catalog-info.yaml
```

The four identity options are always required:

| Option | Meaning |
| --- | --- |
| `--catalog-id` | Stable, deployment-independent ID used in generated entity identities |
| `--catalog-namespace` | Explicit lowercase namespace used by every generated entity |
| `--default-owner-ref` | Full lowercase `group:<namespace>/<name>` or `user:<namespace>/<name>` reference |
| `--lifecycle` | Exact lifecycle assigned to generated processing Components |

Other options are conditional or optional:

| Option | When to use it |
| --- | --- |
| `--owner-map PATH` | Required in practice when a source or model declares an `owner`; every declared label must resolve exactly |
| `--kafka-cluster-ref` | Required when the project exports any source or Kafka model-output Resource |
| `--gateway-cluster-ref` | Required when the project exports any Gateway virtual-topic Resource |
| `--domain-ref` | Optional full Domain reference for the generated System |
| `--output-file PATH` | Atomically writes canonical YAML instead of writing it to stdout |
| `--project-dir PATH` | Selects the project directory; defaults to the current directory |
| `--env ENV` | Selects the normal streamt environment |

Cluster and domain references are catalog relationships supplied by the
caller. Offline export validates their syntax but cannot prove that the target
entities exist in Backstage.

## Map streamt owners

streamt owner labels are not Backstage entity references. Map them explicitly
with strict JSON:

```json
{
  "version": 1,
  "owners": {
    "payments-platform": "group:platform/payments",
    "fraud-analytics": "user:data/fraud-owner"
  }
}
```

The file must contain exactly `version` and `owners`; `version` must be integer
`1`. Labels are matched exactly and case-sensitively. Duplicate JSON keys,
wildcards, abbreviated references, unmapped declared owners, and unknown keys
fail closed. Declarations without an owner use `--default-owner-ref`.

## Entity mapping

| Compiled streamt fact | Backstage output |
| --- | --- |
| Project plus effective environment | One `System` |
| Source topic | `Resource` with type `kafka-topic`; not assigned to the generated System |
| Plain topic model output | `Resource` with type `kafka-topic`; no invented producer Component |
| Flink model | `Component` plus its Kafka output `Resource` |
| Gateway virtual-topic model | `Component` plus a `kafka-virtual-topic` `Resource` |
| Connect sink model | `Component`; no destination Resource is invented |
| Exposure | Omitted because there is no safe core-entity mapping |

Component dependencies are the compiler's exact direct input Resources, not a
transitive graph. Model-output Resources depend on their producing Component
when one exists and on the explicit Kafka or Gateway cluster Resource.
Generated references close within the document; caller-provided owner, domain,
and cluster references are the only external references.

The exporter includes only allowlisted names, descriptions, tags, owner
resolution, physical topic or alias names, direct dependencies, process kind,
effective environment, and the model contract state `declared` or `enforced`.
It does not include SQL, columns, schema bodies, connector configuration,
runtime endpoints, credentials, deployment ownership, or provider evidence.

## Output and warnings

Without `--output-file`, text mode writes only canonical YAML to stdout and
writes warnings to stderr. With a file, streamt validates and serializes the
complete export before atomically replacing the target; successful text mode
does not print a banner.

Global JSON mode returns the ordinary streamt envelope:

```bash
streamt --output json docs backstage \
  --catalog-id payments-prod \
  --catalog-namespace payments \
  --default-owner-ref group:platform/payments \
  --lifecycle production \
  --kafka-cluster-ref resource:platform/kafka-prod
```

Its `data` object contains `standard`, `release`, `api_version`, `entities`,
exact `System`/`Resource`/`Component` counts, and `output_file`. The entity
array is semantically identical to a safe YAML parse of text output. Repeated
exports with identical project inputs and options are byte-identical.

Two bounded warnings describe intentionally omitted metadata:

| Code | Meaning |
| --- | --- |
| `W113_BACKSTAGE_SINK_OUTPUT_OMITTED` | A sink Component was emitted, but its provider-specific destination was not |
| `W114_BACKSTAGE_EXPOSURE_OMITTED` | One exposure declaration was omitted; duplicate declarations produce separate warnings |

Invalid identity, ownership, mapping, entity, or output construction fails with
`E507_BACKSTAGE_INVALID`. The command never emits a partial catalog.

## Support and security boundary

The packaged validator uses the exact seven-schema closure from Backstage
`v1.54.2`. Release gates verify those resources in both wheel and source
distribution, run export from an isolated installed wheel with network and
subprocess access denied, and independently check representative YAML with
`@backstage/catalog-model@1.10.0`. Node.js is not a streamt runtime dependency.

Treat the generated file as sensitive metadata. It intentionally names
projects, environments, topics and aliases, owners, clusters, descriptions,
tags, and dependencies. Review the file and its destination permissions before
committing or sharing it.

DataHub export, Backstage API publication, and Conduktor Console metadata
publication remain deferred. None is enabled by this command or implied by a
successful export. See the [normative specification](../specs/backstage-catalog-export.md)
and [implementation plan](../plans/2026-09-02-backstage-catalog-export.md) for
the exact validation, identity, and release contracts.
