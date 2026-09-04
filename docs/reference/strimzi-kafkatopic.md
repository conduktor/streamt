---
title: Strimzi KafkaTopic Export
description: Export deterministic Strimzi 1.2.0 KafkaTopic YAML without contacting Kubernetes
---

# Strimzi KafkaTopic export

`streamt export strimzi` compiles one project offline and emits deterministic
Strimzi `KafkaTopic` resources for its managed topic artifacts. The output is a
canonical multi-document YAML stream targeting exactly Strimzi `1.2.0`,
`kafka.strimzi.io/v1`, and kind `KafkaTopic`.

This is a GitOps artifact boundary. streamt does not contact Kubernetes,
install Strimzi, apply the YAML, read a cluster, manage credentials, run a
GitOps controller, or record Kubernetes ownership state.

## Export managed topics

Supply the destination namespace and existing Strimzi Kafka cluster name
explicitly:

```bash
streamt export strimzi \
  --namespace payments-prod \
  --cluster-name payments-kafka \
  --output-file kafkatopics.yaml
```

The command options are:

| Option | Meaning |
| --- | --- |
| `--namespace NAMESPACE` | Required exact Kubernetes DNS-1123 namespace emitted on every resource |
| `--cluster-name NAME` | Required exact DNS-1123 value for the `strimzi.io/cluster` label |
| `--output-file PATH` | Atomically write canonical YAML instead of writing it to stdout |
| `--project-dir PATH` | Project directory; defaults to the current directory |
| `--env ENVIRONMENT` | Select the normal streamt project environment |

Both identity values are validated before project parsing. They are not
trimmed, inferred from bootstrap servers, or read from the current Kubernetes
context.

Without `--output-file`, text mode reserves stdout for only the YAML stream. In
text mode with a file, stdout is empty after success. Global JSON mode returns
the normal streamt envelope with defensive document copies, counts, the pinned
release, API version, kind, manifest checksum, and nullable output path; it does
not interleave YAML on stdout.

```bash
streamt --output json export strimzi \
  --namespace payments-prod \
  --cluster-name payments-kafka
```

## Selection and identity

The export reads compiled topic artifacts, not live Kafka or a catalog
projection:

- Every topic whose ownership is `managed` becomes one `KafkaTopic`, including
  compiler-created managed dead-letter topics.
- An `external` topic is omitted with
  `W120_STRIMZI_EXTERNAL_TOPIC_OMITTED`.
- An `adopted`, malformed, or inconsistently owned topic fails closed. Offline
  YAML cannot prove an existing topic is safe for the Topic Operator to claim.
- Schemas, Flink jobs, tests, Connectors, Connector removals, Gateway rules, and
  Gateway removals are not represented. Their presence produces one bounded
  `W121_STRIMZI_ARTIFACTS_OMITTED` warning.

Valid Kafka topic names that are also Kubernetes DNS-1123 labels are retained
as resource names. Every other valid Kafka topic name receives a deterministic
`streamt-topic-<full-lowercase-sha256>` Kubernetes name while `spec.topicName`
retains the exact Kafka identity. Distinct topics therefore cannot collide
through case folding or truncation.

Each resource contains only the closed metadata and spec fields owned by this
export. Topic configuration values are normalized to strings under the pinned
Strimzi contract. A stable whole-manifest checksum and the logical owner
identity are annotations; runtime credentials and configuration are never
included.

## Failure and file safety

Invalid targets, artifacts, mappings, schema validation, serialization, or
file writes fail with `E509_STRIMZI_INVALID` and the fixed message `Strimzi
export failed safely`. No rejected topic name, configuration value, SQL,
credential, endpoint, or exception text is copied into that error.

File output is assembled and validated completely before a same-directory
atomic replacement. Existing non-regular destinations, links, races detected
by the frozen checks, synchronization failures, or replacement failures leave
no accepted partial artifact. Select a destination directory that is not
writable by an untrusted actor.

Generated YAML intentionally contains public GitOps data: namespaces, cluster
labels, physical topic names, partitions, replicas, topic configuration,
logical owner names, and the manifest checksum. Review the file and its
destination permissions before sharing it.

## Validated compatibility boundary

The package carries the exact Strimzi 1.2.0 `KafkaTopic` CRD and validates every
document both against that pinned schema and a stricter closed local contract.
Source, wheel, and direct-sdist exports are byte-identical across Python 3.10
through 3.14.

A separate test-only acceptance lane installs the built wheel, creates a
digest-pinned disposable kind/Kubernetes 1.35 cluster, starts the pinned
Strimzi 1.2.0 operator with Kafka 4.3.1, applies both topic identities using
server-side apply, verifies exact Kubernetes and broker read-back, and repeats
the same stream to prove replay stability. The decisive normal-mode evidence is
[CI run 33909664040, job 101143523418](https://github.com/conduktor/streamt/actions/runs/33909664040/job/101143523418),
following reviewed runtime-image discovery in
[pilot run 33908141332](https://github.com/conduktor/streamt/actions/runs/33908141332).

That evidence supports only deterministic offline `KafkaTopic` output for the
pinned target. It is not a production topology recommendation and does not
support:

- direct Kubernetes or Strimzi apply, update, deletion, pruning, rollback, or
  drift detection;
- operator, Kafka cluster, namespace, RBAC, Secret, or credential management;
- GitOps-controller installation, configuration, reconciliation, or deletion
  policy;
- adopted or external topic takeover, deletion safety, ownership-state
  integration, or multi-namespace inference; or
- compatibility with other Strimzi, Kubernetes, or Kafka versions.

See the [normative Strimzi export specification](../specs/strimzi-kafkatopic-export.md)
for the complete mapping, validation, warning, secrecy, and evidence contract.
