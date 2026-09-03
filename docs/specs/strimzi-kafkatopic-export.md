# Strimzi KafkaTopic GitOps export

Status: proposed. Nothing in this specification is implemented or supported
until every release gate below passes.

This specification defines a deterministic, offline export of managed streamt
topic artifacts as Strimzi `KafkaTopic` custom resources. It defines a GitOps
artifact boundary, not a Kubernetes client or a second deployment-state engine.

## Normative language

`MUST`, `MUST NOT`, `SHOULD`, and `MAY` are normative. When an identity,
ownership decision, or topic setting cannot be represented exactly, the command
MUST fail. It MUST NOT normalize away the conflict or silently broaden its
scope.

## Pinned target and primary sources

The first target is the stable Strimzi Kafka Operator `1.2.0`, released on
2026-08-20, at immutable commit
[`6c7b43c4af0db547c10463ba09d1dfa6f5e156a0`](https://github.com/strimzi/strimzi-kafka-operator/commit/6c7b43c4af0db547c10463ba09d1dfa6f5e156a0).
The emitted resource is exactly `kafka.strimzi.io/v1`, kind `KafkaTopic`.
Strimzi 1.2.0 supports only its `v1` custom-resource API.

The frozen upstream evidence is:

| Input | Immutable location | SHA-256 |
| --- | --- | --- |
| Release | [Strimzi 1.2.0](https://github.com/strimzi/strimzi-kafka-operator/releases/tag/1.2.0) | tag resolves to `6c7b43c4af0db547c10463ba09d1dfa6f5e156a0` |
| Release archive | [strimzi-1.2.0.tar.gz](https://github.com/strimzi/strimzi-kafka-operator/releases/download/1.2.0/strimzi-1.2.0.tar.gz) | `94f66d5387e6fe653e71df3520833d63155f81d78c7dc180d9a0656f7e7ddecb` |
| Complete CRD bundle | [strimzi-crds-1.2.0.yaml](https://github.com/strimzi/strimzi-kafka-operator/releases/download/1.2.0/strimzi-crds-1.2.0.yaml) | `6f300e77128dc0963b8a6153c520a0f11b6f8999486a9b8e361c06aaa17860a3` |
| Cluster Operator install | [strimzi-cluster-operator-1.2.0.yaml](https://github.com/strimzi/strimzi-kafka-operator/releases/download/1.2.0/strimzi-cluster-operator-1.2.0.yaml) | `a0b1ae3e375a7da0674eb3894df8aa955bc50abe8190bc9344e30b83bbee4775` |
| KafkaTopic CRD | [`043-Crd-kafkatopic.yaml`](https://raw.githubusercontent.com/strimzi/strimzi-kafka-operator/6c7b43c4af0db547c10463ba09d1dfa6f5e156a0/packaging/install/cluster-operator/043-Crd-kafkatopic.yaml) | `36390f0731c699448076d4ee739e8b7f331d083e91a7fb71500aaa830ab1127e` |
| KafkaTopic example | [`examples/topic/kafka-topic.yaml`](https://raw.githubusercontent.com/strimzi/strimzi-kafka-operator/6c7b43c4af0db547c10463ba09d1dfa6f5e156a0/examples/topic/kafka-topic.yaml) | `58b929f6d1d07e208d99ce44c3884b36e2e8ddd1e8400ae6ca5cf302ae0d352c` |

The normative behavior is also grounded in the versioned official
[topic-management and naming documentation](https://strimzi.io/docs/operators/1.2.0/deploying.html#assembly-using-the-topic-operator-str),
the pinned
[`KafkaTopicSpec`](https://github.com/strimzi/strimzi-kafka-operator/blob/6c7b43c4af0db547c10463ba09d1dfa6f5e156a0/api/src/main/java/io/strimzi/api/kafka/model/topic/KafkaTopicSpec.java),
and the pinned Topic Operator
[`configValueAsString`](https://github.com/strimzi/strimzi-kafka-operator/blob/6c7b43c4af0db547c10463ba09d1dfa6f5e156a0/topic-operator/src/main/java/io/strimzi/operator/topic/TopicOperatorUtil.java#L206-L230)
conversion. Kafka topic names are checked against Apache Kafka 4.3.1
[`Topic.validate`](https://github.com/apache/kafka/blob/26b251a451ce941d3d7a55e6487bcb7f16b5ad48/clients/src/main/java/org/apache/kafka/common/internals/Topic.java).

Newer Strimzi, Kubernetes, or Kafka behavior is not evidence for this
contract. Any target-version change requires a separate compatibility review,
new source digests, byte fixtures, package parity, and real-cluster acceptance.

## Scope and non-claims

The command emits only `KafkaTopic` resources for compiled topic artifacts
whose lifecycle ownership is exactly `managed`. Compiler-generated managed DLQ
topics are included. It does not emit source declarations merely because they
refer to Kafka topics.

The command does not:

- contact Kubernetes, Strimzi, Kafka, DNS, HTTP, sockets, or a subprocess;
- construct a streamt deployer, deployment planner, state service, reviewed
  operation, or provider client;
- apply, patch, delete, prune, observe, import, adopt, reconcile, or roll back a
  resource;
- emit `Kafka`, `KafkaNodePool`, `KafkaUser`, `KafkaConnector`, Connect,
  MirrorMaker, Schema Registry, Flink, Gateway, ACL, quota, or secret resources;
- accept arbitrary labels, annotations, raw custom-resource fragments, Helm,
  Kustomize, or templating input;
- infer namespace or cluster identity from bootstrap servers, runtime endpoints,
  the selected environment, state, or a current Kubernetes context; or
- claim that the resulting stream represents the complete streamt project.

Strimzi documents that creating, changing, or deleting a `KafkaTopic` causes
the Topic Operator to perform the corresponding Kafka operation. Consequently,
deletion of generated YAML from Git, pruning by a GitOps controller, deletion
of the Kubernetes resource, finalizer behavior, and deletion of the Kafka topic
are explicitly outside this contract. streamt emits no tombstone and grants no
destructive authority by absence.

## Command contract

```text
streamt export strimzi \
  --namespace <KUBERNETES-NAMESPACE> \
  --cluster-name <STRIMZI-KAFKA-CLUSTER> \
  [--output-file <PATH>] \
  [--project-dir <PATH>] \
  [--env <ENVIRONMENT>]
```

`--namespace` and `--cluster-name` are required and MUST be validated before
project parsing, compilation, or file creation. Neither value is trimmed,
case-folded, slugged, or inferred.

The namespace MUST be a Kubernetes DNS-1123 label: 1 through 63 lower-case
ASCII alphanumeric or `-` characters, beginning and ending with an
alphanumeric character. The cluster name MUST satisfy the same rule because it
is emitted as the `strimzi.io/cluster` label value. This deliberately narrow
boundary avoids depending on a longer Kubernetes object name that cannot be
represented as the required Strimzi label.

The command uses standard project and environment resolution and runs
`Compiler.compile(dry_run=True)` exactly once. It reads only the resulting
manifest's topic artifacts and the stable `manifest_checksum()` that excludes
`compiled_at`. It MUST NOT use the catalog projection: that projection does not
contain partitions, replicas, or topic configuration.

### Output modes

In text mode without `--output-file`, stdout is only the canonical YAML stream
and warnings go only to stderr. In text mode with `--output-file`, the complete
stream is atomically written and stdout is empty. Quiet mode suppresses text
and warnings but not errors or a requested file write. Quiet mode without an
output file is invalid because it has no artifact sink.

JSON mode emits one ordinary streamt formatter envelope. Its `data` contains
exactly `target_release`, `api_version`, `kind`, `manifest_checksum`,
`documents`, `counts`, and nullable `output_file`; it does not interleave raw
YAML. `documents` contains defensive JSON-compatible copies in canonical YAML
order. `counts` contains exactly `emitted_topics`, `external_topics_omitted`,
and `other_artifacts_omitted`. `other_artifacts_omitted` is the sum of the
lengths of the `schemas`, `flink_jobs`, `test_jobs`, `connectors`,
`connector_removals`, `gateway_rules`, and `gateway_rule_removals` artifact
collections. A missing additive removal collection has length zero. An
unexpected artifact collection fails closed rather than being silently
omitted.

An export with no managed topics succeeds and its canonical YAML is zero bytes.

## Topic selection and ownership

Each input topic artifact MUST be parsed with a closed, strict parser. The
required fields are `name`, integer `partitions` from 1 through 2,147,483,647,
integer `replication_factor` from 1 through 32,767, object `config`, and exact
ownership. The partition maximum is the representable maximum of pinned
Strimzi's Java `Integer` property; the CRD itself specifies only the minimum.
Extra or malformed fields fail the whole export; malformed artifacts MUST NOT
be skipped.

- `managed`: emit exactly one `KafkaTopic`.
- `external`: emit nothing and produce
  `W120_STRIMZI_EXTERNAL_TOPIC_OMITTED` with fixed message `External topic
  artifact omitted from Strimzi export` and location
  `artifacts/topics/<INDEX>/ownership`, where `<INDEX>` is the unpadded,
  zero-based decimal index in the compiled topic collection. The warning
  contains no topic or owner name and no configuration value.
- `adopted`: fail with `E509_STRIMZI_INVALID`. An offline artifact cannot prove
  prior state or that an existing topic is safe for a new Topic Operator to
  claim.
- absent, unknown, mismatched-project, or inconsistent ownership: fail with
  `E509_STRIMZI_INVALID`.

If schemas, Flink jobs, test jobs, connectors, Connector removals, Gateway
rules, or Gateway removal artifacts are present, the command emits
`W121_STRIMZI_ARTIFACTS_OMITTED` once. Its fixed message is `Non-topic
artifacts omitted from Strimzi export`. Its exact location is
`artifacts/omitted/schemas=<S>,flink_jobs=<F>,test_jobs=<T>,connectors=<C>,connector_removals=<D>,gateway_rules=<G>,gateway_rule_removals=<R>`,
where each placeholder is the unpadded decimal collection length. The location
always contains all seven kinds in that order, including zero counts, and
never contains names or artifact contents.

Warnings are immutable and sorted by `(location, code)`. Raw stderr uses fixed
human messages without relying on codes; JSON warning objects contain exact
`code`, `message`, and `location` keys in the ordinary formatter order.

## Exact identity and naming

Kafka identity is the physical `TopicArtifact.name`. Under the pinned Kafka
4.3.1 rule it MUST:

- contain 1 through 249 ASCII characters;
- contain only letters, digits, `.`, `_`, and `-`; and
- not equal `.` or `..`.

The value is never case-folded or normalized. Duplicate physical topic names
fail before serialization.

The Kubernetes `metadata.name` is deterministic:

1. If the Kafka name is also a DNS-1123 label of at most 63 characters,
   `metadata.name` is the exact Kafka name.
2. Otherwise it is `streamt-topic-` followed by all 64 lower-case hexadecimal
   characters of `sha256(topic_name.encode("utf-8"))`.

The full hash MUST NOT be truncated. A collision among generated Kubernetes
names fails. `spec.topicName` is always present and is always the exact Kafka
name, including when it equals `metadata.name`. This makes the physical Kafka
identity explicit and immutable; changing it produces a different artifact
rather than an in-place rename claim.

## Exact document shape

Each document contains only this shape and key order:

The `streamt:skip` comment is a documentation-test marker and is not part of
the emitted document.

```yaml
# streamt:skip -- normative third-party Strimzi custom resource shape
apiVersion: kafka.strimzi.io/v1
kind: KafkaTopic
metadata:
  name: <deterministic-kubernetes-name>
  namespace: <exact-namespace>
  labels:
    strimzi.io/cluster: <exact-cluster-name>
    app.kubernetes.io/managed-by: streamt
  annotations:
    streamt.dev/manifest-checksum: sha256:<64-lowercase-hex>
    streamt.dev/owner-name: <logical-owner-name>
    streamt.dev/owner-type: <model-or-source>
    streamt.dev/ownership-mode: managed
    streamt.dev/project: <exact-project-name>
    streamt.dev/strimzi-release: 1.2.0
spec:
  topicName: <exact-kafka-topic-name>
  partitions: <integer-1-through-2147483647>
  replicas: <integer-1-through-32767>
  config: {}
```

The lifecycle owner type MUST be exactly `model` or `source`; a source owner is
expected for a compiler-generated managed DLQ attached to a source test. Any
other owner type fails. All annotation keys and values are strings. Project and
logical owner annotation values reject NUL, Unicode control characters, and
surrogate code points but are otherwise exact. The complete annotation map MUST
remain within the Kubernetes 256 KiB annotation limit. `config` is always
present; an empty map is emitted exactly as `config: {}`. No field in the shape
above is omitted. Documents are sorted by
`(spec.topicName, metadata.name)` using Unicode code point order.

The checksum annotation binds every document to the current
`manifest_checksum()` secret-neutral canonical projection of the entire
successfully compiled manifest rather than only the represented topic. That
projection excludes `compiled_at` and replaces values whose keys match the
repository sensitive-key policy with the fixed `"<redacted>"` marker. Changes
to represented non-secret manifest content therefore change the bytes even
when the `KafkaTopic` specs do not; changes only to redacted secret values do
not. The checksum is evidence of this secret-neutral compiler projection, not
the original secret-bearing input and not proof of review, approval, live
absence, or safe application.

## Topic configuration scalar boundary

Configuration keys MUST be non-empty, single-line ASCII strings, MUST NOT
contain control characters, and MUST NOT match this exact case-insensitive
sensitive-key expression, which is the repository policy at this proposal's
freeze point:
`(^|[._-])(?:password|passwd|secret|token|api[_-]?key|authorization|credentials?|basic[._-]auth[._-]user[._-]info|sasl[._-]jaas[._-]config)($|[._-])`.
This rejects both `credential` and `credentials`, and `apikey`, `api_key`, and
`api-key`, at dot, underscore, or hyphen boundaries.

Only exact Python `str`, `bool`, and non-boolean `int` values are accepted.
Lists, mappings, nulls, floats, non-finite values, bytes, secret wrappers, and
all other types fail without echoing the value. Although pinned Strimzi also
converts numeric, boolean, and list values, this first slice deliberately
excludes lists and floating-point spellings.

All accepted values are emitted as YAML strings because Kafka configuration is
textual at the Topic Operator boundary:

- strings are preserved byte-for-byte after rejecting NUL, Unicode control
  characters, and surrogate code points;
- booleans become lower-case `"true"` or `"false"`; and
- integers become their base-10 representation without a leading `+`.

Configuration keys are sorted lexicographically. Parsing the emitted YAML MUST
recover every configuration value as a string.

## Canonical YAML and atomic files

Serialization is UTF-8, LF-only, Unicode-unescaped, block-style YAML with no
aliases, anchors, explicit tags, end markers, comments, timestamps, or
environment-dependent values. Every non-empty document begins with `---`; the
stream has one final newline and no trailing `...`. Mapping order is exactly
the order specified above, with sorted configuration keys.

The complete document tuple MUST pass strict local validation before any byte
is written. For `--output-file`, serialization completes in memory, then a
same-directory private temporary file is written, flushed, `fsync`ed, and
atomically replaced. Failure preserves an existing target and removes every
staging file. Symlink and non-regular-file targets fail closed.

## Secret-neutral failure boundary

The exporter never reads runtime endpoints or constructs provider/state
objects. It MUST NOT expose project runtime secrets, connection configuration,
connector configuration, SQL, environment-variable values, Python exception
text, temporary paths, or rejected configuration values in output, warnings,
errors, logs, or object representations.

All failures use `E509_STRIMZI_INVALID`, a fixed secret-neutral message, and a
safe structural location. Tests MUST seed distinct confidential sentinels in
Kafka runtime endpoints, Schema Registry, Flink, Connect, Gateway, state,
connection configuration, SQL, omitted tags, and rejected topic-config values
and prove that none reaches success or failure surfaces. Separate public
identity sentinels MUST be used for project, physical topic, and logical owner
names. For managed topics those values MUST appear only in their exact
allowlisted document fields and JSON defensive copies; they MUST remain absent
from warnings, errors, logs, exception text, and unrelated fields. An omitted
external topic's public identities MUST not appear because it has no emitted
document. Expected public identities in those allowlisted fields are not
classified as secrets.

## Validation evidence levels

The following evidence MUST remain distinct:

1. **Closed local validation** checks the exact streamt document shape,
   ownership, identities, scalar normalization, cardinality, sorting, and
   canonical bytes.
2. **Pinned CRD validation** validates each document against the `v1`
   `openAPIV3Schema` extracted from the exact bundled KafkaTopic CRD whose hash
   is recorded above. Kubernetes extensions are handled explicitly. Because
   the CRD preserves unknown `config` fields and leaves metadata broadly typed,
   this is not a substitute for the closed validator.
3. **Kubernetes API acceptance** performs server-side dry-run against an
   ephemeral API server with the exact CRD installed.
4. **Real-cluster acceptance** applies the installed-wheel output to the pinned
   Strimzi Topic Operator and Kafka cluster, waits for `Ready=True`, and proves
   exact custom-resource and broker read-back plus idempotent replay.

Passing levels 1 and 2 supports only the claim “offline validated Strimzi 1.2.0
KafkaTopic output.” A working GitOps integration claim requires all four.
None of the levels supports production Kubernetes apply, credentials, TLS,
multi-namespace operation, deletion, reconciliation ownership transfer, or
upgrade safety.

## Release boundary

Source, installed-wheel, and installed-sdist exports MUST be byte-identical on
Python 3.10 through 3.14. Separate wheel and sdist installations run in clean
environments outside the checkout. The smoke harness may start each installed
executable, but the exporter process is guarded against network, DNS, HTTP,
socket, and child-subprocess use and against provider, deployer,
deployment-planner, and deployment-state construction. The wheel and sdist
MUST contain the pinned schema and notices, contain no Kubernetes or Strimzi
Python SDK, and add no runtime dependency or extra.

The real gate uses installed streamt output generated before any cluster
mutation. It is test-only and pinned to:

- kind `v0.33.0`, annotated tag object
  `49aeee6b958d818ae881752fe5b09220b39b6f55`, peeled to source commit
  `407a9675e6d9af1200b5f57f9ca52ec6cdacce74`;
- `kindest/node:v1.35.8@sha256:07b2536e30b803ed61d1677a79df6115f798ce64c80f9e22f6ed45afd09323c0`;
- Strimzi operator
  `quay.io/strimzi/operator@sha256:77f8fa8121a67561c3418de985783d197f51b8931e9a47f793dc0437dc6bb21f`;
- Kafka 4.3.1
  `quay.io/strimzi/kafka@sha256:e90a1a74af4226f3ca4d1ebef3ab13bdb09754ae17ca4c1444f7fcbb0ca8ea9a`;
  and
- `kubectl` v1.35.8, whose official Linux amd64 and arm64 SHA-256 values are
  respectively `874d5e72dbb819f43cff16bcd1e4f8bac5b7f2361fe1e55049b0a6c676fb0cbf`
  and `cc749967b62f4422260bc9c0aa7a7c55f45175ae38cb8d95767b5d2b7e04c1fd`.

The pinned Strimzi support matrix includes Kubernetes 1.35 and Kafka 4.3.1.
The test cluster is single-node, KRaft, ephemeral, loopback/internal-only, and
contains no production data. It validates interoperability; it is not a
production deployment recommendation.
