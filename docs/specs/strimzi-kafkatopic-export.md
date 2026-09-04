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
| Single-node example | [`examples/kafka/kafka-single-node.yaml`](https://raw.githubusercontent.com/strimzi/strimzi-kafka-operator/6c7b43c4af0db547c10463ba09d1dfa6f5e156a0/examples/kafka/kafka-single-node.yaml) | `2e7739e13dc250ccd00872bc6acf08dbf7fe768b9b76afcbef0dc733ede7b9ea` |
| Ephemeral-storage example | [`examples/kafka/kafka-ephemeral.yaml`](https://raw.githubusercontent.com/strimzi/strimzi-kafka-operator/6c7b43c4af0db547c10463ba09d1dfa6f5e156a0/examples/kafka/kafka-ephemeral.yaml) | `dd12c1e217e7ff348f5be81f9289a6f8c809db5bf4d5bb6b14e24ef7156d4930` |

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

The command validates those required values itself rather than delegating the
missing-value case to Click, so absence and malformed input use the same
`E509_STRIMZI_INVALID` boundary defined below.

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

Quiet success with an output file emits no stdout or stderr, including when the
root output mode is JSON. Quiet failure still emits the ordinary JSON error
envelope in JSON mode and the fixed stderr error because quiet never suppresses
errors. A non-quiet JSON success emits warnings only in the envelope and keeps
stderr empty; a non-quiet text success emits each warning occurrence to stderr
in its frozen order.

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

When present, JSON `output_file` is the exact lexical argument supplied by the
caller, not a resolved absolute path. Project-parser compatibility warnings are
suppressed at this boundary; only the frozen W120 and W121 mapper warnings can
appear in Strimzi output.

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
logical owner annotation values reject NUL, Unicode control characters,
surrogate code points, and the exact code points U+FEFF, U+FFFE, U+FFFF, and
U+10FFFF that the pinned canonical YAML emitter would backslash-escape; accepted
values are otherwise exact. The complete annotation map MUST remain within the
Kubernetes 256 KiB annotation limit. `config` is always
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
  characters, surrogate code points, and U+FEFF, U+FFFE, U+FFFF, and U+10FFFF,
  which the pinned canonical YAML emitter would otherwise backslash-escape;
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

Strings matching the frozen conservative quoting predicate derived from the
Kubernetes YAML conversion stack's
[go-yaml v2 resolver](https://github.com/go-yaml/yaml/blob/v2.4.0/resolve.go)
MUST use single-quoted scalar style. The predicate contains the resolver's
exact boolean, null, merge-key, infinity, and NaN words. It also removes every
underscore and quotes lexical decimal/scientific-number forms, base-prefixed
integer forms, and values with a timestamp-shaped `YYYY-M-D` prefix. This
includes exponent-only floats such as `1e3`, YAML 1.2 octal values such as
`0o7`, and ambiguous mapping keys. It is intentionally a conservative lexical
superset: values such as `_1`, invalid timestamp-shaped strings, or numeric
overflow strings MAY be quoted even when the resolver would retain a string.
The quoting preserves the exact string type and value; it is not normalization.
Strings outside this frozen predicate retain the pinned emitter's ordinary
style, so the rule does not change the reviewed fixture bytes.

No accepted scalar is serialized with a backslash-u or backslash-U Unicode
escape. The narrow four-code-point exclusion above is part of the canonical
transport contract; it does not normalize or rewrite accepted input.

The complete document tuple MUST pass strict local validation before any byte
is written. For `--output-file`, serialization completes in memory, then a
same-directory private temporary file is written, flushed, `fsync`ed, and
atomically replaced. Failure preserves an existing target and removes every
staging file. Symlink and non-regular-file targets fail closed.

Missing parent directories are created before staging. The destination is
checked with `lstat` before staging and again immediately before replacement;
directories, FIFOs, sockets, devices, symlinks, and a destination swapped to
one of those types by either observation all fail. The randomized staging file
is in the destination directory with mode `0600`, and its identity is likewise
checked against the open descriptor immediately before replacement. Cleanup
covers descriptor creation, wrapping, write, flush, `fsync`, close, and
replacement failures, including `BaseException`, while command-level error
conversion does not catch `BaseException`.

The output directory is a caller-controlled trust boundary. As with portable
same-directory temporary-file writers generally, mutation by another actor
with write access after either final `lstat` sample and before `os.replace`
cannot be distinguished atomically on every supported platform. `os.replace`
does not follow a destination symlink, so a post-sample destination swap cannot
write through to its referent; nevertheless, callers MUST NOT select a
directory writable by an untrusted actor. Tests cover every mutation observable
at the frozen checks and prove that replacement never follows a destination
symlink.

## Secret-neutral failure boundary

The exporter never reads runtime endpoints or constructs provider/state
objects. It MUST NOT expose project runtime secrets, connection configuration,
connector configuration, SQL, environment-variable values, Python exception
text, temporary paths, or rejected configuration values in output, warnings,
errors, logs, or object representations.

All failures contained after construction of the export formatter use
`E509_STRIMZI_INVALID` with the exact message `Strimzi export failed safely`
and one safe structural location. Click's argument-tokenization errors (for
example, an option token with no following value or an unknown option) and a
failure to construct the formatter itself remain framework/bootstrap failures
outside this boundary. Missing or domain-invalid namespace and cluster-name
values are parsed successfully and MUST use the E509 boundary. The location for
a contained failure is selected from this closed table:

| Location | Failure phase |
| --- | --- |
| `target.namespace` | missing or invalid namespace |
| `target.cluster_name` | missing or invalid cluster name |
| `output` | quiet without an output-file sink or another output-mode invariant |
| `project` | project path resolution, parsing, environment selection, or validation |
| `manifest` | dry-run compilation |
| `manifest_checksum` | whole-manifest identity calculation |
| a mapper location from its closed allowlist | strict artifact mapping or document validation |
| `output_file` | parent creation, destination validation, staging, write, sync, close, or replace |
| `stdout` | final raw-text or JSON-envelope write/flush |
| `export` | unexpected ordinary-exception containment fallback |

The mapper allowlist is exactly `project`, `manifest_checksum`, `target`,
`artifacts`, `artifacts/<KIND>` for one of the eight frozen artifact collection
keys, `documents`, and the result-factory locations `export.manifest_checksum`,
`export.counts`, `export.documents`, `export.warnings`, and `export.yaml`. Any
other mapper location becomes `export`.

The command catches its named parse, validation, compile, identity, mapper,
schema, YAML, and I/O failures at the phase locations above. A final
`except Exception` containment guard maps every remaining ordinary exception to
`export` without exposing its text; it deliberately does not catch
`BaseException`. Before emitting an error it clears any materialized data,
warnings, and prior errors, so an error-envelope retry cannot include material
that stdout has not already accepted. Parser and compiler logging is suppressed
for this command even under root `--verbose`.

Stdout is a non-transactional boundary. A write can accept some or all bytes
and then fail during that write or its flush; accepted bytes cannot be
retracted. When the stream is known to be untouched, JSON mode retries with the
fixed E509 envelope. Once a write may have begun, the command exits `1` and
prints the fixed E509 message to stderr but MUST NOT append a second JSON
envelope to the possibly partial or complete first one. Raw text has the same
irretractable-write limitation. This exception does not permit confidential
data on the success surface: only the already-frozen public document and
defensive result fields can have been accepted before the transport failure.

Tests MUST seed distinct confidential sentinels in Kafka runtime endpoints,
Schema Registry, Flink, Connect, Gateway, state,
connection configuration, SQL, omitted tags, and rejected topic-config values
and prove that none reaches success or failure surfaces. Separate public
identity sentinels MUST be used for project, physical topic, and logical owner
names. For managed topics those values MUST appear only in their exact
allowlisted document fields and JSON defensive copies; they MUST remain absent
from warnings, errors, logs, exception text, and unrelated fields. An omitted
external topic's public identities MUST not appear because it has no emitted
document. Expected public identities in those allowlisted fields are not
classified as secrets.

A fresh import of `streamt.cli` and of its Strimzi export command module MUST
not import deployers, the deployment planner, providers, state backends, state
services, network clients, or subprocess helpers. Command invocation may load
`streamt.core.runtime` and `streamt.core.deployment_state` only as project
configuration models needed by the existing parser/compiler; it MUST NOT
construct or import deployment behavior from those layers.

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
   Kafka identity metadata MUST validate before polling state is classified. An
   absent or null initial `.status` is pending, a present non-mapping status is
   invalid, and a structurally valid but not-yet-observed or not-yet-Ready
   status remains pending.

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
  `quay.io/strimzi/kafka@sha256:fef34b5438e8556cc08c01f3e254e47346f061b53a4e38d4289853777e0ea7f1`;
  and
- `kubectl` v1.35.8, whose official Linux amd64, Linux arm64, and Darwin arm64
  SHA-256 values are respectively
  `874d5e72dbb819f43cff16bcd1e4f8bac5b7f2361fe1e55049b0a6c676fb0cbf`,
  `cc749967b62f4422260bc9c0aa7a7c55f45175ae38cb8d95767b5d2b7e04c1fd`,
  and `b8be50ae0c6665b646fb009f904a52cad30806deee19ab3b4fe5af2d68bd82eb`.

The official kind v0.33.0 Darwin arm64 binary used by the reviewed local pilot
has SHA-256
`0c8c7dbe5e23594a198b786c4bc13dacc101fa6196b0cb0b23a1ca44e61f4b4f`.
Host-tool selection and container-image selection are separate: the local
pilot uses Darwin arm64 kind/kubectl binaries with Linux arm64 image children,
while the release lane uses Linux amd64 for both. The gate MUST derive the
image platform from the Docker server, not from the host Python platform.

Locked GitHub release URLs may follow exactly one HTTPS redirect from
`github.com` to `release-assets.githubusercontent.com`; the signed destination
query is transport metadata and is not a provenance identity. Every other
redirect source, target, or count fails. Raw GitHub and `dl.k8s.io` inputs are
required to remain direct. In every case the bounded downloaded bytes MUST
match the frozen SHA-256 before use.

The reviewed 27-document operator asset contains exactly three Kafka image-map
environment variables: `STRIMZI_KAFKA_IMAGES`,
`STRIMZI_KAFKA_CONNECT_IMAGES`, and
`STRIMZI_KAFKA_MIRROR_MAKER_2_IMAGES`. Each source value MUST be a canonical,
newline-terminated map with the exact ordered keys `4.2.0`, `4.2.1`, `4.3.0`,
and `4.3.1`; the operator contract freezes both that ordered key list and the
complete source-value SHA-256. The structural rewrite MUST retain all four keys
in that order and map every key to the same selected-platform pinned Kafka
4.3.1 child reference. Missing, extra, duplicate, reordered, malformed,
indirect, or mutable source or rewritten entries fail closed. Versions 4.2.0,
4.2.1, and 4.3.0 are startup-compatibility aliases required by Strimzi's full
lookup, not supported test workloads: the Kafka custom resource MUST still
select exactly 4.3.1, pull policy remains `Never`, and workload image closure
permits only the pinned child.

The node, operator, and Kafka digests above are immutable multi-platform index
identities. The gate's image lock MUST additionally freeze, for each supported
Linux runner architecture, the selected child-manifest digest and that
manifest's config digest:

| Image | Linux amd64 manifest / config | Linux arm64 manifest / config |
| --- | --- | --- |
| kind node | `sha256:0c58cebbb66d7fa5fd497235dfae1e4e722ff84104e24a6f736ce8cd607cbe7c` / `sha256:194068f84949f79dca8527c1e0578d9cd90f0bcd82a359bdb0d2d5bfe9d61185` | `sha256:b38a25576c835bfedc9d06368f87ec40863459a4d5dcbdbab2fd5f58ecf97466` / `sha256:664b3989afaffcd2268ece28d6cf012b27700e6b8e81c3c7641cc167889075f5` |
| Strimzi operator | `sha256:6df3bf9f92d3d1907aca08ade8c6df6cdacd2e235756afad419ad582ce6a2c4e` / `sha256:307ebd6e0fd9121e0775b1cf0f06a5658cece38c58d46082512b910a7d095ce3` | `sha256:ee8d9fb08ede3778120c33c42c70da16762b531d70e32790b9e2ff932e040927` / `sha256:693db9e33a50f7cc1cd84cb763ee083c5209412b16f9f198ca013546da44f4f1` |
| Kafka 4.3.1 | `sha256:1699c345852618c02ed58a168923871ad3a4d9012e4181ecaa138c9bc55a8b6d` / `sha256:ba984c01faaf5b9d9ccc2aeba9ec7e2177a970caec767dfa477b8d8a94df98f3` | `sha256:ffba1669b6daa7e186a17b0c49b48f4dfd8ef5872720e0eec9bf7c4612dd1bcb` / `sha256:5f6ad7b02f27af240676afddbe36b63c419bc4cdfcf1b012db989b6d4fc4f684` |

The previous test lock labeled Strimzi's official
`1.2.0-kafka-4.3.0` image chain as Kafka 4.3.1. The corrected lock replaces
that index and both platform children/configs in full; no identity from the
mislabeled 4.3.0 chain remains eligible. After the Kafka resource is Ready and
the complete namespace workload/exposure/image closure passes, the gate MUST
execute `/opt/kafka/bin/kafka-topics.sh --version` in the exact selected
`kafka` broker container. Before any KafkaTopic dry-run or apply, that bounded
command MUST return exit zero, exact stdout `4.3.1\n`, and empty stderr. Its
exact stdout is retained as secret-scanned `kafka-version.txt` evidence.

Index digests remain the release-provenance roots. The gate MUST resolve the
runner platform through every frozen index. Docker MUST pull and create the
kind node from the selected node child and verify its frozen config identity.
The gate MUST inspect each selected child manifest directly and require its
`config.digest` to equal the frozen config digest; Docker's local image `Id`
is not, by itself, config-digest evidence. Local inspection accepts only the
two reviewed backend representations: the classic image store reports
`Id=<config-digest>` with no descriptor, while Docker's containerd image store
reports `Id=<child-manifest-digest>` and
`Descriptor.digest=<child-manifest-digest>`. Both forms MUST also report the
exact singleton child `RepoDigests` entry and the selected Linux OS and
architecture. The created kind node's `Config.Image` MUST be that exact child
reference, and its resolved `Image` MUST equal the corresponding frozen config
or child digest according to those same two representations.
The gate MUST pull and load the selected operator and Kafka children into the
node. It MUST sample bounded exact `ctr --namespace=k8s.io images list -q`
output immediately before and after each load. A classic load delta is exactly
the Quay child reference plus the frozen config-digest pseudo-reference. A
Docker Desktop/containerd load delta is exactly the config pseudo-reference
plus two bare, same-date, calendar-valid
`import-YYYY-MM-DD@sha256:<digest>` references: one for the selected child and
one distinct outer digest. Linux OCI conversion, as observed in evidence run
`33899306524`, has the same exact three-name delta but neither import digest is
the selected child. Operator and Kafka MUST use the same one of the three
representations (`classic`, `desktop`, or `oci-converted`). Import sources MUST
be disjoint across loads. Each current inner/outer digest MUST be disjoint from
every prior inner/outer and layer digest, and each current layer digest MUST be
disjoint from prior inner/outer digests. All current transformed and layer
digests MUST also be disjoint from the selected node, operator, and Kafka
manifest/config identities. Layer digests MAY repeat across operator and Kafka:
the pinned images on both supported Linux platforms legitimately share
immutable rootfs layers.

Before classifying either load, the gate MUST write its already validated,
lexicographically sorted before and after inventories as
`ctr-load-<I>-before.txt` and `ctr-load-<I>-after.txt`, where `<I>` is exactly
`0` for operator and `1` for Kafka. From the exact set difference it selects at
most two names accepted by the strict, calendar-valid bare
`import-YYYY-MM-DD@sha256:<digest>` grammar, sorts them lexicographically, and
reads each digest with the existing bounded `ctr content get` command. Exit
status MUST be zero and stderr empty. Each result MUST be a bounded JSON object
parsed with duplicate-key rejection and is written as
`ctr-load-<I>-import-<J>.json`, with `<J>` assigned by that sorted order. The
canonical artifact has exactly `source`, `raw_content_sha256`, and `content`:
the first is the import name, the second is the SHA-256 identity of the exact
raw bytes, and the third is the parsed JSON value. It contains no base64 or
opaque raw-content copy. Invalid JSON, duplicate keys, a secret match, or an
evidence write failure rejects the candidate evidence set and produces only
the fixed marker at staging. A nonzero result, nonempty stderr, runner
exception, subprocess timeout, or exhausted global deadline during any
selected import's read has the same marker-only result; a partial import
diagnostic set is never staged. JSON `NaN`, `Infinity`, and `-Infinity` are
invalid at this and every other gate JSON input boundary. These files are both
staged evidence and the retained, validated inputs to the explicit three-way
load classifier; they do not authorize any fourth or partial representation.

An OCI-converted pair is accepted only when both source suffixes equal the SHA-256
of their exact raw content and the contents are exactly one inner OCI manifest
and one outer OCI index. The inner object has only `schemaVersion`,
`mediaType`, `config`, and `layers`: schema version 2, OCI manifest media type,
a closed OCI config descriptor containing the exact frozen config digest and
positive non-boolean size, and a nonempty ordered layer list. Every layer is a
closed descriptor with OCI uncompressed-layer media type, a unique valid
SHA-256 digest, and positive non-boolean size. The outer object has only
`schemaVersion`, `mediaType`, and `manifests`: schema version 2, OCI index media
type, and one closed OCI-manifest descriptor pointing to the inner raw digest
with size exactly equal to the inner raw byte length. Extra annotations or
fields fail closed.

For OCI conversion the gate also reads the frozen config digest with bounded
`ctr content get`, requires zero status, empty stderr, and raw SHA-256 equality,
and writes canonical `ctr-load-<I>-config.json` evidence. The parsed config may
contain normal image-config root fields, but `rootfs` MUST exist and contain
exactly `{type: "layers", diff_ids: [...]}`. Its ordered, valid SHA-256
`diff_ids` MUST equal the ordered inner layer digests, and the inner config
descriptor size MUST equal the config raw byte length. Config evidence obeys
the same marker-only parse/secret/write failure boundary.

For the Desktop form, the gate MUST additionally validate its previously
reviewed exact-child outer shape by reading the exact outer content by digest.
The raw content SHA-256 MUST equal that outer digest and decode as a closed OCI
image index with schema version 2 and exactly one Docker schema-2 manifest
descriptor. That descriptor MUST identify the frozen child, have a positive
integer size, and contain exactly the locked Quay source annotation for
`strimzi/operator` or `strimzi/kafka`. For either imported form, the gate MUST
tag the validated inner import to the exact Quay child, re-enumerate the names,
remove only the two discovered import
references with `ctr images rm` (never `--sync` or content deletion), and prove
the resulting relevant name set is exactly the prior inventory plus the target
and frozen config pseudo-reference. The pre-removal recheck closes accidental
target expansion; its narrow observation-to-removal race is excluded by the
gate's exclusive ownership of the unique, disposable, pre-workload node.

If and only if both loads used the same imported form (`desktop` or
`oci-converted`), the gate MUST restart containerd exactly once, wait boundedly
for that exact Kubernetes Node to be
Ready, and repeat Docker attachment, loopback-port, empty IPv4/IPv6 default
route, positive local TCP, and negative external TCP controls. It MUST then
prove the normalized `ctr -q` inventory did not change. The final CRI record
for each selected image MUST have no repo tags, contain only the exact Quay
child repo digest, and map it to the frozen config identity. The all-classic
path performs no tag, removal, or restart. Missing, extra, partial, mixed,
ambiguous, or differently shaped identities fail before any Strimzi object is
applied. The gate then uses those child references in applied workloads.
For every selected workload container and init container, `spec.image` MUST be
the exact pinned child reference. Kubernetes `status.*ContainerStatuses[].image`
is a backend display field and may be only that same child reference or its own
frozen bare config digest; an index, manifest digest, other image's config,
tag, absent value, or non-string fails closed. The separate `imageID` field is
the runtime identity: pilot mode records one exact consistent value per child,
and normal mode requires exact equality to that reviewed lock. Accepting the
config form in `status.image` does not relax or substitute the `imageID` check.
Raw Kubernetes 1.35 pod and service collection reads MUST use the exact generic
`apiVersion: v1`, `kind: List` envelope with root keys limited to
`apiVersion`, `kind`, `metadata`, and `items`, and metadata exactly
`{resourceVersion: ""}`. Every item MUST identify itself as `v1` and exactly
`Pod` or `Service` for the requested collection. The gate may normalize a
validated pod collection to an internal `PodList`; raw typed `PodList` and
`ServiceList` envelopes, mixed item kinds, and malformed identities fail
closed.
Before the real lane is enabled, a reviewed pilot MUST record the exact
Kubernetes `imageID` representation produced by the pinned node/runtime
combination; subsequent runs compare exact values from the image lock rather
than accepting an arbitrary index, child, or config form.

The test-only gate exposes pilot mode only while the selected operator and
Kafka `imageID` locks are null. A pilot runs the full reconciliation and replay
flow, proves exact applied image references and CRI content first, records one
consistent observed ID per image in secret-scanned evidence, cleans up, and
returns a distinct unsuccessful result. It is discovery evidence, never an
acceptance pass. Normal mode rejects a null lock before mutation, pilot mode
rejects an already frozen lock, and the permanent CI lane MUST use normal mode.

The disposable kind topology uses one uniquely named Docker `bridge` network
created with the exact option
`com.docker.network.bridge.enable_ip_masquerade=false`. It is deliberately not
a Docker `--internal` network: Docker Desktop suppresses the required
loopback-published API binding on an internal network, and kind cannot finish
bootstrapping DNS there because the node starts without a default route. The
gate MUST require `Driver=bridge`, `Internal=false`, and exactly one of two
reviewed option representations: the classic singleton containing only
`com.docker.network.bridge.enable_ip_masquerade=false`, or Docker
Desktop/containerd's map that additionally reports
`com.docker.network.enable_ipv4=true` and
`com.docker.network.enable_ipv6=false`. Partial maps, wrong values, and unknown
options fail closed. The gate also requires no
attachment before kind and exactly the derived control-plane attachment after
kind. Immediately after kind becomes ready, the gate deletes the node's
IPv4 default route, requires both `ip -4 route show default` and
`ip -6 route show default` to return exact empty output, and only then proves a
positive TCP connection to the node-local API on `127.0.0.1:6443` and a failed
connection to literal external `1.1.1.1:443`. The attachment, sole loopback API
publication, and empty dual-stack default-route inventories are fail-closed.
The gate repeats the network-attachment and dual-stack route checks after topic
replay, before declaring success. No operator or Kafka object may be applied
until the first isolation proof passes.

The gate has an internal deadline shorter than its CI job timeout so it can run
bounded evidence capture and exact cleanup/residue checks before failure-only
artifact staging and upload. Evidence candidates MUST remain outside a fresh
upload directory. The exact evidence inventory includes one
`ctr-load-<I>-{before,after}.txt` pair per attempted load (four files on the
complete two-load path), zero to two conditional sorted
`ctr-load-<I>-import-<J>.json` files per attempted load, one conditional
`ctr-load-<I>-config.json` for each OCI-converted load, and `ctr-images.txt`, a
bounded, secret-scanned, lexicographically sorted canonicalization of
`docker exec <exact-node> ctr --namespace=k8s.io images list -q`; capture MUST
attempt it on success and failure paths, including when the node or kubeconfig
is absent. It accepts at most 4,096 LF-delimited references of at most 512
printable ASCII bytes each, rejects control bytes, URI schemes, credentials,
queries, fragments, and backslashes, and requires every `@` suffix to be an
exact lowercase SHA-256 digest. Syntactically safe unexpected references remain
in the sorted diagnostic inventory rather than being identity-allowlisted. A
nonzero command or zero exit with nonempty stderr produces only the fixed
neutral failure record; stderr is never published. There is no partial
redaction: an unsafe reference, parse failure, secret match, or evidence write
failure rejects the candidate set and yields marker-only staging. An ordinary
evidence command that returns nonzero is represented at
its original filename by fixed JSON containing only its integer return code and
`capture-failed` status; a runner exception or timeout is represented by a
fixed `<filename>.failed` artifact with no raw diagnostic. Either case remains
a complete, safe capture and does not by itself force marker-only staging. Only
bounded files that pass a complete secret scan may be copied into the upload
directory. If the capture deadline prevents representing every scheduled item,
or on a secret match, size violation, or candidate scan/read/write failure, no
candidate file may be staged; the upload directory contains only a fixed
secret-neutral scan-failure marker. Cleanup is enforced for success, ordinary
failure, and internal timeout. Hosted-runner cancellation or an external
SIGKILL can prevent process-level cleanup, so that case is best-effort and
relies on runner disposal; it MUST NOT be described as a guaranteed trap.

On every path that reaches the successful runtime-version proof, the full
evidence inventory also contains `kafka-version.txt` with exactly `4.3.1\n`;
the marker-only staging boundary still applies if a later evidence-safety
failure occurs. A version probe failure is recorded by the fixed
summary/capture failure surfaces and cannot cross the KafkaTopic mutation
boundary.

The pinned Strimzi support matrix includes Kubernetes 1.35 and Kafka 4.3.1.
The test cluster is single-node, KRaft, ephemeral, loopback-published and
route-sealed from external networks, and contains no production data. Kafka
process requests and limits are declared on the `KafkaNodePool`, as required
by the pinned `v1` schema; Topic Operator
requests and limits are declared on `Kafka.spec.entityOperator.topicOperator`.
It validates interoperability; it is not a production deployment
recommendation.
