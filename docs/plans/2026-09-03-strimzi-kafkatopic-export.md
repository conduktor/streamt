# Strimzi KafkaTopic GitOps export implementation plan

## Objective and status

Implement the narrow, deterministic boundary in
[`Strimzi KafkaTopic GitOps export`](../specs/strimzi-kafkatopic-export.md).

Status on 2026-09-04: Slices 0 through 4 are complete. The pinned CRD, license,
provenance notice, reviewed byte fixtures, wheel/sdist boundary checks, strict
topic parser, pure manifest identity, closed document contract, and pinned-CRD
validation are frozen. The pure mapper now emits defensive documents and
canonical Kubernetes-safe YAML with exact omission warnings and counts. The
offline CLI, lazy command registry, secret-neutral failure boundary, and atomic
optional output are implemented. Clean source, wheel, and direct-sdist parity
now runs on Python 3.10 through 3.14. Slice 5's reviewed local Linux arm64 pilot
and subsequent normal-mode run pass the complete real Strimzi 1.2.0/Kafka 4.3.1
flow, and the learned Kubernetes image IDs are frozen. The Linux amd64 CI pilot
and permanent normal-mode CI lane remain before Slice 5 is complete. No current
support claim changes.

The specification owns the public contract. A test or implementation conflict
must be resolved in the specification before code lands.

## Frozen implementation boundary

- additive `streamt export strimzi` command;
- Strimzi `1.2.0`, `kafka.strimzi.io/v1`, `KafkaTopic` only;
- explicit namespace and cluster name;
- one dry-run compile and strict use of manifest topic artifacts;
- every managed compiled topic artifact, including model-owned and source-owned
  compiler-created DLQs;
- deterministic hashed Kubernetes identity when the Kafka name is not a
  DNS-1123 label;
- string, boolean, and integer topic-config inputs normalized to YAML strings;
- whole-manifest secret-neutral checksum annotation;
- canonical multi-document YAML and atomic optional file output;
- no network, provider, state, subprocess, deletion, pruning, or apply behavior;
  and
- installed-package parity plus a separate digest-pinned real Strimzi gate.

## Slice 0 — source freeze and fixtures

### Ownership

- `src/streamt/integrations/gitops/schemas/`
- `tests/fixtures/strimzi/1.2.0/`
- focused distribution-inspection assertions

Do not implement mapping or CLI behavior in this slice.

### Work

1. Download the exact KafkaTopic CRD from commit
   `6c7b43c4af0db547c10463ba09d1dfa6f5e156a0`, verify SHA-256
   `36390f0731c699448076d4ee739e8b7f331d083e91a7fb71500aaa830ab1127e`,
   and package it in the repository's existing compressed/base64 resource
   convention without changing its decoded bytes.
2. Record the Strimzi Apache-2.0 license and a notice containing release,
   commit, source URL, source digest, transformation, and retrieval date.
3. Add reviewed contract fixtures for a DNS-1123 Kafka name and a valid Kafka
   name containing upper-case/underscore characters that exercises hashed
   Kubernetes naming. Include string, boolean, and integer config inputs.
4. Freeze expected JSON document trees, canonical YAML bytes, metadata-name
   hashes, the whole-manifest secret-neutral checksum, exact warning tuples and
   count arithmetic, and empty-stream bytes. Include two manifests that differ
   only in a sensitive-key value and therefore intentionally have the same
   checksum, plus a represented non-secret change that changes it.
5. Make the distribution test reject a missing/changed schema or notice and
   reject Kubernetes, Strimzi, Helm, or OpenShift Python requirements/imports.

### Gate

```text
pytest -q <distribution-inspection-test> <fixture-contract-test>
python -m build
git diff --check -- src/streamt/integrations/gitops/schemas tests/fixtures/strimzi/1.2.0
```

## Slice 1 — strict topic boundary and validation

### Ownership

- `src/streamt/compiler/topic_artifact.py`
- `src/streamt/compiler/__init__.py` (lazy compatibility exports)
- `src/streamt/core/manifest_identity.py`
- `src/streamt/integrations/gitops/strimzi_validation.py`
- `src/streamt/deployer/plan_file.py` (import-only compatibility extraction)
- `tests/unit/test_topic_artifact.py`
- `tests/unit/test_manifest_identity.py`
- `tests/unit/test_strimzi_validation.py`

Do not change the catalog projection, planner/deployer behavior, state, runtime
YAML, or public CLI. The only deployer-file change is to import and re-export
the extracted pure checksum helper without changing its bytes.

### Work

1. Add a strict topic-artifact parser that rejects missing/extra fields,
   booleans masquerading as integers, partitions outside
   `1..2_147_483_647`, replication factors outside `1..32_767`, malformed
   config, absent ownership, project mismatch, and unsupported owner type or
   lifecycle mode. Accept only exact `model` and `source` lifecycle owner types;
   both can own managed compiler-created DLQs. Return an immutable defensive
   value.
2. Implement exact Kafka 4.3.1 topic-name validation and exact Kubernetes
   namespace, label-value, and DNS-1123-label validation.
3. Implement the full-SHA-256 metadata-name rule and reject duplicate Kafka or
   Kubernetes identities.
4. Normalize only string, boolean, and integer config scalars. Reject keys
   matching the exact frozen sensitive-key expression, including singular
   `credential`, and every unsupported value without rendering the value.
5. Load the packaged CRD with `importlib.resources`, verify the decoded digest,
   select exactly the served/storage `v1` schema, and fail on an unexpected
   CRD shape.
6. Validate every generated document twice: first with a closed invariant
   validator, then against the pinned CRD OpenAPI schema. Treat ignored
   Kubernetes `x-` extensions explicitly and document that this is structural
   evidence, not API-server admission.
7. Extract the existing `manifest_checksum()` normalization into the pure
   compiler-facing helper. Preserve its exclusion of `compiled_at` and fixed
   replacement of sensitive-key values, prove byte parity with the previous
   public import, and keep the GitOps integration free of deployer, planner,
   provider, and state imports.
8. Allocate `E509_STRIMZI_INVALID`,
   `W120_STRIMZI_EXTERNAL_TOPIC_OMITTED`, and
   `W121_STRIMZI_ARTIFACTS_OMITTED` without changing other code semantics.
9. Preserve the package-level `Compiler` and `Manifest` exports lazily so that
   importing the strict topic boundary does not eagerly import runtime,
   deployment, planner, provider, or state layers.

### Acceptance

- exact `1` and `2_147_483_647` partition bounds, `1` and `32_767` replica
  bounds, and Kafka-name lengths;
- `.`/`..`, Unicode, control, surrogate, slash, percent, empty, and overlong
  topic-name rejection;
- exact valid direct names and full 64-hex hashed names;
- bool-before-int handling; signed and arbitrarily large config integers
  normalized as strings; float/null/list/map/bytes rejection; string
  preservation; and lower-case boolean conversion;
- invalid ownership, collisions, unexpected CRD version/shape/digest, and
  malformed document fields fail closed;
- checksum parity, redacted-secret stability, non-secret sensitivity, and no
  deployer import are exact; and
- fresh-process imports of the topic and Strimzi validation boundaries load no
  runtime, deployment, planner, provider, or state modules while legacy
  compiler exports retain exact identity; and
- errors and representations contain no confidential sentinels or raw rejected
  values.

### Gate

```text
pytest -q tests/unit/test_topic_artifact.py tests/unit/test_manifest_identity.py tests/unit/test_strimzi_validation.py
ruff check src/streamt/compiler/topic_artifact.py src/streamt/core/manifest_identity.py src/streamt/integrations/gitops/strimzi_validation.py tests/unit/test_topic_artifact.py tests/unit/test_manifest_identity.py tests/unit/test_strimzi_validation.py
mypy src/streamt/compiler/topic_artifact.py src/streamt/core/manifest_identity.py src/streamt/integrations/gitops/strimzi_validation.py
```

## Slice 2 — pure mapper and canonical bytes

### Ownership

- `src/streamt/integrations/gitops/__init__.py`
- `src/streamt/integrations/gitops/strimzi.py`
- `tests/unit/test_strimzi_export.py`

### Work

1. Define immutable target inputs, warning records, and export result. Store
   document payloads defensively and prove YAML reserialization parity in the
   result constructor.
2. Read only strict topic artifacts and a supplied whole-manifest secret-neutral
   checksum. Do not read catalog snapshots, project runtime, SQL, connections,
   or state.
3. Emit every managed topic artifact in canonical physical-name order,
   including model-owned and source-owned DLQs. Omit external topics with W120,
   reject adopted topics, and issue one W121 for the exact seven non-topic
   collection counts, including the additive Connector-removal collection.
   Implement the specification's exact warning messages, locations, ordering,
   and aggregate count arithmetic.
4. Emit the exact keys, labels, annotations, explicit `spec.topicName`,
   partition/replica values, normalized configuration, and release pin from the
   specification.
5. Serialize using a safe no-alias dumper, fixed key insertion order,
   `sort_keys=False`, LF normalization, explicit `---`, no `...`, and one final
   newline. The empty export is exactly `b""`.
6. Re-run closed and pinned-schema validation against defensive document copies
   before constructing the result.

### Acceptance

- empty, one-topic, multiple-topic, model-owned DLQ, source-owned DLQ,
  valid-name, hashed-name, mixed ownership, collision, malformed artifact, and
  omitted-artifact fixtures;
- exact annotation values and stable whole-manifest secret-neutral checksum
  binding;
- deterministic warning order and exact canonical bytes across repeated runs;
- caller mutations cannot affect the result; and
- every accepted Unicode scalar remains unescaped in canonical YAML, while the
  exact four code points the pinned emitter would escape fail closed; and
- Go/Kubernetes-resolver-ambiguous string values and mapping keys retain their
  string type through deterministic single quoting; and
- confidential runtime, connector, SQL, tag, and rejected-config sentinels never
  leak; managed public project/topic/owner sentinels appear only in the exact
  allowlisted document fields; and omitted external identities remain absent.

### Gate

```text
pytest -q tests/unit/test_strimzi_export.py tests/unit/test_strimzi_validation.py tests/unit/test_topic_artifact.py
ruff check src/streamt/integrations/gitops src/streamt/compiler/topic_artifact.py tests/unit/test_strimzi_export.py
mypy src/streamt/integrations/gitops
```

## Slice 3 — CLI and atomic output

### Ownership

- `src/streamt/cli/commands/export.py`
- `src/streamt/cli/__init__.py`
- `src/streamt/core/errors.py`
- `tests/unit/test_cli_strimzi.py`

Do not add target fields to streamt project YAML or runtime configuration.

### Work

1. Add the `export` group and `strimzi` command with only the frozen options.
   Convert top-level CLI registration to a lazy command registry first so a
   fresh CLI import does not load every deployment command and state backend;
   preserve all current names, aliases, help behavior, and command identities
   on resolution.
2. Validate namespace and cluster name before parsing. Parse and validate the
   project, compile dry-run exactly once, compute the secret-neutral
   `manifest_checksum()` through the pure helper, map, validate, serialize
   fully, then optionally write.
3. Implement exact raw-text, file, quiet, and JSON behavior. Convert mapper
   warnings to structured warnings without changing order or exposing names.
   Track whether stdout may already have accepted bytes so a transport failure
   never appends a second JSON envelope to a partial or complete first one.
4. Reuse or extract a small same-directory atomic writer only when its behavior
   remains byte-for-byte compatible with existing exporters. Reject symlinks
   and non-regular destinations observed at either frozen identity check and
   clean staging files on every exception. Treat the output directory as a
   caller-controlled boundary because portable replacement cannot close the
   final sample-to-replace race against another actor with directory write
   access; prove that destination replacement never follows a symlink.
5. Catch the defined parse/environment/export/schema/YAML/I/O failures at their
   frozen phase locations, followed by one ordinary-`Exception` containment
   guard at `export`. Emit E509 with the exact fixed message and never print
   arbitrary exception text. Do not catch `BaseException` at command level.
6. Prove the command/import boundary does not import deployer, planner,
   provider, or state modules and that no Kubernetes client, socket,
   subprocess, or HTTP session is constructed.
7. Suppress parser/compiler logging for this command even under `--verbose`,
   suppress parser compatibility warnings, retain duplicate mapper warnings,
   and clear all formatter data and diagnostics before every failure flush.

### Acceptance

- required-option validation precedes parse/compile/write;
- compilation count is exactly one on success and zero for primitive failures;
- text stdout is raw-only, warnings are stderr-only, JSON has exact keys and
  order, quiet/no-file fails, and file mode has empty stdout;
- a pre-write JSON transport failure can emit one clean E509 envelope, while a
  write/flush failure after possible acceptance emits no concatenated retry;
- existing-file preservation, same-directory private staging, flush/fsync/
  replace order, permission errors, serialization errors, broken pipes, and
  cleanup are covered; and
- project paths, temporary paths, exception text, environment values, and all
  confidential sentinels remain absent; public project/topic/owner identity is
  confined to the specification's allowlisted success fields.

### Gate

```text
pytest -q tests/unit/test_cli_strimzi.py tests/unit/test_strimzi_export.py tests/unit/test_strimzi_validation.py
ruff check src/streamt/cli/commands/export.py src/streamt/cli/__init__.py src/streamt/core/errors.py tests/unit/test_cli_strimzi.py
mypy src/streamt/cli/commands/export.py
```

## Slice 4 — installed-package parity

### Ownership

- `tests/package/strimzi_package_smoke.py`
- focused CI workflow and release-workflow contract tests

### Work

1. Build wheel and sdist once and inspect both archives for the exact bundled
   CRD and notice. Reject target SDK dependencies, extras, imports, or vendored
   top-level namespaces.
2. For each Python 3.10 through 3.14 lane, create separate clean wheel and sdist
   environments. Install the wheel in one and install the verified sdist
   directly in the other with build isolation inputs already provisioned; do
   not substitute the wheel artifact for the sdist lane. Run `pip check` in
   both, move outside the checkout, and invoke each environment's installed
   `streamt` executable for both identity fixtures.
3. Run the same fixtures once through the reviewed source entry point, then
   compare exact source, installed-wheel, and installed-sdist artifact bytes,
   JSON envelopes, warnings, failure surfaces, and decoded CRD digest.
4. The smoke orchestrator may create environments and launch the three target
   processes. Guards injected into each target process deny socket, DNS, HTTP,
   and child-subprocess calls and deny import, construction, or use of all
   streamt deployers, planner, state services, and provider clients during the
   export itself.
5. Seed distinct confidential runtime, connection, SQL, tag, project-path,
   output-path, and rejected-config sentinels and inspect stdout, stderr,
   structured data, exceptions, artifacts, and object representations. Seed
   separate public project/topic/owner sentinels and require them only in exact
   allowlisted success fields.

### Gate

```text
python -m build
python tests/package/strimzi_package_smoke.py --wheel dist/<wheel> --sdist dist/<sdist> --source-root .
pytest -q tests/unit/test_release_workflow.py <distribution-inspection-test>
```

## Slice 5 — pinned real Strimzi acceptance

### Ownership

- `tests/integration/strimzi/1.2.0/`
- `tests/integration/strimzi_gate.py`
- `tests/unit/test_strimzi_gate_contract.py`
- `.github/workflows/ci.yml`

This slice consumes the wheel artifact from Slice 4. It does not import
production code from the checkout.

### Pinned topology

- kind `v0.33.0`, whose annotated tag object is
  `49aeee6b958d818ae881752fe5b09220b39b6f55` and whose peeled source commit is
  `407a9675e6d9af1200b5f57f9ca52ec6cdacce74`;
- Linux kind binary SHA-256:
  `aee6151561422756b764a4ae28e7f44cda5af5a9eead3cc9985112b1de8d8e0d`
  for amd64 and
  `20022bee6cfcd5086cb7234d218e3454e6090022f2a8f55d1fa7fcf42c3867a2`
  for arm64; the reviewed local pilot instead uses the official Darwin arm64
  binary with SHA-256
  `0c8c7dbe5e23594a198b786c4bc13dacc101fa6196b0cb0b23a1ca44e61f4b4f`;
- Kubernetes `v1.35.8` node image at
  `kindest/node:v1.35.8@sha256:07b2536e30b803ed61d1677a79df6115f798ce64c80f9e22f6ed45afd09323c0`;
- pinned upstream fixture inputs from commit
  `6c7b43c4af0db547c10463ba09d1dfa6f5e156a0`:
  `examples/kafka/kafka-single-node.yaml` at SHA-256
  `2e7739e13dc250ccd00872bc6acf08dbf7fe768b9b76afcbef0dc733ede7b9ea`
  and `examples/kafka/kafka-ephemeral.yaml` at SHA-256
  `dd12c1e217e7ff348f5be81f9289a6f8c809db5bf4d5bb6b14e24ef7156d4930`;
- exact Strimzi install asset and digest from the specification, rewritten by a
  strict checked script according to the exhaustive image closure below;
- operator image
  `quay.io/strimzi/operator@sha256:77f8fa8121a67561c3418de985783d197f51b8931e9a47f793dc0437dc6bb21f`;
- Kafka 4.3.1 image
  `quay.io/strimzi/kafka@sha256:e90a1a74af4226f3ca4d1ebef3ab13bdb09754ae17ca4c1444f7fcbb0ca8ea9a`;
  and
- exact `kubectl` v1.35.8 binary/checksum selected by runner architecture.

`images.lock.json` freezes Linux amd64, Linux arm64, and Darwin arm64 host-tool
downloads. The first two serve release runners; Darwin arm64 serves the local
pilot and uses the exact kubectl digest recorded in the specification. Host
tools are selected from the host OS/architecture, while image children are
selected independently from Docker server `OSType` and `Architecture`; an
unsupported or inconsistent pair fails closed.

The three image references above are multi-platform index pins and remain the
provenance roots. `images.lock.json` also freezes the Linux amd64 and arm64
child-manifest and config digests listed in the specification. At runtime the
gate selects only the runner's locked platform and proves each child belongs to
its frozen index. Docker pulls and creates the kind node from its selected
child. The gate pulls and loads the operator and Kafka children into that node
and proves the CRI content maps them to the locked configs. Applied Strimzi
operator and Kafka image references use selected child digests, not index
digests.

The rewriter MUST parse YAML and classify every executable image field and
every Cluster Operator environment variable ending in `_IMAGE` or `_IMAGES`;
text replacement is insufficient. It MUST make exactly this closure and fail
on a missing, duplicate, or additional image-bearing field:

- rewrite the Cluster Operator Deployment image and
  `STRIMZI_DEFAULT_TOPIC_OPERATOR_IMAGE`,
  `STRIMZI_DEFAULT_USER_OPERATOR_IMAGE`, and
  `STRIMZI_DEFAULT_KAFKA_INIT_IMAGE` to the selected-platform operator child
  reference;
- rewrite `STRIMZI_DEFAULT_KAFKA_EXPORTER_IMAGE` and
  `STRIMZI_DEFAULT_CRUISE_CONTROL_IMAGE` to the selected-platform Kafka child
  reference;
- replace each of `STRIMZI_KAFKA_IMAGES`,
  `STRIMZI_KAFKA_CONNECT_IMAGES`, and
  `STRIMZI_KAFKA_MIRROR_MAKER_2_IMAGES` only after proving the exact ordered
  source keys `4.2.0`, `4.2.1`, `4.3.0`, and `4.3.1`, then rewrite all four
  ordered keys to `<SELECTED-PLATFORM-KAFKA-CHILD-REFERENCE>` with one canonical
  newline-terminated entry per key; and
- remove the optional and unreachable Kafka Bridge, Kaniko, Buildah, and Maven
  builder default-image environment variables. The exact Maven variable is
  `STRIMZI_DEFAULT_MAVEN_BUILDER`; it is part of the closed inventory even
  though its name does not end in `_IMAGE` or `_IMAGES`. No Bridge, Connect,
  build, MirrorMaker, User Operator, Kafka Exporter, or Cruise Control resource
  is permitted in the fixture.

The first three Kafka version-map entries are startup-compatibility aliases
required by Strimzi's complete supported-version lookup; they do not expand the
tested Kafka versions. The reviewed Kafka custom resource still selects exactly
`4.3.1`, every alias resolves to the same pinned 4.3.1 child, pull policy remains
`Never`, and the runtime workload closure permits no other Kafka image.

The same structural pass MUST close over namespaces. It adds
`metadata.namespace` to exactly these seven namespaced objects in the
27-document operator asset: the four RoleBindings named below and the
ServiceAccount, Deployment, and ConfigMap each named
`strimzi-cluster-operator`. It also rewrites exactly seven ServiceAccount
subject namespaces from `myproject` to the unique test namespace. The subjects
occur in the three
ClusterRoleBindings `strimzi-cluster-operator`,
`strimzi-cluster-operator-kafka-broker-delegation`, and
`strimzi-cluster-operator-kafka-client-delegation`, plus the four RoleBindings
`strimzi-cluster-operator-watched`,
`strimzi-cluster-operator-entity-operator-delegation`,
`strimzi-cluster-operator`, and
`strimzi-cluster-operator-leader-election`. A missing, duplicate, unexpected,
or already-divergent namespace target fails closed; `kubectl -n` is not treated
as a namespace rewriter.

Set `STRIMZI_IMAGE_PULL_POLICY=Never`. After rewriting, no tag-form image
reference may remain in an executable image field or `_IMAGE`/`_IMAGES`
environment value of the applied documents, and the Cluster Operator
Deployment's own `imagePullPolicy` MUST also be `Never`. The Kafka test fixture
is a reviewed one-node, dual-role KRaft derivative of the pinned Strimzi
single-node example, uses ephemeral storage and an internal plaintext listener,
enables only the Topic Operator, and contains explicit CPU/memory requests and
limits. Kafka process resources are set on `KafkaNodePool.spec.resources`, not
the schema-invalid `Kafka.spec.kafka.resources`; Topic Operator resources are
set on `Kafka.spec.entityOperator.topicOperator.resources`.

### Isolation and execution

1. Use unique, validated names for the kind cluster, Docker network, namespace,
   Kafka cluster, temp directory, and kubeconfig. Kubeconfig permissions are
   `0600`; its contents are never logged or uploaded.
2. Download and verify all tools and assets and pull the selected-platform kind
   node, operator, and Kafka child manifests by exact digest while runner egress
   is still available. Verify every frozen index-to-child relationship and each
   child manifest's own `config.digest` against `images.lock.json`. Treat the
   host image-store `Id` as backend-specific: accept only classic
   `Id=<config>` with no descriptor or containerd-store `Id=<child>` with an
   exact child `Descriptor.digest`, in both cases with the exact singleton child
   repo digest and selected Linux platform. Require the created node's resolved
   image to use the corresponding frozen config-or-child form. A locked GitHub
   release URL may follow exactly one HTTPS redirect from `github.com` to
   `release-assets.githubusercontent.com`; all other redirect behavior fails
   closed. Raw GitHub and `dl.k8s.io` URLs remain direct. Install the
   supplied wheel in a clean location outside the checkout, generate canonical
   YAML twice before creating any Docker network or cluster, and prove exact
   bytes under the offline exporter guards.
3. Create the unique Docker network explicitly with `docker network create
   --driver bridge --opt
   com.docker.network.bridge.enable_ip_masquerade=false`. Inspect the exact
   network ID and require `Driver=bridge`, `Internal=false`, and exactly one of
   two inspected option maps: the classic singleton containing only
   `com.docker.network.bridge.enable_ip_masquerade=false`, or Docker
   Desktop/containerd's triple that additionally reports
   `com.docker.network.enable_ipv4=true` and
   `com.docker.network.enable_ipv6=false`. Partial maps, wrong values, and
   unknown options fail closed. Require no
   initial container attachment before giving the network to kind. Do not use
   Docker `--internal`: Docker Desktop suppresses the loopback host binding
   needed by this gate, and kind cannot bootstrap DNS on that topology. Set
   `KIND_EXPERIMENTAL_DOCKER_NETWORK` to the exact network name and invoke pinned
   kind with the API server bound to `127.0.0.1`. This selector is experimental
   in kind v0.33.0, not a supported public interface; the gate deliberately pins
   and tests that implementation and MUST fail if the expected
   unsupported-selector warning or attachment behavior changes.
4. After cluster creation, inspect Docker rather than trusting configuration:
   require the one derived control-plane node to be the network's only
   attachment and to be attached to no other Docker network, require the only
   published cluster port to be the Kubernetes API bound to `127.0.0.1`, and
   require Kafka and operator services to have no host port. Immediately after
   kind is ready, run exact `docker exec <node> ip route del default`, then
   require exact empty output from both `ip -4 route show default` and
   `ip -6 route show default`. Check that `bash` and `timeout` exist in the
   pinned node, prove that the local `127.0.0.1:6443` TCP control succeeds, and
   require a bounded TCP probe to literal `1.1.1.1:443` to fail. A skipped or
   inoperable control, a nonempty default-route inventory, a successful external
   probe, or an unexpected attachment/port fails closed. Repeat the exact
   attachment and dual-stack route inventories after replay so later activity
   cannot silently reopen the node. Apply no operator or Kafka object before the
   initial isolation proof passes.
5. Only after the nodes exist, load the exact selected-platform operator and
   Kafka child images into every node. Sample bounded exact `ctr
   --namespace=k8s.io images list -q` output immediately before and after each
   load and classify the exact inventory delta. The all-classic representation
   adds only the exact Quay child plus its frozen config pseudo-reference. The
   all-Docker-Desktop representation adds only the config pseudo-reference and
   two bare, same-date, calendar-valid `import-YYYY-MM-DD@sha256:<digest>`
   references: one child digest and one distinct outer digest. Linux OCI
   conversion from evidence run `33899306524` has the same exact delta but
   neither import is the selected child. Require both images to use exactly one
   common mode (`classic`, `desktop`, or `oci-converted`). Reject shared import
   sources; current inner/outer overlap with prior inner/outer or layers;
   current-layer overlap with prior inner/outer; and any transformed/layer
   overlap with selected node/operator/Kafka manifest or config identities.
   Explicitly allow layer-layer reuse across images because the pinned operator
   and Kafka images share immutable rootfs layers on both supported platforms.

   Before the three-way classifier runs, persist the already validated sorted
   before and after inventories as `ctr-load-<I>-before.txt` and
   `ctr-load-<I>-after.txt` (`I=0` operator, `I=1` Kafka). Select no more than
   two newly added names matching the strict bare, calendar-valid import
   grammar, sort them, and read their exact digests with the bounded existing
   `ctr content get` command. Require zero status and empty stderr. Strictly
   parse each bounded response as a duplicate-key-free JSON object and write
   canonical `ctr-load-<I>-import-<J>.json` containing exactly its source,
   raw-byte SHA-256 identity, and parsed content. Do not store base64 or opaque
   raw bytes. Nonzero status, stderr, runner exception, timeout, deadline
   exhaustion, non-finite/invalid/duplicate-key JSON, secret-scan failure, or
   write failure makes final evidence marker-only; never stage a partial import
   diagnostic set. Reject `NaN` and both infinities at every JSON input
   boundary. Retain these validated results as classifier inputs; they are not
   passive evidence and do not admit a fourth or partial mode.

   In converted mode, require source-suffix/raw-hash equality and exactly one
   closed OCI manifest plus one closed OCI index. The manifest has a closed OCI
   config descriptor
   for the frozen config and a nonempty ordered list of closed, unique,
   positive-sized OCI uncompressed-layer descriptors. The index contains one
   closed positive-sized OCI-manifest descriptor to the inner digest, with size
   exactly equal to the inner raw length. Reject every extra field, annotation,
   alternate media type, invalid digest, boolean/nonpositive size, or partial
   shape.

   For a converted candidate, boundedly read the frozen config content, require
   zero status, empty stderr, and raw hash equality, and write canonical
   `ctr-load-<I>-config.json`. Allow normal image-config root fields but require
   a closed `rootfs` equal in shape to `{type: layers, diff_ids: [...]}`; its
   ordered diff IDs must exactly equal the inner ordered layer digests, and the
   inner config descriptor size must equal the config raw length. Apply the
   same marker-only safety boundary as other load diagnostics.

   For each Desktop delta, separately validate the reviewed exact-child outer
   content by digest, require its raw
   SHA-256 to equal that digest, and validate a closed schema-version-2 OCI
   index containing exactly one positive-sized Docker schema-2 descriptor to
   the frozen child with the exact corresponding Quay source annotation. Tag
   the validated inner import to the exact target, re-enumerate, then remove only the two
   discovered import references using `ctr images rm` without `--sync` or
   content deletion. Require the post-removal names to be exactly the prior
   inventory plus the target and config pseudo-reference. The unique,
   disposable, pre-workload node provides exclusive ownership across the
   otherwise unavoidable final validation-to-removal interval.

   After both same-mode imported images are normalized, restart containerd exactly once,
   wait boundedly for the exact Kubernetes Node to report Ready, and repeat
   node attachment, loopback API publication, dual-stack empty default-route,
   positive local TCP, and negative external TCP checks. Prove the normalized
   `ctr -q` inventory remains unchanged, then enumerate CRI and require each
   selected record to have no repo tags, only its exact Quay child repo digest,
   and its frozen config identity. The all-classic path performs no tag,
   removal, or restart. Complete every check before applying any Strimzi object;
   `Never` pull policy prevents reconciliation from substituting or fetching a
   tag.
6. Install the exact rewritten operator manifest in one namespace, create the
   single-node Kafka fixture, and poll with monotonic bounded deadlines: five
   minutes for operator availability, ten minutes for Kafka `Ready=True`, five
   minutes for all first-pass topic reconciliation, and five minutes for replay.
   For the exact Kafka identity, an absent or null initial `.status` is pending;
   any present non-mapping status is invalid, while a valid status whose
   generation or `Ready` condition has not converged remains pending.
   The CI job timeout is 30 minutes. The gate has a shorter global internal
   deadline that reserves bounded time for evidence capture, cleanup/residue
   verification, and failure-only upload.
7. Run server-side dry-run on the already-generated canonical stream, then
   apply the first copy. Enumerate all Cluster Operator and test-namespace Pod
   container and init-container image references and require only the exact
   selected-platform operator and Kafka child references permitted by the
   closure. Each `spec.image` must be the exact child. Its corresponding
   Kubernetes status `image` may be only that child or its own frozen bare
   config digest, covering the two reviewed backend display forms without
   admitting an index, manifest, tag, missing value, or another image's config.
   Bind raw pod and service reads to Kubernetes 1.35's exact generic `v1/List`
   envelope with metadata `{resourceVersion: ""}` and require every item to be
   exact `v1/Pod` or `v1/Service` for that collection before normalization.
   Reject typed raw list forms, mixed kinds, or extra/missing envelope fields.
   A first reviewed pilot records the exact Kubernetes `imageID`
   representation for each workload under the pinned kind node/containerd
   combination in `images.lock.json`; CI then requires exact equality to those
   frozen values. It MUST NOT accept an arbitrary index, child-manifest, or
   config-digest form. This check is scoped to Strimzi workloads; Kubernetes
   system images are inherited from the pinned kind node image.
   Pilot mode is explicit and allowed only while both selected runtime-ID locks
   are null. It completes the same read-back and replay flow, writes one
   consistent observed ID per exact image reference to bounded secret-scanned
   evidence, cleans up, and exits with a distinct unsuccessful status. Normal
   mode fails before mutation on a null lock; pilot mode fails on frozen locks;
   permanent CI never passes `--pilot`.
8. For both the direct and hashed identities, require the exact namespace,
   metadata name, cluster label, annotations, spec, `status.topicName`, nonempty
   `status.topicId`, `status.observedGeneration == metadata.generation`, and a
   `Ready=True` condition.
9. Read the broker through an in-cluster Kafka 4.3.1 tool invocation and prove
   exact topic name, partition count, replication factor, and all configured
   string values. No external Kafka endpoint is created.
10. Record UID, generation, topic ID, broker description, and configs; apply the
   exact bytes a second time; poll again; and require the same UID, generation,
   topic ID, spec, broker shape, and configs. This is replay/idempotency
   evidence, not update or deletion evidence.
11. Do not delete a `KafkaTopic` during the assertion phase. Cleanup destroys the
   disposable kind cluster as a whole and is not used as deletion-contract
   evidence.

### Failure artifacts and cleanup

On success, ordinary failure, and internal timeout, the orchestrator first
captures bounded, secret-scanned artifacts: Kubernetes version and node image,
the canonical sorted node containerd image-reference inventory from exact
`ctr --namespace=k8s.io images list -q`, CRD digest, operator Deployment/Pod
descriptions, operator and Topic Operator logs, Kafka/NodePool/Topic JSON,
events, broker descriptions/configs, generated YAML checksums, and poll
timeline. The exact load-diagnostic inventory is one sorted before/after pair
per attempted load (four files on the complete two-load path), plus zero to two
sorted canonical import-content JSON files per attempted load, under the names
frozen in step 5, plus one config JSON file per converted load. `ctr-images.txt`
is attempted even when the node or kubeconfig is
absent. Its closed transform permits at most 4,096 LF-delimited, 512-byte
printable-ASCII references, rejects control/URI/credential/query/fragment/
backslash forms, requires an exact lowercase SHA-256 after every `@`, and sorts
the retained lines. Safe unexpected references remain useful diagnostics. A
nonzero result or zero result with stderr produces only the fixed neutral
placeholder. Unsafe parsing, a secret match, or a write failure performs no
partial redaction and forces marker-only staging.
Kubeconfig, Secrets, service-account tokens,
environment dumps, registry configuration, and unbounded logs are never
uploaded. A capture command returning nonzero produces fixed JSON at the
original evidence filename with only its integer return code and a
`capture-failed` status. A runner exception or timeout produces a fixed
`<filename>.failed` artifact without raw diagnostic data. These neutral records
complete an ordinary partial-cluster capture without forcing marker-only
staging. Failure to represent every scheduled item before the capture deadline,
or any candidate evidence secret/write failure, rejects the complete set.

After capture, cleanup deletes the exact kind cluster, removes the exact
non-masquerading Docker bridge and temporary runtime material, and verifies
that no container/network with the unique prefix remains. Bounded capture and
cleanup evidence remains in a candidate directory that is never uploaded directly.
The staging step creates a fresh upload directory and copies only files for
which the complete bounded candidate set passed its final secret scan. On any
secret match, size violation, or scan/read failure, it stages none of the
candidate evidence, replaces the upload directory with only a fixed
secret-neutral scan-failure marker, and fails the job. A failure-only,
short-retention upload then publishes either the fully passing evidence set or
that marker, so cleanup/residue failures are represented without exposing a
rejected candidate. Hosted-runner cancellation or external SIGKILL can prevent
these process-level steps, so cleanup in that case is best-effort and runner
disposal is the final isolation boundary.

### Acceptance

- contract tests verify every source URL, annotated tag object, peeled commit,
  digest, image transformation, timeout, experimental network selector,
  isolation assertion, pull policy, log allowlist, cleanup target, and wheel
  handoff statically;
- runtime checks verify the exact non-masquerading bridge, sole node attachment,
  empty IPv4 and IPv6 default-route inventories both before apply and after
  replay, loopback-only API publication, positive local and negative external
  TCP controls, local image digests, and exact image IDs; no mutable Strimzi
  workload image or unverified downloaded byte is instantiated;
- server-side admission, Topic Operator reconciliation, exact Kubernetes and
  Kafka read-back, and replay all pass; and
- the gate cannot be confused with production apply, update, adoption,
  deletion, TLS, authentication, multi-node durability, or GitOps-controller
  behavior.

## Slice 6 — public claims

Only after Slices 0 through 5 and the full release workflow are green:

1. Add a focused guide and CLI reference for `streamt export strimzi`.
2. Add navigation, release-note, support-matrix, and roadmap changes.
3. Mark only deterministic Strimzi 1.2.0 `KafkaTopic` GitOps output as
   supported. Keep direct Kubernetes apply and every unsupported boundary from
   the specification explicit.
4. Do not mark the broader Kubernetes, Flink Kubernetes Operator,
   Terraform/OpenTofu, or Confluent Cloud Flink Statements backends supported.
5. Do not claim deletion safety, production cluster credentials, controller
   installation, drift detection, or ownership-state integration.

## Final release gate

```text
pytest -q
ruff check .
mypy src/streamt
python -m build
mkdocs build --strict
```

The release workflow additionally requires Python 3.10 through 3.14 package
parity and the single pinned real-cluster lane. A skipped, softened, or failed
real-cluster lane leaves the feature proposed; offline schema validation alone
cannot promote the support claim.
