# Strimzi KafkaTopic GitOps export implementation plan

## Objective and status

Implement the narrow, deterministic boundary in
[`Strimzi KafkaTopic GitOps export`](../specs/strimzi-kafkatopic-export.md).

Status on 2026-09-03: specification and upstream pins prepared; implementation
has not started. No current support claim changes.

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
2. Validate namespace and cluster name before parsing. Parse and validate the
   project, compile dry-run exactly once, compute the secret-neutral
   `manifest_checksum()` through the pure helper, map, validate, serialize
   fully, then optionally write.
3. Implement exact raw-text, file, quiet, and JSON behavior. Convert mapper
   warnings to structured warnings without changing order or exposing names.
4. Reuse or extract a small same-directory atomic writer only when its behavior
   remains byte-for-byte compatible with existing exporters. Reject symlinks
   and non-regular destinations and clean staging files on every exception.
5. Catch only defined parse/environment/export/schema/YAML/I/O failures at the
   command boundary. Emit E509 with a fixed safe message and structural
   location; never print arbitrary exception text.
6. Prove the command/import boundary does not import deployer, planner,
   provider, or state modules and that no Kubernetes client, socket,
   subprocess, or HTTP session is constructed.

### Acceptance

- required-option validation precedes parse/compile/write;
- compilation count is exactly one on success and zero for primitive failures;
- text stdout is raw-only, warnings are stderr-only, JSON has exact keys and
  order, quiet/no-file fails, and file mode has empty stdout;
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
  for arm64;
- Kubernetes `v1.35.8` node image at
  `kindest/node:v1.35.8@sha256:07b2536e30b803ed61d1677a79df6115f798ce64c80f9e22f6ed45afd09323c0`;
- exact Strimzi install asset and digest from the specification, rewritten by a
  strict checked script according to the exhaustive image closure below;
- operator image
  `quay.io/strimzi/operator@sha256:77f8fa8121a67561c3418de985783d197f51b8931e9a47f793dc0437dc6bb21f`;
- Kafka 4.3.1 image
  `quay.io/strimzi/kafka@sha256:e90a1a74af4226f3ca4d1ebef3ab13bdb09754ae17ca4c1444f7fcbb0ca8ea9a`;
  and
- exact `kubectl` v1.35.8 binary/checksum selected by runner architecture.

The rewriter MUST parse YAML and classify every executable image field and
every Cluster Operator environment variable ending in `_IMAGE` or `_IMAGES`;
text replacement is insufficient. It MUST make exactly this closure and fail
on a missing, duplicate, or additional image-bearing field:

- rewrite the Cluster Operator Deployment image and
  `STRIMZI_DEFAULT_TOPIC_OPERATOR_IMAGE`,
  `STRIMZI_DEFAULT_USER_OPERATOR_IMAGE`, and
  `STRIMZI_DEFAULT_KAFKA_INIT_IMAGE` to the pinned operator digest;
- rewrite `STRIMZI_DEFAULT_KAFKA_EXPORTER_IMAGE` and
  `STRIMZI_DEFAULT_CRUISE_CONTROL_IMAGE` to the pinned Kafka digest;
- replace each of `STRIMZI_KAFKA_IMAGES`,
  `STRIMZI_KAFKA_CONNECT_IMAGES`, and
  `STRIMZI_KAFKA_MIRROR_MAKER_2_IMAGES` with the single exact mapping
  `4.3.1=<PINNED-KAFKA-DIGEST>`; and
- remove the optional and unreachable Kafka Bridge, Kaniko, Buildah, and Maven
  builder default-image environment variables. No Bridge, Connect, build,
  MirrorMaker, User Operator, Kafka Exporter, or Cruise Control resource is
  permitted in the fixture.

Set `STRIMZI_IMAGE_PULL_POLICY=Never`. After rewriting, no tag-form image
reference may remain in an executable image field or `_IMAGE`/`_IMAGES`
environment value of the applied documents, and the Cluster Operator
Deployment's own `imagePullPolicy` MUST also be `Never`. The Kafka test fixture
is a reviewed one-node, dual-role KRaft derivative of the pinned Strimzi
single-node example, uses ephemeral storage and an internal plaintext listener,
enables only the Topic Operator, and contains explicit CPU/memory requests and
limits.

### Isolation and execution

1. Use unique, validated names for the kind cluster, Docker network, namespace,
   Kafka cluster, temp directory, and kubeconfig. Kubeconfig permissions are
   `0600`; its contents are never logged or uploaded.
2. Download and verify all tools and assets and pull the kind node, operator,
   and Kafka images by exact digest while runner egress is still available.
   Verify each local image's `RepoDigests` contains the exact pin. Install the
   supplied wheel in a clean location outside the checkout, generate canonical
   YAML twice before creating any Docker network or cluster, and prove exact
   bytes under the offline exporter guards.
3. Create the unique Docker network explicitly with `docker network create
   --internal`. Inspect the exact network ID and require `Internal=true` before
   giving it to kind. Set `KIND_EXPERIMENTAL_DOCKER_NETWORK` to that exact name
   and invoke pinned kind with the API server bound to `127.0.0.1`. This selector
   is experimental in kind v0.33.0, not a supported public interface; the gate
   deliberately pins and tests that implementation and MUST fail if the
   expected unsupported-selector warning or attachment behavior changes.
4. After cluster creation, inspect Docker rather than trusting configuration:
   require every cluster node to be attached to the exact internal network and
   no other Docker network, require the only published cluster port to be the
   Kubernetes API bound to `127.0.0.1`, and require Kafka and operator services
   to have no host port. Check that `bash` and `timeout` exist in the pinned node,
   then run a bounded TCP probe from its network namespace to a literal external
   IP; any successful connection fails the gate. `Internal!=true`, a skipped or
   inoperable probe, or unexpected attachment/port also fails closed.
5. Only after the nodes exist, load the exact operator and Kafka digest images
   into every node. Enumerate the node's containerd store and require both exact
   digests before applying any Strimzi object; `Never` pull policy prevents
   reconciliation from substituting or fetching a tag.
6. Install the exact rewritten operator manifest in one namespace, create the
   single-node Kafka fixture, and poll with monotonic bounded deadlines: five
   minutes for operator availability, ten minutes for Kafka `Ready=True`, five
   minutes for all first-pass topic reconciliation, and five minutes for replay.
   The job timeout is 30 minutes.
7. Run server-side dry-run on the already-generated canonical stream, then
   apply the first copy. Enumerate all Cluster Operator and test-namespace Pod
   container and init-container image references and require only the exact
   operator and Kafka digest references permitted by the closure. Require each
   corresponding runtime `imageID` to contain the expected digest. This check is
   scoped to Strimzi workloads; Kubernetes system images are inherited from the
   pinned kind node image.
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

A trap runs on success, failure, cancellation, and timeout. Before cleanup it
captures bounded, secret-scanned artifacts: Kubernetes version and node image,
CRD digest, operator Deployment/Pod descriptions, operator and Topic Operator
logs, Kafka/NodePool/Topic JSON, events, broker descriptions/configs, generated
YAML checksums, and poll timeline. Kubeconfig, Secrets, service-account tokens,
environment dumps, registry configuration, and unbounded logs are never
uploaded.

Artifacts are uploaded on failure with short retention. Cleanup then deletes
the exact kind cluster, removes the exact internal Docker network and temporary
directory, and verifies that no container/network with the unique prefix
remains. Cleanup failures fail the job after evidence upload.

### Acceptance

- contract tests verify every source URL, annotated tag object, peeled commit,
  digest, image transformation, timeout, experimental network selector,
  isolation assertion, pull policy, log allowlist, cleanup target, and wheel
  handoff statically;
- runtime checks verify the internal network, node attachments, loopback-only
  API publication, failed egress probe, local image digests, and exact image
  IDs; no mutable Strimzi workload image or unverified downloaded byte is
  instantiated;
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
