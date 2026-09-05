# Kafka Streams runner

This maintained runtime executes streamt's version-1 projection/filter plans.
It is one foreground JVM process per model. It has no SQL parser, HTTP service,
scheduler, or in-process application restart. The Python compiler chooses the
plan; the deployment layer owns resource creation and offset initialization.

The runner version is `0.1.1`, the plan version is `1`, and Kafka Streams is
`4.3.1`. The plan format matches the earlier
[isolated proof](../../experiments/kafka-streams/README.md). That experiment is
historical evidence, not a dependency of this runtime.

## Build and inspect

With streamt installed, from any directory:

```bash
streamt -o json runtime build --dry-run
streamt -o json runtime build
```

The dry run reads packaged sources without Docker access, subprocesses, or
writes. An actual build needs local Docker Engine with Buildx and network access
to download pinned base images and Maven dependencies. It uses the
[default builder bound to that Docker daemon](https://docs.docker.com/build/builders/),
not a selected remote builder. The build timeout defaults to 600 seconds;
`--timeout` accepts 30 through 1800. Daemon checks have separate short bounds.

Set `runtime.kafka_streams.image` to the returned `data.image` SHA-256 ID.
The result also reports runner/plan versions and a hash of the supplied build
inputs. The CLI verifies the local image ID and version labels. It does not tag
or publish an image. Build logs are not printed because provider errors can
contain credentials. Inspect the local daemon separately if a build fails.

The wheel contains only the Dockerfile, Maven project, license, image lock, and
main/test Java sources for this build. It does not contain prebuilt JARs or local
acceptance evidence. Editable installs read the same files from this maintained
subtree, never from the current directory or the historical proof. Builds use
an owned temporary context and remove it after completion. Image IDs can differ
because build attestations include build metadata; byte-reproducible images are
not claimed.

For JVM development in the checkout:

```bash
mvn -q -f runtimes/kafka-streams/pom.xml package
```

The Dockerfile pins both base images by digest: Maven 3.9.11 with JDK 21 for the
build, and Temurin JRE 21 for execution. The Maven build compiles for Java 17 and
runs JVM tests. Runtime dependencies retain their original JAR files and license
resources under `/opt/streamt/runner/lib`; the application JAR is
`/opt/streamt/runner/runner.jar`. Direct dependency and plugin versions are
declared in `pom.xml`. An independent distribution/security review is still
needed before publication.

The image labels are `io.streamt.runner.version=0.1.1` and
`io.streamt.plan.version=1`. It runs as UID/GID `10001:10001`, declares no
anonymous volumes, and creates `/var/lib/streamt/state` with that ownership.
A named volume mounted there retains source-client state across container
replacement. Mount permissions remain the caller's responsibility.

## Process interface

The image entrypoint is `java -jar /opt/streamt/runner/runner.jar`, with a JVM
memory-percentage setting. It accepts:

```text
--plan /run/streamt/plan.json
--client-properties /run/streamt/client.properties
--application-id orders-clean
--state-dir /var/lib/streamt/state
--expected-cluster-id <reviewed-cluster-id>
--expected-input-topic-id <reviewed-input-topic-UUID>
--expected-output-topic-id <reviewed-output-topic-UUID>
```

All seven arguments are required. Duplicate or unknown arguments are rejected.
The application ID is stable across an update. The state directory is absolute.
Cluster IDs use 1 through 200 ASCII letters, digits, `_`, or `-`.
Topic IDs are distinct, nonzero, canonical 22-character Kafka base64url UUIDs.
These identities are runtime bindings, not credentials or fields in plan v1.

`--validate-only` adds a local validation pass and exits. It reads and validates
the plan and connection properties, including local TLS material, but constructs
no Kafka client, resolves no broker hostname, and writes no runtime state. Run
this pass with Docker `--network=none` and the same read-only file mounts intended
for execution. Expected identity arguments remain required, but this pass checks
only their syntax. It does not test broker credentials, connectivity, or identity.

Normal execution queries the cluster ID and both topic UUIDs through a read-only
[Kafka Admin client](https://kafka.apache.org/43/javadoc/org/apache/kafka/clients/admin/Admin.html)
using the container's own connection properties. All must match the expected
values before the runner constructs Kafka Streams. This prevents an internal
bootstrap address for a different cluster from processing records under the
same topic names. Requests share a 10-second wait deadline; API, request, and
socket setup timeouts are fixed at 10 seconds for this check, with a separate
2-second Admin close bound. Host DNS resolution still depends on the host/JVM
resolver; the launcher must enforce its own overall startup deadline.

The check needs permission to describe the cluster and both topics. Failure,
including unavailable identity metadata, returns `identity_verification_failed`
without starting a consumer/producer or writing status. This is a startup
identity check, not an atomic lock against external deletion or later changes
to broker routing. Streamt still requires exclusive control of managed resources
during deployment.

`--version` is standalone and returns:

```json
{"runner_version":"0.1.1","plan_version":1,"kafka_version":"4.3.1"}
```

The launcher sets `--restart=no`, a bounded stop timeout greater than 15 seconds,
and resource limits. A read-only root filesystem works with a writable named
state volume and `/tmp` tmpfs. Both input files should be mounted read-only.
Neither credentials nor connection settings belong in the plan.

## Plan and record contract

[`examples/plan.json`](examples/plan.json) declares one input topic, one output
topic, a flat source schema, direct column projections, and a conjunction of
predicates. The runner rejects unknown fields or operators before constructing
a Kafka client. Input/output names must differ; internal topics are rejected.

STRING, BOOLEAN, and signed 64-bit BIGINT are the supported types. Comparisons
are column-to-literal `eq`, `ne`, `gt`, `ge`, `lt`, and `le`, plus `is_null` and
`not_null`. Ordering comparisons require BIGINT. A comparison with NULL does
not pass WHERE. String equality is exact and case-sensitive, without locale
rules or Unicode normalization. No implicit conversions occur.

Values must be UTF-8 JSON objects with exactly the declared fields. Missing or
unknown fields, duplicate keys, additional JSON documents, malformed UTF-8,
out-of-range integers, and type mismatches stop processing. Nullable fields
require explicit JSON null. Validation occurs before filtering. The runner
preserves raw Kafka key bytes, including null keys, and drops tombstones.
This is append-stream behavior: there are no retractions or table/upsert
semantics. Arrays, nested objects, decimal values, dates, functions, joins,
windows, and aggregations remain outside this runner's plan contract.

## Connection properties

The separate file uses UTF-8 Java `.properties` syntax. Escapes are interpreted
by the Java properties parser, and duplicate decoded keys are errors.
[`examples/client.properties`](examples/client.properties) is a plaintext
example without credentials.

The accepted properties are:

- `bootstrap.servers`, `security.protocol`, `sasl.mechanism`, `sasl.jaas.config`.
- `ssl.truststore.location`, `ssl.truststore.password`, `ssl.truststore.type`,
  `ssl.keystore.location`, `ssl.keystore.password`, `ssl.keystore.type`,
  `ssl.key.password`.
- `ssl.truststore.certificates`, `ssl.keystore.certificate.chain`,
  `ssl.keystore.key`, `ssl.endpoint.identification.algorithm`.
- `request.timeout.ms`, `connections.max.idle.ms`,
  `socket.connection.setup.timeout.ms`, `socket.connection.setup.timeout.max.ms`.
  These timeout values must be between 1 and 300000 milliseconds.

Other settings are rejected, including consumer-group overrides, offset-reset
policy, processing guarantees, custom callback classes, serializers, and
configuration providers. Bootstrap addresses are explicit `host:port` pairs;
URLs with embedded credentials are invalid.

Security protocols are PLAINTEXT, SSL, SASL_PLAINTEXT, and SASL_SSL. SASL supports
PLAIN and SCRAM-SHA-256/512 only. The JAAS value must contain one corresponding
Kafka PlainLoginModule or ScramLoginModule entry with the `required` control
flag and exactly `username` and `password` options. OAuth and Kerberos are not
implemented in this contract.

TLS hostname verification stays `https`. Trust/key store types are JKS, PKCS12,
or PEM, and file paths must be absolute paths visible inside the container.
Inline PEM requires `type=PEM`; it cannot be combined with a store location.
An inline client key needs its certificate chain. PEM store passwords are not
supported; `ssl.key.password` may unlock an encrypted private key. Encode PEM
line breaks as literal `\n` escapes in the properties file. Local validation
checks the decoded certificate/key material before any Kafka client is created.

Kafka library logging is disabled. The runner emits only the structured fields
below and fixed failure codes; it never prints properties, records, exception
messages, or stack traces. This prevents library diagnostics from leaking
connection material but gives less detail for broker troubleshooting. Inspect
broker access separately when a fixed failure code is insufficient.

## Offsets, shutdown, and status

The runner fixes `processing.guarantee=exactly_once_v2`, one stream thread, and
`consumer.auto.offset.reset=none`. Source progress and output transactions are
managed by Kafka Streams. The caller initializes each source partition's group
offset explicitly before first execution. Missing or out-of-range committed
offsets fail; the runner never chooses earliest/latest or clears state.
These are the [Kafka consumer offset semantics](https://kafka.apache.org/43/configuration/consumer-configs/).

SIGTERM requests an explicit group departure through
[CloseOptions](https://kafka.apache.org/43/javadoc/org/apache/kafka/streams/CloseOptions.html),
with a 15-second close bound. The caller verifies a completed `closed` status
after stopping the container. A failed close remains a failure. Normal signal
termination can still produce Docker exit code 143.

A processing error stops the client and leaves its failed status intact. With
the classic Kafka group protocol, a fatal client shutdown can retain membership
until session expiry. Container exit alone therefore does not prove that the
group is empty. The caller must check group inactivity before initializing or
changing offsets. The runner has a 30-second startup readiness bound and no
application restart loop.

After the identity check succeeds, the runner atomically replaces
`<state-dir>/status.json` and emits the same one-line JSON on each transition:

```json
{
  "runner_version": "0.1.1",
  "plan_version": 1,
  "application_id": "orders-clean",
  "plan_sha256": "sha256:<SHA-256 of the exact plan-file bytes>",
  "cluster_id": "<verified-cluster-id>",
  "input_topic_id": "<verified-input-topic-UUID>",
  "output_topic_id": "<verified-output-topic-UUID>",
  "state": "running",
  "reason": null,
  "updated_at": "<UTC ISO-8601 timestamp>"
}
```

States are `starting`, `running`, `closing`, `closed`, and `failed`. A failure
survives later close callbacks. The caller checks the live container identity
and plan hash, verified cluster/topic IDs, and timestamp against container start;
a leftover file is not proof of a running process. `running`
means Kafka Streams reached its RUNNING state, not that expected data has
already been produced.

Failure reasons include `missing_or_invalid_offsets`, `processing_failed`,
`startup_failed`, `startup_timeout`, `runtime_failed`, `shutdown_timeout`, and
`status_write_failed`. Pre-start failures return exit code 2 with a fixed stage
code (`invalid_arguments`, `plan_invalid`, `client_properties_invalid`,
`identity_verification_failed`, or
`local_state_unavailable`). Runtime failures return 3. Validation/version success
returns 0. No shutdown callback turns a processing failure into success.

## Docker acceptance

```bash
.venv/bin/python runtimes/kafka-streams/acceptance.py
```

This builds the image and uses a uniquely labelled broker, network, named state
volume, and model containers. It checks refusal without offsets, explicit
fixture initialization, a projection/filter update with stable identity and
offsets, fail-stop behavior, out-of-range offsets, and network-disabled local
validation. Wrong expected cluster, input UUID, and output UUID each fail with
pending records and valid offsets, without advancing offsets, producing output,
or replacing the previous status. It checks key preservation and a secret
canary's absence from logs.
Cleanup validates ownership and removes only the fixture's containers, volumes,
and network. Images and local evidence under `target/acceptance/` remain.

`--image sha256:<local-image-id>` runs the same checks on an already-built local
image without pulling it. The image must carry the runner/plan version labels.
The single-node fixture does not establish broker-failure tolerance, crash
recovery during a transaction, stateful upgrades, distributed rollout safety,
schema evolution, or throughput. TLS/SASL parsing and local material checks are
covered separately from the plaintext broker acceptance; authenticated broker
acceptance remains a distribution prerequisite.

The checked-in [verification summary](verification.json) records the bounded
acceptance results and exact image identities. Cleanup regression tests run with
`python -m unittest discover -s runtimes/kafka-streams -p test_acceptance.py -q`.
