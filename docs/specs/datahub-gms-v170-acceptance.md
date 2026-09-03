# DataHub v1.7.0 GMS acceptance gate

Status: proposed. An empirical feasibility run has passed, but no server-
acceptance or live-lineage support claim may be published until the repeatable
CI gate in this specification lands and passes.

This specification defines a test-only conformance gate that installs the built
streamt wheel, generates its supported offline DataHub metadata files through
the installed `streamt` executable, ingests those exact bytes into one fresh,
pinned DataHub v1.7.0 Generalized Metadata Service (GMS), then reads the stored
aspects and graph relationships back. It does not add a DataHub connection,
publisher, or lifecycle command to production streamt.

The offline artifact contract remains owned by
[`DataHub catalog export`](datahub-catalog-export.md). This gate consumes its
unchanged installed-wheel output; it does not redefine its identities, aspect
shapes, ordering, warnings, or bytes.

## Normative language

`MUST`, `MUST NOT`, `SHOULD`, and `MAY` are normative. A missing, normalized,
extra, stale, or unbounded result MUST fail the gate. Tests must not weaken an
assertion merely to accommodate server behavior.

## Pinned upstream contract

The only supported server target for this gate is official DataHub `v1.7.0` at
immutable commit
[`7f81ccbfe27b9acc947f5f600fcf9ddb72138a80`](https://github.com/datahub-project/datahub/commit/7f81ccbfe27b9acc947f5f600fcf9ddb72138a80).
The client and metadata types are exact `acryl-datahub==1.7.0`.

Normative upstream evidence is:

- [the v1.7.0 release](https://github.com/datahub-project/datahub/releases/tag/v1.7.0),
  which pins the server release and Python SDK;
- [the pinned quickstart compose file](https://github.com/datahub-project/datahub/blob/7f81ccbfe27b9acc947f5f600fcf9ddb72138a80/docker/quickstart/docker-compose.quickstart-profile.yml),
  which defines the coordinated GMS, SystemUpdate, Kafka, MySQL, and OpenSearch
  topology;
- [the simplified MCP file contract](https://github.com/datahub-project/datahub/blob/7f81ccbfe27b9acc947f5f600fcf9ddb72138a80/docs/advanced/writing-mcps.md)
  and [MCP wrapper](https://github.com/datahub-project/datahub/blob/7f81ccbfe27b9acc947f5f600fcf9ddb72138a80/metadata-ingestion/src/datahub/emitter/mcp.py);
- [the Rest.li emitter](https://github.com/datahub-project/datahub/blob/7f81ccbfe27b9acc947f5f600fcf9ddb72138a80/metadata-ingestion/src/datahub/emitter/rest_emitter.py),
  which submits MCPs to GMS; and
- [the graph client](https://github.com/datahub-project/datahub/blob/7f81ccbfe27b9acc947f5f600fcf9ddb72138a80/metadata-ingestion/src/datahub/ingestion/graph/client.py)
  and pinned entity/aspect PDL linked from the offline specification, which
  define the pinned REST.li and typed aspect representations under test.

Newer SDK, image, compose, or server behavior is not evidence. A version or
image change requires a separate compatibility review and new locks.

## Empirical feasibility evidence

A manual disposable probe established that the boundary is technically
reachable:

- the official v1.7.0 quickstart started successfully;
- `GET /config` reported
  `versions.acryldata/datahub.version == "v1.7.0"` and
  `versions.acryldata/datahub.commit ==
  "7f81ccbfe27b9acc947f5f600fcf9ddb72138a80"`;
- the current representative streamt example produced 11 proposals;
- official `datahub ingest mcps` wrote all 11 proposals with zero warnings and
  zero failures;
- REST.li `entitiesV2`, also observed through `datahub get`, returned the
  expected `dataJobInfo` and `dataJobInputOutput`;
- `/relationships` with an exact `types` filter returned the expected one
  `Consumes` and one `Produces` destination in `relationships[*].entity`; and
- `/openapi/entities/v1/latest` returned HTTP 400 for the persisted aspects
  because `SystemMetadata` lacked `__type`, so that endpoint is not an
  acceptance surface for this exact release.

This proves feasibility only. It was not the repeatable installed-wheel,
two-artifact, isolation, replay, cleanup, and evidence-retention gate below and
therefore does not change current public support claims.

## Exact boundary

The gate MAY mutate only a disposable GMS created for that one test attempt. It
MUST NOT contact, authenticate to, discover, or modify a developer, staging,
production, DataHub Cloud, or shared quickstart instance.

The accepted GMS address is exactly one of:

- `http://datahub-gms:8080` on the gate's private internal Docker network; or
- a dynamically allocated port bound only to `127.0.0.1` for a local
  developer or CI invocation.

CI MUST use the loopback form unless the oracle itself runs on the private
network. `0.0.0.0`, a LAN address, a public
hostname, HTTPS termination, a reverse proxy, and a pre-existing DataHub
profile are outside the boundary. No GitHub, cloud, production, Kafka,
Gateway, or DataHub credential is accepted by the gate.

Production `streamt docs datahub` remains offline and unchanged. It continues
to construct no DataHub SDK, REST, GMS, Kafka, Gateway, state, provider, or
subprocess client. The official DataHub CLI and SDK exist only in the isolated
gate runner.

## Frozen installed-package inputs and generated artifacts

The gate consumes the one wheel built and already inspected by the package job.
In a new Python 3.10 virtual environment outside the checkout, it installs only
that wheel, runs `pip check`, proves `acryl-datahub` is absent, and proves the
imported `streamt` module and executable do not resolve from the checkout.

A checked-in representative project and a small contract fixture define the
expected logical declarations, URNs, aspect pairs/counts, and direct edges.
They MUST NOT contain a generated MCP document or substitute for invoking the
shipped command. The gate supplies this installed `streamt` executable and
generates both canonical files during the current job:

| Artifact | Kafka identity | Expected proposal count |
| --- | --- | ---: |
| `with-instance.json` | explicit Kafka platform instance `main` | 15 |
| `without-kafka-instance.json` | no Kafka platform instance or Kafka `dataPlatformInstance` aspect | 12 |

Both include the explicit Gateway platform `conduktor-gateway` and instance
`edge,west`. They cover one DataFlow; Kafka and Gateway Datasets; process-free
topic behavior; Flink, Gateway, and Connect DataJobs; direct inputs and actual
outputs; a sink with no output; and declared/enforced contract custom
properties.

For each variant, the preparatory runner invokes the executable twice with the
same exact project and arguments. Both invocations MUST exit zero, emit the
expected secret-neutral warnings, produce canonical bytes, equal the expected
contract fixture, and be byte-identical. The gate MAY also compare those bytes
with the existing source baseline handed off by the package job, but a baseline
is secondary evidence and never the artifact sent to GMS.

Only the files freshly generated by that installed executable are sent by
the host-side server oracle. The gate records their SHA-256 values, changes their mode to
read only before ingestion, and proves the bytes are unchanged afterward. A
checked-in generated JSON fixture, previously uploaded MCP artifact, or
artifact produced by an editable/source installation MUST NOT be used as the
primary GMS input.

Each artifact runs against its own fresh Compose project and fresh volumes.
Shared DataFlow and DataJob URNs therefore cannot let one variant overwrite or
mask a failure in the other.

## Pinned service topology and images

The reviewed Compose invocation MUST start only:

1. the v1.7.0 Kafka KRaft broker;
2. MySQL primary storage;
3. OpenSearch search and graph storage;
4. one successful `SystemUpdate` container;
5. GMS.

Frontend, Actions, Neo4j, an external Schema Registry, and unrelated streamt
infrastructure MUST NOT start. GMS uses the pinned quickstart's internal schema
registry and OpenSearch graph implementation. GMS MUST depend on successful
completion of `SystemUpdate`; the oracle MUST depend on healthy GMS.

Every image MUST use a readable tag plus an immutable OCI digest. The initial
reviewed locks are:

| Image | OCI index digest |
| --- | --- |
| `acryldata/datahub-gms:v1.7.0` | `sha256:54bc4431402846a72d1c1bdb69fae1148f74a59425144aa947fdf1c3506461f7` |
| `acryldata/datahub-upgrade:v1.7.0` | `sha256:21e77ad964be64b2b5a7f74c9685897ed79e8854995242eaa5e5c426395b88c0` |
| `confluentinc/cp-kafka:8.2.2` | `sha256:8e01c0305844d6c05bfb8e86479f5f363bb6a53497625395943a9da780de67ce` |
| `mysql:8.2` | `sha256:212fe73edca5df6ff14826d5eb975c914bfb91f82a2e923f9050568f99525da1` |
| `opensearchproject/opensearch:2.19.3` | `sha256:e96cc6ae1500a073d973c0906f30f7cf4d9c461f32f855f9242a2da933660cdd` |

The lock record also includes the Linux/amd64 manifest digest and the pinned
upstream compose SHA-256
`ec476d12f6f278c50d657a617357a050510565ef00b570a69cbe9123a932a7b7`.
CI pulls each digest in a distinct bootstrap step. Runtime Compose services use
`pull_policy: never`, so the acceptance phase cannot silently fetch another
image.

## Isolation and resource contract

All services MUST use one uniquely named Compose project and an
`internal: true` Docker network. CI publishes only GMS, on a dynamically
allocated `127.0.0.1` port, and mounts no Docker socket. The installed-wheel generation environment receives the
representative input project and writes only to a fresh CI temporary directory.
The host-side GMS oracle receives only its script, project fixture, installed
executables, and freshly generated artifact. No repository source tree, home
directory, cloud directory, or user configuration is mounted into the GMS
network.

The gate removes inherited `DATAHUB_*`, proxy, Python-path, and user-site
configuration before setting only its reviewed local values. It sets
`DATAHUB_TELEMETRY_ENABLED=false`, disables server usage aggregation, uses no
debug logging, and refuses a token. Required token-service key/salt and
database password values are fresh, non-production, per-attempt values held in
a mode-0600 file under the CI temporary directory. They are never stored in
the repository, printed, passed through a workflow secret, or uploaded.

The runner MUST preflight Docker plus sufficient free disk and memory. The
Compose file MUST bound Java heaps and container memory so one failed service
cannot exhaust the runner. The job has a 35-minute outer timeout; image pull,
each stack startup, each ingestion, read-back polling, diagnostics, and teardown
also have smaller explicit timeouts.

Network access is allowed only for the separate image/package bootstrap. Once
the services start, containers run only on the internal Docker network and the
host-side oracle talks only to the loopback GMS listener. The service network
has no external route.

## Per-artifact lifecycle

First, the gate installs the supplied wheel outside the checkout and generates
both variants twice through the installed `streamt` executable, proving their
canonical bytes, deterministic equality, expected warnings, exact contract,
and absence of the DataHub SDK. It then performs this complete lifecycle for
each freshly generated artifact before moving to the next variant:

1. Create a new unique Compose project and new named volumes.
2. Start Kafka, MySQL, and OpenSearch and require their exact health checks.
3. Run `SystemUpdate` once and require exit status zero.
4. Start GMS and poll `/health` for at most 180 seconds.
5. Fetch `/config` and require the exact v1.7.0 version and full commit above.
6. Verify the gate runner contains exact `acryl-datahub==1.7.0`.
7. Ingest the read-only file with official `datahub ingest mcps`.
8. Require the official report to show exactly the artifact proposal count
   written, zero warnings, and zero failures.
9. Read back and compare every emitted aspect and every expected graph edge.
10. Ingest the identical bytes a second time, repeat every report and read-back
    assertion, and require no change in the exact result.
11. Collect bounded evidence, destroy the Compose project and volumes, and
    prove no project-labeled container or volume remains.

The second ingestion proves repeatable GMS handling of the exact `UPSERT`
artifact only. It does not establish streamt publication idempotency,
reconciliation, ownership, deletion, or recovery behavior.

## Ingestion contract

The publishing command is intentionally test-only:

```text
DATAHUB_GMS_URL=http://127.0.0.1:<DYNAMIC-PORT> \
DATAHUB_TELEMETRY_ENABLED=false \
datahub ingest mcps <READ-ONLY-ARTIFACT>
```

It MUST use the exact official v1.7.0 executable from the locked ingestion
image, skip inherited profiles, use no token, and complete within 120 seconds.
The gate parses the official report rather than accepting a success-looking
log line. A nonzero exit; mismatched written count; warning; failure; traceback;
retry exhaustion; or mutation of the input bytes fails the gate.

No other MCP, MCE, key aspect, platform entity, status, run record, ownership,
tag, or deletion proposal may be submitted by the gate. Automatically created
server key aspects are observed server behavior and are not streamt output.

## Exact aspect read-back

The existing offline v1.7.0 oracle reconstructs every input with the pinned
`MetadataChangeProposalWrapper` and validates its generated aspect classes.
The live oracle then independently reads each original entity URN from the pinned REST.li
`/entitiesV2/{percent-encoded-entity-urn}` endpoint while requesting exactly
that entity's emitted aspect names. It extracts each returned aspect's JSON
value and compares it with the exact emitted JSON value.

The response MUST be HTTP 200 and name every requested aspect exactly once.
GMS v1.7.0 also returns the entity's server-owned key aspect even when an
aspect filter is supplied; that one key aspect is allowed but excluded from
the comparison. Any other unrequested aspect fails the gate. Every emitted
aspect value MUST equal the generated value exactly. This assertion covers:

- `dataFlowInfo`;
- `datasetProperties`;
- conditional `dataPlatformInstance`;
- `dataJobInfo`; and
- `dataJobInputOutput`.

The comparison excludes server-owned key aspects and system metadata, but it
does not ignore, add, or normalize a field inside the streamt-owned aspect. A
404, wrong union representation, missing empty array, deprecated-array
population, or extra lineage destination fails. The gate MUST NOT fall back to
`/openapi/entities/v1/latest`: that route returned HTTP 400 with a
`SystemMetadata.__type` decoding error during the pinned v1.7.0 probe.

`entitiesV2` is the exact acceptance endpoint selected for this pinned,
test-only topology. Its use is not a production streamt API promise and does
not establish compatibility with later DataHub versions.

Primary aspect read-back uses bounded polling for at most 30 seconds with
two-second intervals. Authentication errors, schema errors, and other 4xx
responses fail immediately; only connection/5xx startup races may retry within
the deadline.

## Exact graph relationship read-back

For each `dataJobInputOutput`, the gate derives expected relationships only
from its direct edge arrays:

- every `inputDatasetEdges[].destinationUrn` is exactly one outgoing
  `Consumes` Dataset relationship from that DataJob; and
- every `outputDatasetEdges[].destinationUrn` is exactly one outgoing
  `Produces` Dataset relationship from that DataJob.

For each DataJob, the gate makes separate pinned GMS `/relationships` requests
for outgoing `Consumes` and `Produces`, supplies the exact relationship name in
the `types` query parameter, and reads Dataset destinations only from
`relationships[*].entity`. Each returned relationship MUST match the requested
type and direction. The gate polls until those exact destination sets equal the
expectations. It rejects a missing, duplicate, transitive, self, stale, or
extra Dataset edge. A Connect sink MUST have its expected `Consumes`
relationship and zero `Produces` relationships. The process-free topic MUST
have no invented DataJob proposal and therefore no queried job relationship.

Graph indexing may be asynchronous, so relationship polling has a 90-second
deadline and two-second intervals. A successful primary aspect read is not a
substitute for this graph assertion. Derived job-to-job relationships MAY be
reported by another relationship type, but they do not satisfy or broaden the
exact Dataset `Consumes`/`Produces` sets.

## Diagnostics, evidence, and cleanup

The gate retains, on success and failure:

- the built wheel filename and SHA-256 value plus installed-distribution
  identity evidence;
- the exact installed `streamt docs datahub` arguments and bounded result
  counts, without local paths or project secrets;
- input filenames, sizes, and SHA-256 values;
- the locked image names/digests actually used;
- bounded Compose status and service logs without ANSI color;
- only container `State` and `Health` fields, never complete inspection data;
- sanitized SystemUpdate and GMS readiness results;
- the official ingestion count summary for both attempts; and
- a canonical JSON summary of exact aspect and graph relationship checks.

Logs are tail-bounded and retained for 14 days. The gate MUST NOT upload its
temporary environment file, rendered Compose configuration, full `docker
inspect`, client profile, credentials, tokens, connector configuration, or
unredacted response bodies. The release artifacts contain deliberate catalog
identities but none of the sentinel secrets excluded by the package gate.

Cleanup runs unconditionally after evidence collection, using the exact
Compose project name with `down --volumes --remove-orphans`. Cleanup MUST NOT
issue DataHub delete, soft-delete, rollback, patch, or reconciliation calls.
Destroying the disposable containers and volumes is the only cleanup of
ingested metadata. Failure to remove every project-labeled container, network,
and volume fails the job.

## Acceptance and claims

The repeatable gate passes only when the supplied installed wheel generates
both deterministic artifact variants with exact contract parity and no SDK,
then both variants complete two ingestions each with exact counts, exact
aspect read-back, exact graph Dataset relationships, unchanged artifact bytes,
bounded evidence, and verified teardown under the locked v1.7.0 topology.

After that gate passes in the complete release workflow, streamt MAY claim:

- the shipped `streamt` executable generates exact supported offline artifacts
  that are accepted and persisted by the pinned DataHub v1.7.0 quickstart GMS
  topology;
- all five emitted aspect types survive exact server read-back;
- the tested Kafka-instance, Kafka-no-instance, and explicit Gateway
  identities are accepted; and
- the tested direct `Consumes`/`Produces` Dataset lineage, including empty sink
  output, appears through the pinned server graph relationship surface.

Those claims are conformance evidence, not a production capability. The
support matrix MUST continue to report `Observe: No` and `Direct plan/apply:
No`. Production streamt still has no GMS endpoint, token, publisher, state,
read, reconciliation, delete, or recovery interface.

## Deferred work and non-claims

This gate does not support or prove:

- authenticated, authorized, TLS, proxy, cloud, or public-network publication;
- DataHub Cloud, Kubernetes, Helm, high availability, external databases,
  non-quickstart settings, or any server other than the exact pinned topology;
- production retries, rate limits, batching, partial-failure recovery,
  idempotency keys, conflict handling, review, or rollback;
- reads, discovery, import, authoritative replacement, stateful ingestion,
  reconciliation, soft deletion, hard deletion, or cleanup in a durable GMS;
- existence or bootstrap of the `streamt`, Kafka, Gateway, or platform-instance
  entities referenced by URNs;
- ownership, tags, native DataContracts, assertions, schemas, columns,
  field-level lineage, destinations, exposures, domains, containers, status,
  subtypes, or telemetry; or
- deployed Kafka, Flink, Gateway, or Connect runtime lineage and health.

Each requires a separately reviewed product and lifecycle contract. A green
test-only GMS gate MUST NOT be described as live publication or synchronization.
