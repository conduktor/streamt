# Kafka Streams execution proof: architecture decision

## Decision and status

The 2026-09-04 execution authorization covers a bounded experiment, not a new
supported backend. For that experiment, use a fixed Java Kafka Streams runner
that accepts a versioned JSON plan compiled from a closed SQL subset.

The implementation lives in `experiments/kafka-streams/`, outside `src/streamt`.
It is excluded from Python wheel/source distributions and adds no CLI command,
runtime configuration, automatic executor selection, or published JAR/image.
Existing Flink compilation and update blockers are unchanged.
Maintaining and distributing a runtime requires the product owner's next decision.

## Problem and alternatives

Kafka users need a processing route when they do not operate Flink. Kafka Streams
provides JVM application APIs, not an SQL server. A small runner can reduce setup
for a narrow transform, but streamt would own translation, types, packaging, and
the update protocol. See the [Kafka Streams documentation](https://kafka.apache.org/43/streams/).

| Route | Deployment and distribution | Work streamt would maintain | Decision for this proof |
| --- | --- | --- | --- |
| Generate a Java application per SQL change | Build and deliver a JAR/image for each compiled query | Code generation, dependency builds, artifact identity, diagnostics, lifecycle | Not selected; a build per filter change is unnecessary for this subset |
| Fixed runner with a versioned plan | Deliver a pinned runner once; deliver a validated plan per change | Plan/compiler compatibility, record semantics, capability checks, runner lifecycle and security | Selected only for the isolated proof |
| Integrate an existing SQL runtime such as ksqlDB | Connect to or operate its servers; deliver its supported SQL | Dialect mapping, exact query/resource identity, ownership, deployment and recovery integration | Alternative for broader SQL; not installed or benchmarked in this proof |

ksqlDB already translates SQL into Kafka Streams topologies and provides a server
and REST interface. Interactive and headless deployment have different lifecycle
surfaces. These are useful alternatives, not evidence that either can be substituted
for Flink without a contract. See the
[official architecture and deployment modes](https://docs.confluent.io/platform/current/ksqldb/operate-and-deploy/how-it-works.html).

Before distribution, inventory transitive dependencies, preserve their licenses
and notices, scan the chosen artifacts, and define the supported JDK/platform
matrix. A shaded proof JAR is not that distribution work. For the existing-runtime
option, ksqlDB's repository uses the Confluent Community License, including an
excluded-purpose restriction on competing online services; review the exact
chosen distribution and intended use before bundling it. See the
[upstream license](https://github.com/confluentinc/ksql/blob/master/LICENSE).

## Closed proof contract

The Python compiler emits plan version 1. Java independently validates the plan
before constructing a Kafka client. SQL parsing is not runtime evaluation, and
neither layer may silently accept fields or constructs it does not understand.

- One unqualified source, direct column projection with optional aliases, one output.
- Optional AND-only predicates: typed column/literal comparison or IS [NOT] NULL.
  Ordering comparisons require BIGINT; equality on strings is exact and case-sensitive.
- STRING, BOOLEAN, and signed 64-bit BIGINT only; no implicit coercion.
- UTF-8 JSON object values; every declared field is required, with explicit null
  allowed only for nullable fields. Unknown/duplicate fields, malformed records,
  and extra JSON documents fail before filtering.
- Raw Kafka keys, including null keys, are preserved. Tombstones are dropped;
  this is an append-stream transform, not a table/upsert or deletion protocol.
- No expressions, functions, star projection, joins, aggregation, windows, OR,
  casts, table aliases, sorting, LIMIT, comments, or state migration. Unsupported
  SQL fails before Docker or Kafka is contacted by the test script.

The experiment README is the detailed runnable contract. Its source schema is
explicit test input, not an inferred guarantee from import or Schema Registry.
This subset is not advertised as Flink SQL or ksqlDB compatible.

## Identity and lifecycle evidence

The application ID, input/output topics, named operators, and state-directory
path stay fixed across separate Java processes. A clean stop requests bounded
close with explicit group departure before the replacement starts. Source/output
topics belong to the fixture; the runner does not create them.

Valid committed offsets take precedence over `auto.offset.reset=earliest`.
However, earliest can also reset an expired or out-of-range offset. Productization
must define and test missing/invalid-offset behavior before making a no-replay
promise. Kafka documents identity and processing options in the
[Streams configuration guide](https://kafka.apache.org/43/streams/developer-guide/config-streams/).

The proof selects `exactly_once_v2`; the verifier uses `read_committed`. This
single-broker, replication-factor-1 scenario establishes neither broker-failure
tolerance nor an exactly-once production update protocol.

After two initial local passes and an independent review, run `479233760018`
passed with stricter clean-stop assertions and verified volume cleanup. It used
Kafka client and broker 4.3.1, Java 25, and Python 3.12: 60 Python tests, 35 JVM
unit tests, and one real-Kafka acceptance test. The source compiles for Java 17;
Java 17 execution was not tested. The recorded acceptance sequence is:

| Phase | Verified result |
| --- | --- |
| Initial filter, amount >= 50 | Six inputs consumed; output a120, d90, e110; committed offset 6 |
| Clean stop and restart, amount >= 100 | Same identities; three new inputs; only g130 added; committed offset 9 |
| Invalid typed record at offset 9 | Process fails; committed offset remains 9; no extra visible output |

The complete output is exactly a120, d90, e110, g130. No state deletion, offset
reset command, application rename, or fresh cluster is used between these phases.
The changed predicate applies to new input; it does not retract old output d90.

Reproduction command from the repository root:

```bash
.venv/bin/python experiments/kafka-streams/run_proof.py
```

The test script uses a digest-pinned, uniquely named local Docker broker, verifies its
actual version before creating topics, and records plans and process results.
Reviewed cleanup must remove only that run's container and its anonymous volumes.
Initial runs exposed an anonymous-volume cleanup omission; old unprovenanced
orphan volumes must not be removed through a broad prune.

The authoritative evidence is
`experiments/kafka-streams/verification/2026-09-05-reviewed.json`; raw local
logs are under the ignored `target/real-proof/` directory. These are engineering
acceptance results, not a time-to-value, throughput, or independent-user benchmark.

## Recommendation and next gate

The result supports continuing with a fixed runner for a deliberately small
Kafka-only path if the owner accepts maintaining it. Keep Flink SQL for workloads
outside that path. Do not start with a general SQL server or distributed control
plane merely because the prototype can process records.

If productization is approved, the next logical sequence is:

1. Fix the public executor/capability contract, serialization scope, and missing
   offset policy. Reject unsupported work before mutation; no silent fallback.
2. Specify immutable runner distribution, authentication/TLS, process ownership,
   environment isolation, and deterministic application identity.
3. Bind create/update to reviewed plans and durable state. Test interrupted close,
   concurrent deployment, crash during processing, retention loss, verification,
   and recovery on the same identities before removing any update blocker.
4. Ship one installed-package orders walkthrough that seeds data, proves output,
   and proves its supported change. Then reuse it in CI.

SQL expansion, stateful operators, custom-language builds, arbitrary image/JAR
scheduling, cloud backends, and additional catalogs remain outside that sequence.
Until this gate is approved and implemented, M3-M5 are not complete and the
prototype must not appear in the support matrix as a working product executor.
