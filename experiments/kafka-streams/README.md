# Kafka Streams execution proof

This experiment tests whether a small SQL projection/filter can run on Kafka
Streams without Flink. It is separate from streamt's compiler, configuration,
deployers, and installed Python package. It does not add a supported executor.

Python parses SQL with sqlglot and emits a version-1 JSON plan. A Java runner
validates the plan independently, builds a Kafka Streams topology, and runs it.
The same JAR runs both example queries. Generating Java per query would require
a new build for every change; the fixed runner instead needs a maintained plan
contract and validation on both sides. This proof does not settle that tradeoff
for a production implementation.

## Run

From the repository root, with its development virtual environment installed:

```bash
.venv/bin/python experiments/kafka-streams/run_proof.py
```

Prerequisites: Python 3.10+, the packages pinned in `requirements.txt`, JDK 17+, Maven, Docker, and
access to Maven Central and the pinned Kafka image. The proof uses Kafka Streams
4.3.1. The script checks the broker's actual version before creating topics.
JDK 25 is the local verification target; the Java sources compile for JDK 17.

The script runs offline compiler tests and JVM tests, packages the runner, then
creates a uniquely named broker on a random loopback port. It writes plans,
broker details, process logs, and the result under
`target/real-proof/<run-id>/`. A result file is written only after the assertions
pass. No existing Kafka cluster is used. Cleanup checks the container's ownership
label and removes only that run's broker and its anonymous volumes. It records
the mounted volume names, checks that they are gone, and writes `cleanup.json`.
Logs and plans remain on disk. Early runs used container-only removal and may
have left anonymous volumes; those older volumes have not been deleted because
their exact provenance was not retained.

For offline checks only:

```bash
.venv/bin/python -m pytest -q -c /dev/null -p no:cacheprovider experiments/kafka-streams/test_compile_plan.py experiments/kafka-streams/test_run_proof.py
mvn -q -f experiments/kafka-streams/pom.xml package
```

The real Kafka test is skipped without `STREAMT_PROOF_BOOTSTRAP`. Prefer the
script over setting that variable manually: the test creates fixture topics and
writes records. Authentication, TLS, and a production scheduler are outside the
experiment.

## Accepted SQL

```sql
SELECT id AS order_id, amount
FROM orders
WHERE amount >= 50 AND active = TRUE
```

One source, direct column projections with optional aliases, one output, and an
optional conjunction of predicates. Identifiers are unquoted lowercase ASCII
names. Predicates compare a column with a typed literal using `=`, `!=`, `>`,
`>=`, `<`, or `<=`, or use `IS NULL` / `IS NOT NULL`. Ordering comparisons require
BIGINT. The AND-only predicate list keeps a row only when every comparison is
true; an ordinary comparison with NULL does not pass the filter.

Supported types are STRING, BOOLEAN, and signed 64-bit BIGINT. No implicit
coercion occurs. String equality is exact, case-sensitive equality without
locale rules or Unicode normalization. Floats, decimal, dates, arrays, nested
objects, arithmetic, functions, casts, star projection, qualified names, table
aliases, OR, parentheses, joins, aggregations, windows, sorting, LIMIT, comments,
multiple statements, and unknown plan fields are rejected. This is a closed
proof subset, not a claim of Flink SQL or ksqlDB compatibility.

## Record and lifecycle contract

- Values are UTF-8 JSON objects. Every declared field must be present; nullable
  fields may contain explicit JSON null. Unknown fields, duplicate JSON fields,
  extra JSON documents, malformed UTF-8, type mismatches, and out-of-range
  integers fail processing. Records are checked before filtering.
- The runner preserves raw key bytes, including null keys. It drops input
  tombstones. This is an append-stream projection, not table/upsert semantics;
  it does not forward deletions or retract previously emitted rows.
- Output values contain only projected columns and aliases. Kafka Streams
  supplies the record-processing mechanics. No Python evaluator runs records.
- The application ID, source/output topics, named operators, and state-directory
  path stay fixed between example runs. Source topics and output topics are
  created by the fixture, not the runner. There is no stateful operator or
  repartition in this topology.
- `auto.offset.reset=earliest` starts a new application at retained input. An
  existing valid committed offset takes precedence. The proof does not expire
  offsets or remove retained data; earliest can also reset an out-of-range
  offset. A product backend would need an explicit retention/offset-loss policy
  before promising that it never replays data unexpectedly.
- `processing.guarantee=exactly_once_v2` is selected, and the verifier reads with
  `read_committed`. The single-node fixture uses replication factor 1. These
  settings do not prove broker-failure tolerance or a production update protocol.
- SIGTERM requests a bounded close and explicit group departure through
  [CloseOptions](https://kafka.apache.org/43/javadoc/org/apache/kafka/streams/CloseOptions.html).
  Each tested update requires a live child before SIGTERM and a completed close
  without failure or timeout markers. Finally-block cleanup cannot satisfy this
  assertion. A bad record shuts down the application and
  exits nonzero. The record is not skipped or sent to a dead-letter topic; it
  remains a problem on the next start. There is no automatic repair.

Kafka documents application IDs, offset and transaction configuration in the
[Streams configuration guide](https://kafka.apache.org/43/streams/developer-guide/config-streams/).
The implementation uses the [Streams DSL](https://kafka.apache.org/43/streams/developer-guide/dsl-api/).

## Acceptance evidence

The Kafka acceptance test starts separate Java processes against one broker. It sends six
records, including a null key, a nullable amount, and a tombstone. The original
predicate emits `a`, `d`, and `e`; the source commits offset 6. After a clean
shutdown, the same application starts with `amount >= 100`, receives three new
records, and commits offset 9. The complete output must then be exactly
`a`, `d`, `e`, `g`: no replay of prior committed input, and the new predicate
filters the new record `f` with amount 75.

A third process receives an invalid typed record at offset 9. It must fail,
leave the committed source offset at 9, and add no visible output. The result
file records these assertions, actual output rows, and the application identity.

This tests a clean, sequential, stateless update and fail-stop handling. It does
not test a crash during a transaction, rebalances, concurrent old/new runners,
broker failure, state migration, schema evolution, rollback, or throughput.
Those gaps remain even if this test passes.

The [reviewed verification record](verification/2026-09-05-reviewed.json) contains
the observed versions and output: 60 Python tests, 35 JVM unit tests, and one
Kafka acceptance test passed. That run also verifies removal of its three
recorded anonymous volumes. The [earlier record](verification/2026-09-05-local.json)
is retained with its cleanup limitation stated explicitly.

## Decision after the proof

Review the actual records and failure evidence before deciding whether to
maintain a runtime. If this route is selected, the next work is the product
contract: explicit executor choice, capability checks, offset-loss policy,
artifact distribution, and a reviewed update/recovery protocol. Keep SQL
expansion and stateful operations out of that first decision. An existing SQL
runtime remains an alternative when broader SQL is needed.
