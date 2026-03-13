# Antithesis Reliability Audit
**Target:** streamt | **Date:** 2026-03-13 | **Scope:** Full repository
**Stack:** Python 3.10+, Click CLI, requests HTTP, confluent-kafka, Pydantic

## Executive Summary
**Risk Level**: HIGH
**Key Finding**: Flink job update (cancel + resubmit) has no failure recovery -- a failed resubmit leaves the pipeline down with no rollback.
**Top Systemic Issue**: No coordination or rollback across the 5-deployer sequential apply -- partial failures leave infrastructure in inconsistent state.

| Severity | Count |
|----------|-------|
| Critical | 3     |
| High     | 7     |
| Medium   | 10    |

## Critical Findings (P0)

### BUG-001: Flink Job Update Cancel+Resubmit Is Non-Atomic
- **Antithesis Term**: Partial Write / Interrupted Operation
- **Confidence**: High
- **Location**: `src/streamt/deployer/flink.py:528-535`
- **Code**:
```python
elif change.action == "update":
    if change.current and change.current.job_id:
        self.cancel_job(change.current.job_id)    # point of no return
    self.submit_sql(artifact.sql)                  # can fail here
    self._sql_hashes[artifact.name] = self._sql_hash(artifact.sql)
    self._save_hashes()
```
- **Attack Scenario**: During `streamt apply`, a Flink job update cancels the running job, then `submit_sql()` fails (SQL Gateway timeout, network error, Ctrl+C). The pipeline is now DOWN: old job cancelled, new job never started. No hash saved, so next `plan` sees job as non-existent and plans "submit" -- but the user has an unrecoverable downtime window. Additionally, `cancel_job` sends an async PATCH request; the old job may still be in `CANCELLING` state when the new SQL is submitted, risking two jobs writing to the same sink simultaneously.
- **Remediation**:
```python
elif change.action == "update":
    if change.current and change.current.job_id:
        self.cancel_job(change.current.job_id)
    try:
        self.submit_sql(artifact.sql)
    except Exception:
        logger.error(
            "Job '%s' was cancelled but resubmit failed. "
            "Pipeline is DOWN. Re-run 'streamt apply' to recover.",
            artifact.name,
        )
        self._sql_hashes.pop(artifact.name, None)
        self._save_hashes()
        raise
    self._sql_hashes[artifact.name] = self._sql_hash(artifact.sql)
    self._save_hashes()
```
- **Agents**: Concurrency #3, Consistency C4, Fault Handling #3, Coordination #5

---

### BUG-002: Duplicate Flink Jobs on Concurrent Apply
- **Antithesis Term**: Missing Idempotency
- **Confidence**: High
- **Location**: `src/streamt/deployer/flink.py:519-527`
- **Code**:
```python
def apply_job(self, artifact: FlinkJobArtifact) -> str:
    change = self.plan_job(artifact)
    if change.action == "submit":
        self.submit_sql(artifact.sql)  # no dedup -- Flink allows duplicate job names
```
- **Attack Scenario**: Two CI pipelines trigger `streamt apply` concurrently. Both `plan_job()` calls see no running job. Both call `submit_sql()` with the same INSERT INTO statement. Flink creates two identical streaming jobs writing to the same sink topic, causing every event to be written twice. No guard exists -- Flink does not reject duplicate job names.
- **Remediation**: Use Flink's `pipeline.name` configuration for deterministic naming, then verify no job with that name exists immediately before submit. Or implement a project-level file lock around apply.

- **Agents**: Coordination #4, Fault Handling #5

---

### BUG-003: Consumer Resource Leak in `get_topic_message_count`
- **Antithesis Term**: Resource Leak
- **Confidence**: High
- **Location**: `src/streamt/deployer/kafka.py:431-442`
- **Code**:
```python
consumer = Consumer(consumer_config)
total = 0
for partition in range(partition_count):
    tp = TopicPartition(topic, partition)
    low, high = consumer.get_watermark_offsets(tp, timeout=DEFAULT_TIMEOUT)
    total += high - low
consumer.close()    # only reached on success
return total
# except Exception: consumer.close() never called
```
- **Attack Scenario**: `streamt status` calls this for each topic. If broker is intermittently unreachable, `get_watermark_offsets` times out, leaking a `Consumer` object (background thread + socket). Repeated status calls in a monitoring loop exhaust file descriptors. Compare with `get_consumer_group_lag` at line 374 which correctly uses `try/finally`.
- **Remediation**:
```python
consumer = Consumer(consumer_config)
try:
    total = 0
    for partition in range(partition_count):
        tp = TopicPartition(topic, partition)
        low, high = consumer.get_watermark_offsets(tp, timeout=DEFAULT_TIMEOUT)
        total += high - low
    return total
except Exception as e:
    logger.warning(f"Failed to get message count for {topic}: {e}")
    return 0
finally:
    consumer.close()
```
- **Agents**: Concurrency #5, Storage H2, Error Handling C4

---

## High Findings (P1)

### BUG-004: No Rollback Across 5-Deployer Sequential Apply
- **Antithesis Term**: Partial Failure / Non-Atomic Multi-Object Write
- **Confidence**: High
- **Location**: `src/streamt/deployer/planner.py:316-440`
- **Attack Scenario**: `streamt apply` creates 3 schemas and 5 topics, then fails on the 2nd Flink job. Schemas and topics are deployed but Flink jobs consuming from them are not. If Flink job A started before job B failed, job A is running against incomplete infrastructure. No rollback of successful resources.
- **Remediation**: Add `--fail-fast` mode (stop on first error). Track cross-phase dependencies -- if topic creation fails, skip Flink jobs that depend on that topic.
- **Agents**: Concurrency #4, Consistency C1, Fault Handling #1, Error Handling H1

### BUG-005: Missing `fsync` Before Atomic Rename in Hash Persistence
- **Antithesis Term**: Durability Violation
- **Confidence**: High
- **Location**: `src/streamt/deployer/flink.py:455-461`
- **Attack Scenario**: Power loss between `fd.close()` and data reaching disk. File renamed (metadata updated) but content may be zero-length on recovery. Next `_load_hashes` silently resets to empty, losing all SQL change detection state.
- **Remediation**: Add `fd.flush(); os.fsync(fd.fileno())` before `fd.close()`.
- **Agents**: Concurrency #2, Storage H1

### BUG-006: Hash File Has No File Locking (TOCTOU)
- **Antithesis Term**: Lost Update P4
- **Confidence**: High
- **Location**: `src/streamt/deployer/flink.py:437-467`
- **Attack Scenario**: Two concurrent `streamt apply` processes. Process A deploys job_X and writes hash. Process B deploys job_Y and overwrites the file, discarding job_X's hash. Next `plan` cannot detect SQL changes for job_X.
- **Remediation**: Add `fcntl.flock()` around the read-modify-write cycle in `_save_hashes`. Re-read from disk under lock before merging.
- **Agents**: Concurrency #1, Coordination #1

### BUG-007: `ErrorCode.MISSING_CONFIG` Does Not Exist -- Runtime AttributeError
- **Antithesis Term**: Missing Error Handling
- **Confidence**: High
- **Location**: `src/streamt/cli/commands/init.py:199`
- **Code**: `StructuredError(code=ErrorCode.MISSING_CONFIG, ...)`
- **Attack Scenario**: `streamt init --discover` without `--kafka` crashes with `AttributeError: type object 'ErrorCode' has no attribute 'MISSING_CONFIG'` instead of a clean error message.
- **Remediation**: Add `MISSING_CONFIG = "E208_MISSING_CONFIG"` to `ErrorCode` class in `core/errors.py`.
- **Agents**: Error Handling M10

### BUG-008: Flink Timeout Creates Ghost Statements
- **Antithesis Term**: Indefinite Treated as Definite
- **Confidence**: High
- **Location**: `src/streamt/deployer/flink.py:379-417`
- **Attack Scenario**: `submit_sql` times out waiting for a statement. The SQL Gateway continues executing server-side. On retry, `apply_job` submits the same SQL again, creating a duplicate running job. The timed-out statement's `operationHandle` is discarded -- no cancellation sent.
- **Remediation**: Before raising timeout, attempt to cancel the outstanding operation via the SQL Gateway API.
- **Agents**: Fault Handling #2

### BUG-009: FlinkDeployer Leaked in TestRunner
- **Antithesis Term**: Resource Leak
- **Confidence**: High
- **Location**: `src/streamt/testing/runner.py:199`
- **Code**: `deployer = FlinkDeployer(cluster.rest_url)` -- never closed
- **Attack Scenario**: Each `streamt test` with continuous tests leaks an HTTP session to the Flink cluster.
- **Remediation**: Use `with FlinkDeployer(cluster.rest_url) as deployer:`.
- **Agents**: Storage H3, Error Handling H7

### BUG-010: Apply Errors Classified as PARSE_ERROR
- **Antithesis Term**: Wrong Error Classification
- **Confidence**: High
- **Location**: `src/streamt/cli/commands/apply.py:157, 170`
- **Attack Scenario**: Deployment failures (topic creation error, Flink SQL error) reported as `E501_PARSE_ERROR`. Automated tools filtering by error code cannot distinguish parse failures from deployment failures. Line 170 catch-all labels everything "Cannot connect" even for non-connection errors.
- **Remediation**: Use `ErrorCode.FLINK_SQL_ERROR` or a new `DEPLOY_ERROR` code for deployment-phase errors. Use a generic `INTERNAL_ERROR` for the catch-all.
- **Agents**: Error Handling H5, H6

## Medium Findings (P2)

### BUG-011: No Pre-Flight Connectivity Check
- **Location**: `src/streamt/cli/commands/apply.py:110-122`
- All deployers have `check_connection()`/`health_check()` but they are never called before planning. User discovers Flink is down only after schemas and topics are already deployed.
- **Fix**: Call `check_connection()` on each deployer before planning.
- **Agents**: Fault Handling #6, Error Handling M7

### BUG-012: Pydantic Models Accept Extra Fields Silently
- **Location**: `src/streamt/core/models.py`, `src/streamt/core/runtime.py`
- No `extra="forbid"` on config models. Typos like `boostrap_servers` silently ignored.
- **Fix**: Add `model_config = ConfigDict(extra="forbid")` to all config models.
- **Agents**: Storage M2

### BUG-013: Schema Compatibility Bypass When Both Compatibility Level and Schema Change
- **Location**: `src/streamt/deployer/schema_registry.py:317-326`
- `apply_schema` sets compatibility to NONE first, then registers breaking schema. Plan checked against old level but apply uses new (relaxed) level.
- **Fix**: Check compatibility under the *target* level in plan, or validate before relaxing.
- **Agents**: Consistency C6, Storage M3

### BUG-014: No Retry in Kafka AdminClient Operations
- **Location**: `src/streamt/deployer/kafka.py` (entire file)
- All HTTP deployers retry 3x on transient failures. Kafka AdminClient operations are fire-once. A transient broker leadership change causes immediate hard failure.
- **Fix**: Add retry wrapper around AdminClient future resolution.
- **Agents**: Fault Handling #4

### BUG-015: Connect/SchemaRegistry/Flink Deployers Missing Closed-State Guard
- **Location**: `src/streamt/deployer/connect.py:95-97`, `schema_registry.py:98-100`, `flink.py:157-160`
- No `_closed` flag. Using after `close()` produces obscure `requests` errors instead of clear messages. KafkaDeployer and GatewayDeployer have this guard; these three do not.
- **Fix**: Add `_closed` flag and check, matching KafkaDeployer pattern.
- **Agents**: Error Handling M4, M5, M6

### BUG-016: Orphan Detection Deletes Resources Across Projects
- **Location**: `src/streamt/deployer/planner.py:270-314`
- `_detect_orphans` lists all cluster resources and marks any not in the manifest for deletion. No namespace filtering. One project's apply can delete another project's topics.
- **Fix**: Add project namespace prefix or ownership labels to orphan detection.
- **Agents**: Coordination #7, Consistency C9

### BUG-017: Plan Command Has No Catch-All for Deployer Errors
- **Location**: `src/streamt/cli/commands/plan.py:106-108`
- Only catches `EnvVarError, ParseError, EnvironmentError`. A deployer timeout produces a raw Python traceback instead of structured error output.
- **Fix**: Add catch-all `except Exception` with structured error like apply has.
- **Agents**: Fault Handling #9, Error Handling H2

### BUG-018: No Signal Handler -- Ctrl+C Shows Raw Traceback
- **Location**: `src/streamt/cli/commands/apply.py`
- No `KeyboardInterrupt` handler. Ctrl+C during apply shows raw traceback. The `finally` block does close deployers (verified), but no clean exit message.
- **Fix**: Add `except KeyboardInterrupt: fmt.print_error("Interrupted."); sys.exit(130)`.
- **Agents**: Concurrency #8, Error Handling C3

### BUG-019: Statement Polling Uses Accumulated Sleep, Not Wall-Clock Time
- **Location**: `src/streamt/deployer/flink.py:374-414`
- `elapsed += poll_interval` counts only sleep time, not HTTP request time. If status endpoint is slow (up to 30s per retry), actual wall-clock time far exceeds `statement_timeout`.
- **Fix**: Use `time.monotonic()` for elapsed time calculation.
- **Agents**: Fault Handling #8

### BUG-020: Manifest Write Is Not Atomic
- **Location**: `src/streamt/compiler/manifest.py:147-151`
- Direct `open(path, "w")` + `f.write()` without tempfile+rename. Crash mid-write produces truncated JSON.
- **Fix**: Use tempfile+rename pattern like hash persistence does.
- **Agents**: Storage M4

## Safe Harbor
Things that look risky but are acceptable:

- **YAML parsing**: All sites use `yaml.safe_load`. No injection risk.
- **Single-threaded concurrency**: Python GIL protects in-process state. No thread races within a single CLI invocation.
- **Double-plan in apply_***: Each deployer re-plans internally during apply. While it creates extra API calls, it's actually a safety feature -- ensures current state is checked before mutation.
- **Connection pooling**: `pool_connections=5, pool_maxsize=10` is appropriate for single-threaded CLI. No pool exhaustion risk.
- **Kafka Consumer `enable.auto.commit=False`**: No offsets committed to broker for internal counters. Safe.
- **Environment name validation**: `validate_env_name` rejects path traversal. Correct.
- **SSL file path validation**: `_validate_ssl_path` checks existence at parse time. Correct.

## Systemic Issues

### SYSTEMIC-001: No Coordination Layer for Apply
**Description**: There is no locking, fencing, or coordination for `streamt apply`. Multiple concurrent invocations can create duplicate Flink jobs (BUG-002), lose hash state (BUG-006), and delete each other's resources (BUG-016). The tool assumes single-operator, single-instance usage but is designed for CI/CD where concurrent execution is common.

**Affected**: BUG-001, BUG-002, BUG-006, BUG-016
**Fix**: Implement a project-level file lock (`fcntl.flock` on `.streamt/deploy.lock`) acquired before plan and released after apply. Document that CI pipelines must serialize apply (e.g., GitHub Actions concurrency groups).

### SYSTEMIC-002: No Rollback or Transaction Semantics in Multi-Deployer Apply
**Description**: The planner applies resources across 5 deployers sequentially with no rollback. A failure midway leaves the system in a partially deployed state. The planner continues applying resources from failed phases' downstream phases.

**Affected**: BUG-001, BUG-004, BUG-011
**Fix**: Add a dependency-aware fail-fast mode. After each phase, check if any errors in that phase should block dependent resources in the next phase. Use the compiler's DAG to identify dependencies.

### SYSTEMIC-003: Inconsistent Deployer Contracts
**Description**: The 5 deployers have inconsistent error handling, lifecycle management, and retry behavior:
- KafkaDeployer + GatewayDeployer have `_closed` guards; the other 3 do not
- HTTP deployers have retry logic; KafkaDeployer has none
- GatewayDeployer has `health_check()`; others have `check_connection()`; none are called
- Error codes in CLI are all `PARSE_ERROR` regardless of the actual failure domain

**Affected**: BUG-010, BUG-014, BUG-015, BUG-017
**Fix**: Define a `BaseDeployer` abstract class with standardized `_closed` guard, retry wrapper, and `check_connection()` interface. Enforce via tests.

## Testing Recommendations

- **Property-based tests**: Test that `apply` is idempotent -- running apply twice with the same manifest produces no changes on the second run.
- **Chaos/fault injection**: Simulate network failures during apply (especially between cancel_job and submit_sql) and verify recovery path.
- **Load tests**: Run concurrent `streamt apply` invocations and verify no duplicate Flink jobs are created.
- **Integration tests**: Test the full plan-apply-status cycle with a live Flink cluster where job submission is slow.

## Monitoring Recommendations

- **Alert**: Flink job count per sink table > 1 (detects BUG-002)
- **Metric**: `streamt_apply_partial_failures` counter when apply returns errors alongside successes (detects BUG-004)
- **Log pattern**: `"was cancelled but resubmit failed"` -- immediate page-level alert (BUG-001)
- **Alert**: Hash file age > 24h without update when apply is running regularly (detects BUG-005/006)

## Prioritized Fix List

1. [ ] **BUG-003** - Consumer leak: add `finally: consumer.close()` (1 line, prevents resource exhaustion)
2. [ ] **BUG-007** - Add `MISSING_CONFIG` to ErrorCode (1 line, prevents runtime crash)
3. [ ] **BUG-005** - Add `fd.flush(); os.fsync(fd.fileno())` before close (2 lines, prevents data loss)
4. [ ] **BUG-001** - Wrap cancel+resubmit with error recovery (10 lines, prevents pipeline downtime)
5. [ ] **BUG-009** - Use context manager for FlinkDeployer in TestRunner (1 line change)
6. [ ] **BUG-010** - Fix error codes in apply.py (2 lines, fixes error classification)
7. [ ] **BUG-018** - Add KeyboardInterrupt handler (3 lines, clean exit)
8. [ ] **BUG-015** - Add `_closed` guards to 3 deployers (pattern copy from KafkaDeployer)
9. [ ] **BUG-017** - Add catch-all to plan command (3 lines, structured errors)
10. [ ] **BUG-019** - Use `time.monotonic()` for statement polling (3 lines, correct timeout)
11. [ ] **BUG-006** - Add file locking to hash persistence (15 lines)
12. [ ] **BUG-011** - Pre-flight connectivity checks (10 lines)
13. [ ] **BUG-013** - Schema compatibility validation order (5 lines)
14. [ ] **BUG-020** - Atomic manifest writes (5 lines)
15. [ ] **BUG-012** - Add `extra="forbid"` to Pydantic models (5 lines)
16. [ ] **SYSTEMIC-001** - Project-level file lock for apply (20 lines)
17. [ ] **BUG-004** - Dependency-aware fail-fast in planner (design decision)
18. [ ] **BUG-002** - Flink job dedup via pipeline.name (design decision)
19. [ ] **BUG-016** - Namespace orphan detection (design decision)
20. [ ] **BUG-008** - Cancel outstanding SQL Gateway operations on timeout (design decision)

## Appendix
Individual agent reports: `.reliability-reports/findings-*.md`
- `findings-concurrency.md` - 9 findings (2 HIGH, 4 MEDIUM, 2 LOW)
- `findings-consistency.md` - 10 findings (3 HIGH, 6 MEDIUM, 1 LOW)
- `findings-fault-handling.md` - 12 findings (3 HIGH, 4 MEDIUM, 5 LOW)
- `findings-storage.md` - 11 findings (3 HIGH, 5 MEDIUM, 3 LOW)
- `findings-coordination.md` - 10 findings (1 CRITICAL, 4 HIGH, 4 MEDIUM, 1 LOW)
- `findings-error-handling.md` - 21 findings (4 CRITICAL, 7 HIGH, 10 MEDIUM, 7 LOW)
