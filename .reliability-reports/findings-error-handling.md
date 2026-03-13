# Error Handling Audit Findings

Auditor: Error Handling Auditor
Date: 2026-03-13
Scope: `src/streamt/deployer/`, `src/streamt/cli/commands/`, `src/streamt/core/errors.py`, `src/streamt/output.py`, `src/streamt/core/validator.py`, `src/streamt/testing/runner.py`, `src/streamt/compiler/`

---

## CRITICAL

### C1. Flink apply_job: hash saved before job confirmed running

**Antithesis Term**: Silent Corruption
**Confidence**: High
**File**: `src/streamt/deployer/flink.py:519-535`
**Code**:
```python
def apply_job(self, artifact: FlinkJobArtifact) -> str:
    change = self.plan_job(artifact)
    if change.action == "submit":
        self.submit_sql(artifact.sql)
        self._sql_hashes[artifact.name] = self._sql_hash(artifact.sql)  # saved immediately
        self._save_hashes()
        return "submitted"
```
**Guard Check**: Yes -- `submit_sql` waits for FINISHED status on statements, but does not verify the resulting Flink job reaches RUNNING. Confirmed no post-submit job status check exists.
**Attack Scenario**: Flink job SQL executes (statements FINISH) but the job crashes immediately (OOM, bad checkpoint config). Hash file records the new SQL. Next `plan` returns `action="none"` because hash matches. Pipeline is down but `streamt plan` and `streamt status` show "no changes needed."
**Remediation**: After `submit_sql`, poll `get_job_state` to confirm the job reaches RUNNING before persisting the hash. Alternatively, always compare hash AND live job status in `plan_job`.

---

### C2. Flink apply_job "update" path: cancel succeeds, re-submit fails, no rollback

**Antithesis Term**: Incomplete Cleanup / Partial State
**Confidence**: High
**File**: `src/streamt/deployer/flink.py:528-535`
**Code**:
```python
elif change.action == "update":
    if change.current and change.current.job_id:
        self.cancel_job(change.current.job_id)  # old job cancelled
    self.submit_sql(artifact.sql)                # if this fails, old job is gone
    self._sql_hashes[artifact.name] = self._sql_hash(artifact.sql)
    self._save_hashes()
    return "submitted"
```
**Guard Check**: Yes -- no rollback logic exists. No re-submission of old SQL on failure.
**Attack Scenario**: During a deploy, cancel_job succeeds for a running Flink pipeline but submit_sql fails (SQL Gateway down, bad SQL). The old pipeline is cancelled, the new one never starts. Pipeline is fully down with no automatic recovery.
**Remediation**: Wrap cancel+submit in a transaction-like pattern. If re-submit fails, log a CRITICAL-level message naming the job that was cancelled but not replaced. Consider storing the old SQL to enable rollback.

---

### C3. No signal handling: Ctrl+C during apply leaves partial state

**Antithesis Term**: Shutdown Issues
**Confidence**: High
**File**: `src/streamt/cli/commands/apply.py`, `src/streamt/deployer/planner.py:316-441`
**Code**: No `signal`, `SIGINT`, `SIGTERM`, or `KeyboardInterrupt` handling anywhere in the codebase (verified via `rg KeyboardInterrupt|SIGINT|signal src/streamt/` -- zero matches).
**Guard Check**: Yes -- confirmed no signal handling exists in any file.
**Attack Scenario**: During `DeploymentPlanner.apply()`, resources are deployed sequentially (schemas, topics, Flink jobs, connectors, gateway rules). User presses Ctrl+C midway through topic creation. Some topics are created, others not. Flink jobs submitted next run would reference non-existent topics. No record of which resources were applied before interruption. The `close_deployers` in `finally` closes HTTP sessions but cannot undo partial deployments.
**Remediation**: Add a `KeyboardInterrupt` handler in `DeploymentPlanner.apply()` that captures which resources were already applied and surfaces this in the result dict.

---

### C4. KafkaDeployer.get_topic_message_count: consumer not closed on exception

**Antithesis Term**: Resource Leak
**Confidence**: High
**File**: `src/streamt/deployer/kafka.py:431-442`
**Code**:
```python
consumer = Consumer(consumer_config)
total = 0
for partition in range(partition_count):
    tp = TopicPartition(topic, partition)
    low, high = consumer.get_watermark_offsets(tp, timeout=DEFAULT_TIMEOUT)
    total += high - low
consumer.close()   # NOT in finally block
return total
```
**Guard Check**: Yes -- compared with `get_consumer_group_lag` at line 374-380 which correctly uses `try/finally`. This method does not.
**Attack Scenario**: `get_watermark_offsets` raises on timeout or broker disconnect. `consumer.close()` is never called. Repeated failures (e.g., `streamt status --lag` in a loop) exhaust broker connection limits.
**Remediation**: Wrap in `try/finally` like `get_consumer_group_lag`.

---

### C5. Credentials embedded in compiled Flink SQL written to disk and error messages

**Antithesis Term**: Credential Leakage
**Confidence**: High
**File**: `src/streamt/compiler/flink_ddl.py:69-74`, `src/streamt/compiler/compiler.py:968-970`, `src/streamt/deployer/flink.py:405`
**Code**:
```python
# flink_ddl.py:69-74 -- plaintext password embedded in SQL
jaas = (
    f'{module} required '
    f'username="{kafka.sasl_username}" '
    f'password="{_secret(kafka.sasl_password)}";'
)
```
**Guard Check**: Yes -- verified the SQL is written to manifest.json (manifest.py:147-151), to per-job JSON files (compiler.py:968-970), and included in Flink SQL error messages (flink.py:405: `errors.flink_sql_error(error_msg, statement[:200])`).
**Attack Scenario**: (1) `streamt compile` writes manifest.json with SASL passwords in plaintext SQL to disk, potentially committed to git. (2) If Flink SQL execution fails, the error message includes `statement[:200]` which contains the JAAS config with plaintext password. This error propagates through `_sanitize_error` in planner.py, but `_sanitize_error` only matches `password=xxx` patterns -- the JAAS format `password="actual_password"` with quotes may not match depending on whitespace. (3) ssl.key.password is also written to the SQL string.
**Remediation**: (1) Redact secrets from compiled SQL before writing to disk -- use `${ENV_VAR}` references in DDL and resolve at submit time, or add manifest.json to .gitignore by default. (2) Truncate SQL in error messages to exclude WITH clause properties. (3) Extend `_sanitize_error` to handle quoted credential patterns.

---

## HIGH

### H1. DeploymentPlanner.apply continues after errors (no fail-fast option)

**Antithesis Term**: Wrong Error Classification
**Confidence**: High
**File**: `src/streamt/deployer/planner.py:316-441`
**Code**: Every resource deployment is wrapped in `try/except Exception` that appends to `results["errors"]` and continues.
**Guard Check**: Yes -- no circuit breaker or dependency tracking exists.
**Attack Scenario**: Schema Registry is down. All schema registrations fail. Planner continues to create topics (succeeds) then submits Flink jobs. Flink jobs reference schemas that don't exist and fail with confusing "table not found" errors. User sees 20 errors instead of 1 root cause.
**Remediation**: Add `fail_fast` mode (default for interactive). Track which schemas/topics failed and skip dependent resources.

---

### H2. Plan command lacks catch-all exception handler

**Antithesis Term**: Missing Error Propagation
**Confidence**: High
**File**: `src/streamt/cli/commands/plan.py:106-108`
**Code**:
```python
except (EnvVarError, ParseError, EnvironmentError) as e:
    handle_parse_error(fmt, e, ErrorCode.PARSE_ERROR)
# No catch-all -- compare with apply.py:169 which has except Exception
```
**Guard Check**: Yes -- compared apply.py (has catch-all), validate.py (has catch-all), compile.py (has catch-all). Plan is the outlier.
**Attack Scenario**: In JSON output mode, a deployer timeout during planning produces a raw Python traceback to stderr with no JSON envelope on stdout. Consumers expecting JSON get nothing parseable.
**Remediation**: Add `except Exception as e` with structured error output, matching apply/compile/validate pattern.

---

### H3. Planner orphan detection silently swallows failures

**Antithesis Term**: Swallowed Exception
**Confidence**: High
**File**: `src/streamt/deployer/planner.py:270-314`
**Code**:
```python
try:
    for subject in self.schema_registry_deployer.list_subjects():
        ...
except Exception as e:
    logger.error("Failed to list subjects for orphan detection: %s", e)
```
**Guard Check**: Yes -- the plan returned contains no indication orphan detection was incomplete. No warnings field exists on DeploymentPlan.
**Attack Scenario**: User runs `streamt plan`, orphan detection fails silently. Plan shows "no deletes." User assumes no orphaned resources exist, but orphaned topics/schemas accumulate, wasting cluster resources.
**Remediation**: Add a `warnings` field to `DeploymentPlan`. Append orphan detection failures there. Surface in `plan.details()`.

---

### H4. Deployer creation failures allow apply to proceed with missing deployers

**Antithesis Term**: Missing Error Propagation
**Confidence**: High
**File**: `src/streamt/cli/helpers.py:104-213`, `src/streamt/cli/commands/apply.py:111-115`
**Code**: `make_*_deployer` functions catch exceptions, warn, return None. Apply proceeds with None deployers -- planner silently skips those resource types.
**Guard Check**: Yes -- `DeploymentPlanner.__init__` accepts Optional deployers and skips them in plan/apply.
**Attack Scenario**: Kafka is unreachable. `make_kafka_deployer` returns None. Apply silently skips topic creation. Flink jobs are submitted referencing non-existent topics. Exit code is 0. User thinks deployment succeeded.
**Remediation**: After creating deployers, verify required deployers are not None by checking if the manifest contains artifacts for that resource type. Abort with error before calling `planner.apply()`.

---

### H5. apply.py catch-all uses wrong error code and misleading message

**Antithesis Term**: Wrong Error Classification
**Confidence**: High
**File**: `src/streamt/cli/commands/apply.py:169-173`
**Code**:
```python
except Exception as e:
    fmt.add_error(StructuredError(code=ErrorCode.PARSE_ERROR, message=f"Cannot connect: {e}"))
```
**Guard Check**: Yes -- ErrorCode.PARSE_ERROR is E501, but this catches all exceptions including compiler bugs, serialization errors, permission issues.
**Attack Scenario**: A TypeError in the compiler is reported as "E501_PARSE_ERROR: Cannot connect: 'NoneType' object has no attribute 'name'". User investigates network connectivity when the issue is a code bug.
**Remediation**: Use `_classify_connection_error()` for connection errors. Add a generic `E500_INTERNAL_ERROR` code for unexpected exceptions.

---

### H6. apply.py classifies deployment errors as PARSE_ERROR

**Antithesis Term**: Wrong Error Classification
**Confidence**: High
**File**: `src/streamt/cli/commands/apply.py:157`
**Code**:
```python
fmt.add_error(StructuredError(code=ErrorCode.PARSE_ERROR, message=item))
```
**Guard Check**: Yes -- deployment errors from planner.apply() (e.g., "topic:orders: Failed to create") are classified as E501_PARSE_ERROR instead of a deployment error code.
**Attack Scenario**: CI/CD pipeline filters errors by code to decide retry strategy. Deployment failures are classified as parse errors, so the pipeline does not retry (parse errors are deterministic).
**Remediation**: Use a deployment-specific error code (add `DEPLOY_ERROR` to E4xx range).

---

### H7. TestRunner._run_continuous_test creates FlinkDeployer without close()

**Antithesis Term**: Resource Leak
**Confidence**: High
**File**: `src/streamt/testing/runner.py:199-210`
**Code**:
```python
deployer = FlinkDeployer(cluster.rest_url)
job_state = deployer.get_job_state(f"test_{test.name}")
# deployer.close() never called
```
**Guard Check**: Yes -- no `finally` block, no `with` statement. requests.Session inside deployer is leaked.
**Attack Scenario**: Running `streamt test` with N continuous tests leaks N HTTP sessions.
**Remediation**: Use `with FlinkDeployer(cluster.rest_url) as deployer:`.

---

### H8. init command leaks KafkaDeployer and SchemaRegistryDeployer

**Antithesis Term**: Resource Leak
**Confidence**: High
**File**: `src/streamt/cli/commands/init.py:211, 236`
**Code**:
```python
kafka_deployer = KafkaDeployer(kafka)     # line 211 -- never closed
sr_deployer = SchemaRegistryDeployer(schema_registry)  # line 236 -- never closed
```
**Guard Check**: Yes -- searched for `.close()` and `with` in init.py, zero matches.
**Attack Scenario**: `streamt init --discover` creates deployers that are never cleaned up. On CLI exit Python GC handles it, but if the command is called programmatically or in a long-running process, connections leak.
**Remediation**: Wrap in `with` statements or add `finally: close_deployers(kafka_deployer, sr_deployer)`.

---

### H9. CLI error messages expose unsanitized exception strings (potential credential leakage)

**Antithesis Term**: Credential Leakage
**Confidence**: Medium
**File**: `src/streamt/cli/commands/status.py:92,143,170,198,242`, `src/streamt/cli/helpers.py:86`, `src/streamt/cli/commands/apply.py:170`
**Code**:
```python
# status.py -- raw exception in user-facing output
fmt.add_error(StructuredError(code=ErrorCode.CONNECTION_REFUSED, message=f"Schema Registry: {e}"))
fmt.print(f"  [yellow]Cannot connect to Schema Registry: {e}[/yellow]")

# helpers.py -- raw exception in error message
return ("", f"Cannot connect to {service}: {e}")
```
**Guard Check**: Yes -- `_sanitize_error` from planner.py is never imported or used in CLI commands or helpers. Confirmed by `rg _sanitize_error src/streamt/cli/` returning zero matches.
**Attack Scenario**: Exception from `requests` includes URL with embedded credentials (e.g., `http://admin:secret@registry:8081`). This appears in CLI output, JSON error envelopes, and log files. `_sanitize_error` only covers `password=xxx` patterns, not URL-embedded credentials.
**Remediation**: Create a shared `sanitize_error_message()` utility used by both planner.py and all CLI commands. Add URL credential pattern: `re.compile(r'://[^:]+:[^@]+@')`.

---

## MEDIUM

### M1. _sanitize_error regex misses URL-embedded credentials

**Antithesis Term**: Credential Leakage
**Confidence**: High
**File**: `src/streamt/deployer/planner.py:19-32`
**Code**:
```python
_SENSITIVE_KV = re.compile(
    r"(password|passwd|secret|token|api_key|apikey)\s*[=:]\s*\S+", re.IGNORECASE,
)
```
**Guard Check**: Yes -- tested: `_sanitize_error('http://admin:s3cr3t@host:8888')` returns the input unchanged. Also does not match JAAS-style `password="value"` (quoted format from flink_ddl.py).
**Attack Scenario**: Connection error from `requests` includes full URL with embedded credentials. Error passes through `_sanitize_error` unmasked and appears in `planner.apply()` results.
**Remediation**: Add URL credential pattern: `re.compile(r'://([^:]+):([^@]+)@')` and replace with `://***:***@`. Also handle quoted values: `password\s*=\s*"[^"]*"`.

---

### M2. Flink/Connect/SR/Gateway _request retries 500s but not 429 (rate limiting)

**Antithesis Term**: Wrong Error Classification
**Confidence**: Medium
**File**: `src/streamt/deployer/flink.py:200`, `src/streamt/deployer/connect.py:116`, `src/streamt/deployer/schema_registry.py:120`, `src/streamt/deployer/gateway.py:169`
**Code**: All four deployers retry on `status_code >= 500` but not on 429.
**Guard Check**: Yes -- consistent across all deployer _request methods.
**Attack Scenario**: Rate-limited API returns 429 with Retry-After header. Deployer treats it as a permanent client error and raises immediately.
**Remediation**: Add `status_code == 429` to retry condition. Read `Retry-After` header.

---

### M3. Flink _request: retries=0 produces confusing TypeError

**Antithesis Term**: Missing Validation
**Confidence**: Medium
**File**: `src/streamt/deployer/flink.py:130-132, 196-210`
**Code**: `self._retries = retries or 3` -- if retries=0 is passed, `0 or 3` evaluates to 3 (masked). But if the internal field were somehow 0, `range(0)` produces no iterations, `for/else` fires, and `raise None` produces `TypeError`.
**Guard Check**: Yes -- the `or 3` prevents retries=0 in practice, but the intent is unclear. Explicit validation would be better.
**Attack Scenario**: Edge case. If someone subclasses and sets `_retries = 0`, they get `TypeError: exceptions must derive from BaseException`.
**Remediation**: Add `if self._retries < 1: raise ValueError("retries must be >= 1")` in `__init__`.

---

### M4. ConnectDeployer has no _check_closed guard

**Antithesis Term**: Missing Validation
**Confidence**: Medium
**File**: `src/streamt/deployer/connect.py:95-97`
**Code**: `close()` closes HTTP session but sets no `_closed` flag. No guard on public methods.
**Guard Check**: Yes -- compared with KafkaDeployer (has `_closed` + `_check_closed`), GatewayDeployer (has `_closed` + check in `_request`). ConnectDeployer is the outlier.
**Attack Scenario**: After `close_deployers()` is called, if any code path references the deployer, operations use a closed session producing obscure errors.
**Remediation**: Add `_closed` flag and `_check_closed()`, matching KafkaDeployer.

---

### M5. SchemaRegistryDeployer has no _check_closed guard

**Antithesis Term**: Missing Validation
**Confidence**: Medium
**File**: `src/streamt/deployer/schema_registry.py:98-100`
**Code**: Same as M4. `close()` closes HTTP session with no flag or guard.
**Guard Check**: Yes.
**Remediation**: Add `_closed` flag and check.

---

### M6. FlinkDeployer has no _check_closed guard

**Antithesis Term**: Missing Validation
**Confidence**: Medium
**File**: `src/streamt/deployer/flink.py:157-160`
**Code**: `close()` calls `close_session()` + `_http_session.close()` but sets no `_closed` flag.
**Guard Check**: Yes.
**Remediation**: Add `_closed` flag and check.

---

### M7. No pre-flight connectivity check before plan/apply

**Antithesis Term**: Missing Validation
**Confidence**: Medium
**File**: `src/streamt/cli/commands/apply.py:110-122`
**Code**: Deployers are created and immediately used for planning. `check_connection()` / `health_check()` methods exist but are never called.
**Guard Check**: Yes -- confirmed no command calls health check methods.
**Attack Scenario**: User waits through schema + topic deployment only to discover Flink is down at job submission phase. Wasted time and partial deployment.
**Remediation**: Call `check_connection()` on each deployer after construction and before planning.

---

### M8. validator silently ignores unparseable retention.ms

**Antithesis Term**: Silent Corruption
**Confidence**: Medium
**File**: `src/streamt/core/validator.py:550-559`
**Code**:
```python
try:
    retention_val = int(retention)
    ...
except (ValueError, TypeError):
    pass
```
**Guard Check**: Yes -- no warning or log emitted when retention.ms is unparseable.
**Attack Scenario**: User sets `retention.ms: "7d"` (human-readable but invalid for Kafka). Validation passes silently. Topic creation succeeds with Kafka's default retention, not the user's intended 7 days.
**Remediation**: Add a warning when retention.ms cannot be parsed as int.

---

### M9. compile command re-raises exception after structured error output

**Antithesis Term**: Missing Error Propagation
**Confidence**: High
**File**: `src/streamt/cli/commands/compile.py:100-104`
**Code**:
```python
except Exception as e:
    fmt.add_error(StructuredError(code=ErrorCode.PARSE_ERROR, message=str(e)))
    fmt.print_error(str(e))
    fmt.flush()
    raise  # <-- produces traceback after structured output
```
**Guard Check**: Yes -- all other commands use `sys.exit(1)`.
**Attack Scenario**: Users see both a structured error message and a raw Python traceback. In JSON mode, the traceback corrupts the expected output format.
**Remediation**: Replace `raise` with `sys.exit(1)`.

---

### M10. init command uses undefined ErrorCode.MISSING_CONFIG

**Antithesis Term**: Missing Validation
**Confidence**: High
**File**: `src/streamt/cli/commands/init.py:199`
**Code**:
```python
StructuredError(code=ErrorCode.MISSING_CONFIG, message="--kafka is required with --discover")
```
**Guard Check**: Yes -- `rg MISSING_CONFIG src/streamt/core/errors.py` returns zero matches. The attribute does not exist.
**Attack Scenario**: `streamt init --discover` (without `--kafka`) raises `AttributeError: type object 'ErrorCode' has no attribute 'MISSING_CONFIG'` instead of the intended user-friendly error.
**Remediation**: Add `MISSING_CONFIG = "E210_MISSING_CONFIG"` to `ErrorCode`, or use existing `PARSE_ERROR`.

---

## LOW

### L1. Validator _validate_column_types swallows all exceptions with no logging

**Antithesis Term**: Swallowed Exception
**Confidence**: Medium
**File**: `src/streamt/core/validator.py:469-472`
**Code**:
```python
try:
    issues = checker.check_model(model)
except Exception:
    continue  # no logging at all
```
**Guard Check**: Yes -- no `logger.debug` or any other output.
**Attack Scenario**: A bug in ColumnTypeChecker is completely invisible. Type checking silently does nothing.
**Remediation**: Add `logger.debug("Column type check failed for model '%s': %s", model.name, e)`.

---

### L2. type_checker._extract_source_column_refs catches sqlglot parse failure silently

**Antithesis Term**: Swallowed Exception
**Confidence**: Medium
**File**: `src/streamt/core/type_checker.py:234`
**Code**:
```python
except Exception:
    return referenced  # empty set, no logging
```
**Guard Check**: Yes -- no logging.
**Attack Scenario**: Column reference validation silently skipped for any SQL sqlglot cannot handle.
**Remediation**: Add `logger.debug`.

---

### L3. masking.py regex fallback can produce incorrect SQL

**Antithesis Term**: Silent Corruption
**Confidence**: Medium
**File**: `src/streamt/compiler/masking.py:125-142`
**Code**: Regex replaces first occurrence of column name anywhere in SQL (including WHERE, JOIN), not just SELECT. Only triggered when AST path fails.
**Guard Check**: Yes -- AST path at line 74-111 correctly scopes to SELECT expressions.
**Attack Scenario**: Column name appears in WHERE clause. Regex masks it there instead of in SELECT, producing invalid SQL.
**Remediation**: Log a warning (not just debug) when falling back to regex.

---

### L4. KafkaDeployer.get_consumer_group_lag returns None on any exception

**Antithesis Term**: Wrong Error Classification
**Confidence**: Medium
**File**: `src/streamt/deployer/kafka.py:410-412`
**Code**:
```python
except Exception as e:
    logger.warning(f"Failed to get consumer group lag for {group_id}: {e}")
    return None
```
**Guard Check**: Yes -- catches TypeError, KeyError, programming bugs. Caller cannot distinguish "group doesn't exist" from "broker down" from "code bug".
**Remediation**: Catch only Kafka-specific exceptions. Let programming errors propagate.

---

### L5. GatewayDeployer.health_check only catches ConnectionError

**Antithesis Term**: Missing Error Propagation
**Confidence**: Medium
**File**: `src/streamt/deployer/gateway.py:197-206`
**Code**:
```python
except requests.ConnectionError:
    return False
# Timeout, SSLError propagate unhandled
```
**Guard Check**: Yes -- other deployers catch `Exception` in their health check methods.
**Attack Scenario**: `Timeout` during health check crashes instead of returning `False`.
**Remediation**: Catch `(requests.ConnectionError, requests.Timeout, requests.exceptions.SSLError)` or `Exception`.

---

### L6. Flink _save_hashes: temp file descriptor leaked on write failure

**Antithesis Term**: Resource Leak
**Confidence**: Low
**File**: `src/streamt/deployer/flink.py:455-467`
**Code**:
```python
fd = tempfile.NamedTemporaryFile(mode="w", dir=path.parent, suffix=".tmp", delete=False)
try:
    _json.dump(self._sql_hashes, fd)
    fd.close()
    Path(fd.name).replace(path)
except Exception:
    # fd is never closed before unlink
    Path(fd.name).unlink(missing_ok=True)
```
**Guard Check**: Yes -- `fd.close()` is inside the try block, not in a finally.
**Attack Scenario**: On Linux, file descriptor leaks until GC. On Windows, unlink would fail (file locked).
**Remediation**: Move `fd.close()` to a `finally` block.

---

### L7. No exit code differentiation across CLI commands

**Antithesis Term**: Missing Error Propagation
**Confidence**: Low
**File**: All CLI commands
**Code**: Every failure exits with `sys.exit(1)` regardless of error type.
**Guard Check**: Yes -- no command uses any exit code other than 0 or 1.
**Attack Scenario**: CI/CD systems cannot distinguish retriable errors (connection timeout) from deterministic errors (invalid YAML).
**Remediation**: Consider exit code conventions: 1=parse/validation, 2=connection, 3=deployment.

---

### L8. Flink SQL error messages may include JAAS credentials

**Antithesis Term**: Credential Leakage
**Confidence**: Medium
**File**: `src/streamt/deployer/flink.py:405`
**Code**:
```python
raise RuntimeError(errors.flink_sql_error(error_msg, statement[:200]))
```
**Guard Check**: Yes -- the `statement[:200]` truncation may or may not include the WITH clause containing JAAS credentials depending on SQL length. Short CREATE TABLE statements will include the WITH clause with `password="..."`.
**Attack Scenario**: A CREATE TABLE with JAAS config fails. The first 200 chars include the password in the error message shown to the user and stored in apply results.
**Remediation**: Strip WITH clause content from SQL before including in error messages, or pass only the statement type and table name.

---

## Summary

| Severity | Count | Key themes |
|----------|-------|------------|
| CRITICAL | 5     | Hash file consistency, partial apply state, no interrupt handling, consumer leak, credential leakage in compiled SQL |
| HIGH     | 9     | No fail-fast, missing catch-all in plan, wrong error codes, deployer null handling, resource leaks, unsanitized exceptions |
| MEDIUM   | 10    | No pre-flight checks, no closed-state guards, rate limiting, undefined error code |
| LOW      | 8     | Swallowed exceptions, regex masking risk, exit code uniformity, fd leak |

### Most impactful fixes (ordered by risk reduction):

1. **M10** - Fix `ErrorCode.MISSING_CONFIG` AttributeError (runtime crash, trivial fix)
2. **C5** - Stop writing credentials to compiled manifest/SQL files on disk
3. **H9** - Sanitize exception messages in CLI before displaying to user
4. **C3** - Add KeyboardInterrupt handling to apply
5. **C4** - Fix consumer leak in `get_topic_message_count`
6. **C1** - Don't persist hash until job is confirmed running
7. **H4** - Validate deployer availability against manifest before apply
8. **M7** - Pre-flight connectivity checks
9. **H1** - Add fail-fast / dependency-aware apply
10. **M9** - Replace `raise` with `sys.exit(1)` in compile command
