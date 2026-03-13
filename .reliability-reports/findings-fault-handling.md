# Fault Handling Audit Findings

**Auditor**: Fault Handling Auditor (Antithesis Reliability Audit)
**Scope**: `src/streamt/deployer/`, `src/streamt/cli/commands/apply.py`, `src/streamt/cli/helpers.py`
**Date**: 2026-03-13

---

## Finding 1

**[CRITICAL]** Partial Deployment Without Rollback in DeploymentPlanner.apply()
**Antithesis Term**: Partial Failure Without Rollback
**Confidence**: High
**File**: `src/streamt/deployer/planner.py:316-440`
**Code**:
```python
def apply(self, plan: Optional[DeploymentPlan] = None) -> dict[str, object]:
    # Apply schemas first
    for change in plan.schema_changes:
        try:
            result = self.schema_registry_deployer.apply_schema(change.desired)
            ...
        except Exception as e:
            results["errors"].append(f"schema:{change.subject}: {_sanitize_error(e)}")
    # Apply topics ...
    # Apply Flink jobs ...
    # Apply connectors ...
    # Apply gateway rules ...
    return results
```
**Guard Check**: Yes. Each resource type catches exceptions individually and appends to `results["errors"]`, but no rollback of previously successful steps occurs. Only `GatewayDeployer.apply()` implements local rollback for interceptor creation failures.
**Attack Scenario**: Schema registers successfully, topic creates successfully, Flink SQL submission fails. The system is now in a half-deployed state: schemas and topics exist but no Flink jobs are processing data. On retry, `plan()` sees the schema and topic as "unchanged" and only retries the Flink job -- but if the schema was *updated* (not created), the old version is gone forever. In a multi-model project with 20 topics + 20 jobs, failure at job #10 leaves 10 orphaned topics and 9 orphaned jobs processing stale data against newly-created schemas.
**Remediation**: Implement a two-phase apply with rollback tracking. On any error in a downstream phase, roll back the current phase's completed work. Alternatively, adopt a persistent partial-state file so the next `apply` can resume from where it left off:
```python
def apply(self, plan, fail_fast: bool = False):
    committed = {"schemas": [], "topics": [], ...}
    try:
        for change in plan.schema_changes:
            self._apply_schema(change)
            committed["schemas"].append(change)
        ...
    except Exception:
        self._rollback(committed)
        raise
```

---

## Finding 2

**[CRITICAL]** Flink Job Update: Cancel Succeeds but Re-submit Fails Leaves No Running Job
**Antithesis Term**: Partial Failure Without Rollback
**Confidence**: High
**File**: `src/streamt/deployer/flink.py:528-535`
**Code**:
```python
elif change.action == "update":
    # Cancel the running job, then re-submit with new SQL
    if change.current and change.current.job_id:
        self.cancel_job(change.current.job_id)
    self.submit_sql(artifact.sql)
    self._sql_hashes[artifact.name] = self._sql_hash(artifact.sql)
    self._save_hashes()
    return "submitted"
```
**Guard Check**: Yes. No try/except around the cancel+submit pair. No rollback if `submit_sql` fails after `cancel_job` succeeds. Additionally, `cancel_job` sends `PATCH /jobs/{id}` which is asynchronous -- the old job transitions through CANCELLING before reaching CANCELLED. The new job may start before the old one fully stops, causing dual-write to the same sink.
**Attack Scenario**: A Flink job update triggers `cancel_job()` which succeeds, then `submit_sql()` fails (SQL syntax error in new version, Gateway timeout, session expired). The old job is cancelled, the new job never started. The pipeline is **down with no running processor**. The hash file is not updated (lines 533-534 are unreachable), so the system still has the old hash -- but there is no running job. This is an unrecoverable data pipeline outage requiring manual intervention.
**Remediation**: Wrap in try/except. On submit failure, log a CRITICAL error and consider re-submitting the old SQL if available:
```python
elif change.action == "update":
    if change.current and change.current.job_id:
        self.cancel_job(change.current.job_id)
        # Optionally: poll until CANCELLED before re-submitting
    try:
        self.submit_sql(artifact.sql)
    except Exception:
        logger.critical("Re-submit failed after cancelling job '%s'. Pipeline is DOWN.", artifact.name)
        raise
    self._sql_hashes[artifact.name] = self._sql_hash(artifact.sql)
    self._save_hashes()
```

---

## Finding 3

**[HIGH]** Flink Statement Polling Timeout Treated as Definite Failure -- Statement May Still Complete Server-Side
**Antithesis Term**: Indefinite Treated as Definite
**Confidence**: High
**File**: `src/streamt/deployer/flink.py:379-417`
**Code**:
```python
while elapsed < statement_timeout:
    status_response = self._request(...)
    status = status_response.get("status")
    if status == "FINISHED":
        break
    elif status in ("RUNNING", "PENDING"):
        time.sleep(poll_interval)
        elapsed += poll_interval
        poll_interval = min(poll_interval * 2, max_poll_interval)

if elapsed >= statement_timeout:
    raise RuntimeError(f"Timeout waiting for statement: {statement[:50]}...")
# ...
except Exception:
    self.close_session()
    raise
```
**Guard Check**: Yes. The `close_session()` is called on exception (line 416), but the statement's operation is NOT cancelled on the Flink SQL Gateway side. The operation handle is discarded and never tracked.
**Attack Scenario**: A `CREATE TABLE` or `INSERT INTO` statement takes 62 seconds (just over the 60s default). The client raises `RuntimeError("Timeout")`, closes the session, and reports failure. But the Flink SQL Gateway completes the statement 2 seconds later. The resource IS created. On retry, `apply_job()` calls `submit_sql()` again, potentially creating a duplicate INSERT job running in parallel against the same sink, causing data duplication. The SQL hash is never saved on timeout, so the next `plan_job()` will always see the job as needing submission.
**Remediation**: On timeout, attempt to cancel the operation before raising, and report the timeout as indeterminate:
```python
if elapsed >= statement_timeout:
    try:
        self._request("POST",
            f"/v1/sessions/{session_id}/operations/{operation_handle}/close",
            use_sql_gateway=True)
    except Exception:
        pass
    raise RuntimeError(
        f"Timeout ({statement_timeout}s) waiting for statement. "
        f"Operation may still be completing server-side: {statement[:50]}..."
    )
```

---

## Finding 4

**[HIGH]** Kafka Consumer Resource Leak in get_topic_message_count on Exception
**Antithesis Term**: Resource Leak on Error Path
**Confidence**: High
**File**: `src/streamt/deployer/kafka.py:431-442`
**Code**:
```python
def get_topic_message_count(self, topic: str) -> int:
    ...
    try:
        consumer = Consumer(consumer_config)
        total = 0
        for partition in range(partition_count):
            tp = TopicPartition(topic, partition)
            low, high = consumer.get_watermark_offsets(tp, timeout=DEFAULT_TIMEOUT)
            total += high - low
        consumer.close()   # <-- only on happy path
        return total
    except Exception as e:
        logger.warning(f"Failed to get message count for {topic}: {e}")
        return 0           # <-- consumer never closed
```
**Guard Check**: Yes. The `consumer.close()` is on the happy path but NOT in a `finally` block. If `get_watermark_offsets` raises (timeout on partition 3 of 6), the consumer is never closed, leaking a connection, a socket, and a consumer group membership. Contrast with `get_consumer_group_lag()` at line 374-380 which correctly uses `try/finally`.
**Attack Scenario**: `streamt status --lag` is called against a cluster with network instability. Each call to `get_topic_message_count` that times out leaks a Kafka consumer. Across multiple topics and repeated status checks, this exhausts file descriptors or socket limits. The leaked consumer group `_streamt_internal_count` also holds group membership, which can block rebalances for other consumers on the same group.
**Remediation**: Use `try/finally`:
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
    logger.warning(...)
    return 0
finally:
    consumer.close()
```

---

## Finding 5

**[HIGH]** No Circuit Breaker -- Failing Dependency Cascades Through All Resources
**Antithesis Term**: No Circuit Breaker
**Confidence**: High
**File**: `src/streamt/deployer/planner.py:330-431`
**Code**:
```python
# Each resource type catches Exception and continues:
for change in plan.topic_changes:
    try:
        result = self.kafka_deployer.apply_topic(change.desired)
        ...
    except Exception as e:
        results["errors"].append(f"topic:{change.topic}: {_sanitize_error(e)}")
```
**Guard Check**: Yes. Every single resource application catches `Exception` and appends a string to `results["errors"]`. There is no short-circuit or fail-fast mode. The original exception type, traceback, and structure are lost. `check_connection()` methods exist on Flink, Connect, SchemaRegistry, and Gateway deployers but are **never called** by the planner or CLI commands.
**Attack Scenario**: The Kafka cluster is down. All 20 topic creates fail. Each topic create incurs `DEFAULT_TIMEOUT=10s` per AdminClient future. With 20 topics, that is 200 seconds of blocking. The planner then continues to attempt all 20 Flink jobs (which also fail because topics don't exist) -- each with `3 retries x 30s timeout = 90s`. Then all connectors, then all gateway rules. With 50 total resources, the apply takes many minutes to ultimately report "everything failed" when a single 10s health check would have shown "Kafka is down."
**Remediation**: Add a circuit breaker or fail-fast mode. Call `check_connection()` before each deployer phase, or track consecutive failures and short-circuit:
```python
def apply(self, plan, fail_fast: bool = False):
    ...
    consecutive_failures = 0
    for change in plan.topic_changes:
        try:
            ...
            consecutive_failures = 0
        except Exception as e:
            consecutive_failures += 1
            results["errors"].append(...)
            if fail_fast or consecutive_failures >= 3:
                logger.error("Circuit breaker: %d consecutive failures, skipping remaining topics", consecutive_failures)
                break
```

---

## Finding 6

**[HIGH]** Kafka Topic Create Not Idempotent -- Retry on Transient Error Raises TopicAlreadyExistsError
**Antithesis Term**: No Idempotency
**Confidence**: High
**File**: `src/streamt/deployer/kafka.py:223-239`
**Code**:
```python
def create_topic(self, artifact: TopicArtifact) -> None:
    new_topic = NewTopic(artifact.name, ...)
    futures = self.admin.create_topics([new_topic])
    for topic, future in futures.items():
        try:
            future.result(timeout=DEFAULT_TIMEOUT)
        except Exception as e:
            raise RuntimeError(f"Failed to create topic '{topic}': {e}") from e
```
**Guard Check**: Yes. No check for "topic already exists" in the exception. The caller `apply_topic()` calls `plan_topic()` first to check existence, but a TOCTOU race exists: between `plan_topic()` and `create_topic()`, another instance could create the topic (parallel CI, another operator). There is no filtering for `TopicAlreadyExistsError`.
**Attack Scenario**: CI pipeline runs two `streamt apply` in parallel. Both plan "create topic X". Both call `create_topic`. One succeeds, the other gets `TopicAlreadyExistsError` wrapped as `RuntimeError`, and the entire apply fails even though the desired state is achieved.
**Remediation**: Catch `TopicAlreadyExistsError` and treat it as success:
```python
except Exception as e:
    if "TOPIC_ALREADY_EXISTS" in str(e):
        logger.info("Topic '%s' already exists, treating as success", topic)
        return
    raise RuntimeError(...) from e
```

---

## Finding 7

**[HIGH]** Schema Registry apply_schema: Compatibility Set but Registration Fails Leaves Orphaned Compatibility
**Antithesis Term**: Partial Failure Without Rollback
**Confidence**: Medium
**File**: `src/streamt/deployer/schema_registry.py:297-330`
**Code**:
```python
def apply_schema(self, artifact: SchemaArtifact) -> str:
    change = self.plan_schema(artifact)
    if change.action == "register":
        if artifact.compatibility:
            self.set_compatibility(artifact.subject, artifact.compatibility)  # Step 1
        self.register_schema(artifact.subject, artifact.schema, ...)          # Step 2
        return "registered"
    elif change.action == "update":
        if change.changes and "compatibility" in change.changes:
            self.set_compatibility(artifact.subject, artifact.compatibility)  # Step 1
        if change.changes and "schema" in change.changes:
            self.register_schema(...)                                          # Step 2
        return "updated"
```
**Guard Check**: Yes. No rollback of `set_compatibility` if `register_schema` fails.
**Attack Scenario**: User sets `compatibility: NONE` to allow a breaking schema change. `set_compatibility` succeeds, changing the subject from `BACKWARD` to `NONE`. Then `register_schema` fails (network error, malformed schema). The subject now has `NONE` compatibility permanently -- anyone can push any schema to it. This is a **schema governance bypass** that persists silently across retries.
**Remediation**: Record the previous compatibility level and restore on failure:
```python
old_compat = current.compatibility if current.exists else None
if artifact.compatibility:
    self.set_compatibility(artifact.subject, artifact.compatibility)
try:
    self.register_schema(...)
except Exception:
    if old_compat and artifact.compatibility:
        self.set_compatibility(artifact.subject, old_compat)
    raise
```

---

## Finding 8

**[MEDIUM]** Flink Statement Polling Elapsed Time Drift -- Does Not Account for Request Duration
**Antithesis Term**: Unbounded Polling
**Confidence**: High
**File**: `src/streamt/deployer/flink.py:374-413`
**Code**:
```python
poll_interval = 0.5
max_poll_interval = 5.0
elapsed = 0.0

while elapsed < statement_timeout:
    status_response = self._request(...)  # This itself can take up to 30s x 3 retries
    ...
    time.sleep(poll_interval)
    elapsed += poll_interval              # Only counts sleep, not request time
    poll_interval = min(poll_interval * 2, max_poll_interval)
```
**Guard Check**: Yes. Only the `time.sleep()` duration is counted toward `elapsed`. The `_request()` call itself (which can take up to `_timeout` * `_retries` = 90s with defaults) is not counted.
**Attack Scenario**: Each status poll `_request` takes 25s due to slow network. `poll_interval` caps at 5s. Each loop iteration = 25s request + 5s sleep = 30s real time, but `elapsed` only increments by 5s. For a 60s `statement_timeout`, actual wall-clock time is `(60/5) * 30 = 360s` (6 minutes) instead of the expected 60 seconds.
**Remediation**: Track wall-clock time using `time.monotonic()`:
```python
deadline = time.monotonic() + statement_timeout
while time.monotonic() < deadline:
    status_response = self._request(...)
    ...
    time.sleep(poll_interval)
    poll_interval = min(poll_interval * 2, max_poll_interval)
```

---

## Finding 9

**[MEDIUM]** Gateway health_check Catches Only ConnectionError, Not Timeout
**Antithesis Term**: Missing Timeout Handling
**Confidence**: High
**File**: `src/streamt/deployer/gateway.py:197-206`
**Code**:
```python
def health_check(self) -> bool:
    try:
        response = self._session.get(
            f"{self.admin_url}/health",
            timeout=DEFAULT_TIMEOUT,
        )
        return response.status_code == 200
    except requests.ConnectionError:
        return False
```
**Guard Check**: Yes. Only `requests.ConnectionError` is caught. `requests.Timeout` is NOT caught. All other deployers' `check_connection()` methods catch `Exception`.
**Attack Scenario**: Gateway is overloaded and responding slowly. `health_check` makes a request that takes >10s, raises `requests.Timeout`, which propagates up as an unhandled exception, crashing the caller instead of returning `False`.
**Remediation**:
```python
except (requests.ConnectionError, requests.Timeout):
    return False
```

---

## Finding 10

**[MEDIUM]** Kafka update_topic: Irreversible Partition Increase Before Reversible Config Update
**Antithesis Term**: Partial Failure Without Rollback
**Confidence**: High
**File**: `src/streamt/deployer/kafka.py:241-282`
**Code**:
```python
def update_topic(self, artifact: TopicArtifact, changes: dict) -> None:
    if "partitions" in changes:
        ...  # Step 1: partition increase (IRREVERSIBLE)
        future.result(timeout=DEFAULT_TIMEOUT)

    if config_changes:
        ...  # Step 2: config update (reversible)
        future.result(timeout=DEFAULT_TIMEOUT)
```
**Guard Check**: Yes. If step 1 (partition increase) succeeds and step 2 (config update) fails, partitions have been irreversibly increased but config is unchanged.
**Attack Scenario**: Topic update changes partitions from 3 to 6 AND sets `retention.ms`. Partition increase succeeds (irreversible), then config update fails (permission error). The topic now has 6 partitions but wrong retention. On retry, `plan_topic` sees partitions are correct but config still needs changing -- benign for retry, but if config repeatedly fails, the extra partitions persist permanently.
**Remediation**: Apply config changes first (reversible), then partition increase (irreversible):
```python
def update_topic(self, artifact, changes):
    if config_changes:
        ...  # Config first (can be reverted)
    if "partitions" in changes:
        ...  # Partitions last (irreversible)
```

---

## Finding 11

**[MEDIUM]** Connect create_connector: No Handling of HTTP 409 Conflict
**Antithesis Term**: No Idempotency
**Confidence**: Medium
**File**: `src/streamt/deployer/connect.py:163-169`, `254-265`
**Code**:
```python
def create_connector(self, artifact: ConnectorArtifact) -> dict:
    payload = {"name": artifact.name, "config": artifact.to_dict()["config"]}
    return self._request("POST", "/connectors", json=payload)
```
**Guard Check**: Yes. `plan_connector` calls `get_connector_state` to check existence, but there is a TOCTOU race. `POST /connectors` returns HTTP 409 if the connector already exists. This 409 passes through `_request` which calls `response.raise_for_status()`, raising `HTTPError`.
**Attack Scenario**: Two CI runners execute `streamt apply` simultaneously. Both plan "create connector X". The second one gets HTTP 409 Conflict, which is raised as an error even though the desired state is achieved.
**Remediation**: Handle 409 in `create_connector` or fall back to update:
```python
def create_connector(self, artifact):
    try:
        return self._request("POST", "/connectors", json=payload)
    except requests.HTTPError as e:
        if e.response.status_code == 409:
            return self.update_connector(artifact)
        raise
```

---

## Finding 12

**[MEDIUM]** Flink close_session Bypasses Retry Logic and Logs at Debug Level
**Antithesis Term**: Silent Failures / Resource Leak
**Confidence**: Medium
**File**: `src/streamt/deployer/flink.py:162-173`
**Code**:
```python
def close_session(self) -> None:
    if self.session_id:
        try:
            self._http_session.delete(
                f"{self.sql_gateway_url}/v1/sessions/{self.session_id}",
                timeout=HEALTH_CHECK_TIMEOUT,
            )
        except Exception as e:
            logger.debug(f"Failed to close session {self.session_id}: {e}")
        finally:
            self.session_id = None
```
**Guard Check**: Yes. This deliberately bypasses the `_request` retry wrapper, making a raw `delete` call. No retries on transient failures. The `debug` log level means operators will never see session cleanup failures unless debug logging is enabled. The session handle is set to `None` regardless, so it cannot be retried.
**Attack Scenario**: Network blip during session cleanup. The Flink SQL Gateway session is never closed. Over hundreds of deploy runs, leaked sessions accumulate, consuming server-side resources (memory, connection slots). No operator-visible log message indicates the leak.
**Remediation**: Log at `warning` level and use the `_request` method for retry:
```python
def close_session(self) -> None:
    if self.session_id:
        try:
            self._request("DELETE", f"/v1/sessions/{self.session_id}",
                          use_sql_gateway=True, timeout=HEALTH_CHECK_TIMEOUT)
        except Exception as e:
            logger.warning("Failed to close Flink session %s: %s", self.session_id, e)
        finally:
            self.session_id = None
```

---

## Finding 13

**[MEDIUM]** No Retry in KafkaDeployer AdminClient Operations
**Antithesis Term**: Missing Retry / Inconsistent Retry Policy
**Confidence**: High
**File**: `src/streamt/deployer/kafka.py` (entire file)
**Code**:
```python
# Every AdminClient operation is fire-once:
futures = self.admin.create_topics([new_topic])
for topic, future in futures.items():
    future.result(timeout=DEFAULT_TIMEOUT)  # No retry
```
**Guard Check**: Yes. All four HTTP-based deployers (Flink, Connect, SchemaRegistry, Gateway) implement retry with backoff in their `_request()` methods (3 attempts, 0.5s linear backoff, retrying on ConnectionError/Timeout/5xx). KafkaDeployer uses `confluent_kafka.AdminClient` (binary protocol) with zero retry logic on any operation: `create_topics`, `create_partitions`, `incremental_alter_configs`, `delete_topics`, `describe_configs`, `list_consumer_groups`, `list_consumer_group_offsets`.
**Attack Scenario**: A transient broker leadership change during `create_topics` causes a `NOT_LEADER_FOR_PARTITION` error. The HTTP deployers would survive a 1-2s network blip via retry, but the Kafka deployer fails immediately. This inconsistency means Kafka operations are the weakest link in the deploy chain.
**Remediation**: Add a retry wrapper for AdminClient future resolution:
```python
def _await_future(self, future, timeout=DEFAULT_TIMEOUT, retries=3):
    for attempt in range(retries):
        try:
            return future.result(timeout=timeout)
        except KafkaException as e:
            if attempt < retries - 1 and is_transient(e):
                time.sleep(0.5 * (attempt + 1))
                continue
            raise
```

---

## Finding 14

**[MEDIUM]** Deployers Created Outside try/finally in CLI Commands
**Antithesis Term**: Resource Leak on Error Path
**Confidence**: Medium
**File**: `src/streamt/cli/commands/apply.py:111-165`
**Code**:
```python
sr = make_sr_deployer(project, fmt)
kafka = make_kafka_deployer(project, fmt)
flink = make_flink_deployer(project, fmt, state_dir=project_path / ".streamt")   # if this raises...
connect = make_connect_deployer(project, fmt)
gateway = make_gateway_deployer(project, fmt)
try:                                         # ... sr and kafka are never closed
    ...
finally:
    close_deployers(sr, kafka, flink, connect, gateway)
```
**Guard Check**: Yes. The `close_deployers` function exists in `finally`, but only covers the inner `try` block. If `make_flink_deployer` raises an exception not caught by its internal try/except (e.g., `MemoryError`, `KeyboardInterrupt`), the already-created `sr` and `kafka` deployers are NOT closed because the `finally` block has not been entered yet.
**Attack Scenario**: User hits Ctrl+C during deployer creation (between `make_kafka_deployer` and `make_flink_deployer`). `KeyboardInterrupt` is raised, bypassing the inner `finally`, and the Kafka deployer's resources are leaked.
**Remediation**: Create deployers inside the try/finally or use `contextlib.ExitStack`:
```python
from contextlib import ExitStack, nullcontext
with ExitStack() as stack:
    sr = stack.enter_context(make_sr_deployer(...) or nullcontext())
    kafka = stack.enter_context(make_kafka_deployer(...) or nullcontext())
    ...
```

---

## Finding 15

**[MEDIUM]** Gateway delete() Does Not Isolate Per-Interceptor Errors
**Antithesis Term**: Partial Failure Without Rollback
**Confidence**: Medium
**File**: `src/streamt/deployer/gateway.py:485-502`
**Code**:
```python
def delete(self, name: str) -> bool:
    deleted = False
    if self.delete_alias_topic(name):
        deleted = True
    interceptors = self.list_interceptors()
    for interceptor in interceptors:
        int_name = ...
        if int_name.startswith(f"{name}_"):
            self.delete_interceptor(int_name)  # No try/except
            deleted = True
    return deleted
```
**Guard Check**: Yes. If any `delete_interceptor` call fails, subsequent interceptors are not deleted. The exception propagates, leaving orphaned interceptors.
**Attack Scenario**: Deleting a gateway rule with 5 interceptors. Interceptor #3 fails to delete (server error). Interceptors #4 and #5 are never attempted. The rule is partially deleted with orphaned interceptors that may continue applying transformations to data.
**Remediation**: Wrap each deletion in try/except and collect errors:
```python
errors = []
for interceptor in interceptors:
    try:
        self.delete_interceptor(int_name)
        deleted = True
    except Exception as e:
        errors.append(f"{int_name}: {e}")
if errors:
    raise RuntimeError(f"Partial delete of rule '{name}': {'; '.join(errors)}")
```

---

## Finding 16

**[LOW]** Flink _save_hashes Temp File Descriptor Leak on Write Failure
**Antithesis Term**: Resource Leak on Error Path
**Confidence**: Medium
**File**: `src/streamt/deployer/flink.py:449-467`
**Code**:
```python
fd = tempfile.NamedTemporaryFile(mode="w", dir=path.parent, suffix=".tmp", delete=False)
try:
    _json.dump(self._sql_hashes, fd)
    fd.close()
    Path(fd.name).replace(path)
except Exception:
    logger.debug("Failed to save hashes to %s", path)
    try:
        Path(fd.name).unlink(missing_ok=True)
    except Exception:
        pass
```
**Guard Check**: Yes. The cleanup attempt exists but `fd` is not closed before `unlink` in the except block. The file descriptor leaks until GC.
**Remediation**: Close `fd` in the except block before unlinking:
```python
except Exception:
    logger.debug("Failed to save hashes to %s", path)
    try:
        fd.close()
    except Exception:
        pass
    try:
        Path(fd.name).unlink(missing_ok=True)
    except Exception:
        pass
```

---

## Finding 17

**[LOW]** Unbounded list_jobs(include_details=True) in Flink get_job_state()
**Antithesis Term**: Unbounded Polling / Performance Degradation
**Confidence**: High
**File**: `src/streamt/deployer/flink.py:244-278`
**Code**:
```python
def get_job_state(self, job_name: str) -> FlinkJobState:
    jobs = self.list_jobs(include_details=True)  # 1 + N HTTP requests
```
**Guard Check**: Yes. `list_jobs(include_details=True)` makes one call to `/jobs`, then one call per job for `/jobs/{id}`. No pagination, no status filtering, no caching.
**Attack Scenario**: Cluster with 500 historical jobs and 10 Flink job artifacts. Plan/status calls `get_job_state` 10 times, each fetching 501 HTTP requests. That is 5010 HTTP requests (each with 30s timeout, 3 retries) for a single `plan` command. At best this takes minutes; at worst with slow responses, it could take hours.
**Remediation**: Cache the job list across calls, or filter by status (exclude FINISHED/CANCELED):
```python
def list_jobs(self, include_details=False, status_filter=None):
    ...
    if status_filter:
        jobs = [j for j in jobs if j.get("status") in status_filter]
```

---

## Finding 18

**[LOW]** AdminClient Constructor Blocks Without Configurable Timeout
**Antithesis Term**: Missing Timeout
**Confidence**: Medium
**File**: `src/streamt/deployer/kafka.py:90-95`
**Code**:
```python
def __init__(self, bootstrap_servers: str, **kafka_config: dict) -> None:
    config = {"bootstrap.servers": bootstrap_servers}
    config.update(kafka_config)
    self._config = dict(config)
    self.admin = AdminClient(config)  # Blocks up to socket.timeout.ms (default 60s)
```
**Guard Check**: Yes. `confluent_kafka.AdminClient` constructor initiates a connection attempt. The internal timeout is `socket.timeout.ms` (default 60s), not controllable via streamt's `DEFAULT_TIMEOUT`. If the broker is unreachable, the constructor blocks for up to 60 seconds with no output to the user.
**Attack Scenario**: User runs `streamt plan` against a misconfigured Kafka URL. The CLI hangs for 60 seconds with no feedback before reporting an error.
**Remediation**: Set `socket.timeout.ms` in the config to align with `DEFAULT_TIMEOUT`:
```python
config.setdefault("socket.timeout.ms", str(DEFAULT_TIMEOUT * 1000))
```

---

## Summary Table

| # | Finding | Severity | Deployer(s) | Antithesis Term |
|---|---------|----------|-------------|-----------------|
| 1 | No rollback in planner.apply() | CRITICAL | All | Partial Failure Without Rollback |
| 2 | Cancel + failed re-submit = pipeline down | CRITICAL | Flink | Partial Failure Without Rollback |
| 3 | Timeout != failure, ghost statements | HIGH | Flink | Indefinite Treated as Definite |
| 4 | Consumer leak on exception in get_topic_message_count | HIGH | Kafka | Resource Leak on Error Path |
| 5 | No circuit breaker, cascading timeouts | HIGH | All | No Circuit Breaker |
| 6 | Topic create not idempotent (TOCTOU) | HIGH | Kafka | No Idempotency |
| 7 | Compatibility set but registration fails | HIGH | Schema Registry | Partial Failure Without Rollback |
| 8 | Polling elapsed time drift | MEDIUM | Flink | Unbounded Polling |
| 9 | health_check misses Timeout exception | MEDIUM | Gateway | Missing Timeout Handling |
| 10 | Irreversible partition change before config | MEDIUM | Kafka | Partial Failure Without Rollback |
| 11 | No HTTP 409 handling on connector create | MEDIUM | Connect | No Idempotency |
| 12 | Session close bypasses retry, logs at debug | MEDIUM | Flink | Silent Failures |
| 13 | No retry in AdminClient operations | MEDIUM | Kafka | Missing Retry |
| 14 | Deployers created outside try/finally | MEDIUM | CLI | Resource Leak on Error Path |
| 15 | Gateway delete() no per-interceptor isolation | MEDIUM | Gateway | Partial Failure Without Rollback |
| 16 | Temp file fd leak in _save_hashes | LOW | Flink | Resource Leak on Error Path |
| 17 | Unbounded list_jobs N+1 query pattern | LOW | Flink | Unbounded Polling |
| 18 | AdminClient constructor blocks 60s | LOW | Kafka | Missing Timeout |

**Top 3 Recommendations by Blast Radius:**

1. **Circuit breaker / fail-fast in DeploymentPlanner.apply()** (Findings 1, 5) -- prevents cascading timeouts and limits blast radius when a service is down.
2. **Flink cancel+submit atomicity** (Finding 2) -- a failed re-submit after cancel causes data pipeline outage with no automated recovery.
3. **Consumer `finally` blocks + idempotency guards** (Findings 4, 6, 11) -- resource leaks and TOCTOU races are the most likely to hit in normal operation.
