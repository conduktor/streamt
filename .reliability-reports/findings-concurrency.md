# Concurrency Audit Findings

**Auditor**: Concurrency Auditor
**Date**: 2026-03-13
**Scope**: File-level races, resource management, atomicity gaps in single-threaded Python CLI
**Revision**: 2 (merged with prior audit, added new findings)

---

## Finding 1

**[HIGH]** File-Based State Race on `flink_hashes.json`
**Antithesis Term**: Lost Update (P4) / TOCTOU Race
**Confidence**: High
**File**: `src/streamt/deployer/flink.py`:437-467
**Code**:
```python
def _load_hashes(self) -> None:
    path = self._hashes_file
    if path is None or not path.exists():  # T1 reads here
        return
    data = _json.loads(path.read_text())    # T1 reads stale data
    ...

def _save_hashes(self) -> None:
    path = self._hashes_file
    ...
    fd = tempfile.NamedTemporaryFile(
        mode="w", dir=path.parent, suffix=".tmp", delete=False,
    )
    try:
        _json.dump(self._sql_hashes, fd)
        fd.close()
        Path(fd.name).replace(path)          # T2 overwrites T1's write
    ...
```
**Guard Check**: Searched for `flock`, `fcntl`, `FileLock`, `lockf` across entire `src/streamt/` -- no file locking mechanism exists. Yes, verified.
**Attack Scenario**: Two `streamt apply` invocations run concurrently against the same project directory (e.g., CI retry, parallel pipeline stages, or user double-clicking). Both load `flink_hashes.json` at startup. Process A deploys job_X and writes its hash. Process B deploys job_Y and writes its hash. Process B's `_save_hashes()` call overwrites Process A's file via `Path.replace()`, silently discarding job_X's hash. On the next `streamt plan`, job_X appears as "no prior SQL hash" and the change detection is broken -- the planner cannot detect SQL changes, potentially skipping a required cancel+resubmit.

Additionally, `_load_hashes` has a TOCTOU between `path.exists()` and `path.read_text()`. If the file is deleted between those calls, an uncaught `FileNotFoundError` is raised.
**Remediation**:
```python
import fcntl

def _load_hashes(self) -> None:
    path = self._hashes_file
    if path is None:
        return
    try:
        data = _json.loads(path.read_text())
        if isinstance(data, dict):
            self._sql_hashes.update(data)
    except FileNotFoundError:
        return
    except Exception:
        logger.warning("Corrupt state file %s -- starting with empty hashes", path)

def _save_hashes(self) -> None:
    path = self._hashes_file
    if path is None:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    lock_path = path.with_suffix(".lock")
    with open(lock_path, "w") as lock_fd:
        fcntl.flock(lock_fd, fcntl.LOCK_EX)
        # Re-read to merge any concurrent writes
        if path.exists():
            try:
                disk_data = _json.loads(path.read_text())
                if isinstance(disk_data, dict):
                    disk_data.update(self._sql_hashes)
                    self._sql_hashes = disk_data
            except Exception:
                pass
        fd = tempfile.NamedTemporaryFile(
            mode="w", dir=path.parent, suffix=".tmp", delete=False,
        )
        try:
            _json.dump(self._sql_hashes, fd)
            fd.flush()
            os.fsync(fd.fileno())
            fd.close()
            Path(fd.name).replace(path)
        except Exception:
            Path(fd.name).unlink(missing_ok=True)
            raise
```

---

## Finding 2

**[MEDIUM]** Missing `fsync` Before Rename -- Non-Durable "Atomic" Write
**Antithesis Term**: Durability Violation
**Confidence**: High
**File**: `src/streamt/deployer/flink.py`:455-461
**Code**:
```python
fd = tempfile.NamedTemporaryFile(
    mode="w", dir=path.parent, suffix=".tmp", delete=False,
)
try:
    _json.dump(self._sql_hashes, fd)
    fd.close()                      # no fd.flush() + os.fsync(fd.fileno())
    Path(fd.name).replace(path)     # rename before data hits disk
```
**Guard Check**: Searched for `os.fsync` and `flush()` in the deployer directory -- none found on this file descriptor. Yes, verified.
**Attack Scenario**: Power loss or kernel crash between `fd.close()` and the data actually reaching the disk platter. On ext4 with default `data=ordered` mount, `close()` does not guarantee the data is flushed to stable storage. The rename succeeds (metadata update), but the file content may be zero-length or corrupt on recovery. Next `_load_hashes` hits the `except Exception` branch and silently resets to empty hashes, losing all change-detection state.
**Remediation**:
```python
_json.dump(self._sql_hashes, fd)
fd.flush()
os.fsync(fd.fileno())
fd.close()
Path(fd.name).replace(path)
```

---

## Finding 3

**[HIGH]** Non-Atomic Cancel+Resubmit Leaves Job in Limbo on Interrupt
**Antithesis Term**: Partial Write / Interrupted Operation (no rollback)
**Confidence**: High
**File**: `src/streamt/deployer/flink.py`:528-535
**Code**:
```python
elif change.action == "update":
    # Cancel the running job, then re-submit with new SQL
    if change.current and change.current.job_id:
        self.cancel_job(change.current.job_id)    # Step 1: cancel succeeds
    self.submit_sql(artifact.sql)                  # Step 2: may fail or be interrupted
    self._sql_hashes[artifact.name] = self._sql_hash(artifact.sql)
    self._save_hashes()
```
**Guard Check**: Searched for `signal`, `atexit`, `KeyboardInterrupt`, `SIGINT`, `SIGTERM` across entire `src/streamt/` -- no signal handlers exist. The `apply.py` command has no `try/except KeyboardInterrupt`. Yes, verified.
**Attack Scenario**: During `streamt apply`, a Flink job update is in progress. `cancel_job()` succeeds and the old job stops processing. Then `submit_sql()` fails (network timeout, SQL Gateway error, Ctrl+C). The pipeline is now down: the old job is cancelled, the new one never started, and the hash file still contains the old hash (or no hash at all since `_save_hashes` was never reached). There is no automatic recovery -- the user must manually re-run `apply`, but the planner may now see the job as non-existent and treat it as a fresh submit rather than an update.
**Remediation**:
```python
elif change.action == "update":
    if change.current and change.current.job_id:
        self.cancel_job(change.current.job_id)
    try:
        self.submit_sql(artifact.sql)
    except Exception:
        logger.error(
            "Job '%s' was cancelled but resubmit failed. "
            "The pipeline is DOWN. Re-run 'streamt apply' to recover.",
            artifact.name,
        )
        self._sql_hashes.pop(artifact.name, None)
        self._save_hashes()
        raise
    self._sql_hashes[artifact.name] = self._sql_hash(artifact.sql)
    self._save_hashes()
```
Additionally, add a `KeyboardInterrupt` handler in `apply.py` that logs which jobs were partially updated.

---

## Finding 4

**[MEDIUM]** Sequential Apply With No Rollback on Partial Failure
**Antithesis Term**: Partial Failure / Inconsistent State
**Confidence**: High
**File**: `src/streamt/deployer/planner.py`:316-440
**Code**:
```python
def apply(self, plan: Optional[DeploymentPlan] = None) -> dict[str, object]:
    # Apply schemas first
    for change in plan.schema_changes:
        ...  # may succeed
    # Apply topics
    for change in plan.topic_changes:
        ...  # may succeed
    # Apply Flink jobs
    for change in plan.flink_changes:
        ...  # if one fails, prior schemas/topics are already deployed
    # Apply connectors
    for change in plan.connector_changes:
        ...
```
**Guard Check**: The `apply()` method catches exceptions per-resource and appends to `results["errors"]`, so it does not abort on first failure. However, there is no rollback of successfully-applied resources when later resources fail. Yes, verified -- no rollback logic exists in the planner.
**Attack Scenario**: `streamt apply` creates 3 schemas and 5 topics successfully, then fails on the 2nd Flink job (e.g., SQL syntax error in a model). The schemas and topics are now deployed but the Flink jobs that consume from them are not. If this is a first deployment, there are dangling resources. If the user fixes and re-runs, the planner sees the schemas/topics as "unchanged" but may see the Flink jobs as needing "submit" -- this mostly self-heals. The real risk is when the **first** Flink job starts processing before the second one fails, and those jobs have inter-dependencies (job A feeds job B). Job A is now running against incomplete infrastructure.
**Remediation**: This is a design-level concern. Options:
1. Add a `--dry-run` mode that validates all resources can be created before applying any (partially implemented via `plan`).
2. Add a two-phase apply: validate all resources first (test connectivity, validate SQL), then apply.
3. Document that `apply` is best-effort and not transactional.

---

## Finding 5

**[MEDIUM]** Consumer Resource Leak on Exception in `get_topic_message_count`
**Antithesis Term**: Resource Leak
**Confidence**: High
**File**: `src/streamt/deployer/kafka.py`:414-442
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
        consumer.close()    # Only reached on success
        return total
    except Exception as e:
        logger.warning(...)
        return 0            # consumer.close() never called
```
**Guard Check**: Compared with `get_consumer_group_lag` at line 373 which correctly uses `try/finally`. Yes, verified -- `get_topic_message_count` lacks `finally`.
**Attack Scenario**: `streamt status --lag` calls `get_topic_message_count` for each topic. If `get_watermark_offsets` times out or throws (common with unreachable brokers), the `Consumer` object is not closed. The confluent-kafka Consumer holds a background thread and network connections. Repeated calls (e.g., `streamt status` in a monitoring loop) leak Consumer objects, exhausting file descriptors and background threads. The `_streamt_internal_count` consumer group may also accumulate stale group memberships in the broker.
**Remediation**:
```python
def get_topic_message_count(self, topic: str) -> int:
    ...
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

---

## Finding 6

**[HIGH]** Resource Leak: FlinkDeployer HTTP Session Never Closed in TestRunner
**Antithesis Term**: Resource Leak
**Confidence**: High
**File**: `src/streamt/testing/runner.py`:199-210
**Code**:
```python
deployer = FlinkDeployer(cluster.rest_url)
job_state = deployer.get_job_state(f"test_{test.name}")

if job_state.exists:
    return {
        "name": test.name,
        ...
    }
```
**Guard Check**: Yes -- no `close()` call, no context manager (`with`), no `finally` block. The `FlinkDeployer.__init__` creates a `requests.Session` at line 136. This session is never closed.
**Attack Scenario**: `streamt test` is invoked repeatedly (e.g., in a CI loop or watch mode). Each invocation of `_run_continuous_test` creates a `FlinkDeployer` with its own `requests.Session` and connection pool. These sessions accumulate file descriptors and TCP connections. On systems with low `ulimit -n` (e.g., 256 in containers), this leads to `OSError: [Errno 24] Too many open files` after ~128 test runs, causing the entire CLI to crash.
**Remediation**:
```python
def _run_continuous_test(self, test: DataTest) -> dict[str, object]:
    ...
    if cluster.rest_url:
        try:
            with FlinkDeployer(cluster.rest_url) as deployer:
                job_state = deployer.get_job_state(f"test_{test.name}")
                if job_state.exists:
                    return {
                        "name": test.name,
                        "status": "passed" if job_state.status == "RUNNING" else "failed",
                        "job_status": job_state.status,
                        "message": f"Continuous test job is {job_state.status}",
                    }
        except Exception as e:
            logger.warning(f"Failed to check Flink job status for test '{test.name}': {e}")
```

---

## Finding 7

**[MEDIUM]** Plan-Then-Apply TOCTOU in `apply_job` (Double State Read)
**Antithesis Term**: TOCTOU (Time-of-Check-to-Time-of-Use)
**Confidence**: Medium
**File**: `src/streamt/deployer/flink.py`:519-541 and `src/streamt/deployer/planner.py`:372-376
**Code**:
```python
# planner.py line 374 - plan_job reads Flink state
change = self.flink_deployer.plan_job(artifact)

# ... other resources are deployed in between ...

# planner.py line 376 - apply_job reads Flink state AGAIN inside plan_job
result = self.flink_deployer.apply_job(change.desired)

# flink.py apply_job:
def apply_job(self, artifact: FlinkJobArtifact) -> str:
    change = self.plan_job(artifact)  # second call to plan_job -> second API call
```
**Guard Check**: Yes, verified. `apply_job` calls `plan_job` internally, and the planner's `apply` method also calls `plan_job` during the planning phase. The artifact is passed through, but the state is re-read.
**Attack Scenario**: Between the `plan()` call (which shows the user what will happen) and the `apply()` call, external state changes. For example, another team member starts the same Flink job manually. The plan shows "submit" but `apply_job` internally re-plans and may find the job already running, returning "unchanged". This is actually a safety feature in most cases, but it means the plan output shown to the user may not match what actually happened. More concerning: if a job transitions from RUNNING to FAILED between plan and apply, the planner planned "none" but apply_job will re-plan as "submit" -- applying a change the user never approved.
**Remediation**: Either:
1. Have `apply_job` accept a `FlinkJobChange` directly instead of re-planning, or
2. Document that `plan` output is advisory and `apply` rechecks state (current behavior is actually safer for most cases, but violates the "plan then apply" contract).

---

## Finding 8

**[MEDIUM]** Non-Atomic Manifest Write
**Antithesis Term**: Non-Atomic File Write
**Confidence**: Medium
**File**: `src/streamt/compiler/manifest.py`:147-151
**Code**:
```python
def save(self, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        f.write(self.to_json())
```
**Guard Check**: Yes -- no temp-file-then-rename pattern. Contrast with `_save_hashes` in `flink.py` which correctly uses atomic write via `NamedTemporaryFile` + `replace()`.
**Attack Scenario**: A `streamt apply` process reads `manifest.json` while a concurrent `streamt compile` is writing it. The reader gets truncated/partial JSON. This causes a `json.JSONDecodeError` in `Manifest.load()`, crashing the apply with an opaque error. Even a single process can hit this: if `compile` is killed mid-write, the manifest is corrupted and stays corrupted until the next successful compile.
**Remediation**: Use the same atomic write pattern as `flink.py:_save_hashes`:
```python
def save(self, path: Path) -> None:
    import tempfile
    path.parent.mkdir(parents=True, exist_ok=True)
    fd = tempfile.NamedTemporaryFile(
        mode="w", dir=path.parent, suffix=".tmp", delete=False,
    )
    try:
        fd.write(self.to_json())
        fd.flush()
        os.fsync(fd.fileno())
        fd.close()
        Path(fd.name).replace(path)
    except Exception:
        Path(fd.name).unlink(missing_ok=True)
        raise
```

---

## Finding 9

**[MEDIUM]** Non-Atomic Multi-File Write in Compiler: Partial Artifact Generation
**Antithesis Term**: Non-Atomic Compound Operation
**Confidence**: Medium
**File**: `src/streamt/compiler/compiler.py`:937-992
**Code**:
```python
def _write_artifacts(self) -> None:
    self.output_dir.mkdir(parents=True, exist_ok=True)

    if self.schemas:
        schemas_dir = self.output_dir / "schemas"
        schemas_dir.mkdir(exist_ok=True)
        for schema in self.schemas:
            path = schemas_dir / f"{schema.subject}.json"
            with open(path, "w") as f:              # Non-atomic write
                json.dump(schema.to_dict(), f, indent=2)

    topics_dir = self.output_dir / "topics"
    topics_dir.mkdir(exist_ok=True)
    for topic in self.topics:
        path = topics_dir / f"{topic.name}.json"
        with open(path, "w") as f:                  # Non-atomic write
            json.dump(topic.to_dict(), f, indent=2)
    # ... more writes for flink, connect, gateway, manifest
```
**Guard Check**: Yes -- no write-to-temp-then-rename pattern, no cleanup of stale artifacts from previous compilations.
**Attack Scenario**: (1) `streamt compile` crashes (e.g., OOM, SIGKILL) mid-write. The `generated/` directory has schemas from the new compilation but topics from the old compilation. A subsequent `streamt apply` using this manifest deploys a mix of old and new artifacts, potentially creating schema/topic mismatches. (2) Two `streamt compile` runs in parallel write to the same `generated/` directory, interleaving files from different compilations. (3) Model `old_model` is removed from the project. `streamt compile` writes new artifacts but does not delete `generated/flink/old_model.sql`. Stale artifacts persist.
**Remediation**: Write to a temporary directory, then atomically swap:
```python
def _write_artifacts(self) -> None:
    import shutil
    tmp_dir = self.output_dir.parent / f".{self.output_dir.name}.tmp"
    if tmp_dir.exists():
        shutil.rmtree(tmp_dir)
    tmp_dir.mkdir(parents=True)
    # ... write all files to tmp_dir ...
    if self.output_dir.exists():
        shutil.rmtree(self.output_dir)
    tmp_dir.rename(self.output_dir)
```

---

## Finding 10

**[MEDIUM]** No Signal Handler -- Ctrl+C During Apply Skips Deployer Cleanup
**Antithesis Term**: Resource Leak on Interrupt
**Confidence**: Medium
**File**: `src/streamt/cli/commands/apply.py`:116-165
**Code**:
```python
try:
    planner = DeploymentPlanner(...)
    deployment_plan = planner.plan()
    results = planner.apply(deployment_plan)
    ...
finally:
    close_deployers(sr, kafka, flink, connect, gateway)
```
**Guard Check**: Searched for `signal`, `atexit`, `KeyboardInterrupt` across `src/streamt/` -- none found. The `finally` block does exist and calls `close_deployers`. Yes, verified.
**Attack Scenario**: When `Ctrl+C` is pressed during `planner.apply()`, Python raises `KeyboardInterrupt`. The `finally` block at line 164 **does** execute for `KeyboardInterrupt` in Python (since it's a `BaseException` subclass), so `close_deployers` is called. However, `close_deployers` calls each deployer's `.close()`, which for `FlinkDeployer` calls `close_session()` then `_http_session.close()`. If the interrupt arrives during `_save_hashes()` (between `_json.dump` and `Path.replace`), the temp file is orphaned on disk. The outer `except Exception` at line 169 does NOT catch `KeyboardInterrupt`, so the error is unhandled and prints a raw traceback instead of a clean message.
**Remediation**:
```python
except KeyboardInterrupt:
    fmt.print_error("Interrupted. Some resources may have been partially deployed.")
    fmt.flush()
    sys.exit(130)
except (EnvVarError, ParseError, EnvironmentError) as e:
    handle_parse_error(fmt, e, ErrorCode.PARSE_ERROR)
```

---

## Finding 11

**[MEDIUM]** Silent Error Swallowing in `_save_hashes` Leaves In-Memory State Diverged from Disk
**Antithesis Term**: Error Suppression / Silent Failure
**Confidence**: High
**File**: `src/streamt/deployer/flink.py`:449-467
**Code**:
```python
def _save_hashes(self) -> None:
    ...
    try:
        _json.dump(self._sql_hashes, fd)
        fd.close()
        Path(fd.name).replace(path)
    except Exception:
        logger.debug("Failed to save hashes to %s", path)   # DEBUG only
        try:
            Path(fd.name).unlink(missing_ok=True)
        except Exception:
            pass
```
**Guard Check**: Yes -- the caller (`apply_job`, `set_sql_hash`) does not check the return value or handle failure from `_save_hashes`.
**Attack Scenario**: Disk is full, or `.streamt/` directory permissions change. `_save_hashes` silently fails with a debug-level log. The in-memory `_sql_hashes` dict has the new hash, so the current session works fine. But the hash is never persisted to disk. On the next `streamt apply`, the hash file lacks this entry, so the planner sees "no prior hash" and cannot detect whether SQL changed. This means either: (a) a no-op deploy that should have been an update is skipped, or (b) every deploy is treated as "cannot detect changes", leading to unnecessary restarts of Flink jobs.
**Remediation**: Elevate to `logger.warning` and re-raise so the caller can report the state persistence failure:
```python
except Exception as e:
    logger.warning("Failed to persist SQL hashes to %s: %s", path, e)
    try:
        Path(fd.name).unlink(missing_ok=True)
    except Exception:
        pass
    raise  # Let caller know state wasn't persisted
```

---

## Finding 12

**[LOW]** Hardcoded Consumer Group ID Creates Cross-Invocation State Pollution
**Antithesis Term**: Shared Mutable State
**Confidence**: Medium
**File**: `src/streamt/deployer/kafka.py`:428
**Code**:
```python
consumer_config["group.id"] = "_streamt_internal_count"
consumer_config["enable.auto.commit"] = False
```
**Guard Check**: `enable.auto.commit` is False, so no offsets are committed. Yes, verified.
**Attack Scenario**: Because `auto.commit` is disabled, no offsets are persisted to the broker under this group ID. The group will appear as an empty/dead consumer group in broker metadata. If two concurrent `streamt status --lag` calls use this same group ID, the Kafka broker's group coordinator may see conflicting heartbeats (though with the short-lived Consumer pattern and no subscription, this is unlikely). The practical impact is low: the group just shows up as a ghost in `kafka-consumer-groups --list` output, causing confusion for operators.
**Remediation**: Use a unique, ephemeral group ID:
```python
import uuid
consumer_config["group.id"] = f"_streamt_internal_{uuid.uuid4().hex[:8]}"
```

---

## Finding 13

**[LOW]** TOCTOU in `init` Command: Project File Existence Check
**Antithesis Term**: TOCTOU
**Confidence**: Low
**File**: `src/streamt/cli/commands/init.py`:123-131
**Code**:
```python
project_file = project_path / "stream_project.yml"
if project_file.exists() and not force and not dry_run:   # CHECK
    ...
    sys.exit(1)
# ... later ...
with open(project_path / "stream_project.yml", "w") as f:  # USE (overwrite)
    yaml.dump(config, f, ...)
```
**Guard Check**: Yes -- no file locking. Same pattern exists in `_init_discover` at line 187-195.
**Attack Scenario**: Two `streamt init` processes race on the same directory. Process A checks `exists()` -> False, Process B checks `exists()` -> False, both proceed to write. One overwrites the other. In a CI scaffold step where multiple jobs initialize the same workspace, this can silently lose one project's configuration.
**Remediation**: Use `open(..., "x")` (exclusive creation) to atomically check+create:
```python
try:
    with open(project_path / "stream_project.yml", "x") as f:
        yaml.dump(config, f, ...)
except FileExistsError:
    if not force:
        fmt.print_error("stream_project.yml already exists...")
        sys.exit(1)
```

---

## Summary

| Severity | Count | Key Theme |
|----------|-------|-----------|
| HIGH     | 3     | File state race (1), non-atomic cancel+resubmit (3), FlinkDeployer leak in TestRunner (6) |
| MEDIUM   | 7     | Missing fsync (2), no rollback (4), consumer leak (5), plan-apply TOCTOU (7), non-atomic manifest (8), non-atomic compile artifacts (9), no signal handler (10), silent error suppression (11) |
| LOW      | 3     | Hardcoded group ID (12), init TOCTOU (13) |

### Priority Remediation Order

1. **Finding 5** (Consumer leak) -- one-line `finally` block, prevents production resource exhaustion
2. **Finding 6** (FlinkDeployer leak in TestRunner) -- wrap in `with`, prevents FD exhaustion in CI
3. **Finding 2** (Missing fsync) -- one-line fix, prevents silent data loss on crash
4. **Finding 1** (File state race) -- add advisory file lock, prevents hash corruption in CI/CD
5. **Finding 3** (Cancel+resubmit) -- add error handling around the gap, prevents pipeline downtime
6. **Finding 8** (Non-atomic manifest write) -- use temp+rename pattern (already exists in codebase)
7. **Finding 11** (Silent error suppression) -- elevate log level and re-raise
8. **Finding 10** (KeyboardInterrupt) -- add handler for clean exit message
9. **Finding 9** (Non-atomic compile) -- write to temp dir then swap
10. **Finding 4** (No rollback) -- design decision, document or implement two-phase apply

### Positive Patterns Already in Use

- `FlinkDeployer._save_hashes` uses `NamedTemporaryFile` + `replace()` for atomic writes
- All deployers implement context manager protocol (`__enter__`/`__exit__`)
- CLI commands use `close_deployers()` in `finally` blocks for proper cleanup
- `KafkaDeployer.get_consumer_group_lag` correctly uses `try/finally` for consumer cleanup
- `GatewayDeployer.apply()` has rollback logic for partially-created interceptors
- No threading, asyncio, or multiprocessing usage found -- all concurrency concerns are multi-process
