# Distributed Coordination Audit Findings

**Auditor**: Distributed Coordination Auditor (Deep Pass)
**Date**: 2026-03-13
**Scope**: `src/streamt/deployer/`, `src/streamt/cli/commands/apply.py`, `src/streamt/cli/commands/plan.py`, `src/streamt/compiler/manifest.py`
**Result**: 10 findings (2 CRITICAL, 4 HIGH, 3 MEDIUM, 1 LOW)

---

## Finding 1

**[CRITICAL]** No Instance Locking -- Concurrent CLI Invocations Corrupt State and Cluster
**Antithesis Term**: Split-Brain
**Confidence**: High
**File**: `src/streamt/cli/commands/apply.py`:59-138
**Code**:
```python
# apply.py -- no lock acquisition anywhere in the entire command flow
parser = ProjectParser(project_path, environment=environment, ...)
project = parser.parse()
# ... validate, compile, plan, apply -- all unguarded
```
**Guard Check**: Searched for `lock`, `Lock`, `flock`, `fcntl`, `pid`, `PID`, `process_id` across all Python files in `src/streamt/`. Zero results for any file-locking or PID-file mechanism. Yes, verified -- no guard exists.
**Attack Scenario**: Two CI pipelines or two developers run `streamt apply -e staging` simultaneously on the same project directory. Both read the same initial cluster state during `plan()`, both compute the same diff, and both attempt to apply. Results:
- Double topic creation attempts (one fails with `TopicAlreadyExistsError`, raising `RuntimeError` -- the deploy halts in a half-applied state)
- Two Flink SQL Gateway sessions submit the same INSERT job concurrently, creating duplicate running jobs consuming from the same topic (Flink does not reject duplicate job names)
- Hash file (`flink_hashes.json`) written by both -- last writer wins, potentially recording hashes for jobs the other instance deployed differently
- Orphan detection in one instance sees resources created by the other mid-flight and marks them for deletion
**Remediation**: Acquire an exclusive file lock on `<project_path>/.streamt/deploy.lock` before entering the plan/apply critical section:
```python
import fcntl
lock_path = project_path / ".streamt" / "deploy.lock"
lock_path.parent.mkdir(parents=True, exist_ok=True)
lock_fd = open(lock_path, "w")
try:
    fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
except BlockingIOError:
    fmt.print_error("Another streamt process is running against this project. Aborting.")
    sys.exit(1)
try:
    # ... plan + apply ...
finally:
    fcntl.flock(lock_fd, fcntl.LOCK_UN)
    lock_fd.close()
```
For cross-machine coordination, document that CI must serialize apply (e.g., GitHub Actions `concurrency` groups, or a deploy queue).

---

## Finding 2

**[CRITICAL]** Flink Job Submit is Not Idempotent -- Duplicate Running Jobs
**Antithesis Term**: Missing Idempotency / Data Duplication
**Confidence**: High
**File**: `src/streamt/deployer/flink.py`:519-541
**Code**:
```python
def apply_job(self, artifact: FlinkJobArtifact) -> str:
    change = self.plan_job(artifact)
    if change.action == "submit":
        self.submit_sql(artifact.sql)        # No dedup check
        self._sql_hashes[artifact.name] = self._sql_hash(artifact.sql)
        self._save_hashes()
        return "submitted"
```
And `submit_sql` at line 341-419 sends raw SQL to SQL Gateway with no `pipeline.name` configuration to set a deterministic Flink job name.
**Guard Check**: Searched for `pipeline.name`, `SET.*pipeline`, `job.*name.*config` across all Python files. No results. `_generate_flink_set_statements` (compiler.py:647) only emits `parallelism.default`, `table.exec.state.ttl`, and `execution.checkpointing.interval`. Yes, verified -- no idempotency guard exists.
**Attack Scenario**:
1. Process A plans job "orders_processor" -- not found, action = "submit"
2. Process B plans job "orders_processor" -- not found (same timing), action = "submit"
3. Process A submits SQL -- Flink creates job `insert-into_default_catalog.default_database.orders_sink`
4. Process B submits SQL -- Flink creates a **second** identical job
Flink does NOT reject duplicate job names. Both streaming jobs run simultaneously, reading from the same source topic and writing to the same sink topic, producing every event twice.
**Remediation**: Inject `SET 'pipeline.name' = '<job_name>';` into the generated SQL so Flink assigns a deterministic name. Then check for existing running jobs with that name before submitting:
```python
def apply_job(self, artifact: FlinkJobArtifact) -> str:
    # Re-check live state right before submit
    current = self.get_job_state(artifact.name)
    if current.exists and current.status in ("RUNNING", "CREATED"):
        return "unchanged"  # Already running, skip
    self.submit_sql(f"SET 'pipeline.name' = '{artifact.name}';\n{artifact.sql}")
    ...
```

---

## Finding 3

**[HIGH]** Flink Job Update Has Non-Atomic Cancel-Then-Submit Gap
**Antithesis Term**: Missing Fencing / Inconsistency Window
**Confidence**: High
**File**: `src/streamt/deployer/flink.py`:528-534
**Code**:
```python
elif change.action == "update":
    if change.current and change.current.job_id:
        self.cancel_job(change.current.job_id)
    self.submit_sql(artifact.sql)       # If this fails, job is cancelled with no replacement
    self._sql_hashes[artifact.name] = self._sql_hash(artifact.sql)
    self._save_hashes()
```
**Guard Check**: No try/except around the cancel+submit pair. No rollback. No savepoint. Yes, verified -- no guard exists.
**Attack Scenario**: During a Flink job update, `cancel_job` succeeds but `submit_sql` fails (SQL Gateway timeout, network partition, invalid SQL after model change). The old job is now cancelled, the new job was never submitted, and the pipeline has a processing gap. Data flowing through Kafka during this window is never processed by Flink. The hash file is never updated, so the next `plan` sees "no prior hash" and may report `action=none` (line 498-503), masking the outage.
**Remediation**: At minimum, wrap in try/except with explicit error escalation:
```python
elif change.action == "update":
    if change.current and change.current.job_id:
        self.cancel_job(change.current.job_id)
    try:
        self.submit_sql(artifact.sql)
    except Exception:
        logger.critical("PIPELINE DOWN: Re-submit failed after cancel for job '%s'", artifact.name)
        raise
    self._sql_hashes[artifact.name] = self._sql_hash(artifact.sql)
    self._save_hashes()
```
Better: use Flink savepoints -- take a savepoint before cancelling, cancel with savepoint, then restore from savepoint on failure.

---

## Finding 4

**[HIGH]** Hash File Race Condition Under Concurrent Writers
**Antithesis Term**: Lost Update / Write-Write Conflict
**Confidence**: High
**File**: `src/streamt/deployer/flink.py`:437-467
**Code**:
```python
def _load_hashes(self) -> None:
    """Load SQL hashes from state file if available."""
    path = self._hashes_file
    if path is None or not path.exists():
        return
    try:
        data = _json.loads(path.read_text())
        if isinstance(data, dict):
            self._sql_hashes.update(data)        # Loaded once at init
    except Exception:
        logger.warning("Corrupt state file %s", path)

def _save_hashes(self) -> None:
    """Persist SQL hashes to state file (atomic write)."""
    # ... writes self._sql_hashes (in-memory dict loaded once at init)
    _json.dump(self._sql_hashes, fd)
    Path(fd.name).replace(path)               # Atomic rename, but full dict from stale memory
```
**Guard Check**: The write uses atomic rename (good), but `_sql_hashes` is loaded once at `__init__` and never re-read before saving. No `flock` around read-modify-write. Yes, verified.
**Attack Scenario**: Instance A loads hashes `{job_x: "abc"}`. Instance B loads the same. A deploys `job_y`, saves `{job_x: "abc", job_y: "def"}`. B deploys `job_z`, saves `{job_x: "abc", job_z: "ghi"}` -- overwriting A's entry for `job_y`. Hash for `job_y` is lost. Next deploy sees no prior hash for `job_y`, skips change detection (line 498-503 falls through to `action="none"`), missing a needed update.
**Remediation**: Re-read the file before saving and merge, combined with `fcntl.flock`:
```python
def _save_hashes(self) -> None:
    path = self._hashes_file
    if path is None:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    lock_fd = open(path.with_suffix(".lock"), "w")
    fcntl.flock(lock_fd, fcntl.LOCK_EX)
    try:
        on_disk = {}
        if path.exists():
            on_disk = _json.loads(path.read_text())
        merged = {**on_disk, **self._sql_hashes}
        # atomic write of merged dict
        fd = tempfile.NamedTemporaryFile(mode="w", dir=path.parent, suffix=".tmp", delete=False)
        _json.dump(merged, fd)
        fd.close()
        Path(fd.name).replace(path)
    finally:
        fcntl.flock(lock_fd, fcntl.LOCK_UN)
        lock_fd.close()
```

---

## Finding 5

**[HIGH]** Stale Plan -- No Plan Fingerprint or Expiry Between `plan` and `apply`
**Antithesis Term**: Stale Read / TOCTOU
**Confidence**: High
**File**: `src/streamt/cli/commands/plan.py`:57-101, `src/streamt/cli/commands/apply.py`:107-138
**Code**:
```python
# plan.py: generates plan, displays it, exits. Plan is never persisted with a fingerprint.
deployment_plan = planner.plan()
fmt.print(deployment_plan.details())

# apply.py: generates a FRESH plan at apply time
deployment_plan = planner.plan()  # This is a NEW plan, not the reviewed one
results = planner.apply(deployment_plan)
```
**Guard Check**: Searched for `plan.*fingerprint`, `plan.*hash`, `plan.*version`, `plan.*token`, `plan.*epoch`. No results. Yes, verified -- no guard exists.
**Attack Scenario**: User runs `streamt plan -e prod`, reviews output showing "2 topics to create, 0 to delete". Meanwhile, a teammate pushes a model deletion and the source YAML changes on disk. User runs `streamt apply -e prod --confirm`. The freshly-computed plan now includes deletions the user never reviewed. The `--confirm` flag was given based on a stale plan. Critical subtlety: each deployer's `apply_*` method re-plans internally (e.g., `kafka.py:297` calls `plan_topic(artifact)` again), so the apply-time cluster state is fresh, but the **manifest itself** may have changed if files were modified on disk between plan and apply.
**Remediation**: Persist the plan to `.streamt/plan.json` with a content hash. At apply time, re-compute the plan and compare:
```python
# In plan command:
plan_hash = hashlib.sha256(json.dumps(plan_data, sort_keys=True).encode()).hexdigest()[:16]
(project_path / ".streamt" / "plan.json").write_text(json.dumps({"hash": plan_hash, "ts": time.time()}))

# In apply command:
saved = json.loads((project_path / ".streamt" / "plan.json").read_text())
if saved["hash"] != current_plan_hash:
    sys.exit("Plan changed since last review. Re-run `streamt plan`.")
```

---

## Finding 6

**[HIGH]** Apply Continues After Partial Failure -- No Rollback
**Antithesis Term**: Partial Failure / Inconsistent State
**Confidence**: High
**File**: `src/streamt/deployer/planner.py`:316-440
**Code**:
```python
def apply(self, plan=None):
    # Apply schemas first
    for change in plan.schema_changes:
        try: ...
        except Exception as e:
            results["errors"].append(...)     # Swallow and continue
    # Apply topics -- continues even if schemas failed
    for change in plan.topic_changes:
        try: ...
        except: results["errors"].append(...)
    # Apply Flink jobs -- continues even if topics failed
    for change in plan.flink_changes:
        ...
    # Apply connectors, gateway rules...
    return results  # Returns with errors; no rollback
```
**Guard Check**: Each resource type catches exceptions and appends to `results["errors"]`, then continues to the next resource. The caller (`apply.py`:138-160) checks for errors only after all resources have been attempted. No rollback mechanism. Yes, verified. Only `gateway.py:apply()` (line 412) has a partial rollback for interceptors.
**Attack Scenario**: Schema registration succeeds, topic creation succeeds, but Flink job submission fails (SQL Gateway down). System is now: topics exist, schemas registered, no Flink jobs processing. Connectors are then created that read from topics with no data flowing. The user sees partial results with errors. On the next `apply`, schemas and topics show "unchanged" and only the Flink job is retried -- but if the SQL changed in the interim, hash comparison may produce wrong results (Finding 4).
**Remediation**: Implement fail-fast by default with `--continue-on-error` opt-in:
```python
def apply(self, plan=None, fail_fast=True):
    for change in plan.schema_changes:
        try: ...
        except Exception as e:
            results["errors"].append(...)
            if fail_fast:
                return results  # Stop here, report what was applied
    # Only proceed to topics if schemas all succeeded
```

---

## Finding 7

**[MEDIUM]** Orphan Detection Races With Concurrent Applies / Multi-Tenant Clusters
**Antithesis Term**: Time-of-Check-to-Time-of-Use
**Confidence**: High
**File**: `src/streamt/deployer/planner.py`:270-314
**Code**:
```python
def _detect_orphans(self, plan, planned_subjects, planned_topics, planned_connectors):
    for subject in self.schema_registry_deployer.list_subjects():
        if subject not in planned_subjects:
            plan.schema_changes.append(SchemaChange(subject=subject, action="delete"))
    for topic in self.kafka_deployer.list_topics():
        if topic not in planned_topics:
            plan.topic_changes.append(TopicChange(topic=topic, action="delete"))
```
**Guard Check**: No namespacing. `list_topics()` filters internal topics (starting with `_`) but returns ALL user topics in the cluster. No ownership metadata. Yes, verified.
**Attack Scenario**: Team A's project manages topics `orders`, `payments`. Team B shares the same Kafka cluster with topic `inventory`. Team B runs `streamt apply` -- orphan detection sees `orders` and `payments` as orphans (not in Team B's manifest) and includes `action="delete"` for both. With `--force`, it deletes Team A's production topics.
**Remediation**: Add namespace/prefix filtering and make orphan deletion opt-in:
```python
def _detect_orphans(self, plan, ...):
    # Only consider resources with the project's prefix
    prefix = f"{self.manifest.project_name}_"
    for topic in self.kafka_deployer.list_topics():
        if topic.startswith(prefix) and topic not in planned_topics:
            plan.topic_changes.append(TopicChange(topic=topic, action="delete"))
```
Alternatively, add a `--prune` flag and disable orphan detection by default.

---

## Finding 8

**[MEDIUM]** Concurrent Topic Creation Not Idempotent -- Crashes Apply
**Antithesis Term**: Missing Idempotency
**Confidence**: High
**File**: `src/streamt/deployer/kafka.py`:223-239
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
**Guard Check**: No special handling for `TopicAlreadyExistsError`. All exceptions become `RuntimeError`. Yes, verified.
**Attack Scenario**: Two concurrent applies both plan to create topic "orders". First succeeds. Second gets `TopicAlreadyExistsError` wrapped as `RuntimeError`, causing the apply to partially fail at the topic phase. Remaining resources (Flink jobs, connectors) are never deployed.
**Remediation**: Catch `TopicAlreadyExistsError` specifically:
```python
from confluent_kafka import KafkaException, KafkaError
for topic, future in futures.items():
    try:
        future.result(timeout=DEFAULT_TIMEOUT)
    except KafkaException as e:
        if e.args[0].code() == KafkaError.TOPIC_ALREADY_EXISTS:
            logger.info("Topic '%s' already exists, skipping creation", topic)
        else:
            raise RuntimeError(f"Failed to create topic '{topic}': {e}") from e
```

---

## Finding 9

**[MEDIUM]** Manifest File Writes Are Not Atomic
**Antithesis Term**: Torn Write
**Confidence**: Medium
**File**: `src/streamt/compiler/manifest.py`:147-151
**Code**:
```python
def save(self, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        f.write(self.to_json())
```
**Guard Check**: Unlike `flink.py:_save_hashes()` which uses `NamedTemporaryFile` + atomic rename, `manifest.save()` writes directly to the target path. The same non-atomic pattern is used for all artifact writes in `compiler.py:937-991` (`with open(path, "w")`). Yes, verified.
**Attack Scenario**: Two concurrent `streamt compile` invocations write to `generated/manifest.json` simultaneously. A reader (CI script, another tool) reads a partially-written manifest. On NFS or network filesystems, this produces torn writes -- truncated or interleaved JSON. On local ext4, Python's `write()` for a single buffer is practically atomic, but `f.write(self.to_json())` may issue multiple kernel writes for large manifests.
**Remediation**: Use the same atomic write pattern as `_save_hashes`:
```python
def save(self, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    import tempfile
    fd = tempfile.NamedTemporaryFile(mode="w", dir=path.parent, suffix=".tmp", delete=False)
    try:
        fd.write(self.to_json())
        fd.close()
        Path(fd.name).replace(path)
    except Exception:
        Path(fd.name).unlink(missing_ok=True)
        raise
```

---

## Finding 10

**[LOW]** Schema Registry Apply Has Check-Then-Act Race for Compatibility
**Antithesis Term**: TOCTOU
**Confidence**: Low
**File**: `src/streamt/deployer/schema_registry.py`:230-295, 297-330
**Code**:
```python
def plan_schema(self, artifact):
    # ... checks compatibility
    is_compatible = self.check_compatibility(artifact.subject, artifact.schema, ...)

def apply_schema(self, artifact):
    change = self.plan_schema(artifact)   # Re-plans, checking compatibility
    # ... then registers, but between check and register another schema version may appear
    self.register_schema(artifact.subject, artifact.schema, artifact.schema_type)
```
**Guard Check**: Schema Registry itself enforces compatibility at registration time, so the worst case is a misleading error message (plan said "compatible" but registration fails because the baseline changed). Yes, verified -- Schema Registry is the true guard.
**Attack Scenario**: Two concurrent applies register different schema versions. Both check compatibility against version N. Both find compatible. First registers version N+1. Second tries to register -- Schema Registry re-checks compatibility against N+1 (not N) and may reject. The error message is confusing but data integrity is preserved by Schema Registry.
**Remediation**: Low priority. Consider wrapping registration errors with a re-check and better error message explaining that the baseline changed concurrently.

---

## Summary Table

| # | Finding | Severity | Category |
|---|---------|----------|----------|
| 1 | No instance locking (file lock/PID) | CRITICAL | Split-Brain |
| 2 | Flink job submit not idempotent (duplicate jobs) | CRITICAL | Data Duplication |
| 3 | Cancel-then-submit gap (job update) | HIGH | Inconsistency Window |
| 4 | Hash file lost update (concurrent writers) | HIGH | Lost Update |
| 5 | Stale plan TOCTOU (no fingerprint) | HIGH | Stale Read |
| 6 | Apply continues after partial failure (no rollback) | HIGH | Partial Failure |
| 7 | Orphan detection deletes cross-project resources | MEDIUM | TOCTOU |
| 8 | Topic creation not idempotent (crashes apply) | MEDIUM | Idempotency |
| 9 | Manifest file writes not atomic | MEDIUM | Torn Write |
| 10 | Schema Registry check-then-act race | LOW | TOCTOU |

**Critical**: 2 | **High**: 4 | **Medium**: 3 | **Low**: 1

## Coordination Primitives Inventory

| Primitive | Present? | Location |
|-----------|----------|----------|
| File lock (flock/fcntl) | No | -- |
| PID file | No | -- |
| Distributed lock | No | -- |
| Fencing token / epoch | No | -- |
| Plan fingerprint / serial | No | -- |
| Atomic file write | Partial | `flink.py:_save_hashes` uses temp+rename; `manifest.py:save` and `compiler.py:_write_artifacts` do not |
| Idempotent apply | Partial | Schema/connector creates are idempotent via API; topic creates crash on duplicate; Flink jobs produce duplicates |
| Rollback on failure | Minimal | Gateway `apply()` has partial rollback for interceptors; nothing else has rollback |
| Cross-resource ordering | Partial | Planner enforces schemas-before-topics-before-flink-before-connectors-before-gateway order (correct), but failure in early phase does not prevent later phases from executing |
