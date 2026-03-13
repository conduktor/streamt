# Consistency Model Audit -- Findings

**Auditor**: Consistency Model Auditor
**Date**: 2026-03-13
**Scope**: `deployer/planner.py`, `deployer/flink.py`, `deployer/kafka.py`, `deployer/schema_registry.py`, `deployer/connect.py`, `deployer/gateway.py`, `cli/commands/apply.py`, `cli/commands/plan.py`

---

## FINDING-C1: Partial Apply Without Rollback Across Deployer Phases

**[CRITICAL]** Non-atomic multi-resource write / Partial failure without rollback
**Antithesis Term**: Partial Failure, Dual Write
**Confidence**: High
**File**: `src/streamt/deployer/planner.py:316-440`
**Code**:
```python
# planner.py:330-389 -- five sequential phases, no cross-phase rollback
# Phase 1: schemas
for change in plan.schema_changes:
    ...
    except Exception as e:
        results["errors"].append(...)   # error recorded, execution continues

# Phase 2: topics (proceeds regardless of schema errors)
if self.kafka_deployer:
    for change in plan.topic_changes:
        ...

# Phase 3: flink jobs (proceeds regardless of topic errors)
if self.flink_deployer:
    for change in plan.flink_changes:
        ...
```
**Guard Check**: Yes. Only `GatewayDeployer.apply()` (lines 412-433) has rollback logic, and it only covers interceptors within a single rule. The planner-level orchestration at lines 316-440 has zero rollback. No other guard exists in `apply.py`, `planner.py`, or any deployer.
**Attack Scenario**: A production deploy defines 3 schemas, 5 topics, and 4 Flink jobs. Schema registration succeeds for all 3. Topic creation succeeds for topics 1-3 but fails on topic 4 (e.g., Kafka broker resource exhaustion). The planner records the error but continues to the Flink phase. Flink jobs referencing topic 4 are submitted, immediately fail at runtime because the topic does not exist, and start consuming retry budget. Meanwhile, topics 1-3 and all 3 schemas are live with no corresponding running Flink jobs to process them. Data accumulates on topics 1-3 with no consumer, causing consumer lag alerts and potentially triggering retention-based data loss.
**Remediation**:
```python
# Option A: Fail-fast mode -- abort after first error in any phase
def apply(self, plan, fail_fast=True):
    ...
    for change in plan.schema_changes:
        try:
            ...
        except Exception as e:
            results["errors"].append(...)
            if fail_fast:
                return results  # stop before mutating more state

# Option B: Dependency-aware phase gating
# After topic phase, check if any topic errors block pending Flink jobs
failed_topics = {t.split(":")[1].split(":")[0] for t in results["errors"] if t.startswith("topic:")}
for change in plan.flink_changes:
    referenced_topics = extract_topics_from_sql(change.desired.sql)
    if referenced_topics & failed_topics:
        results["errors"].append(f"flink_job:{change.job_name}: blocked by failed topic(s)")
        continue
```

---

## FINDING-C2: Stale Plan -- plan() and apply() Are Temporally Decoupled

**[HIGH]** Stale reads
**Antithesis Term**: Stale Read, Lost Update P4
**Confidence**: High
**File**: `src/streamt/cli/commands/apply.py:122-138`
**Code**:
```python
# apply.py:122
deployment_plan = planner.plan()       # reads external state at time T1

# apply.py:124-136 -- safety checks, potentially interactive confirmation prompt
if parser.env_config and not parser.env_config.safety.allow_destructive:
    if deployment_plan.deletes > 0:
        ...  # user interaction, --force check

# apply.py:138
results = planner.apply(deployment_plan)  # applies plan from T1 at time T2
```
**Guard Check**: Yes. Individual deployers re-plan internally (see C3), which partially mitigates this. However, the planner-level decision about *which resources to act on* and *in what order* is based on stale data. Delete operations from orphan detection (lines 270-314) are particularly dangerous -- a resource created between T1 and T2 would be scheduled for deletion based on the stale plan.
**Attack Scenario**: CI pipeline A runs `streamt plan` and computes a plan at 10:00:00. An operator manually creates topic "audit_events" at 10:00:05 via Kafka CLI. Pipeline A's plan includes "delete topic:audit_events" from orphan detection. Pipeline A runs `streamt apply` at 10:00:10, deleting the manually-created topic. The destructive safety guard only blocks if `allow_destructive=false`, which defaults to true for non-protected environments.
**Remediation**:
```python
# Add a plan fingerprint that captures a hash of external state
class DeploymentPlan:
    state_fingerprint: str  # hash of list_subjects + list_topics + list_jobs

# In apply(), re-read the fingerprint and compare
def apply(self, plan):
    current_fingerprint = self._compute_fingerprint()
    if plan.state_fingerprint != current_fingerprint:
        raise StateChangedError(
            "External state changed since plan was computed. Re-run 'streamt plan'."
        )
```

---

## FINDING-C3: Double-Plan TOCTOU Race in Every Deployer

**[HIGH]** Lost Update P4 / TOCTOU
**Antithesis Term**: Lost Update P4, Time-of-check-to-time-of-use
**Confidence**: High
**File**: `src/streamt/deployer/flink.py:519-521`, `src/streamt/deployer/kafka.py:295-297`, `src/streamt/deployer/schema_registry.py:297-299`, `src/streamt/deployer/connect.py:254-256`
**Code**:
```python
# planner.py:354-355 -- planner passes artifact, NOT the pre-computed change
result = self.kafka_deployer.apply_topic(change.desired)

# kafka.py:295-297 -- apply_topic re-reads state from Kafka
def apply_topic(self, artifact: TopicArtifact) -> str:
    change = self.plan_topic(artifact)  # SECOND state read
```
**Guard Check**: Yes. This pattern is identical in all four deployers: `apply_job` (flink.py:521), `apply_topic` (kafka.py:297), `apply_schema` (schema_registry.py:299), `apply_connector` (connect.py:256). No deployer accepts a pre-computed Change object. The planner dispatches based on the plan's action but the deployer independently re-decides. If the plan says "create" but the deployer's re-plan sees the resource already exists, the deployer returns "unchanged" -- a semantic mismatch from the original plan.
**Attack Scenario**: Two CI pipelines deploy simultaneously. Pipeline A plans "create topic:orders" and "submit flink_job:orders_processor". Pipeline B also plans the same. Pipeline A executes first -- `apply_topic` creates the topic. Pipeline B's `apply_topic` re-plans, sees the topic already exists, returns "unchanged". But pipeline B's planner records "unchanged" for what should have been "created". More critically, both pipelines proceed to submit the same Flink job, potentially creating duplicate running instances. The Flink deployer's `get_job_state` prioritizes RUNNING jobs (flink.py:331), so the second deploy might see the first's job as "exists, RUNNING" and skip submission -- or it might see the job still in CREATED state and submit a duplicate.
**Remediation**:
```python
# Refactor apply_* to accept an optional pre-computed Change
def apply_topic(self, artifact: TopicArtifact, change: Optional[TopicChange] = None) -> str:
    if change is None:
        change = self.plan_topic(artifact)
    ...

# In planner.apply(), pass the change:
result = self.kafka_deployer.apply_topic(change.desired, change=change)
```

---

## FINDING-C4: Flink Job Update is Non-Atomic (Cancel + Resubmit Gap)

**[CRITICAL]** Non-atomic multi-resource write
**Antithesis Term**: Non-atomic write, Partial Failure
**Confidence**: High
**File**: `src/streamt/deployer/flink.py:528-535`
**Code**:
```python
# flink.py:528-535
elif change.action == "update":
    # Cancel the running job, then re-submit with new SQL
    if change.current and change.current.job_id:
        self.cancel_job(change.current.job_id)   # POINT OF NO RETURN
    self.submit_sql(artifact.sql)                 # can fail here
    self._sql_hashes[artifact.name] = self._sql_hash(artifact.sql)
    self._save_hashes()
    return "submitted"
```
**Guard Check**: Yes. No savepoint is taken before cancellation. No fallback to re-submit the old SQL. The `cancel_job` call (flink.py:421-423) is a fire-and-forget PATCH request -- it does not wait for the job to fully stop. The `submit_sql` method (lines 341-419) splits SQL into multiple statements and executes them sequentially. If statement 3 of 4 fails, CREATE TABLE statements from statements 1-2 have already executed on the Flink cluster. The session is closed on error (line 416), but the DDL side effects remain.
**Attack Scenario**: Production Flink job "orders_processor" is RUNNING, processing 50K events/sec. A deploy updates the SQL (e.g., adds a new field). `cancel_job` succeeds, stopping the pipeline. `submit_sql` creates two new tables via DDL (statements 1-2), but the INSERT INTO statement (statement 3) fails due to a SQL syntax error in the new SQL. The old job is cancelled, the new job never starts. Data accumulates on input topics. The hash is NOT written (exception propagates before line 533). The next deploy will re-plan as "submit" (job doesn't exist), but the broken SQL will fail again. Manual intervention required to fix the SQL and re-deploy, with data loss proportional to the outage window. Additionally, `cancel_job` uses `PATCH /jobs/{id}` which is asynchronous in Flink -- the job may still be processing while the new SQL is submitted, creating a window where duplicate processing occurs.
**Remediation**:
```python
def apply_job(self, artifact: FlinkJobArtifact) -> str:
    change = self.plan_job(artifact)
    if change.action == "update":
        old_sql = None
        # Attempt to preserve old SQL for rollback
        if change.current and change.current.job_id:
            old_sql = self._sql_hashes.get(artifact.name)
            self.cancel_job(change.current.job_id)
            # Wait for cancellation to complete
            self._wait_for_job_terminal(change.current.job_id, timeout=30)
        try:
            self.submit_sql(artifact.sql)
        except Exception:
            if old_sql:
                logger.error("New SQL failed, attempting rollback to previous version")
                # old_sql is just a hash; would need to store full SQL for rollback
            raise
        ...
```

---

## FINDING-C5: Hash File Diverges from Actual Flink Cluster State

**[HIGH]** State file divergence
**Antithesis Term**: Stale Read, State Divergence
**Confidence**: High
**File**: `src/streamt/deployer/flink.py:449-467` (`_save_hashes`) and `src/streamt/deployer/flink.py:474-517` (`plan_job`)
**Code**:
```python
# flink.py:449-467 -- _save_hashes silently swallows write failures
def _save_hashes(self) -> None:
    path = self._hashes_file
    if path is None:
        return
    ...
    except Exception:
        logger.debug("Failed to save hashes to %s", path)  # debug level, not warning
        try:
            Path(fd.name).unlink(missing_ok=True)
        except Exception:
            pass

# flink.py:488-503 -- plan_job skips change detection when no hash exists
if artifact.name in self._sql_hashes:
    desired_hash = self._sql_hash(artifact.sql)
    if desired_hash != self._sql_hashes[artifact.name]:
        return FlinkJobChange(job_name=artifact.name, action="update", ...)
else:
    logger.debug("No prior SQL hash for job '%s'; cannot detect SQL changes.", ...)
    # Falls through to return "none" -- silently skips SQL change detection
```
**Guard Check**: Yes. Three failure modes, none guarded:
1. `_save_hashes` fails silently at `debug` level (line 463). Next session loses all hash state.
2. `state_dir` is `None` (line 433-434): hashes are never persisted, SQL change detection is permanently disabled with only a `debug`-level log (line 499-503).
3. Corrupt state file is handled by resetting to empty (line 447), causing all jobs to appear "unchanged" -- SQL changes made since last successful hash write become invisible.
**Attack Scenario**: Deploy A submits job "payments_processor" with SQL v1. Hash is saved. A disk-full condition occurs. Deploy B modifies the SQL to v2. `_load_hashes` reads the old hash. `_sql_hash` of v2 differs from v1's hash. Deploy B correctly plans "update". But `_save_hashes` fails silently due to disk full. Deploy B's in-memory hash is correct, so it works. Deploy C starts fresh (new FlinkDeployer instance), loads old v1 hash from disk (or no hash if disk was wiped). Deploy C compares v2's SQL against v1's hash and plans "update" again, cancelling the already-updated job and resubmitting v2 -- a needless disruption.
**Remediation**:
```python
def _save_hashes(self) -> None:
    ...
    except Exception as e:
        # Promote to warning so operators can see hash persistence failures
        logger.warning("Failed to save SQL hashes to %s: %s", path, e)
        raise  # Or return a bool so callers can handle

def apply_job(self, artifact: FlinkJobArtifact) -> str:
    ...
    self._sql_hashes[artifact.name] = self._sql_hash(artifact.sql)
    try:
        self._save_hashes()
    except Exception as e:
        logger.warning("Job submitted but hash not persisted: %s", e)
        # Don't fail the deploy, but warn loudly
```

---

## FINDING-C6: Schema Compatibility Change Bypasses Plan-Phase Safety Check

**[HIGH]** Dual write / Consistency bypass
**Antithesis Term**: Non-atomic write, Safety bypass
**Confidence**: High
**File**: `src/streamt/deployer/schema_registry.py:230-295` (plan) and `src/streamt/deployer/schema_registry.py:297-328` (apply)
**Code**:
```python
# plan_schema (line 251) checks compatibility under CURRENT rules
is_compatible = self.check_compatibility(
    artifact.subject, artifact.schema, artifact.schema_type,
)

# apply_schema (lines 317-326) changes compatibility BEFORE registering
if change.changes and "compatibility" in change.changes:
    self.set_compatibility(artifact.subject, artifact.compatibility)  # relax first
if change.changes and "schema" in change.changes:
    self.register_schema(...)  # register under relaxed rules
```
**Guard Check**: Yes. The plan-phase compatibility check at line 251 uses the current compatibility level. But `apply_schema` at line 318-319 changes the level before registering. If a user changes both schema and compatibility (e.g., BACKWARD -> NONE + breaking schema change), the plan may reject the change as incompatible, but `apply_schema` relaxes the rules first and then the registration succeeds. The plan's check becomes a false negative. No guard prevents this ordering inversion.
**Attack Scenario**: Schema "orders-value" has `BACKWARD` compatibility. A developer changes the compatibility to `NONE` and simultaneously removes a required field. `plan_schema()` checks the breaking change against `BACKWARD` rules and marks it as `schema_incompatible`. But wait -- it also records a `compatibility` change. In `apply_schema()`, the compatibility check is re-done (via `plan_schema` re-call due to C3), but this time the code at line 318 changes compatibility to `NONE` first, then registers the breaking schema. Downstream consumers expecting the old field crash. The plan output had warned about incompatibility, but the apply succeeded anyway because the rules were relaxed first.
**Remediation**:
```python
def apply_schema(self, artifact: SchemaArtifact) -> str:
    change = self.plan_schema(artifact)
    if change.action == "update":
        # Register schema first under CURRENT compatibility rules
        if change.changes and "schema" in change.changes:
            self.register_schema(artifact.subject, artifact.schema, artifact.schema_type)
        # Then relax compatibility if needed (safe order)
        if change.changes and "compatibility" in change.changes:
            self.set_compatibility(artifact.subject, artifact.compatibility)
        return "updated"
```

---

## FINDING-C7: Cross-Phase Dependency Violations -- Errors Do Not Block Dependent Phases

**[HIGH]** Cross-deployer consistency
**Antithesis Term**: Non-atomic multi-resource write, Order dependency violation
**Confidence**: High
**File**: `src/streamt/deployer/planner.py:350-389`
**Code**:
```python
# planner.py:350-369 -- topic errors recorded but don't block flink phase
for change in plan.topic_changes:
    ...
    except Exception as e:
        results["errors"].append(f"topic:{change.topic}: {_sanitize_error(e)}")

# Execution falls through to flink phase regardless
# planner.py:372-389
if self.flink_deployer:
    for change in plan.flink_changes:
        ...  # submitted even if dependent topics failed
```
**Guard Check**: Yes. No dependency graph exists at the planner level. The `apply()` method at lines 316-440 hardcodes the order (schemas -> topics -> flink -> connectors -> gateway) but never checks whether prior-phase errors should block later phases. The compiler has a DAG (`src/streamt/core/dag.py`) but it is not used during deployment.
**Attack Scenario**: Topic "user_events" fails to create (Kafka broker at capacity). The planner records the error. Flink job "user_events_processor" is submitted with SQL containing `CREATE TABLE user_events WITH ('connector' = 'kafka', 'topic' = 'user_events')`. Flink creates the table definition but the INSERT INTO fails at runtime because the Kafka topic doesn't exist. The Flink job enters RUNNING state briefly then transitions to FAILED. The deploy reports both the topic error and a successful Flink job submission. The operator sees "created: flink_job:user_events_processor" alongside "error: topic:user_events" -- a contradictory result.
**Remediation**:
```python
def apply(self, plan, fail_fast=False):
    ...
    # After topic phase, check for failures that should block Flink
    if results["errors"] and self.flink_deployer:
        failed_resources = {e.split(":")[0] + ":" + e.split(":")[1] for e in results["errors"]}
        # Block dependent Flink jobs
        for change in plan.flink_changes:
            if change.action in ("submit", "update") and change.desired:
                # Check if this job references any failed topics
                for failed in failed_resources:
                    if failed.startswith("topic:"):
                        topic_name = failed.split(":", 1)[1].split(":")[0]
                        if topic_name in change.desired.sql:
                            results["errors"].append(
                                f"flink_job:{change.job_name}: blocked by failed {failed}"
                            )
```

---

## FINDING-C8: Flink apply_job Re-Plans and May Cancel Wrong Job During Concurrent Deploys

**[HIGH]** Lost Update P4
**Antithesis Term**: Lost Update P4, ABA problem
**Confidence**: Medium
**File**: `src/streamt/deployer/flink.py:519-535`
**Code**:
```python
# flink.py:519-521 -- apply_job re-plans, getting a fresh job_id
def apply_job(self, artifact: FlinkJobArtifact) -> str:
    change = self.plan_job(artifact)  # re-reads cluster state, gets new job_id

    elif change.action == "update":
        if change.current and change.current.job_id:
            self.cancel_job(change.current.job_id)  # cancels whatever job_id re-plan found
```
**Guard Check**: Yes. The planner's plan already contains a `change.current.job_id` from the original plan phase. But `apply_job` discards it and re-reads. There is no job_id comparison between what was planned and what is being cancelled. No optimistic concurrency control (e.g., expected job_id assertion).
**Attack Scenario**: Deploy A plans job "orders_processor", sees job_id=J1 (RUNNING, old SQL). Deploy B concurrently runs, also sees J1, cancels it, submits new SQL -> J2 created. Deploy A's `apply_job` re-plans, calls `get_job_state`, finds J2 (RUNNING, Deploy B's version). The hash check compares Deploy A's desired SQL against the stored hash. If Deploy A's SQL differs from Deploy B's, Deploy A cancels J2 (Deploy B's freshly submitted job) and submits its own version J3. Deploy B's deployment is silently undone.
**Remediation**:
```python
def apply_job(self, artifact: FlinkJobArtifact, expected_job_id: Optional[str] = None) -> str:
    change = self.plan_job(artifact)
    if change.action == "update" and expected_job_id:
        if change.current and change.current.job_id != expected_job_id:
            raise ConflictError(
                f"Job '{artifact.name}' changed since plan: "
                f"expected {expected_job_id}, found {change.current.job_id}"
            )
    ...
```

---

## FINDING-C9: Gateway delete() Has Non-Atomic Multi-Resource Deletion

**[MEDIUM]** Non-atomic multi-resource write
**Antithesis Term**: Partial Failure, Dual Write
**Confidence**: High
**File**: `src/streamt/deployer/gateway.py:485-502`
**Code**:
```python
def delete(self, name: str) -> bool:
    deleted = False
    # Step 1: Delete alias topic
    if self.delete_alias_topic(name):
        deleted = True
    # Step 2: Delete related interceptors (by prefix)
    interceptors = self.list_interceptors()
    for interceptor in interceptors:
        int_name = interceptor.get("metadata", {}).get("name") or interceptor.get("name", "")
        if int_name.startswith(f"{name}_"):
            self.delete_interceptor(int_name)  # can fail per-interceptor
            deleted = True
    return deleted
```
**Guard Check**: Yes. Unlike `GatewayDeployer.apply()` (which has rollback at lines 412-433), the `delete()` method has no rollback. If `delete_alias_topic` succeeds but `delete_interceptor` fails for one interceptor, the alias is gone but orphaned interceptors remain. No error is raised -- the method returns `True` because it did delete *something*. The planner at line 428-431 records "deleted" in results. The orphaned interceptors are invisible to future plans because orphan detection only checks alias topics, not interceptors directly.
**Attack Scenario**: A gateway rule "filter_pii" has an alias topic and 3 interceptors. Deletion removes the alias successfully. The first interceptor is deleted. Gateway API times out on the second interceptor. The `delete_interceptor` call raises `requests.Timeout`, which propagates up through `_request` (line 174). The exception is caught by the planner at line 430-431 and recorded as an error. But the alias is already gone, and interceptor 1 is deleted. Interceptors 2 and 3 remain orphaned with no alias topic pointing to them. They continue processing traffic on the virtual cluster, potentially applying stale data transformations.
**Remediation**:
```python
def delete(self, name: str) -> bool:
    # Delete interceptors FIRST (they depend on the alias)
    interceptors = self.list_interceptors()
    failed = []
    for interceptor in interceptors:
        int_name = interceptor.get("metadata", {}).get("name") or interceptor.get("name", "")
        if int_name.startswith(f"{name}_"):
            try:
                self.delete_interceptor(int_name)
            except Exception as e:
                failed.append(f"{int_name}: {e}")
    if failed:
        raise GatewayError(f"Cannot delete rule '{name}': interceptor cleanup failed: {failed}")
    # Only delete alias after all interceptors are gone
    self.delete_alias_topic(name)
    return True
```

---

## FINDING-C10: Schema Registration + Compatibility Is Two Separate API Calls (Non-Atomic)

**[MEDIUM]** Non-atomic multi-resource write
**Antithesis Term**: Dual Write, Partial Failure
**Confidence**: High
**File**: `src/streamt/deployer/schema_registry.py:301-310`
**Code**:
```python
# schema_registry.py:301-310 -- register path
if change.action == "register":
    if artifact.compatibility:
        self.set_compatibility(artifact.subject, artifact.compatibility)  # API call 1
    self.register_schema(artifact.subject, artifact.schema, ...)          # API call 2
    return "registered"
```
**Guard Check**: Yes. No transaction or rollback between the two API calls. Schema Registry does not support multi-operation transactions. If `set_compatibility` succeeds but `register_schema` fails (e.g., schema validation error, network timeout), the subject exists in Schema Registry with a compatibility level but zero schema versions. This is a valid but unintended state. The next `plan_schema` call will see `exists=False` (because `get_schema_state` fetches `/subjects/{subject}/versions/latest`, which returns 404 for a subject with no versions), and will plan a "register" action again -- but the compatibility level is already set, so it will be set a second time (idempotent, but wasteful).
**Attack Scenario**: First deploy of schema "orders-value" with compatibility "FULL_TRANSITIVE". `set_compatibility` succeeds. `register_schema` fails because the schema JSON is malformed (e.g., invalid Avro). The subject now has a compatibility level in Schema Registry but no schema versions. An operator manually registers a schema for this subject via the Schema Registry UI. The next `streamt apply` sees the subject exists (the manual schema), plans "update" because the schema content differs, and proceeds. But the compatibility level was already set from the failed first deploy. If the operator set a different compatibility level via UI, the next deploy may either overwrite it or leave it alone depending on whether the artifact specifies compatibility.
**Remediation**:
```python
if change.action == "register":
    # Register schema first (validates content)
    self.register_schema(artifact.subject, artifact.schema, artifact.schema_type)
    # Then set compatibility (only after schema is valid and registered)
    if artifact.compatibility:
        try:
            self.set_compatibility(artifact.subject, artifact.compatibility)
        except Exception as e:
            logger.warning("Schema registered but compatibility not set: %s", e)
    return "registered"
```

---

## FINDING-C11: Orphan Detection Races with Concurrent Resource Creation

**[MEDIUM]** Lost Update P4
**Antithesis Term**: Lost Update P4, Phantom Read
**Confidence**: Medium
**File**: `src/streamt/deployer/planner.py:270-314`
**Code**:
```python
# planner.py:284-302 -- orphan detection lists all resources, deletes unknown ones
if self.schema_registry_deployer:
    for subject in self.schema_registry_deployer.list_subjects():
        if subject not in planned_subjects:
            plan.schema_changes.append(
                SchemaChange(subject=subject, action="delete")
            )
```
**Guard Check**: Yes. The `planned_subjects` set (line 182) is populated during artifact planning (lines 190-202). If a manifest has a malformed artifact, it is excluded from `planned_subjects` (caught by `except KeyError` at line 201). This means a malformed entry for "orders-value" causes it to be excluded from `planned_subjects`, and if the schema already exists in the registry, orphan detection will schedule it for deletion. The code comment at line 179-181 acknowledges this and calls it intentional. However, the broader race condition remains: resources created by other teams or systems between `list_subjects()` and `apply()` will be scheduled for deletion.
**Attack Scenario**: Team A owns schemas "orders-value" and "payments-value". Team B independently registers "audit-value" directly in Schema Registry. Team A runs `streamt apply`. Orphan detection lists all subjects, finds "audit-value" not in Team A's manifest, and schedules deletion. If `allow_destructive=true` (or `--force`), "audit-value" is deleted, breaking Team B's pipeline.
**Remediation**:
```python
# Add namespace/prefix filtering to orphan detection
def _detect_orphans(self, plan, planned_subjects, planned_topics, planned_connectors):
    project_prefix = self.manifest.project_name  # e.g., "myapp"
    if self.schema_registry_deployer:
        for subject in self.schema_registry_deployer.list_subjects():
            if subject not in planned_subjects and subject.startswith(f"{project_prefix}"):
                plan.schema_changes.append(...)
```

---

## FINDING-C12: Gateway apply() Rollback Does Not Cover Updated Alias Topics

**[MEDIUM]** Partial Failure without rollback
**Antithesis Term**: Partial Failure
**Confidence**: High
**File**: `src/streamt/deployer/gateway.py:371-433`
**Code**:
```python
# gateway.py:371-376 -- alias is created/updated before interceptors
alias_existed = self.get_alias_topic(artifact.virtual_topic) is not None
self.create_alias_topic(
    name=artifact.virtual_topic,
    physical_topic=artifact.physical_topic,
)

# gateway.py:412-433 -- rollback only reverts alias if it was NEWLY created
except Exception:
    ...
    if not alias_existed:          # only rolls back NEW aliases
        self.delete_alias_topic(artifact.virtual_topic)
    raise
```
**Guard Check**: Yes. The rollback logic at line 422 checks `if not alias_existed` before rolling back the alias. If the alias already existed (an update scenario), and interceptor creation fails, the alias has been updated to point to a new physical topic but the interceptors for the new configuration were not created. The old interceptors may have been partially deleted (step 4 at line 435-439 runs after interceptor creation, not before, so in the error path it is not reached). But the alias now points to a different physical topic than the old interceptors expect.
**Attack Scenario**: Gateway rule "filter_pii" exists with alias "orders_virtual" -> "orders_v1" and interceptor "filter_pii_filter_0". A deploy updates the physical topic to "orders_v2" and changes the interceptor config. `create_alias_topic` updates the alias to point to "orders_v2". Interceptor creation fails (Gateway API error). Rollback does NOT revert the alias (because `alias_existed=True`). The alias now points to "orders_v2" but the old interceptor "filter_pii_filter_0" was configured for "orders_v1". Consumers reading through the gateway get unfiltered data from "orders_v2" because the interceptor's SQL references "orders_v1".
**Remediation**:
```python
except Exception:
    ...
    if not alias_existed:
        self.delete_alias_topic(artifact.virtual_topic)
    else:
        # Restore old alias mapping
        old_physical = ...  # need to capture before update
        try:
            self.create_alias_topic(
                name=artifact.virtual_topic,
                physical_topic=old_physical,
            )
        except Exception as revert_err:
            rollback_failures.append(f"alias revert: {revert_err}")
    raise
```

---

## FINDING-C13: Flink cancel_job Is Fire-and-Forget (No Wait for Terminal State)

**[MEDIUM]** Non-atomic write
**Antithesis Term**: Non-atomic write, Race condition
**Confidence**: High
**File**: `src/streamt/deployer/flink.py:421-423`
**Code**:
```python
def cancel_job(self, job_id: str) -> None:
    """Cancel a running job."""
    self._request("PATCH", f"/jobs/{job_id}", json={"state": "cancelled"})
```
**Guard Check**: Yes. The Flink REST API `PATCH /jobs/{id}` is asynchronous -- it triggers cancellation but does not wait for the job to reach CANCELLED state. The deployer immediately proceeds to `submit_sql` (flink.py:532). No polling loop waits for the old job to terminate. This creates a window where both the old and new job may be running simultaneously, causing duplicate processing.
**Attack Scenario**: Job "orders_processor" reads from topic "orders" and writes to topic "orders_sink". `cancel_job` sends the PATCH request. Flink begins graceful shutdown (checkpointing, draining). Before the old job finishes its last checkpoint, `submit_sql` creates a new session and submits the updated SQL. The new INSERT INTO starts reading from "orders" at the latest offset. Meanwhile, the old job's final checkpoint commits offsets and writes output. Both jobs process the same events, writing duplicates to "orders_sink".
**Remediation**:
```python
def cancel_job(self, job_id: str, wait: bool = True, timeout: int = 60) -> None:
    self._request("PATCH", f"/jobs/{job_id}", json={"state": "cancelled"})
    if wait:
        start = time.time()
        while time.time() - start < timeout:
            try:
                details = self._request("GET", f"/jobs/{job_id}")
                state = details.get("state") if isinstance(details, dict) else None
                if state in ("CANCELED", "CANCELLED", "FAILED", "FINISHED"):
                    return
            except Exception:
                return  # Job may have been cleaned up
            time.sleep(1)
        logger.warning("Job %s did not reach terminal state within %ds", job_id, timeout)
```

---

## Summary

| ID  | Severity | Antithesis Term | Description |
|-----|----------|-----------------|-------------|
| C1  | CRITICAL | Partial Failure, Dual Write | No rollback across deployer phases; errors continue to dependent phases |
| C2  | HIGH     | Stale Read, Lost Update P4 | Plan-apply time gap allows external state changes; orphan deletion on stale data |
| C3  | HIGH     | Lost Update P4, TOCTOU | Every deployer re-plans during apply, doubling reads and creating race windows |
| C4  | CRITICAL | Non-atomic write | Flink job update: cancel succeeds, resubmit fails, pipeline dead |
| C5  | HIGH     | State Divergence | Hash file write failures silently disable SQL change detection |
| C6  | HIGH     | Dual Write, Safety bypass | Compatibility relaxed before schema registration, bypassing plan-phase safety |
| C7  | HIGH     | Order dependency violation | Topic errors don't block dependent Flink job submissions |
| C8  | HIGH     | Lost Update P4 | Concurrent deploys cancel each other's Flink jobs via re-planning |
| C9  | MEDIUM   | Partial Failure, Dual Write | Gateway delete removes alias before interceptors; partial deletion leaves orphans |
| C10 | MEDIUM   | Dual Write, Partial Failure | Schema compatibility + registration is two non-atomic API calls |
| C11 | MEDIUM   | Lost Update P4, Phantom Read | Orphan detection deletes resources created by other teams/systems |
| C12 | MEDIUM   | Partial Failure | Gateway rollback skips alias revert on update path |
| C13 | MEDIUM   | Non-atomic write, Race condition | cancel_job is async; old and new jobs can run simultaneously |

### Systemic Root Causes

**1. Two-Layer Architecture Without Coordination** (C1, C3, C7, C8)
The planner reads state and builds a plan, then each deployer independently re-reads state and makes its own decision. Neither layer has a complete view: the planner coordinates ordering but doesn't enforce cross-phase dependencies; deployers ensure local correctness but have no visibility into other deployers' success or failure.

**2. No Transactional Semantics Across External Systems** (C1, C4, C6, C9, C10)
The deployer writes to 5 independent systems (Schema Registry, Kafka, Flink, Connect, Gateway) without distributed transaction support. Each system has its own failure modes and timing. Partial failure leaves the overall system in a state that no single deployer can detect or correct.

**3. Optimistic Concurrency Without Verification** (C2, C3, C8, C11)
No version numbers, ETags, or expected-state assertions are used when mutating resources. Operations assume the state seen during plan is still valid during apply, with no conflict detection.

**4. Fire-and-Forget Mutations** (C4, C5, C13)
Critical state changes (Flink job cancellation, hash file writes) are not verified to have taken effect before proceeding to the next step. Failures are logged at debug level rather than surfaced as errors.
