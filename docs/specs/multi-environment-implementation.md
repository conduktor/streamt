# Multi-Environment Implementation Guide

> **Purpose**: Implementation prompt for multi-environment support. Reference this after context compaction.

## Context

- TDD test suite: `tests/unit/test_environment.py` (57 tests)
- Spec: `docs/specs/multi-environment.md`
- **All tests currently fail** - implementation doesn't exist yet

## Goal

Implement multi-environment support to make all tests pass, following the spec exactly.

---

## Phase 1: Core Loading (Critical Path)

### 1. Create `src/streamt/core/environment.py`

```python
class EnvironmentManager:
    def detect_mode(project_path) -> Literal["single", "multi"]
    def discover_environments(project_path) -> list[str]
    def load_environment(project_path, env_name) -> EnvironmentConfig
    def get_effective_env(cli_flag, env_var) -> str | None
```

### 2. Modify `src/streamt/cli.py`

- Add `--env` option to: `validate`, `compile`, `plan`, `apply`, `lineage`
- Add `--all-envs` option to `validate`
- Add `--confirm` and `--force` options to `apply`
- Read `STREAMT_ENV` environment variable
- CLI flag overrides env var

### 3. Mode Detection Logic

```
IF environments/ dir exists:
    mode = multi-env
    runtime: in stream_project.yml is IGNORED (warn if present)
ELSE:
    mode = single-env
    runtime: in stream_project.yml is REQUIRED
```

### 4. .env File Loading (precedence: later wins)

1. `.env` (base, always loaded)
2. `.env.{environment}` (if exists)
3. Actual environment variables (highest priority)

### 5. Error Messages Must

- List available environments when wrong env specified
- Say "No environments configured" when using `--env` in single-env mode
- Say "Specify --env. Available: dev, staging, prod" in multi-env without flag

---

## Phase 2: Safety Features

### Protected Environments (`environment.protected: true`)

- Interactive mode: prompt "Type 'prod' to confirm:"
- Non-interactive (CI): require `--confirm` flag or error
- Show warning banner for protected env operations

### Destructive Safety (`safety.allow_destructive: false`)

- Block operations that delete resources
- Error: "Destructive operations blocked for 'prod' environment"
- `--force` flag overrides (with warning)

---

## Phase 3: Envs Commands

### Add `streamt envs` command group

```bash
streamt envs list          # Show all environments
streamt envs show <name>   # Show resolved config
```

### `envs list` output format

```
dev        Local development
staging    Staging environment
prod       Production environment [protected]
```

### `envs show` must mask secrets

- Fields containing `password`, `secret`, `key`, `token` → show `****`
- Never expose actual secret values

### Single-env mode message

```
No environments configured (single-env mode)
```

---

## Phase 4: Polish

1. `--all-envs` flag: Validate all environments at once, fail if any invalid
2. Colored warnings for protected environments (optional)

---

## Files to Modify

| File | Changes |
|------|---------|
| `src/streamt/cli.py` | Add `--env`, `--confirm`, `--force`, `--all-envs` flags; add `envs` command group |
| `src/streamt/core/environment.py` | NEW: EnvironmentManager class |
| `src/streamt/core/config.py` | Update to use EnvironmentManager |
| `src/streamt/core/project.py` | Load environment config based on mode |

---

## Documentation Updates

### README.md

- Add "Multi-Environment Support" section under Usage
- Show single-env vs multi-env examples
- Document `--env` flag, `STREAMT_ENV` variable
- Document `streamt envs list/show` commands

### CLI Help Text

- `--env`: "Target environment (reads from STREAMT_ENV if not set)"
- `--confirm`: "Skip confirmation prompt for protected environments"
- `--force`: "Override safety checks (allow destructive operations)"
- `--all-envs`: "Validate all environments"

### Spec Update

- Check off implementation phases in `docs/specs/multi-environment.md` as completed

---

## Validation

Run tests after each phase:

```bash
pytest tests/unit/test_environment.py -v
```

**Success = all 57 tests pass.**

---

## Test Coverage Summary

| Test Class | Count | Focus |
|------------|-------|-------|
| `TestSingleEnvMode` | 3 | No `--env` needed, error if used |
| `TestMultiEnvMode` | 8 | `--env` required, list available |
| `TestStreamtEnvVariable` | 4 | STREAMT_ENV, CLI overrides |
| `TestPerEnvDotenvLoading` | 5 | .env precedence |
| `TestProtectedEnvironments` | 6 | Confirmation, --confirm |
| `TestDestructiveSafety` | 4 | --force, allow_destructive |
| `TestEnvsCommands` | 6 | envs list/show, secret masking |
| `TestRealisticConfigurations` | 3 | Confluent Cloud, multi-cluster |

---

## Edge Cases Checklist

- [ ] Environment name in YAML must match filename (`dev.yml` → `name: dev`)
- [ ] Non-.yml files in `environments/` should be ignored
- [ ] Empty `environments/` directory → helpful error
- [ ] Invalid YAML → clear parse error
- [ ] Path traversal attempts (`--env ../../../etc/passwd`) → blocked safely
