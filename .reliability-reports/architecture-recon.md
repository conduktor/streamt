# Architecture Recon — streamt

## Stack
- **Language**: Python 3.10+, single-threaded CLI
- **Framework**: Click (CLI), Rich (output), Pydantic (models)
- **Build**: setuptools, pyproject.toml
- **Test**: pytest, 771+ tests

## Architecture
```
CLI (Click)
  ├── core/        parser, validator, DAG, models, errors, environment
  ├── compiler/    manifest generation, masking, SQL transforms
  ├── deployer/    kafka, flink, schema_registry, connect, gateway
  ├── testing/     test runner
  └── docs/        doc generator
```

- **Monolith CLI** — single process, sequential execution
- **No threading** — all operations synchronous, single-threaded
- **Deployers** — HTTP REST clients (requests.Session) to Kafka, Flink, Schema Registry, Connect, Gateway
- **State** — local `.streamt/` dir for SQL hashes (JSON), no DB
- **No distributed coordination** — single CLI instance at a time

## Concurrency Model
- Single-threaded Python, GIL protects in-process state
- No threads, no asyncio, no multiprocessing
- HTTP sessions with connection pooling (pool_connections=5, pool_maxsize=10)

## Critical Files (by risk)
1. `src/streamt/deployer/flink.py` — SQL Gateway session management, statement polling, hash persistence
2. `src/streamt/deployer/planner.py` — orchestrates all deployers, error sanitization
3. `src/streamt/deployer/kafka.py` — topic CRUD via confluent_kafka AdminClient
4. `src/streamt/deployer/schema_registry.py` — schema registration, compatibility checks
5. `src/streamt/deployer/connect.py` — connector CRUD
6. `src/streamt/deployer/gateway.py` — Conduktor Gateway interceptor management
7. `src/streamt/deployer/ssl_utils.py` — SSL/mTLS configuration, connection pooling
8. `src/streamt/compiler/compiler.py` — manifest compilation, masking
9. `src/streamt/compiler/masking.py` — AST-based SQL masking
10. `src/streamt/core/parser.py` — YAML parsing, project model construction
11. `src/streamt/core/validator.py` — validation rules
12. `src/streamt/core/environment.py` — environment variable resolution
13. `src/streamt/cli/commands/apply.py` — apply command (destructive)
14. `src/streamt/cli/commands/plan.py` — plan command
15. `src/streamt/cli/commands/status.py` — status command

## Risk Areas
- **No rollback** — apply is fire-and-forget, partial failures leave inconsistent state
- **File-based state** — hash persistence via JSON files, no locking
- **Credential handling** — passwords in YAML config, env vars, error messages
- **HTTP retry** — retry on timeout/5xx but no circuit breaker
- **Session lifecycle** — Flink SQL Gateway sessions may leak on errors
