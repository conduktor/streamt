---
title: CI/CD Integration
description: Integrate streamt into GitHub Actions, GitLab CI, or any CI pipeline
---

# CI/CD Integration

streamt is designed for CI/CD pipelines. Every command supports `--strict` validation, `-o json` structured output, and `--confirm-env` for non-interactive deploys.

## First-party GitHub Action

The repository includes a composite Action that validates the project, creates
a deterministic reviewed plan, writes a concise job summary, and exposes the
plan path and checksum as outputs. It never applies the plan.

```yaml
# streamt:skip — GitHub Actions workflow, not streamt config
jobs:
  streamt-plan:
    runs-on: ubuntu-latest
    permissions:
      contents: read
    steps:
      - uses: actions/checkout@v4
      - id: streamt
        uses: conduktor/streamt@<release-tag-or-commit-sha>
        with:
          project-directory: .
          environment: staging
          offline: true
          plan-path: .streamt/staging.plan.json
```

Offline planning is the safe default and does not contact Kafka, Schema
Registry, Flink, Connect, or Gateway. Set `offline: false` only when the runner
has network connectivity to every configured backend and the required
credentials are provided through GitHub secrets or environment variables.
Pin the Action to a release tag or immutable commit in production workflows.

## Manual GitHub Actions Example

```yaml
# streamt:skip — GitHub Actions workflow, not streamt config
name: Streaming Pipeline

on:
  pull_request:
  push:
    branches: [main]

jobs:
  validate:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.12"
      # Replace <commit-sha> with the same reviewed streamt revision everywhere.
      - run: python -m pip install "git+https://github.com/conduktor/streamt.git@<commit-sha>"

      - name: Validate
        run: streamt validate --strict

      - name: Compile
        run: streamt compile --output-dir ./generated

      - name: Lineage (comment on PR)
        if: github.event_name == 'pull_request'
        run: streamt lineage --format mermaid

  deploy-staging:
    needs: validate
    if: github.ref == 'refs/heads/main'
    runs-on: ubuntu-latest
    environment: staging
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.12"
      - run: python -m pip install "git+https://github.com/conduktor/streamt.git@<commit-sha>"

      - name: Plan
        run: streamt -o json plan --env staging --out staging.plan.json

      - name: Apply reviewed plan
        run: streamt apply --env staging --plan staging.plan.json
        env:
          KAFKA_BOOTSTRAP_SERVERS: ${{ secrets.STAGING_KAFKA }}
          SCHEMA_REGISTRY_URL: ${{ secrets.STAGING_SR_URL }}

  deploy-prod:
    needs: deploy-staging
    runs-on: ubuntu-latest
    environment: production
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.12"
      - run: python -m pip install "git+https://github.com/conduktor/streamt.git@<commit-sha>"

      - name: Plan
        run: streamt -o json plan --env prod --out prod.plan.json

      - name: Apply reviewed plan
        run: streamt apply --env prod --plan prod.plan.json --confirm-env prod
        env:
          KAFKA_BOOTSTRAP_SERVERS: ${{ secrets.PROD_KAFKA }}
          SCHEMA_REGISTRY_URL: ${{ secrets.PROD_SR_URL }}
```

## Key CLI Flags for CI

| Flag | Purpose |
|------|---------|
| `--strict` | Fail on warnings (not just errors) — use in PR validation |
| `-o json` | Machine-readable output for parsing in scripts |
| `--confirm-env ENV` | Non-interactive apply with environment name verification |
| `--confirm` | Skip interactive confirmation (simpler, no name check) |
| `--all-envs` | Validate all environments at once |
| `--dry-run` | Show what would change without writing/deploying |

Protected environments always require a saved reviewed plan. Set
`safety.require_reviewed_plan: true` for any other shared environment that must
use the same protocol. Neither confirmation nor `--force` bypasses this gate.
Keep the saved plan as the reviewed artifact between jobs; if approval happens
in a separate job, download the exact artifact rather than regenerating it.

## PR Validation Pattern

Run `validate --strict` on every PR to catch issues early. Add `compile` to verify artifact generation and `lineage` for reviewability:

```bash
streamt validate --strict
streamt compile --dry-run
streamt lineage
```

## Secrets

Store Kafka credentials as CI secrets and reference them via environment variables:

```yaml
# stream_project.yml or environments/prod.yml
runtime:
  kafka:
    bootstrap_servers: ${KAFKA_BOOTSTRAP_SERVERS}
    security_protocol: SASL_SSL
    sasl_mechanism: PLAIN
    sasl_username: ${KAFKA_API_KEY}
    sasl_password: ${KAFKA_API_SECRET}
```

streamt resolves `${VAR}` from environment variables, `.env` files, and `.env.{env}` files (in that priority order). Never commit `.env` files — add them to `.gitignore`.

## JSON Output for Scripting

Parse structured output in CI scripts:

```bash
# Check for errors programmatically
result=$(streamt -o json validate)
status=$(echo "$result" | jq -r '.status')
if [ "$status" != "ok" ]; then
  echo "$result" | jq '.errors[]'
  exit 1
fi
```
