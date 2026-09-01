# Product direction

## Status

Accepted product direction for the stabilization program beginning September
2026.

## Problem

Streaming changes cross several independent systems: topic configuration,
schemas, processing jobs, connectors, consumer applications, policy, and
runtime state. Existing tools are authoritative for their own layer, but none
can answer the complete review question:

> If this SQL, schema, or topic changes, what breaks, who is affected, and how
> can it be deployed without losing state or touching unrelated resources?

Teams therefore review streaming changes using a mixture of Terraform plans,
Flink consoles, Schema Registry, catalog metadata, consumer-group inspection,
and institutional knowledge.

## Product thesis

streamt should be the Git-authored contract and change-impact layer across
those systems. Its primary value is delivered during authoring and pull-request
review. Deployment is an optional, pluggable consequence of a reviewed plan.

The canonical workflow is:

1. Import or describe existing resources without claiming ownership.
2. Define transformations, contracts, tests, policy, owners, and exposures.
3. Validate the project offline and, when available, enrich it with live
   schemas and runtime evidence.
4. Produce a deterministic impact plan.
5. Review and approve that plan in CI.
6. Apply the exact plan through a suitable backend.
7. Export lineage and contract metadata to the surrounding ecosystem.

## Primary users

- Data engineers adopting a dbt-like workflow for Kafka and Flink.
- Platform teams governing existing shared Kafka estates.
- Application teams publishing or consuming data contracts.
- Reviewers who need to understand downstream and stateful impact without
  being experts in every runtime API.

## Product principles

### Existing estates first

A user must be able to describe two resources in a cluster containing hundreds
without implicitly claiming the rest. Import, observation, adoption, and
management are distinct actions.

### Safe by construction

Unknown configuration, unknown ownership, stale plans, unreviewed destructive
changes, and unsafe stateful upgrades fail closed.

### One semantic model, multiple backends

Sources, models, tests, exposures, contracts, and policies are portable.
Backend-specific deployment fields are isolated and validated by the selected
backend.

### Truthful surface area

Features are documented as supported only when they work end to end against a
real target and have a smoke or integration test. Parse-only compatibility is
labelled separately from deployment support.

### Interoperate rather than duplicate

streamt emits standards and backend artifacts. It does not replace mature
state engines, Kubernetes reconcilers, catalogs, or telemetry stores.

## Primary product capability

The central output is a change-impact plan containing:

- Desired, previous, and live state for each owned resource.
- Schema compatibility and contract changes.
- Topic, connector, and processing-job changes.
- Stateful upgrade requirements.
- Downstream models, exposures, owners, and live consumers.
- Policy decisions and explicit blockers.
- A stable plan identifier and content checksum.

Text output is optimized for humans. JSON output is a stable public interface
for CI, agents, and integrations.

## Non-goals for the stabilization program

- Building a universal replacement for Terraform or Kubernetes operators.
- Supporting every streaming runtime or transport.
- Providing a hosted catalog or metrics database.
- Generating infrastructure from unconstrained natural-language intent.
- Claiming exactly-once or zero-downtime upgrades without target-specific
  lifecycle support.

## Measures of success

- Time from installation to a useful offline plan is under ten minutes.
- A partial project never changes an unowned resource.
- Unknown YAML fields are always rejected with a useful location.
- Every applied change corresponds to a reviewed plan checksum.
- Breaking changes name all known affected consumers and owners.
- A new target is added through a backend interface without changing the core
  semantic model.

