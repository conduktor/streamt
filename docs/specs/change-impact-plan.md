# Change-impact plan

## Status

Product specification for the primary streamt output.

## Purpose

The plan explains what changed, who and what is affected, whether the change is
safe, and how it can be deployed. It is useful without granting streamt write
access to infrastructure.

## Inputs

- Strictly parsed desired project.
- Last applied streamt state or a previous manifest.
- Optional live Kafka, Schema Registry, Flink, Connect, Gateway, and catalog
  observations.
- Environment policy.
- Selection expression, if any.

Plans clearly distinguish verified live facts, stored facts, and inferences.
Missing live access degrades evidence but does not invent a clean state.

## Change classes

Each change has one primary class and zero or more risk flags:

- `create`
- `update`
- `remove`
- `adopt`
- `no_change`
- `unknown`

Risk flags include:

- `schema_breaking`
- `contract_breaking`
- `destructive`
- `stateful_upgrade`
- `savepoint_required`
- `consumer_impact`
- `policy_violation`
- `live_state_unverified`
- `ownership_required`

## Impact graph

For every changed resource, the plan includes:

- Direct upstream and downstream models.
- Transitive downstream models when requested or policy-relevant.
- Exposures and declared owners.
- Contract columns used by exposures.
- Live consumer groups associated with affected topics.
- Tests and policies whose result changes.

Unknown consumers remain visible as an uncertainty; absence of declared
exposures is not treated as proof of no consumers.

## Stateful Flink changes

The planner distinguishes stateless resubmission from stateful changes. For a
stateful job it reports operator/state compatibility evidence, configured state
backend, current job health, checkpoint/savepoint availability, and the
required upgrade mode.

When compatibility cannot be established, direct apply is blocked unless a
target-specific policy explicitly allows a state reset.

## Policy decisions

Policies return structured decisions:

```json
{
  "policy": "no_breaking_contracts_in_prod",
  "decision": "deny",
  "reason": "customer_id is consumed by billing_service",
  "evidence": ["model/payments_clean", "exposure/billing_service"]
}
```

A plan has an overall decision of `allow`, `allow_with_warnings`, `deny`, or
`unknown`.

## Stable JSON contract

The JSON plan contains:

- Plan schema version and streamt version.
- Project, environment, and selection.
- Desired manifest checksum and prior-state serial.
- Evidence timestamps and source types.
- Ordered resource changes.
- Impact graph entries.
- Policy decisions.
- Required approvals.
- Overall decision and plan checksum.

Text, PR summaries, SARIF, and catalog events are renderings of this canonical
object.

## Exit behavior

- Exit `0`: allowed, including warnings unless strict mode is selected.
- Exit `1`: validation failure, policy denial, stale evidence under policy, or
  unsafe change.
- Exit `2`: tool/configuration error prevented a plan.

Machine output always contains structured error codes and does not rely on
parsing human text.

## Minimum viable plan

The first release needs:

1. Safe create/update/no-op behavior for explicitly managed resources.
2. No automatic deletion.
3. Schema and contract diffs.
4. Downstream model and exposure impact.
5. Live consumer groups when available.
6. Deterministic JSON and checksum.
7. A GitHub-friendly Markdown renderer.

