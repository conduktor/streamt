# Change-impact plan

## Status

Product specification for the primary streamt output. Canonical topic-impact
evidence and conservative change-risk classification are implemented;
contract-column, test, and operator-state compatibility remain roadmap work.

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

## Canonical topic identity

Kafka topic names are deployment identities, not graph identities. For each
topic create or update, the planner resolves the physical topic through the
compiled artifact's explicit `ArtifactOwnership` and records both identities:

- `resource`: the physical Kafka topic used for live observation.
- `logical_type`, `logical_name`, and `logical_resource`: the owning project
  `source` or `model` used as the DAG traversal root.

The planner never guesses a logical name from a physical topic string. Missing
or conflicting ownership is emitted as unavailable or failed identity evidence,
with an empty graph result.

## Change actions and risk assessments

Each effective mutation retains its backend action and has one primary risk
assessment:

- `safe`: live absence was explicitly observed before a create/register/submit.
- `risky`: the diff is known but can affect configuration, policy, contracts,
  partitioning, or consumers.
- `destructive`: the resource is removed or the requested transition is
  intrinsically destructive.
- `schema_breaking`: Schema Registry rejected the update under the effective
  compatibility policy.
- `state_migration_required`: an existing Flink job would change without proven
  operator-state and savepoint evidence.
- `unknown`: the available evidence cannot justify a stronger classification.

`unknown` has highest plan-level precedence so incomplete evidence cannot produce
a reassuring summary. No-op resources are omitted from risk counts.

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
- `impact_unverified`
- `schema_impact_unverified`

Compatible Schema Registry output proves registry-policy compatibility only;
schema updates remain `risky` with unverified downstream schema impact. Existing
Flink updates and resubmissions remain state-migration-requiring and blocked.
Offline creates are `unknown` because offline planning does not prove live absence.
An offline plan is preview evidence only and is rejected by `apply` even when
its resource actions happen to match a later live plan.

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

Topic impact currently computes deterministic, transitive downstream model and
exposure sets. Each exposure includes its stable name, all declared owner names,
and its declared consumer group. Live groups are sorted and labelled
`declared: true` only when a downstream exposure declares that exact group;
other observed groups remain in the plan as undeclared consumers.
The entry-level `owners` set is the sorted union of the changed source or model
owner, all downstream model owners, and all impacted exposure owners.

## Evidence semantics

Identity, graph, and live-consumer evidence have explicit status. Live consumer
evidence uses:

- `verified`: group listing and every per-topic group query completed.
- `partial`: listing completed, but one or more group queries failed.
- `unavailable`: Kafka was not configured for the plan or group listing failed.

Failures contain a stable scope and code plus a credential-redacted message.
They are ordered deterministically. A `null` result from the Kafka lag API means
the group has no committed offsets for that topic; it is not emitted as a live
consumer. Backend APIs must raise access/query failures so the planner can
distinguish them from that documented absence result.

Example canonical impact entry:

```json
{
  "resource": "prod.payments.clean.v2",
  "logical_type": "model",
  "logical_name": "payments_clean",
  "logical_resource": "model/payments_clean",
  "change_type": "topic_update",
  "downstream_models": ["fraud_features"],
  "exposures": [
    {
      "name": "fraud_service",
      "owners": ["risk-platform"],
      "consumer_group": "fraud-prod"
    }
  ],
  "owners": ["payments-platform", "risk-platform"],
  "consumers": [
    {
      "group_id": "fraud-prod",
      "lag": 14,
      "declared": true,
      "declared_exposures": ["fraud_service"]
    }
  ],
  "identity_evidence": {
    "status": "verified",
    "source": "manifest_artifact_ownership"
  },
  "graph_evidence": {
    "status": "verified",
    "source": "declared_project_dag"
  },
  "consumer_evidence": {
    "status": "verified",
    "source": "kafka_consumer_groups",
    "reason": null,
    "failures": []
  }
}
```

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
- Desired manifest checksum and exact prior-state backend, store, address,
  serial, and checksum.
- Evidence timestamps and source types.
- Ordered resource changes.
- Ordered per-resource risk assessments and a fixed-shape plan risk summary.
- Impact graph entries.
- Policy decisions.
- Required approvals.
- Overall decision and plan checksum.

Text, PR summaries, SARIF, and catalog events are renderings of this canonical
object.

The impact fields are additive inside reviewed-plan format v2, whose plan
payload is already checksum-protected and extensible, so they do not require a
format-version bump. Structural impact evidence participates in apply-time
drift checks. Consumer lag is intentionally excluded from drift comparison
because it is a volatile metric, while the reviewed checksum still protects the
exact lag value observed at plan creation.

Risk assessments, flags, and evidence participate in the reviewed checksum and
apply-time drift comparison.

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
