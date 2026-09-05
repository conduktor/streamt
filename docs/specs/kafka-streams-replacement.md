# Kafka Streams predicate replacement

## Status and purpose

This contract implements the update part of the approved
[topology/runtime cycle](../plans/2026-09-05-topology-runtime-execution.md).
Creation and no-op repeat apply are supported. Replacement remains blocked
while this protocol is implemented and tested; this document is not a claim
that an update or resume command is available.

The current journal foundation requires exit zero for clean-close evidence.
Real runner 0.1.1 TERM observations returned 143. The cleanup observations did
not verify the full closed-status and inactive-group conditions. This unresolved
mismatch blocks activation: neither the adapter nor the journal may
rewrite the observed exit code to zero. The close contract must explicitly
account for JVM signal termination and be verified with the full runtime proof.

Changing a filter must preserve the topology's resource identities and source
progress. It must also expose the affected downstream applications even when
the output topic and its schema do not change. External declarations remain
outside the mutation set.

## First supported transition

Only the predicates of a running, owned fixed-runner job may change. The first
activation accepts one replacement action and no other provider mutations in
the same apply. Other declared resources may be unchanged. This keeps recovery
of the first update independent of topic creation and connector removal.

The prior and desired artifacts must agree on project, environment, model,
application ID, input/output names, schema, projection, image, Docker network,
initial-offset policy and ownership. Topic UUIDs, Kafka cluster, Docker daemon,
resolved image/network IDs and the existing state-volume instance must match
reviewed evidence. A raw mounted old plan must hash to the running plan label;
the reconstructed old artifact must hash to the protected ownership record.

The retained volume has a generated instance identifier, not just a reusable
name. Replacement verifies the existing volume and actual mounts. It never
creates a missing volume, initializes offsets, builds an image or pulls one.
The fixed runner's container-side Kafka identity gate also applies to the new
process before it can consume or emit records.

Projection, schema, topic, partition, image, application identity, network and
stateful changes remain blocked. A Git revert is another proposed change, not
a rewind of source offsets or records already emitted.

## Reviewed and durable evidence

The reviewed plan binds both artifact preimages, the old exact container ID,
the volume instance and immutable provider identities. Its observed offsets
are retained as lower bounds: ordinary processing may advance while someone
reviews a plan. Fresh observations must not replace those reviewed lower bounds
or make every offset advance look like a changed deployment identity.

Generic plan redaction must continue protecting credentials. Typed lifecycle
evidence must nevertheless retain its full integrity, including the non-secret
volume UUID; a redacted placeholder cannot identify a volume instance.

The existing operation journal owns the lifecycle. Its version-4 envelope holds
the typed action and checkpoints; unrelated operations keep their existing
format. The candidate's labels bind the operation UUID, action index and
immutable evidence fingerprint. These labels are written atomically by Docker
create, so an interrupted response does not erase generation identity.

All runtime calls require the deployment-state lock. Losing the lock stops
further mutations. It does not authorize force-killing a process or clearing
an uncertain pending operation.

## Execution and interruption boundaries

Each decision uses a fresh observation. It permits at most one next action;
the caller durably records a checkpoint before crossing the next boundary.

| Durable boundary | Required observation | Next authorized step |
| --- | --- | --- |
| Intent only | Exact prior runner, valid progress, matching protected state | Record action started |
| Started | Same prior generation and unchanged identities | Request TERM, then wait within the configured close bound |
| Started, close observed | Fresh old-plan closed status, exit zero, no forced/OOM failure, inactive group, resumable monotonic offsets | Record `old_closed` with its final progress |
| `old_closed` | Same stopped old container, volume and inactive progress | Remove that exact stopped container without force or volume removal |
| Old absent after `old_closed` | Exact absence and retained volume/progress | Record `old_removed` |
| `old_removed` | Old and candidate absent, retained volume, inactive resumable progress | Create one stopped candidate with this operation's generation labels |
| Candidate present | Exact desired mounted plan, generation, image, network and volume; never started | Record `replacement_created` with its container ID |
| `replacement_created` | Exact stopped candidate and progress at least as recent as clean close | Start that candidate without any offset initialization |
| Candidate ready | Fresh desired-plan running status, one group member, unchanged identities and monotonic progress | Record action completed, commit desired ownership state, finalize operation |

A lost remove response can be resolved by exact absence after the durable clean
close. A lost create response can be resolved by the uniquely named candidate's
exact generation and mounted inputs. Neither permits creating another application
ID or accepting an unrelated container that happens to use the same name.

Before the candidate first starts, committed positions must equal the durable
clean-close vector. Producers may advance high watermarks; retention may advance
only while the resume positions remain available. A committed offset jump while
the runner is stopped cannot come from that process and is rejected.
Once the candidate has started, its committed positions may advance normally.

Unknown exit, nonzero exit, OOM, stale status, changed volume, replaced topic,
missing offsets or progress outside retention blocks the transition. A timeout
does not permit force kill. A failed candidate readiness attempt may request
TERM for that exact generation, but the request alone is not clean-close proof.

## Resume is not read-only recovery

Read-only recovery reports evidence. It never stops, removes, creates or starts
a container. In particular, old removed plus new absent is not a recovered
topology and cannot clear the pending operation.

An explicit resume continues the same operation and desired artifact under the
same lock. It may cross only the next proven boundary. A candidate that has
already started and then closed can restart only during explicit resume, with
fresh clean-close evidence and retained valid progress. It never receives fresh
initial positions.

Before action started, an unchanged healthy prior runner can prove that this
action did not mutate. After started, a still-running old process cannot prove
that: TERM may already be in flight. A healthy desired candidate can establish
the completed runtime outcome only when the full checkpoint chain is durable.

An action recorded as completed with failure remains terminal in the current
journal format. Resume cannot discard that record or append fictional earlier
checkpoints. Uncertain execution failures must retain the last actual boundary,
not manufacture a completion outcome.

## Acceptance before activation

- Prove the installed import and fresh-init paths through a predicate change,
  exact output, preserved application identity and monotonic offsets, then no-op.
- Inject interruption on both sides of every journal/provider boundary. Prove
  same-operation resume or a precise blocker, never a fresh group or silent reset.
- Prove rejection of foreign generation, changed volume instance, wrong mounted
  plan, wrong topic/cluster/network/image, extra group members and retention loss.
- Keep ordinary create/no-op, legacy reviewed plans, local/PostgreSQL state,
  Connector/Gateway removal and external-resource behavior unchanged.
- Verify the public resume/recovery flow, not only a standalone Java restart.

Docker daemon access and exclusive ownership of the application ID remain trust
boundaries. Kafka does not provide a compare-and-set across topic identities,
group membership, offsets and Docker actions. This contract does not claim
zero downtime, arbitrary crash rollback or protection against a concurrent
out-of-band administrator changing those resources.
