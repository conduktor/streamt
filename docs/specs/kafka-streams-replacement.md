# Kafka Streams predicate replacement

## Status and purpose

This contract implements the update part of the approved
[topology/runtime cycle](../plans/2026-09-05-topology-runtime-execution.md).
Creation and no-op repeat apply are supported. Public online planning now
prepares one evidenced predicate replacement, saved as reviewed format 6.
`apply --plan` executes that exact action through the coordinator;
`state runner-status` diagnoses it and `state resume` explicitly continues the
same operation. Broader runner changes remain blocked. The historical internal
proofs below retain their original scope; public acceptance is recorded separately.

The public CLI implementation is saved as a checkpoint. Work paused on 2026-09-06;
final installed-package acceptance and coupled Kafka/PostgreSQL verification
remain open. The [resume handoff](../plans/2026-09-06-resume-handoff.md) records
exact source cohorts, missing files and the completion checklist. Command
availability in the working tree is not a released-package claim.

Clean-close evidence admits raw exit codes 0 and 143. The latter accounts for
the fixed JVM runner's TERM shutdown. Neither code alone proves success: the
observer also requires a fresh closed status, complete non-OOM/error-free
process evidence and an inactive group with retained offsets. The journal
preserves the actual code; no adapter rewrites 143 to zero. A dedicated real
Docker/Kafka probe now verifies these conditions, separately from the earlier
cleanup observations. Its evidence is in
`tests/package/verification/kafka-streams-replacement-observer.json`.

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

Read-only observation checks the container's environment against the pinned
image, its fixed execution options and actual mounted inputs. It inventories
all containers labelled with the application ID, including renamed ones, and
rejects unaccounted generations. A final process/state re-read must still match
the initial exact-ID observation; a name or image label alone is insufficient.

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

Reviewed format 6 implements this binding for a sole typed replacement in an
online, full-project plan. It hashes the complete typed action separately from
generic display redaction, including schema fields named `token` or `password`.
Fresh offsets may advance without invalidating that action, but apply persists
the exact reviewed tuple. Other plans retain format 5 and its existing checksum.
The format alone cannot bypass a replacement blocker. Public online planning
uses an explicit preparation hook after observing a freshly compiled full
project. It requires one predicate-only update,
unchanged protected ownership and observed runtime evidence. Selected projects,
mixed mutations and generic apply remain blocked. External declarations do not
become runtime observations or mutations.

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
| Started, close observed | Fresh old-plan closed status, raw exit 0 or 143, no forced/OOM failure, inactive group, resumable monotonic offsets | Record `old_closed` with its final progress |
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

Unknown exit, any code other than 0 or 143, OOM, stale status, changed volume, replaced topic,
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

The internal driver now records the ordered boundaries, checks the locked
snapshot before every mutation and leaves state commit/finalization to its
caller. A missing write acknowledgement stops that invocation with its last
acknowledged snapshot. Explicit internal continuation can re-observe a candidate
created by the same operation, without creating another candidate or resetting
offsets. Read-only startup polling is bounded, including when status is briefly
unavailable at the start of a resumed invocation.

The source and installed Docker/Kafka probe covers a lost create response, the same
operation's continuation, changed filter output and offsets advancing from 5 to
8. It uses separate driver invocations in one Python process under the same
held local lock. It leaves the original ownership state and completed pending
journal intact. This is not a public CLI resume, an operating-system crash
test or a remote-state acceptance. See
`tests/package/verification/kafka-streams-replacement-executor.json`.

### Durable resume authorization

Both state backends now implement an internal `resume_operation(snapshot, record)`
transition. It accepts only a recorded interruption of one typed runner update,
with a non-null original reviewed-plan checksum and unchanged protected state.
Completed success and failure boundaries both require finalization or recovery;
neither can be resumed. An interrupted `in_progress` operation must first record
its interruption under a newly acquired lock. The transition itself makes no
Docker or Kafka calls.

The first resume opts that control into version 5. Its intent, actions and
checkpoints retain their exact version-4 bytes. A bounded `resume_history`
retains each authorization UUID, actor, time, state-store identity, original
state checksum, progress position, interrupted-control checksum and full recovery
record. Subsequent progress and interruptions retain that history. The limit
is 32 authorizations per operation; exceeding it blocks further resume.

PostgreSQL commits control and history together. History indexes count every
incident and resume, not only action checkpoints. Local state appends the
authorization to its existing recovery-history file before changing control.
This preserves the incident if the control response is lost or the operation
later completes. The local history envelope becomes version 2 only when it
contains a resume; previous events keep their bytes and checksums.

The local files are not one atomic transaction. An archive entry without the
matching control transition is a pending authorization, not proof that execution
resumed. It permits only an exact retry of that archived record while the
interrupted control still matches. A different authorization or a conflicting
recovery resolution blocks. An unacknowledged transition never authorizes the
runtime driver to continue on a lost lock.

Both backends also expose `pending_resume_authorization(snapshot)` under the
operation lock. The local implementation returns the sole exact archived record
left before a control update, after validating its interrupted snapshot and
re-reading state, control and audit. Matching audit without a pending record
returns `None`; a mismatch or competing recovery is an error. PostgreSQL returns
`None` only after a fresh read-only transaction verifies the supplied snapshot
and the current runner journal, including a recorded interruption. Its atomic
resume transition cannot leave a separate prewritten record.

This lookup writes nothing. Absence is not eligibility or permission to resume.
These backend checks also do not verify the user's current SQL against an
original plan file. The coordinator and public commands perform that check
before runtime observation or continuation.

`PlannedAction` and its durable-action converter now retain the typed runner
evidence without changing its version-4 bytes. Reviewed format-6 round trips
preserve that evidence. Conversion alone does not collect observations or
authorize execution. Public online planning explicitly prepares an evidenced
replacement from its last full-project observation. Generic apply keeps its
blocker; reviewed apply uses the dedicated coordinator.

The separate `kafka_streams_resume_probe.py` acceptance now passes from source
(client 2.13.2) and an installed package (2.15.0). Its first worker loses a real
Docker-create acknowledgement, records the interruption and exits. A second OS
process reacquires the lock, loads the journal, authorizes resume and starts only
the existing candidate. It commits desired ownership and clears control; the
original incident remains in the local audit. Output checks prove the filter
change and offsets 5 to 8. This is a controlled worker exit, not a SIGKILL test;
its reviewed checksum is a test fixture, not public plan-file validation. The
evidence is in `tests/package/verification/kafka-streams-durable-resume.json`.

### Reviewed coordinator and completed-result finalization

The internal coordinator validates the original format-6 checksum and action
tuple, reparses and compiles the full current project, and verifies its runtime,
environment and protected state. It repeats those checks before driver
transitions and before finalization. The desired action must match the actual
compiled runner artifact. Fresh observed offsets may advance; they never replace
the reviewed action's original lower bounds.

Execute begins that exact reviewed intent. Resume loads the original intent
under a new lock, verifies the same reviewed plan and current project, and reuses
an exact prewritten authorization when present. It does not replan against a
half-replaced runtime. A missing journal acknowledgement retains only the last
acknowledged boundary; a newer durable boundary cannot be overwritten with an
invented interruption record.

`finalize_completed_runner(snapshot)` is a storage-only transition. It requires
the complete successful checkpoint sequence for one reviewed runner update.
The coordinator must first re-observe the exact ready candidate. Completion may
follow an active operation or a recorded interruption, but never a failed or
incomplete action. Finalization changes only the runner's artifact checksum and
increments ownership serial once.

Local state writes a `runner_completed` receipt before ownership and control
clear. The receipt retains the full terminal control, original intent, incidents,
resume history and prior/result state checksums. The existing local history
envelope becomes version 3 only when it contains a completion event; historical
event bytes and checksums remain unchanged. The receipt freezes the terminal
control so ordinary progress, resume and recovery cannot reopen it.

If ownership was written but clear was interrupted, a fresh process reconstructs
the prior state by undoing only the sole runner checksum change and serial
increment. Its full checksum must match the original intent. Any unrelated
ownership change blocks finalization. With the original plan, current project,
exact receipt and ready candidate verified, retry clears control without runtime
writes, another ownership write or a second serial increment.

PostgreSQL commits desired ownership, terminal history and clear atomically.
It retains the original interruption and resume rows. An active control paired
with already-written result ownership is invalid there, not a local-style retry.
Neither backend rewrites an incident as if it never happened.

An unacknowledged clear remains an unknown outcome for that invocation. A fresh
public command can now verify an already-cleared operation from its exact
receipt and ready candidate. Finalization errors never authorize deployment retries or a new
incident that would change the archived terminal control.

The real Kafka/Docker coordinator probe passes from source (client 2.13.2) and
an installed wheel (2.15.0). It edits the project's SQL file, prepares and saves
an actual format-6 plan, then reloads that same file in three worker processes.
Worker one loses the Docker-create response; worker two resumes the existing
candidate and stops after ownership commit but before clear; worker three
finalizes with runtime, ownership, authorization and audit writes forbidden.
The original incident and completion receipt survive. Exact output checks prove
the predicate change and offsets 5 to 8. Public plan/apply then return a no-op
without changing providers, ownership or audit. These are controlled worker
exits, not SIGKILL, authenticated Kafka or public update/resume acceptance.
See `tests/package/verification/kafka-streams-reviewed-coordinator.json`.

## Acceptance requirements

- Prove the installed import and fresh-init paths through a predicate change,
  exact output, preserved application identity and monotonic offsets, then no-op.
- Inject interruption on both sides of every journal/provider boundary. Prove
  same-operation resume or a precise blocker, never a fresh group or silent reset.
- Prove rejection of foreign generation, changed volume instance, wrong mounted
  plan, wrong topic/cluster/network/image, extra group members and retention loss.
- Keep ordinary create/no-op, legacy reviewed plans, local/PostgreSQL state,
  Connector/Gateway removal and external-resource behavior unchanged.
- Verify the public resume/recovery flow, not only a standalone Java restart.

## Public command contract

`plan --out` prepares the typed action only from the planner's exact last online
full-project observation. It seals that plan and protected state against edits.
The saved format-6 file is mandatory for replacement; legacy format-5 planning
and creation remain unchanged.

`apply --plan` verifies the original file and enters the coordinator before any
generic mutation, rollback or ownership-commit path. It reparses the actual SQL
and freezes the selected environment policy and state authority throughout the
operation. OpenLineage START is sent only after the intent acknowledgement;
COMPLETE follows verified finalization. Telemetry delivery cannot decide commit.

`state runner-status --plan FILE --operation-id UUID` takes the operation lock
and validates storage/audit before constructing its read-only runtime observer.
It reports the next proven boundary without granting resume authority. The
observer consumes the exact project instance already validated by the coordinator;
an unchecked reparse cannot introduce provider access under changed settings.

`state resume` uses that same original file and UUID. It requires exact
`--confirm-env` when environment policy demands it. SQL, manifest, runtime,
ownership or environment mismatches block before the next mutation. Neither
command creates a new plan or treats missing pending control as success.
The single-environment starter omits `--env`; it must not invent an environment
directory named `default`.

Both backends expose a locked, read-only completed-receipt lookup and a terminal
pending-snapshot validation gate. Local lookup verifies the full archived receipt
and current result, including the one-change reconstruction of prior ownership.
PostgreSQL verifies the complete operation and state history in a read-only
transaction and derives completion time from the durable terminal row. Absent
operation history with orphan state history is corruption, not a missing receipt.
These lookups perform no ownership, control or audit writes. They do not promise
historical reporting after subsequent project or ownership changes.

An unacknowledged response reports `committed: null` and the last acknowledged
boundary. A fresh status or resume can verify the committed receipt without
redeploying, appending audit or incrementing ownership serial. A verified result
followed by a lock-release error retains `committed: true`.

The next product work is declared Git base/head comparison and downstream
impact, after installed public acceptance and coupled Kafka/PostgreSQL validation.

Docker daemon access and exclusive ownership of the application ID remain trust
boundaries. Kafka does not provide a compare-and-set across topic identities,
group membership, offsets and Docker actions. This contract does not claim
zero downtime, arbitrary crash rollback or protection against a concurrent
out-of-band administrator changing those resources.
