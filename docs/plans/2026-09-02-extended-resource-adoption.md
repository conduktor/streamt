# Extended resource adoption plan

## Objective

Extend the existing single-resource, state-only adoption protocol beyond Kafka
topics and Schema Registry subjects without treating a provider name as proof
of resource identity. Delivery is deliberately ordered:

1. make Kafka Connect planning secret-neutral and exact;
2. bind each Connector artifact and state record to one canonical Connect
   cluster locator;
3. add one strict Connector observation request and reuse it for planning;
4. make Connector recovery exact before enabling Connector adoption;
5. normalize a complete, scoped Conduktor Gateway rule aggregate;
6. enable only the exact alias-only Gateway subset initially; and
7. defer Flink adoption until stable job identity, artifact evidence, routing,
   and state-advancement semantics exist.

This plan also records the implemented boundary; it is not by itself a broad
production-readiness claim. Packages 1 through 5 are complete and the public
`adopt` command accepts `topic`, `schema`, and the deliberately narrow
`connector` slice. Package 6 is in progress; Package 7 and Gateway adoption
remain planned.

## Current boundary

The shipped adoption transaction already provides the reusable control-plane
protocol:

- resolve one compiled artifact with explicit `ownership.mode: adopted`;
- hold the configured state operation lock through observation, confirmation,
  re-observation, and commit;
- show a secret-neutral review of observed and desired managed attributes;
- confirm the exact canonical resource ID and effective environment;
- re-observe the provider target and reject live drift;
- reread state and control immediately before intent;
- record a state-only `adopt` operation and commit with compare-and-swap; and
- leave every non-target ownership record unchanged.

Kafka Connect now enters this protocol only through an exact default-cluster
binding: the effective cluster alias, versioned normalized-endpoint
fingerprint, and connector name. One percent-encoded resource `GET` provides
each strict observation, once for review and once after confirmation. An
artifact with no cluster or with the explicit default alias enters this slice;
an explicit non-default cluster fails closed. An identical existing state claim
returns after the first strict observation without confirmation or a state
write. Gateway and Flink do not yet provide enough exact provider evidence to
enter the protocol safely.

## Cross-provider invariants

Every new adoption kind must satisfy all of these requirements before its CLI
choice is exposed.

### Identity

The canonical logical identity remains:

```text
streamt://<project>/<environment>/<kind>/<ownership.owner_name>
```

Provider identity is separate from that logical URI. It includes the effective
runtime backend or cluster binding and every provider key needed to select one
resource without ambiguity. Display labels, provider list order, inferred
prefixes, default-cluster names that were not resolved, and live status are not
identities.

A state record created under a legacy, unbound backend value must never
silently authorize the same resource name against a newly selected cluster or
scope. Such records require an explicit migration with exact evidence or an
explicit re-adoption. There is no automatic compatibility interpretation.

### Desired state and live evidence

Two hashes serve different purposes and must remain distinct:

- the persisted `artifact_checksum` is computed from the canonical compiled
  desired artifact and is identical to the checksum normal planning will
  produce; and
- the confirmation fingerprint is computed from one strict, normalized live
  provider observation, including its provider locator.

Provider comparison must preserve JSON types and string case unless the
provider contract explicitly defines a field-specific normalization. Generic
string coercion, case folding, partial dictionaries, and provider list order
are not safe equality rules.

The first live observation supplies review evidence only. After confirmation,
the command performs the same complete observation again and requires the
canonical fingerprint to be unchanged. It then rereads state and control and
uses the existing operation-intent and compare-and-swap protocol. A provider
error, malformed response, ambiguous result, identity mismatch, unsupported
field, or changed fingerprint fails closed without writing ownership state.

### State-only behavior and secrecy

Adoption may call only the provider read operation specified for that resource.
It must not call create, update, delete, pause, resume, restart, cancel, submit,
or a general planner that can perform incidental writes.

Raw connector configuration and other credential-bearing provider content must
not enter text output, structured output, logs, exception messages, reviewed
plans, operation control, or history. Review output uses non-secret identity,
field names where safe, deterministic checksums, and structured pending-change
categories. Redaction is a defense in depth, not permission to serialize raw
configuration first.

### Recovery before adoption

No provider kind is enabled for adoption until the same normalized observer can
resolve interrupted create/update outcomes in reviewed recovery. Recovery must
prove exact prior or candidate managed content; runtime health alone is not
artifact evidence. This ordering prevents adoption from creating state records
that the recovery workflow cannot later validate.

## Package 1: secret-neutral Connector planning

Status: complete.

Connector planning now uses the strict artifact parser and secret-neutral
change evidence shared with adoption.

Required changes:

1. Parse compiled Connector artifacts through one strict constructor. Require
   non-empty `name`, `connector_class`, and topic names; a declared cluster that
   is either absent or a non-empty string; a JSON-object configuration with
   string keys; and exact ownership. Effective-cluster resolution belongs to
   Package 2.
2. Reject user configuration that overrides reserved generated keys, including
   `name`, `connector.class`, and `topics`. The canonical artifact owns those
   fields and cannot have two competing representations.
3. Preserve the explicitly selected artifact cluster through parsing and plan
   construction. Do not discard it or silently reinterpret it as the default;
   canonical default resolution and routing land in Package 2.
4. Compare canonical current and desired configurations with exact JSON type
   and case semantics. Treat missing and explicit null as different unless the
   Connect API documents equivalence for that specific field.
5. Replace raw `from` and `to` values in plan changes with secret-neutral
   evidence: sanitized changed-key categories, directions, and presence only,
   never per-value fingerprints. Adoption review separately includes
   whole-configuration current and desired checksums. Provider exception text
   passes through the existing sanitizer.
6. Add regression fixtures containing passwords, tokens, JAAS strings, URLs
   with user information, mixed-case values, booleans, numbers, null, and
   nested structures. Assert that no raw value reaches text, JSON, plan files,
   logs, or errors.

Exit gate: direct and reviewed plans remain deterministic, distinguish exact
type/case changes, preserve every declared cluster for Package 2, and contain
no raw provider configuration values.

## Package 2: canonical Connector artifact and cluster binding

Status: complete.

The logical resource ID is based on `ownership.owner_name`. Its provider
locator is the tuple:

```text
(effective Connect cluster alias,
 normalized Connect REST endpoint fingerprint,
 connector name)
```

The endpoint is normalized under a documented versioned algorithm before it is
hashed. Credentials, query strings, and fragments are rejected rather than
hashed. The persisted locator exposes the cluster alias and a versioned SHA-256
endpoint fingerprint, never the endpoint or credentials. The connector name
remains the provider object key.

Required changes:

1. Freeze the provider-locator encoding and use it in ownership checks,
   desired-state generation, planned-action construction, recovery, and
   adoption.
2. Bind artifacts that omit `cluster` to the resolved default alias before
   their desired checksum and provider locator are constructed. A later default
   change must produce an identity mismatch, not transfer authority.
3. Reject two compiled artifacts that resolve to the same provider locator or
   one logical owner that resolves to multiple Connector artifacts.
4. Treat existing records with only `backend: kafka-connect` as legacy and
   unbound. They may not authorize mutation, recovery finalization, or adoption
   idempotency against a selected cluster.
5. Fail closed until an operator separately migrates or removes a legacy claim
   before an explicit re-adoption. Never rewrite legacy records automatically
   during plan, apply, recovery, or adoption.

Exit gate: changing the default cluster alias, its normalized endpoint, the
artifact cluster, or the connector name cannot reuse prior authority.

## Package 3: strict one-request Connector observer

Status: complete.

Observe one connector with one resource request:

```text
GET /connectors/<percent-encoded-name>
```

This endpoint's stable `name` and `config` fields form the managed observation.
Volatile task assignments, worker IDs, runtime status, traces, and error text
are excluded. `/config` plus `/status` multi-request assembly is not an exact
snapshot and is not used by planning, recovery, or adoption.

The observer must:

- percent-encode the connector path segment and require the returned name to
  equal the requested name exactly;
- accept absence only from the endpoint's documented not-found response;
- require one JSON object with an exact name and a configuration object whose
  keys are strings;
- reject partial, duplicate, malformed, non-canonical, or oversized content;
- normalize only explicitly documented provider response behavior;
- return the effective provider locator with the managed configuration;
- compute a canonical live fingerprint without logging or returning raw
  configuration to the presentation layer; and
- remain a read-only API that cannot dispatch a mutation.

Planning must consume this observer before recovery or adoption does, ensuring
all three paths agree about existence, identity, and equality.

Exit gate: unit and real Connect tests prove that one logical observation uses
only the encoded resource GET, detects exact configuration drift, and remains
secret-neutral on success and every failure path.

## Package 4: Connector recovery

Status: complete.

Replace Connector recovery's status/task-based normalization with the strict
managed observation. Reviewed recovery may accept a present connector only
when the provider locator is exact and its stable configuration proves the
requested prior or candidate artifact checksum. Status and tasks may appear in
separate diagnostics, but never authorize state finalization.

Recovery must fail closed for legacy unbound records, absent cluster aliases,
endpoint-fingerprint drift, malformed content, unsupported artifact fields,
or a configuration that cannot reconstruct the exact prior/candidate checksum.

Exit gate: local and PostgreSQL reviewed recovery cover exact prior, exact
candidate, absence where permitted, live drift, malformed responses, secret
neutrality, and operation/control conflicts.

## Package 5: single-Connector adoption

Status: complete.

`streamt adopt --kind connector` resolves exactly one compiled adopted artifact
by `ownership.owner_name`, resolves and binds an omitted or explicitly matching
default cluster alias, and invokes the strict observer twice around
confirmation. An explicit non-default cluster is rejected.

Review output includes the canonical resource ID, cluster alias, endpoint
fingerprint, connector name, whole-configuration current and desired checksums,
sanitized changed-key categories and directions, and whether a later plan has
pending changes. It never contains raw configuration or per-value fingerprints.
The state operation records `adopt` only; no Connect mutation endpoint is
reachable. An identical state claim is idempotent after one strict observation:
it performs no confirmation and no state write.

Source command coverage proves exact selection, zero mutation,
re-observation, idempotency, conflict handling, state policy, local state, and
secret-neutral output. The project release gates also exercise command
availability from the isolated installed wheel, strict observation against a
real Connect service, and the Connector path through the real PostgreSQL v2
writer and operation history.

## Package 6: normalized scoped Gateway aggregate

Status: in progress; adoption remains unsupported. The strict artifact parser,
versioned endpoint/vCluster binding, pure desired aggregate, reusable strict
two-list snapshot, immutable observations and fingerprints, normalized change
model, desired/prior collision gates, online/offline planner integration,
canonical state projection, and secret-neutral reviewed-plan/CLI presentation
are complete. Normalized shared-snapshot status and health, the exact mutation
core, the legitimate normalized delete change model, and planner
apply/delete/rollback routing are also complete; rollback routing is limited to
the exact creates recorded by the reviewed plan. Reviewed recovery now proves
converged creates and updates plus an absent rolled-back create. It remains fail
closed for a rolled-back update because prior state does not retain the prior
provider-surface fingerprint, and normalized delete recovery is not yet
representable. Package 6 and adoption therefore remain incomplete. The frozen
staged contract and release gates are in the
[Gateway normalized aggregate implementation specification](2026-09-02-gateway-normalized-aggregate.md).

A Gateway rule is not one provider object. Its logical identity still uses
`ownership.owner_name`, while its compound provider identity contains:

- the configured Gateway backend binding;
- the exact vCluster scope;
- the AliasTopic key `virtualTopic`; and
- the exact deterministic set of scoped Interceptor keys belonging to the
  rule.

The alias's `physicalTopic` and `spec.physicalCluster`, and each interceptor's
plugin class, priority, scope, and provider-transformed configuration are
managed content. For the initial single-cluster slice, omitted and exact `main`
physical clusters normalize to canonical `main`; every other value fails closed
until physical-cluster selection exists. Logical rule name and virtual topic are
distinct and must never be substituted for one another.

Required changes:

1. Add one immutable normalized Gateway rule aggregate containing a scoped
   alias plus a complete interceptor set. Planning, status, recovery, and
   adoption use this object.
2. Strictly parse alias and interceptor list responses. Reject non-list
   responses, malformed metadata/spec objects, duplicate scoped identities,
   ambiguous same-name aliases, missing priority/configuration, and unexpected
   scope. Normalize only omitted or exact `main` AliasTopic physical clusters to
   canonical `main`; reject every other physical-cluster value.
3. Bind state authority to the Gateway backend and exact vCluster. A legacy
   `backend: conduktor-gateway` record containing only a virtual-topic physical
   name is unbound and requires explicit migration or re-adoption.
4. Build canonical desired provider state with one pure function shared by
   planning and apply. It generates exact interceptor names, plugin classes,
   priority, scope, transformed configuration, and AliasTopic
   `physicalCluster: main` from the compiled artifact.
5. Replace positional comparison and broad `startswith` ownership with exact
   deterministic interceptor identities. Reject namespace collisions across
   compiled rules.
6. Make apply, delete, and rollback carry both logical rule name and alias key.
   They must not delete an alias under the logical name or capture another
   rule's interceptors by prefix.
7. Allow only compiler-emitted declarations with proven exact provider
   transformations. The initial nonempty managed surface accepts one
   compiler-emitted filter. Reject every mask, including empty `forRoles`, until
   DSL method, plugin, topic, Schema Registry, and role semantics are round-trip
   exact. Also reject unsafe legacy `encrypt` and `readonly` transforms.
8. Populate complete current alias and interceptor evidence in normal plans;
   synthetic test-only state is not sufficient.

The canonical live fingerprint includes provider binding, vCluster, alias key,
physical topic, canonical physical cluster, and the sorted complete interceptor
managed surface. It rejects unsupported physical clusters and extra as well as
missing interceptors.

The shipped Gateway 3.15 observer foundation normalizes an Interceptor scope
containing only `username` to exact `vCluster: passthrough` with canonical null
fillers for absent scope axes; that principal scope remains distinct from the
vCluster-only managed scope.

Live Gateway 3.15 provider probes additionally established the following
mutation contract. It is provider evidence behind the shipped exact mutation
core, not a claim that broad delete discovery, all recovery outcomes, or
adoption are complete:

- `PUT` returns HTTP 200 with the exact object shape `{resource, upsertResult}`,
  where `upsertResult` is the string `Created`, `Updated`, or `NotChanged`.
- A passthrough AliasTopic response omits `metadata.vCluster` and includes
  `spec.physicalCluster: main`.
- An Interceptor response null-fills the scope axes and adds an empty comment.
- Interceptor deletion with the exact full null-filled passthrough scope body
  returned 204, then 404 on the same repeated deletion. AliasTopic deletion
  with explicit `vCluster: passthrough` likewise returned 204, then 404.
- Both provider collections were empty after cleanup; the probes left no
  residue.

The strict mutation implementation matches the planned operation to the
provider result. A sent write does not treat `NotChanged`, or another unexpected
`upsertResult`, as success. A 404 while deleting a target that the reviewed
snapshot proved present is concurrency drift, not an idempotent success.

The normalized delete change model is deliberately narrower than delete
discovery: it accepts one complete, present, bound observation and carries the
exact alias and owned interceptor identities with fingerprint-only change
evidence. It does not infer deletion from an absent manifest entry or rediscover
targets from a logical name, prefix, or legacy state record. Reviewed recovery
also does not pretend that this delete surface is reconstructable yet.

Exit gate: plan, status, apply, rollback, and recovery agree on the same scoped
aggregate; ambiguous or partial observations fail closed; logical names that
differ from virtual topics and overlapping name prefixes cannot affect another
rule.

## Package 7: alias-only Gateway adoption

Status: planned; follows Package 6.

The first Gateway adoption release is intentionally restricted to compiled
rules whose desired interceptor list is empty. The command must prove:

- one exact compiled adopted rule;
- one exact alias in the bound backend and vCluster;
- canonical AliasTopic physical cluster `main`;
- a complete observation showing zero rule-owned interceptors; and
- an unchanged aggregate fingerprint after confirmation.

It may show an exact alias mapping difference for later planning, but it cannot
adopt partial, nonempty, unknown, or ambiguously associated interceptor state.
Only Gateway read endpoints are used. Recovery for the same exact alias-only
surface must pass before the CLI kind is exposed.

Full interceptor-rule adoption remains a later package. It requires proven
round-trip equivalence between compiled declarations and provider plugin
configuration, exact extra-interceptor detection, and complete scope and
priority evidence.

Exit gate: unit, command, installed-wheel, and real Gateway tests prove scoped
identity, zero mutation, two-observation drift rejection, state conflicts,
recovery, and rejection of every nonempty or incomplete rule.

## Flink adoption remains deferred

Flink adoption must not be implemented as a status-based variant of topic or
Connector adoption. The current provider surface cannot prove the submitted
SQL and execution settings represented by ownership state.

All of these prerequisites are required first:

1. Define a stable per-job provider name and ID. Remove suffix-based lookup and
   reject ambiguous matches.
2. Route every artifact through its exact effective Flink cluster instead of
   ignoring an artifact cluster or always selecting the default.
3. Reject multiple jobs emitted for one ownership owner unless each has its own
   canonical logical identity and provider locator.
4. Obtain a provider-visible, stable fingerprint covering SQL and every managed
   execution setting, or persist a separately attestable submission artifact
   that can be correlated to one exact live job.
5. Implement exact discovery for present, absent, replaced, suspended, and
   terminal jobs without using display names or runtime status as content
   proof.
6. Prevent desired ownership checksums from advancing when no corresponding
   runtime action or exact provider evidence occurred.
7. Design savepoint, last-state, and explicitly stateless replacement
   semantics, then pass reviewed recovery before exposing adoption.

Until every prerequisite is complete, present Flink jobs remain unsupported
for adoption and fail closed during any recovery path that would require proof
of managed artifact content.

## Test and release gates

Each provider package must add focused unit tests before broader command tests.
The final enablement commit for a kind requires:

- strict artifact and provider-response parsing;
- canonical logical and provider identity collision tests;
- exact desired/live checksum fixtures;
- provider drift between confirmation and commit;
- state and control drift at the final reread;
- idempotent identical claims and rejection of conflicting claims;
- proof that no mutation method or endpoint was called;
- secret-neutral text, JSON, logs, errors, reviewed evidence, and history;
- local and PostgreSQL v2 writer command paths;
- recovery coverage using the same observer;
- source-checkout and isolated installed-wheel execution; and
- strict documentation, lint, type, and packaging checks.

No checkbox or support-matrix entry may claim a provider's adoption support
until that provider's recovery, command, and installed-wheel gates all pass.

## Explicitly out of scope

- bulk adoption;
- automatic legacy-record migration;
- adoption inferred from provider tags, prefixes, status, or display names;
- provider mutation during adoption;
- full Gateway interceptor adoption in the alias-only slice;
- Flink adoption before artifact-content evidence and lifecycle semantics;
- automatic deletion or cleanup of unowned provider resources; and
- weakening the existing exact confirmation, state-lock, recovery, or remote
  PostgreSQL writer requirements.
