# Gateway normalized aggregate implementation specification

## Status and release boundary

Status: Package 6 remains in progress. The strict compiled-artifact parser,
versioned Gateway binding, pure desired aggregate, reusable strict two-list
snapshot, immutable observations and fingerprints, normalized change model,
desired/prior collision gates, online/offline planner integration, canonical
state projection, and secret-neutral reviewed-plan/CLI presentation are shipped.
Shared-snapshot status integration and Stage 9 reviewed recovery remain pending.
The Stage 8 exact managed mutation core is in progress, but planner
apply/delete/rollback routing is not complete. Gateway adoption remains
unsupported.

This specification freezes the implementation contract for Package 6 of the
[extended resource adoption plan](2026-09-02-extended-resource-adoption.md).
Package 6 is not complete, and Gateway adoption remains unsupported.

The work is intentionally staged. The public `streamt adopt` choices must not
include `gateway_rule` until the normalized aggregate is used by planning,
status, state projection, exact mutation and rollback, and reviewed recovery,
and the alias-only release gates in this document pass. A partial observer or a
working happy-path command is not enough to cross that boundary.

| Package 6 component | Status | Boundary |
| --- | --- | --- |
| Strict compiled-artifact parser | Complete foundation | Strict construction is shipped; downstream lifecycle integration remains gated. |
| Versioned endpoint and vCluster binding | Complete foundation | Canonical `passthrough` scope and endpoint-free backend identity are shipped. |
| Strict two-list observer and immutable live aggregate | Complete foundation | Exact GET-only transport, parsing, scope normalization, managed fields, and fingerprint are shipped. |
| Pure desired aggregate | Complete foundation | Initial managed transformation is restricted to zero interceptors or one compiler-emitted filter. |
| Normalized change model and collision gates | Complete | Fingerprint-only evidence plus desired and prior-state collision rejection are shipped. |
| Planner, state, and reviewed-plan presentation | Complete | Online/offline planning, one reusable online snapshot, canonical state projection, and secret-neutral CLI rendering are shipped. |
| Status shared-snapshot integration | Pending | Status has not yet been migrated to the reusable strict snapshot. |
| Exact apply, delete, and rollback | In progress | The managed mutation core is being implemented; planner routing and complete rollback integration remain pending. |
| Reviewed recovery | Pending, fail closed | Normalized Gateway recovery is not implemented and explicitly refuses to infer success from legacy or partial evidence. |
| Alias-only adoption | Planned after Package 6 | The CLI kind remains unsupported. |

This specification covers Conduktor Gateway API v2 AliasTopic and Interceptor
resources. A streamt Gateway rule is a compound provider resource, never an
alias name alone and never a logical-name prefix over the global interceptor
list.

## Non-negotiable invariants

1. Logical ownership and provider identity remain separate. The logical URI
   uses `ownership.owner_name`; the provider locator uses one bound Gateway,
   one exact effective vCluster, one alias key, and exact interceptor keys.
2. Every compiled Gateway artifact is parsed strictly before provider access.
   Malformed or ambiguous artifacts are errors, not skipped entries.
3. One pure desired-state function is the only translation from a compiled
   artifact to provider-managed AliasTopic and Interceptor content.
4. One strict provider snapshot consists of exactly two GET requests: the
   AliasTopic list and the Interceptor list. It cannot dispatch a mutation.
5. Provider objects are identified by exact `(scope, name)` pairs within their
   kind. Missing scope is not a wildcard. Provider list order is never identity.
6. Generated interceptor ownership is recognized only by an anchored parser and
   exact regenerated names. Prefix matching is forbidden.
7. The live aggregate is immutable and complete. Its fingerprint covers the
   binding, scope, alias presence and managed fields, and the sorted complete
   interceptor managed surface.
8. Planning, status, state projection, apply, delete, rollback, recovery, and
   later adoption consume the same binding, desired aggregate, and live
   aggregate types.
9. Legacy `backend: conduktor-gateway` records are unbound. They never silently
   authorize a newly bound endpoint or vCluster.
10. Errors, logs, plans, history, and CLI output do not serialize raw provider
    configuration. Redaction remains defense in depth rather than permission to
    carry arbitrary response content into presentation objects.

## Stage 1: strict compiled-artifact parser

Component status: complete as a shipped foundation. Its use by every lifecycle
consumer remains an integration exit gate.

Add one strict constructor for every entry in `manifest.artifacts.gateway_rules`.
Both offline and live planning, desired-state construction, recovery, and later
adoption use it.

The parser requires:

- a non-empty `name`, `virtualTopic`, and `physicalTopic`, each with the exact
  string case preserved;
- `interceptors` to be a list, never null or an inferred singleton;
- every interceptor declaration to be an object with a non-empty `type` and an
  object-valued `config`;
- exact lifecycle ownership parsed through `ArtifactOwnership`; and
- the ownership project and stable logical owner to remain available without
  substituting the provider alias key.

Unknown keys, invalid JSON values, boolean-as-integer values, non-finite numbers,
unsupported interceptor types, and declaration fields that cannot survive the
provider translation fail closed. No entry is dropped with a warning.

Only compiler-emitted declarations with a proven, exact provider transformation
may pass strict desired construction. The initial managed transformation accepts
zero interceptors or exactly one compiler-emitted `filter`. Every `mask`
declaration is rejected for now, including a mask with missing or empty
`forRoles`: current DSL masking methods, Gateway plugin behavior, topic and
Schema Registry representation, and role semantics are not round-trip exact.
The unsafe legacy `encrypt` and `readonly` transforms are also rejected rather
than accepted from hand-written or older manifests. The alias-only adoption
stage rejects all nonempty interceptor lists regardless of whether their types
are otherwise supported.

## Stage 2: versioned Gateway binding

Component status: complete as a shipped foundation. State and lifecycle
consumers have not yet completed migration to the binding.

Introduce an immutable version-1 Gateway binding derived from:

```text
(normalized Gateway admin endpoint,
 fixed /gateway/v2 API surface,
 exact effective vCluster)
```

Endpoint normalization follows the already proven Connect boundary:

- only HTTP and HTTPS are accepted;
- scheme and IDNA host are normalized to lowercase;
- default ports and one trailing slash are normalized;
- a non-root base path is retained;
- leading or trailing whitespace, control characters, user information, query
  strings, and fragments are rejected; and
- validation errors never echo the endpoint or credentials.

The version-1 endpoint fingerprint is SHA-256 over the normalized admin endpoint
and the fixed `/gateway/v2` API surface. It never contains the endpoint itself.
The canonical state backend is structurally versioned, for example:

```text
conduktor-gateway:v1:<encoded-effective-vcluster>:sha256:<64-lowercase-hex>
```

The vCluster component is an unambiguous encoded representation of the exact
case-sensitive scope. Backend parsing must validate the complete string and
reject unknown versions, malformed encodings, or malformed fingerprints.

### Default `passthrough` normalization boundary

The effective default vCluster is the literal, case-sensitive string
`passthrough`.

- Omitted project `virtual_cluster` and explicit `virtual_cluster: passthrough`
  resolve to the same canonical scope before a binding, desired aggregate,
  checksum, or provider locator is created.
- In an API response, an omitted documented vCluster field normalizes to
  `passthrough` only inside the strict response parser. It does not remain null
  and it never means every scope.
- An empty or whitespace-only configured or observed scope is invalid.
- `Passthrough`, `default`, and every other nonempty vCluster are distinct from
  `passthrough` and from one another.
- An AliasTopic name or Interceptor name repeated in another scope represents a
  distinct provider object. It neither satisfies nor conflicts with the target
  scope unless a separate compiled artifact is explicitly bound there.

The strict observer also preserves a live Gateway 3.15 compatibility boundary.
An Interceptor scope containing an exact `username` but omitting `vCluster` and
`group` normalizes to the canonical tuple `(group=None, username=<exact>,
vCluster=passthrough)`. The provider's null-filled equivalent normalizes to the
same identity. This principal-scoped object is distinct from the vCluster-only
managed target `(group=None, username=None, vCluster=passthrough)` and cannot be
claimed by it.

The project currently selects one Gateway runtime. Every rule compiled for that
runtime is bound to its resolved effective scope before desired checksums and
collision checks are computed.

### Initial `physicalCluster` normalization boundary

AliasTopic `spec.physicalCluster` is managed content, not part of the alias
provider identity. The initial single-physical-cluster slice supports only the
literal, case-sensitive value `main`.

- A missing response `spec.physicalCluster` and an exact string value `main`
  both normalize to canonical `main` inside the strict AliasTopic parser.
- Desired construction always includes canonical `physicalCluster: main`.
- Null, empty, whitespace-only, non-string, `Main`, and every value other than
  exact `main` fail closed.
- No other value is planned as an update or silently rebound. Supporting another
  physical cluster first requires an explicit project selection field, provider
  routing contract, state semantics, and separate compatibility tests.

`physicalCluster` therefore participates in desired/live equality, confirmation
fingerprints, recovery evidence, and adoption review, while `(scope, alias name)`
remains the exact AliasTopic identity. Two declarations for the same alias
identity collide even if they would otherwise name different physical clusters.

## Stage 3: pure desired aggregate

Component status: complete as a shipped foundation. Its planner, state, status,
mutation, and recovery consumers remain integration exit gates.

Add one side-effect-free function that accepts a strict artifact plus the exact
Gateway binding and returns a complete immutable desired aggregate. It must not
read configuration, environment variables, state, or the provider.

The desired alias contains:

- effective scope;
- exact alias key from `virtualTopic`;
- exact physical topic from `physicalTopic`; and
- canonical physical cluster `main`.

Each desired interceptor contains:

- effective scope;
- deterministic generated name;
- exact plugin class;
- integer priority, with booleans rejected;
- complete provider-transformed configuration; and
- its declaration type and ordinal only where needed to prove deterministic
  generation, never as a substitute for provider fields.

For the first managed implementation, the only nonempty accepted set is one
compiler-emitted filter at ordinal zero. Multiple filters, every mask, and legacy
encrypt or readonly declarations fail before provider access. Additional
transformations require their own exact DSL-to-provider-to-observer round-trip
contract and tests before this boundary can expand.

Configuration equality preserves JSON types, string case, nested object keys,
array order, explicit null, and negative zero. Non-finite numbers are rejected.
Interceptor order in the aggregate is canonical `(scope, name)` order; provider
list order is discarded.

The current generated-name format remains:

```text
<logical-rule-name>_<interceptor-type>_<zero-based-index>
```

Generation and recognition share one implementation. Recognition is a full
anchored match constructed with the escaped logical rule name. It accepts a
syntactically valid type token and a canonical non-negative decimal index, then
requires regenerating the parsed components to produce the original name
exactly. It must never use `startswith`, substring containment, or an unanchored
regular expression. A generated-looking name with an unknown type or invalid
index is ambiguous evidence and fails closed when it matches the target rule's
anchored namespace.

## Stage 4: strict two-list GET-only observer

Component status: complete as a shipped foundation.

Create a dedicated Gateway snapshot API whose transport can issue GET only. One
snapshot performs exactly these requests once each:

```text
GET /gateway/v2/alias-topic
GET /gateway/v2/interceptor
```

It does not call health, status, a per-item compatibility helper, plan, apply,
create, update, or delete. It does not follow redirects or retry either request.
Authentication failures, transport failures, redirects, unexpected statuses,
invalid JSON, and oversized bodies are mapped to stable secret-neutral errors.

Both bodies must be bounded canonical JSON arrays. Parsing rejects duplicate
JSON object keys, non-finite values, non-object list entries, malformed
`kind`/`apiVersion`, malformed metadata or spec objects, and missing managed
fields. The parser validates every list entry far enough to establish its exact
identity and managed content; an unrelated malformed entry cannot be treated as
proof that the target is absent.

For AliasTopic entries, require and normalize:

- exact `(scope, name)` identity;
- `spec.physicalName` as a non-empty string;
- `spec.physicalCluster`, with missing and exact `main` normalized to canonical
  `main` and every other shape or value rejected; and
- the expected AliasTopic kind and Gateway v2 API version.

For Interceptor entries, require and normalize:

- exact `(scope, name)` identity;
- `spec.pluginClass` as a non-empty string;
- `spec.priority` as an integer but not a boolean;
- `spec.config` as a complete JSON object; and
- the expected Interceptor kind and Gateway v2 API version.

Duplicate `(scope, name)` identities within either kind are always ambiguous and
fail the snapshot. The same name in another scope is validated but remains a
different object.

Planning and status should fetch one two-list snapshot and derive all requested
rule aggregates from it. A single-target helper used by recovery and later
adoption derives one rule from the same snapshot type. Alias absence is accepted
only after both complete lists were parsed. An absent alias with an exact
generated interceptor belonging to the rule is inconsistent orphan evidence,
not a clean absence.

## Stage 5: immutable managed observation and fingerprint

Component status: complete as part of the shipped strict-observer foundation.

The live rule observation is a frozen value object. It carries the exact
binding, effective scope, requested alias key, alias presence, physical topic
and canonical physical cluster when present, and a sorted tuple of every exact
generated interceptor belonging to the logical rule.

Each observed interceptor carries its `(scope, name)`, plugin class, priority,
and recursively immutable complete configuration. Constructors enforce sorted
unique identities and the alias/interceptor presence invariants. Mutable
dictionaries and lists are not retained by reference. Repr and presentation
objects omit raw configuration.

The canonical live fingerprint hashes canonical JSON containing:

- the versioned backend identity;
- effective vCluster;
- alias key and presence;
- physical topic and canonical physical cluster when present; and
- every sorted interceptor identity, plugin class, priority, scope, and complete
  configuration.

The fingerprint changes for binding, scope, presence, physical topic,
physical-cluster, interceptor identity, plugin class, priority, or exact
configuration drift. It is stable across response order and fields explicitly
outside the managed contract. Extra as well as missing exact rule interceptors
change the aggregate.

## Stage 6: collision gates

Component status: complete. Desired artifacts and prior state are checked before
provider access by the online and offline planning paths.

Before any provider request, parse and bind every compiled Gateway artifact and
reject:

- one logical owner resolving to more than one rule;
- more than one rule resolving to the same bound `(scope, alias name)`;
- duplicate desired interceptor `(scope, name)` identities;
- one generated interceptor name mapping to more than one declared rule;
- malformed generated namespaces that make exact ownership ambiguous; and
- duplicate physical claims in prior state for the same canonical backend and
  alias key.

Collision checks operate on complete exact identities, not display labels or
prefixes. Rules named `orders` and `orders_archive` are independent; neither can
observe, update, delete, or roll back the other's interceptors. `physicalCluster`
does not partition AliasTopic identity: differing physical-cluster content can
never make two declarations for one `(scope, alias name)` non-colliding, and any
non-`main` live value is unsupported evidence rather than another eligible
target.

## Stage 7: planner, status, and state projection

Component status: planner integration, canonical state projection, normalized
change evidence, and reviewed-plan/CLI presentation are complete. Status still
requires migration to the shared strict snapshot.

Online planning obtains one reusable strict snapshot for the complete Gateway
rule set, derives the exact current aggregate for each rule, and compares it
with the pure desired aggregate. Offline planning uses the same desired and
collision boundaries without provider access. Planning returns:

- `create` only for a cleanly absent alias with no orphaned exact interceptors;
- `none` only for exact managed equality; or
- `update` with secret-neutral categories and whole-surface checksums for every
  other complete observation.

Partial or ambiguous evidence raises an error. Normal plans carry the normalized
change model and fingerprint-only current/desired evidence; reviewed plans and
CLI presentation do not serialize raw provider configuration. Synthetic
`AliasTopicState` plus a separate optional interceptor list is not sufficient.

Status consumes the same snapshot and reports observed alias mapping, scope,
canonical physical cluster, binding fingerprint, managed interceptor count, and
drift categories. It does not issue one global list request per desired
interceptor, infer names that the compiler did not emit, or print raw
configuration.

Desired-state projection persists:

- logical resource kind `gateway_rule`;
- provider physical name equal to the alias key, never the logical rule name;
- canonical versioned Gateway backend including effective vCluster; and
- the checksum of the same strict bound artifact used by normal planning.

An existing generic `conduktor-gateway` record is legacy and unbound. Planner,
apply, recovery, and later adoption fail with a state mismatch until a separate
exact migration or explicit re-adoption path replaces it. They never rewrite it
automatically.

## Stage 8: exact apply, delete, and rollback

Component status: in progress. The exact managed mutation core is the active
work item. Planner apply/delete/rollback routing and complete rollback behavior
remain pending, so this stage is not complete.

Mutation APIs accept the exact desired/current aggregate or an equally strict
locator object. A bare logical rule name is not enough to mutate Gateway.

Apply:

- validates and constructs the entire desired aggregate before its first write;
- performs no write for an exact no-op;
- upserts the alias using the exact alias key, scope, physical topic, and
  canonical `physicalCluster: main`;
- upserts only the exact desired interceptor identities and content;
- deletes only exact prior managed interceptor identities absent from desired;
- treats unsupported declarations as errors rather than warnings; and
- does not blindly retry PUT or DELETE after an ambiguous server failure.

Delete carries the logical rule name for ownership reporting and the alias key,
scope, and exact interceptor identities for provider mutation. It never deletes
an alias under the logical name and never scans by prefix. HTTP 200, 204, and
documented absence are distinguished correctly.

### Gateway 3.15 provider evidence

Live mutation probes established the following exact provider behavior. This is
frozen evidence for Stage 8, not a claim that exact mutation, planner routing,
or rollback is complete:

- `PUT` returns HTTP 200 and the exact object `{resource, upsertResult}`;
  `upsertResult` is the string `Created`, `Updated`, or `NotChanged`.
- A passthrough AliasTopic response omits `metadata.vCluster` and includes
  `spec.physicalCluster: main`.
- An Interceptor response null-fills its scope and adds an empty comment.
- Interceptor deletion with the exact full null-filled passthrough scope body
  returned 204, then 404 when repeated. A name-only request is not an adequate
  scoped delete contract.
- AliasTopic deletion with explicit `vCluster: passthrough` returned 204, then
  404 when repeated.
- Both provider collections were empty after cleanup; no probe residue remained.

Sent writes require an exact result match: `NotChanged`, or any other
`upsertResult` inconsistent with the planned operation, is not success. A 404
while deleting a target that the reviewed snapshot proved present is
concurrency drift, not silent idempotent success. Mutation must not blindly
retry either case.

Rollback records exact resources successfully created by the current apply and
reverses only those resources. It cannot rediscover rollback candidates through
a prefix scan. Partial rollback reports each exact unresolved identity without
provider configuration or credentials.

## Stage 9: reviewed recovery

Component status: pending and explicitly fail closed. Until this stage consumes
the normalized snapshot and aggregate, Gateway recovery refuses to finalize an
operation from legacy, status-only, or partial evidence.

Reviewed recovery replans through the strict snapshot and uses the immutable
aggregate. It can finalize a started create or update only when the fresh
binding, scope, alias identity, and complete managed content prove the exact
prior or candidate bound artifact checksum. Allowed absence must also come from
a complete two-list snapshot.

Recovery fails closed for:

- a legacy unbound backend;
- endpoint or vCluster drift;
- alias identity mismatch;
- a malformed or non-`main` physical cluster;
- missing, extra, duplicate, malformed, or wrong-scope interceptor evidence;
- an orphan interceptor with an absent alias;
- content that cannot reconstruct the exact prior or candidate checksum; and
- operation, state, or control conflicts.

Local and PostgreSQL v2 reviewed-recovery gates must pass for the alias-only
surface before Gateway adoption is added to `_SUPPORTED_ACTIONS` or the CLI.

## Stage 10: alias-only adoption

Only after Stages 1 through 9 are complete may `streamt adopt --kind
gateway_rule` be exposed. The initial release accepts exactly one strict bound
artifact whose desired interceptor list is empty and whose ownership mode is
`adopted`.

The first observation must prove:

- the exact compiled logical owner and project;
- one present alias at the exact backend and effective scope;
- an exact alias key, physical topic, and canonical physical cluster `main`; and
- zero exact generated interceptors belonging to the rule, with no ambiguous or
  orphan evidence.

The review contains the logical resource URI, effective vCluster, endpoint
fingerprint, alias key, canonical physical cluster, observed and desired mapping
checksums, desired artifact checksum, and secret-neutral pending-change
categories. The mapping checksums cover both the physical topic and canonical
physical cluster. The review contains no raw interceptor configuration or
endpoint.

After exact confirmation, the command repeats the complete two-list observation
and requires the aggregate fingerprint to be unchanged. It then uses the
existing locked state-only intent and compare-and-swap protocol. Four provider
requests are expected for a successful adoption: two GET lists before
confirmation and the same two GET lists afterward. No mutation endpoint is
reachable. An identical canonical state claim remains idempotent after one
complete two-list observation without confirmation or a state write.

Full interceptor-rule adoption remains unsupported after this stage. It needs a
separate release decision backed by proven declaration-to-provider round-trip
equivalence for every supported plugin.

## Test and release matrix

| Gate | Minimum evidence | Primary locations |
| --- | --- | --- |
| Strict parser and binding | Exact artifact shapes; endpoint normalization; `passthrough` equivalence; other scopes distinct; missing/exact-`main` physical-cluster normalization; invalid/secret-bearing endpoints rejected without echo | `tests/unit/test_gateway_runtime_foundation.py` |
| Desired aggregate | Deterministic alias/interceptor output including `physicalCluster: main`; exact types and case; anchored generated-name parsing; only zero interceptors or one compiler-emitted filter accepted; every mask plus legacy encrypt/readonly rejected; input immutability | `tests/unit/test_gateway_runtime_foundation.py` |
| Strict live snapshot | Exactly two GETs; no redirect/retry/mutation; bounded duplicate-safe JSON; exact scope/name; username-only Gateway 3.15 scope normalization with null fillers; missing/exact-`main` equivalence; non-`main`, duplicate, malformed, orphan, and wrong-scope failures; stable order-independent fingerprint | `tests/unit/test_gateway_runtime_foundation.py` |
| Collision and planning | Duplicate owner, alias, interceptor, and generated namespace rejection before provider access; physical cluster cannot split alias identity; exact create/update/no-op; secret-neutral reviewed plans | `tests/unit/test_planner_gateway_artifacts.py`, `tests/unit/test_planner_ownership.py`, `tests/unit/test_deployment_state.py` |
| Status and mutation | Status uses one strict snapshot; observed mapping is reported; no-op writes nothing; logical name differs from alias; overlapping names cannot cross-delete; scoped delete uses an exact request body; exact rollback | `tests/unit/test_status_command.py`, `tests/unit/test_gateway_gaps.py`, `tests/unit/test_planner_gaps.py` |
| Recovery | Exact prior, exact candidate, allowed absence, legacy backend, endpoint/scope drift, physical-cluster drift/rejection, malformed/extra evidence, and operation/control conflicts | `tests/unit/test_recovery_observer.py`, `tests/unit/test_cli_state_recovery.py` |
| Local alias-only command | Exact selection; nonempty desired rejection; canonical `main` proof; two observations; zero mutation; drift; idempotency; state collision/CAS; planner-record equality; secret-neutral output | `tests/unit/test_cli_adopt_gateway.py` |
| Real Gateway | Gateway 3.15 list shapes; exact default-scope alias observation; real missing/explicit physical-cluster shape; two GET-only snapshots; absence; no mutation; explicit Gateway readiness | `tests/integration/test_gateway_e2e.py`, `tests/integration/helpers/gateway.py`, `tests/integration/helpers/docker.py` |
| PostgreSQL v2 | Production factory and writer; finalized `adopt` history; no local state; exact Gateway backend; two observations; exact reviewed recovery | `tests/postgres/test_postgres_ordinary_factory_commands_real.py`, `tests/postgres/test_postgres_recovery_commands_real.py` |
| Installed wheel | Gateway kind appears in isolated CLI; PostgreSQL command tests execute from the isolated wheel on supported PostgreSQL majors | `.github/workflows/ci.yml` |

The real Gateway release job may run the narrow strict-observer case rather than
the entire Gateway semantic suite, but it must start and explicitly wait for the
Gateway service. Kafka health alone is not Gateway readiness.

## Completion checklist

Package 6 remains in progress until all of these end-to-end integration gates
are checked. Shipped foundation-component status above does not by itself check
an integration gate:

- [ ] Strict artifact parsing is shared by offline and live paths.
- [ ] Versioned endpoint and effective-vCluster binding is persisted everywhere.
- [ ] Default `passthrough` normalization and distinct other scopes are tested.
- [ ] Missing/exact `main` physical clusters normalize identically and every
      other physical-cluster value fails closed.
- [ ] Pure desired aggregate is shared by plan and apply.
- [ ] The bounded two-list GET-only snapshot and immutable fingerprint are live.
- [ ] Exact anchored interceptor ownership replaces every prefix scan.
- [ ] All compiled and prior-state collision gates run before provider access.
- [ ] Planner and status use complete aggregate evidence.
- [ ] State projection rejects legacy unbound authority.
- [ ] Apply, delete, and rollback use exact alias and interceptor identities.
- [ ] Local and PostgreSQL v2 recovery use the same aggregate and pass.
- [ ] Unit, typing, lint, strict docs, and focused real Gateway gates pass.

Package 7 remains planned until Package 6 is complete. Gateway adoption remains
unsupported until the additional alias-only command, installed-wheel, local
state, PostgreSQL v2, and real Gateway gates pass.
