# Backstage Software Catalog export

Status: implemented and supported for the exact offline core-entity export
boundary defined here. Backstage publication, DataHub export, and Conduktor
Console publication remain unsupported.

This specification defines a pure, offline projection of one compiled streamt
project and effective environment into core Backstage Software Catalog entities.
It deliberately separates a private neutral catalog projection from the first
Backstage adapter. Backstage entity YAML is an output format, not streamt's
canonical catalog model.

The first slice has four goals:

1. describe only facts streamt can prove from one successful compilation;
2. produce deterministic, portable `System`, `Resource`, and `Component`
   entities accepted by the pinned Backstage schema set;
3. fail closed on ambiguous identity, ownership, relationships, or invalid
   metadata; and
4. operate without deployment state, provider clients, credentials, or network
   access.

## Normative language

`MUST`, `MUST NOT`, `SHOULD`, and `MAY` are normative. An implementation that
cannot satisfy a `MUST` must stop with a structured error. It must not omit,
merge, normalize, or invent a replacement fact merely to produce output.

## Scope and non-claims

The command exports a static catalog descriptor. It does not:

- publish to, authenticate with, or query a Backstage instance;
- prove that a Kafka topic, Gateway alias, process, owner, domain, or cluster
  entity exists in any live system;
- prove deployment, health, freshness, lineage execution, or ownership state;
- import Backstage metadata into streamt;
- expose deployment state, reviewed plans, recovery evidence, or provider
  observations; or
- define a bidirectional or continuously synchronized catalog.

A `Component` means that compilation produced a deployable process shape. It is
not evidence that a process is running. A `Resource` means that compilation
identified a dataset. It is not a deployment status record.

The first slice uses only Backstage's core entity kinds. It does not emit custom
`DataAsset` kinds and must not describe a Kafka dataset as a deployable
`Component`.

## Command contract

The command is:

```text
streamt docs backstage \
  --catalog-id <STABLE-ID> \
  --catalog-namespace <NAMESPACE> \
  --default-owner-ref <FULL-GROUP-OR-USER-REF> \
  --lifecycle <BACKSTAGE-LIFECYCLE> \
  [--owner-map <STRICT-JSON-FILE>] \
  [--kafka-cluster-ref <FULL-RESOURCE-REF>] \
  [--gateway-cluster-ref <FULL-RESOURCE-REF>] \
  [--domain-ref <FULL-DOMAIN-REF>] \
  [--output-file <PATH>] \
  [--project-dir <PATH>] \
  [--env <ENVIRONMENT>]
```

`--catalog-id`, `--catalog-namespace`, `--default-owner-ref`, and
`--lifecycle` are semantically required. The Click layer may leave them
syntactically optional so failures use the normal structured streamt error
envelope, but no compilation or file write may begin until they validate.

`--catalog-id` is a stable deployment-independent identity chosen by the user.
It MUST match:

```text
^[a-z0-9](?:[a-z0-9._-]{0,126}[a-z0-9])?$
```

Changing it intentionally changes every generated entity identity.
`--catalog-namespace` MUST be an explicit lowercase Backstage namespace. It is
used for every generated entity. The exporter never uses Backstage's implicit
`default` namespace.

`--lifecycle` is copied exactly to every generated processing Component. It MUST
contain 1 to 256 Unicode code points, include at least one non-whitespace
character, and contain no Unicode control or surrogate code point. Environment
is not lifecycle. The exporter must not
derive lifecycle from an environment name, project name, ownership mode, or
deployment configuration.

`--kafka-cluster-ref` becomes semantically required if the projection contains
any source or Kafka model-output Resource. `--gateway-cluster-ref` becomes
semantically required if it contains any Gateway virtual-topic Resource. A
superfluous valid cluster reference MAY be accepted but MUST NOT create an
entity or relationship. `--domain-ref` is optional and applies only to the
generated System.

The command uses the repository's standard `--project-dir`, environment
selection, `--quiet`, and global output-mode behavior. If an environments
directory is present, environment selection follows the existing compiler
rules. The effective environment is the selected environment name; otherwise
it is the canonical string `default`. That exact value participates in entity
identity and is emitted as metadata.

The exporter runs `Compiler.compile(dry_run=True)` exactly once. It MUST NOT
construct a state service or a Kafka, Schema Registry, Flink, Connect, Gateway,
Backstage, or other provider client. It MUST NOT perform DNS, HTTP, socket, or
subprocess-based network access.

### Full external entity references

All user-provided references MUST be complete, three-part Backstage references.
Implicit kinds and namespaces are forbidden:

- owners: `group:<namespace>/<name>` or `user:<namespace>/<name>`;
- Kafka and Gateway clusters: `resource:<namespace>/<name>`; and
- domain: `domain:<namespace>/<name>`.

Kinds in input references MUST be lowercase exactly as shown. Namespace and
name components MUST pass the pinned Backstage validation rules. The complete
reference, including namespace and name, MUST already be lowercase canonical
text; the exporter never case-folds it on the caller's behalf. Whitespace,
fragments, query strings, empty components, uppercase characters, and alternate
kinds are rejected. The exporter validates reference syntax only. Offline export
cannot prove that an externally referenced entity exists or that the caller may
use it.

### Pinned field formats

The official kind schemas intentionally leave several catalog policy formats
open. This exporter freezes the corresponding Backstage `v1.54.2` field policy
for portable offline output:

- an entity name is at most 63 characters and matches
  `^[A-Za-z0-9](?:[-_.]?[A-Za-z0-9])*$`;
- the explicit catalog namespace and every external-reference namespace are at
  most 63 characters and match `^[a-z0-9](?:-?[a-z0-9])*$`;
- every external-reference name is at most 63 characters and matches
  `^[a-z0-9](?:[-_.]?[a-z0-9])*$`;
- a tag is at most 63 characters and matches
  `^[a-z0-9:+#]+(?:-[a-z0-9:+#]+)*$`; and
- exporter annotation keys are the exact allowlist in this specification.
  Annotation values must be strings containing no Unicode control or surrogate
  code point.

Lengths are measured in Unicode code points. These checks are local semantic
validation in addition to the pinned JSON Schemas.

### Owner resolution

streamt owner labels are human labels, not Backstage entity references. They
MUST pass through an explicit mapping before export. The optional owner-map file
is strict JSON with this exact shape:

```json
{
  "version": 1,
  "owners": {
    "payments-platform": "group:platform/payments"
  }
}
```

Both top-level keys are required and no additional key is allowed. `version`
MUST be the integer `1`. `owners` MUST be an object from distinct, non-empty
streamt owner labels to full owner references. A parser that preserves duplicate
JSON keys is required; duplicate keys at either level are errors. Empty labels,
empty references, non-string values, invalid Unicode, excessive file size or
nesting, and invalid references are errors. The UTF-8 file is limited to
1,048,576 bytes, JSON nesting depth to 4, the `owners` object to 10,000 entries,
each owner label to 256 Unicode code points, and each reference to 256 Unicode
code points. Every limit is inclusive. A byte-order mark and unpaired Unicode
surrogate are invalid.

Resolution is exact and case-sensitive:

- a declaration with an owner MUST have that exact label in `owners`;
- a declared but unmapped owner MUST fail and MUST NOT fall back;
- a declaration without an owner uses `--default-owner-ref`; and
- the generated System uses `--default-owner-ref`.

Unused valid mappings are allowed and have no output effect. The exporter MUST
NOT synthesize `Group` or `User` entities and MUST NOT treat deployment
`ownership.mode` as a human owner.

## Neutral projection boundary

The compiler output is first converted to one private, immutable, versioned
neutral snapshot. Suggested internal record names are `CatalogSnapshot`,
`CatalogDataset`, `CatalogProcess`, `CatalogDependency`, `CatalogOwnerLabel`,
and `CatalogContractSummary`; these names are not public API.

The neutral projection MUST:

- use frozen records, tuples, and immutable mapping views;
- defensively copy every accepted nested value before freezing it;
- reject duplicate logical identities, duplicate physical identities,
  inconsistent process/output shapes, missing dependency targets, and unknown
  variants;
- have no public `to_dict` or persistence contract in the first slice;
- contain no Backstage-specific kind, namespace, reference, annotation, or
  entity-name value; and
- be the sole semantic input to the Backstage adapter.

It is built from the safe declared fields on `StreamtProject`, the exact direct
dependencies in `ResolvedModel.dependencies`, and the compiler's
`CompiledModelView` projection. This preserves macro-resolved direct dependency
edges and the actual materialization shape, including Gateway fallback. The
builder MUST NOT use `Manifest.to_dict()`, raw or compiled SQL, macro source,
connector configuration, or runtime configuration as a convenient projection.

The neutral snapshot contains only:

- project name and optional non-blank project description;
- the effective environment;
- source and model logical names, optional non-blank descriptions, validated
  tags, and optional human owner labels;
- physical Kafka topic names or Gateway alias names already established by the
  compiled view;
- dataset transport kind (`kafka` or `gateway`);
- process kind (`flink`, `gateway`, or `connect`) when compilation produced one;
- exact direct logical dataset dependencies;
- whether a model contract is absent, declared, or enforced;
- enough typed identity to distinguish sources, model outputs, and processes;
  and
- each exposure's exact logical name, solely to emit one bounded omission
  warning per declaration. Duplicate exposure declarations remain separate
  omissions and MUST NOT make projection fail.

It contains no other exposure field, clock value, UUID, source path,
environment-variable value, endpoint, credential, provider identifier, or
deployment evidence. Any new field requires a privacy review and an update to
this specification before it can affect an adapter.

## Backstage entity mapping

The adapter emits only `backstage.io/v1alpha1` entities and only these core
kinds:

| streamt fact | Backstage entity | `spec.type` | generated System |
| --- | --- | --- | --- |
| project plus effective environment | one `System` | omitted | n/a |
| source topic | `Resource` | `kafka-topic` | omitted |
| Kafka model output | `Resource` | `kafka-topic` | included |
| Gateway virtual-topic output | `Resource` | `kafka-virtual-topic` | included |
| model with `process_kind` | `Component` | `data-pipeline` | included |
| topic model with no process | no Component | n/a | output Resource only |
| sink model | `Component` | `data-pipeline` | included; no output Resource |
| exposure | omitted with warning | n/a | n/a |

A source is not assigned to the generated System because streamt describes it
as an input and does not prove that this project owns its organizational system.
A model output is assigned to the System even when the topic model has no
processing Component.

A sink produces a Component because the compiled view proves a Connect process.
It produces no external destination Resource because streamt does not expose a
normalized, provider-independent destination identity. Each such omission emits
`W113_BACKSTAGE_SINK_OUTPUT_OMITTED`.

Exposures have no safe core-entity mapping in this slice. The exporter emits no
fake Component, API, or Resource for them. Each omitted exposure emits
`W114_BACKSTAGE_EXPOSURE_OMITTED`.

### Common envelope and metadata

Every entity has exactly the root keys `apiVersion`, `kind`, `metadata`, and
`spec`, in that order. The exporter MUST NOT emit `relations`, `status`, `uid`,
or `etag`; those are generated or managed by Backstage.

`metadata` fields, when applicable, are ordered:

1. `name`;
2. `namespace`;
3. `title`;
4. `description`;
5. `annotations`; and
6. `tags`.

`name` is the generated identity described below. `namespace` is the exact
validated `--catalog-namespace`. `title` is the exact project, source, or model
logical name. A present non-blank declared description is copied exactly;
absent or blank descriptions are omitted. Descriptions are metadata and may be
sensitive, so the sink/exposure warning in this specification applies to the
complete exported file, not only to credentials.

Declared streamt tags are copied only after each exact string passes Backstage
tag validation. Duplicates are errors. Valid tags are sorted by Unicode code
point for deterministic output. A tag is never case-folded, truncated, slugged,
or silently dropped.

Every entity has these exact annotations:

```yaml
# streamt:skip -- Backstage annotation fragment, not streamt project YAML
streamt.dev/catalog-id: <catalog-id>
streamt.dev/project: <exact project name>
streamt.dev/environment: <effective environment>
streamt.dev/logical-kind: <project|source|model>
streamt.dev/logical-name: <exact logical name>
```

For the System, `logical-kind` is `project` and `logical-name` is the exact
project name. Resource entities additionally include
`streamt.dev/physical-name` with the compiled physical topic or alias name.
Component entities include `streamt.dev/process-kind` with the exact compiled
kind.

Model-output Resources include `streamt.dev/contract` only when a Model contract
exists. Its only permitted values are `declared` when `contract.enforced` is
false and `enforced` when it is true. The annotation is a summary, not the
contract body and not proof of provider enforcement. Source schemas are not
Model contracts and do not create this annotation. No independent `API`, schema,
or contract entity is emitted.

No exporter annotation may use the reserved `backstage.io` prefix. Optional
metadata containers MUST be omitted when empty; the output MUST contain no null
optional value.

### System

The single System has:

```yaml
# streamt:skip -- Backstage System fragment, not streamt project YAML
spec:
  owner: <default-owner-ref>
  domain: <domain-ref>  # only when explicitly supplied
```

The exporter does not invent a System type. `domain` is omitted when no
`--domain-ref` was supplied.

### Resources

Every Resource has:

```yaml
# streamt:skip -- Backstage Resource fragment, not streamt project YAML
spec:
  type: <kafka-topic|kafka-virtual-topic>
  owner: <resolved-owner-ref>
  system: <full-generated-system-ref>  # model outputs only
  dependsOn: [<full refs>]              # when non-empty
```

The exact Kafka or Gateway cluster Resource reference is always included in
`dependsOn`. A model-output Resource with a compiled process additionally
depends on its producing Component. Those references are sorted by the
canonical full-ref ordering defined below. A topic model without a process has
no producer dependency: streamt must not invent a data-producing process.

### Components

Every Component has:

```yaml
# streamt:skip -- Backstage Component fragment, not streamt project YAML
spec:
  type: data-pipeline
  lifecycle: <explicit lifecycle>
  owner: <resolved-owner-ref>
  system: <full-generated-system-ref>
  dependsOn: [<direct input Resource refs>]  # when non-empty
```

`dependsOn` contains exactly the Resources for the model's direct logical
inputs as resolved by the compiler. It does not contain transitive inputs,
consumer edges, exposures, or the Component's own output. Gateway physical
input and Connect input facts must resolve through the same neutral dependency
mapping; they are not independently guessed from provider configuration.

The exporter relies on Backstage to derive reverse `dependencyOf`, `ownedBy`,
and `partOf` relations from these descriptor fields. It MUST NOT duplicate
derived relations at the entity root.

The resulting direct graph is:

```text
model output Resource -> producing Component -> direct input Resource(s)
                      \-> external cluster Resource
```

The System relationship is expressed by `spec.system`; ownership is expressed
by `spec.owner`.

## Deterministic identity

Generated names are content-addressed from explicit semantic identities, not
from paths, declaration order, timestamps, or runtime endpoints.

### Canonical seed encoding

Each seed is an array encoded as UTF-8 JSON with:

- `ensure_ascii=False`;
- separators `(',', ':')`;
- no whitespace or trailing newline; and
- strings exactly as validated, without case-folding or Unicode normalization.

The digest is SHA-256 over those exact bytes. The suffix is the first 16
lowercase hexadecimal digest characters. Seed arrays are:

```text
["streamt-backstage-v1","system",catalog_id,environment]
["streamt-backstage-v1","component",catalog_id,environment,"model",model_name]
["streamt-backstage-v1","resource",catalog_id,environment,
 resource_type,cluster_ref,physical_name]
```

The Resource identity is physical: it binds transport kind, the explicit
external cluster entity, and the compiled physical topic or alias name. Logical
names remain titles and annotations. If two logical declarations resolve to the
same physical Resource identity, export fails; the adapter MUST NOT merge them.

### Readable prefix

The readable stem is derived only for display. Its input is frozen by entity
kind: the System uses `catalog_id`, a Component uses its model logical name, and
a Resource uses its physical topic or alias name. No other display field may
affect `metadata.name`.

The stem algorithm is:

1. apply Unicode NFKD to the logical display value;
2. encode ASCII while dropping non-ASCII code points;
3. lowercase;
4. replace each maximal run outside `[a-z0-9]` with one hyphen;
5. strip leading and trailing hyphens; and
6. use `item` if the result is empty.

Names have these forms:

```text
system-<stem>-<digest16>
model-<stem>-<digest16>
topic-<stem>-<digest16>
virtual-topic-<stem>-<digest16>
```

The stem is truncated from the right so the complete name is at most 63
characters; a trailing hyphen left by truncation is removed. The digest is
never truncated. The final name MUST pass Backstage name validation.

Changing a project display name does not change System identity because the
stable catalog ID is the System seed. Changing a model logical name changes its
Component identity. Changing environment, physical resource identity, resource
type, or cluster ref changes the affected identity by design.

All generated full references use lowercase canonical kinds and the explicit
namespace:

```text
system:<namespace>/<name>
component:<namespace>/<name>
resource:<namespace>/<name>
```

Backstage entity identity is case-insensitive. The adapter MUST reject any
duplicate `(kind, namespace, name)` after case-folding, including an injected or
otherwise unexpected digest collision. It must never resolve a collision by
suffix increment, declaration order, or merge.

References inside each collection are sorted by `(kind, namespace, name)` after
case-folding, with original strings as deterministic tie-breakers.

## Validation

Validation has three required layers and all run before output replacement or
stdout success.

### Neutral semantic validation

The snapshot builder validates:

- exactly one project and effective environment;
- complete one-to-one coverage of compiled models;
- only known materialization and process/output shapes;
- unique logical source/model identities;
- unique physical Resource identities;
- exact existing direct dependency targets;
- absence of self-dependencies and cycles;
- required non-blank physical names; and
- copied, non-blank human owner labels when declarations provide them.

Compilation remains the authority for project semantics. The catalog builder
must not repair or reinterpret an invalid compiler result.

Complete resolution through `--default-owner-ref` and the owner map belongs to
the Backstage adapter and closed semantic validation, never to the neutral
projection.

### Backstage schema validation

The Python package vendors the exact JSON Schema closure required for core
entities from Backstage release `v1.54.2`, resolved at commit
`4bfa231152c6e454a2728850f63c6feb3d396191`:

```text
Entity.schema.json
EntityEnvelope.schema.json
EntityMeta.schema.json
shared/common.schema.json
kinds/System.v1alpha1.schema.json
kinds/Resource.v1alpha1.schema.json
kinds/Component.v1alpha1.schema.json
```

The source schemas are Apache-2.0. Their exact upstream path, commit, byte size,
and SHA-256 MUST be recorded alongside the vendored resources. The loader MUST
verify size and checksum before parsing. Resources MAY use the repository's
gzip/base64 vendoring convention, but validation is over the verified decoded
upstream bytes.

Validation uses a Draft 7 validator and a closed local schema registry. Unknown
or remote `$ref` targets are errors. Schema loading and entity validation MUST
work with outbound networking disabled and from an isolated installed wheel.
The exporter has no Node.js runtime dependency.

As a release parity test only, the emitted fixtures are also validated by the
entity validators from `@backstage/catalog-model@1.10.0`, the package version at
the pinned Backstage commit. The gate parses the emitted multi-document stream
with release-test-only `yaml@2.8.1`; neither Node package is a streamt runtime
dependency. This test does not replace the vendored offline runtime validator
and does not imply compatibility with untested Backstage versions.

### Closed semantic validation

Official schemas are necessary but not sufficient. The adapter also rejects:

- a root key outside the exact four-key envelope;
- any kind or API version outside this specification;
- `relations`, `status`, `uid`, `etag`, null values, or empty optional
  containers;
- an invalid name, namespace, tag, annotation key, or annotation value;
- a reserved exporter annotation prefix;
- an abbreviated, malformed, or wrong-kind entity reference;
- a duplicate case-insensitive generated entity identity;
- a duplicate physical Resource identity;
- a generated reference whose target is absent;
- a missing explicit owner, cluster, or conditionally required reference;
- a dependency other than an exact direct relationship permitted above;
- an unsorted entity, tag, dependency, annotation, or warning collection; and
- any secret-forbidden field or value identified by the acceptance fixtures.

Generated references must close within the emitted entity set. Explicit owner,
domain, Kafka cluster, and Gateway cluster references are the only permitted
external references and receive syntax and kind validation only.

All exporter validation failures use the stable code
`E507_BACKSTAGE_INVALID`. Human messages identify the option or safe logical
entity involved but MUST NOT include file contents, runtime configuration,
exception representations, credentials, endpoints, SQL, or connector
configuration.

## Canonical output

### YAML

Text mode emits a canonical UTF-8 YAML multi-document stream. Entity order is:

1. the System;
2. Resources sorted by `(namespace.casefold(), name.casefold(), name)`; and
3. Components using the same ordering.

Mapping key order is the exact order prescribed by this specification.
Annotation keys are sorted lexicographically. Every document begins with
`---\n`; documents have no `...` marker; the stream ends with one newline. The
serializer MUST emit no anchor, alias, merge key, Python tag, timestamp type, or
platform-specific newline. Parsing the output with a safe YAML loader must
recover exactly the validated entity objects.

Without `--output-file`, raw YAML is the only successful stdout content.
Warnings use stderr. With `--quiet`, successful stdout is empty unless the
repository-wide structured-output contract requires an envelope.

### Structured JSON

With global `--output json`, the command uses the standard streamt envelope.
Its `data` object has exactly:

```json
{
  "standard": "Backstage Software Catalog",
  "release": "1.54.2",
  "api_version": "backstage.io/v1alpha1",
  "entities": [],
  "counts": {
    "System": 1,
    "Resource": 0,
    "Component": 0
  },
  "output_file": null
}
```

`entities` contains the same validated objects in the same order as YAML.
`counts` always contains the three keys in the shown order. `output_file` is the
requested display path when a file was written and JSON null otherwise. Raw
YAML MUST NOT be printed before or after the JSON envelope. Structured warnings
use the repository's normal warning field and preserve deterministic ordering.

### Atomic files

`--output-file` always writes canonical YAML, even when stdout is structured
JSON. The implementation MUST:

1. fully build and validate the bytes in memory;
2. create a private temporary file in the destination directory;
3. write, flush, and `fsync` it;
4. atomically replace the exact destination; and
5. clean up the temporary file on every failure.

It MUST NOT truncate or partly replace an existing destination on compile,
validation, serialization, write, or replace failure. Symlink and path handling
must follow the repository's existing safe atomic-output policy. Creating a
missing parent directory is allowed only if that is already the documented
policy for `streamt docs` outputs.

## Warnings and omissions

Warnings are deterministic, sorted by safe logical location and then code, and
emitted once per omitted declaration:

| Code | Condition | Meaning |
| --- | --- | --- |
| `W113_BACKSTAGE_SINK_OUTPUT_OMITTED` | compiled sink model | process Component emitted, unidentifiable destination Resource omitted |
| `W114_BACKSTAGE_EXPOSURE_OMITTED` | declared exposure | no safe core Backstage entity mapping |

Neither warning authorizes exporting connector destination configuration or
exposure content. Warnings are not errors because the remaining projection is
truthful and complete within its declared scope.

The generated catalog file is an intentional metadata exposure and SHOULD be
treated as sensitive even after secret validation. It names projects,
environments, logical datasets, physical topics or aliases, owners, clusters,
descriptions, tags, and dependencies. Users must review the file and its sink
permissions before committing, uploading, or publishing it. `--output-file`
must not be treated as a trusted or secret-redacting sink.

## Privacy and secret neutrality

The exporter may emit only the fields enumerated in this specification. In
particular it MUST NOT emit or hash into visible output:

- runtime configuration, bootstrap servers, URLs, hosts, ports, vcluster
  endpoints, environment-variable names or values, credentials, tokens, or
  certificates;
- raw or compiled SQL, macro definitions, parameters, source locations, local
  paths, or exception representations, except that structured output reports
  the exact caller-supplied `--output-file` path as intentional command
  metadata;
- connector class, connector config, external sink destination, consumer
  credentials, or provider response content;
- schema bodies, schema subjects, columns, classifications, masking rules,
  security policies, assertions, test definitions, or sample data;
- exposure contents other than the exact logical name needed for its omission
  warning, consumer groups, access rules, SLAs, or inferred consumers;
- manifest artifacts, checksums, reviewed actions, deployment ownership,
  recovery history, state addresses, state revisions, or live observations; or
- generated timestamps, UUIDs, machine names, usernames, or process metadata.

The explicit external cluster references are intentional catalog metadata.
They MUST come only from their CLI options and never be derived from runtime
endpoints. Digests use only the identity seeds listed above; they are not a
mechanism for leaking otherwise forbidden values.

Fixtures containing sentinel secrets in every forbidden surface must prove the
sentinels are absent from YAML, JSON, warnings, error messages, generated
temporary filenames, and test snapshots. Exception paths receive the same
scan. A caller-chosen output path is outside that assertion because it is an
explicit input echoed by contract; the exporter must never derive any other
path from project content.

## Failure atomicity and determinism

Given identical project bytes, selected environment, adapter options, and
exporter version, successful YAML and structured entity arrays MUST be
byte-for-byte identical across invocations, platforms, source checkout, and
installed wheel. Declaration-map ordering, owner-map ordering, filesystem
enumeration order, Python hash randomization, and locale MUST NOT affect output.

Compilation or validation failure produces no success payload and leaves an
existing destination unchanged. A warning never changes entity identity. A
failure after temporary-file creation removes that file. There is no partial
catalog result mode.

## Acceptance matrix

The feature remains unsupported until automated tests prove all of the
following.

### Projection and mapping

- source-only, process-free topic model, Flink topic model, Flink
  materialization, Gateway virtual topic, and Connect sink shapes;
- Gateway primary compilation and allowed compiler fallback produce the exact
  compiled shape without adapter inference;
- macro-resolved dependencies become exact direct Component dependencies;
- output Resource to producer Component and cluster dependencies are exact;
- source Resources have no generated System, while model outputs and every
  Component do;
- no-process topic models produce no Component and no invented producer edge;
- sinks and exposures produce the exact deterministic warnings and no invented
  destination/API entity;
- contract absent, declared, and enforced states produce the exact annotation
  behavior; and
- nested mutation of source project/compiler values after projection cannot
  change the frozen snapshot or emitted entities.

### Identity and references

- catalog ID and namespace boundary validation;
- `default` and named environment identity separation;
- ASCII, Unicode, punctuation-only, mixed-case, and overlong display values;
- exact NFKD/ASCII slugging, truncation, canonical seed bytes, digest, and
  63-character limit;
- logical rename, project display rename, environment change, cluster-ref
  change, and physical-name change have the specified identity effects;
- case-insensitive generated identity collision fails, including a collision
  injected below the digest function;
- two logical declarations resolving to one physical Resource fail rather than
  merge;
- valid full Group/User/Resource/Domain refs and every abbreviated,
  wrong-kind, whitespace, empty, or malformed variant;
- declared-owner exact map success, unmapped declared-owner failure, no-owner
  default success, unused mapping neutrality, duplicate JSON-key rejection,
  strict shape, and bounded file limits; and
- generated reference closure plus external-reference syntax-only behavior.

### Validation and serialization

- exact decoded size and SHA-256 for every pinned schema resource;
- all schemas and refs load from both a source checkout and isolated wheel with
  networking disabled;
- representative valid entities pass the vendored Python validator and the
  pinned Node parity validator;
- malformed envelope, kind, API version, metadata, annotation, tag, lifecycle,
  spec field, reference, dependency, and empty/null-container cases fail;
- canonical ordering is unchanged by declaration, mapping, and filesystem
  reordering;
- repeated output is byte-identical and contains no clock or random value;
- YAML safe-load round-trip exactly matches the structured entity array;
- every document marker, final newline, mapping order, and no-anchor rule is
  asserted; and
- structured JSON contains no raw-YAML contamination and exact counts.

### Offline, secrecy, and files

- tests fail if state, provider, DNS, HTTP, socket, or subprocess network seams
  are touched;
- dry-run compilation happens exactly once;
- no opt-in environment or runtime setting can silently enable publication;
- comprehensive sentinel-secret scans cover success, warning, validation,
  serialization, and file-failure paths;
- file creation, replacement, permission, flush, `fsync`, and atomic-rename
  failures preserve the old destination and remove temporary files;
- the existing `streamt docs` commands and global text/JSON/quiet behavior are
  unchanged; and
- an installed-wheel CLI smoke test exports and validates canonical YAML with
  all network access denied.

## Staged extensions

The private neutral projection is intended to permit additional adapters, but
this specification authorizes only Backstage core export.

### DataHub

A future DataHub adapter may consume the neutral snapshot. It requires its own
normative identity/URN mapping, schema and lineage semantics, authentication,
API compatibility, idempotency, publication safety, and real integration gates.
Backstage entity refs and generated names must not enter the neutral model merely
to make that future adapter convenient.

### Conduktor Console

A future Conduktor Console adapter is deferred until an official, supported
metadata API and compatibility contract have been identified and verified.
This specification makes no claim that such an API exists and authorizes no
Console endpoint, payload, or authentication behavior.

### Backstage extensions

Potential later slices include separately specified AsyncAPI/ODCS-backed API or
contract entities, exposure mappings, supported custom DataAsset kinds, and an
authenticated catalog publication workflow. None is implied by this export.
Publication must remain a separate explicit operation with its own review,
authorization, idempotency, rollback, and sink-security contract.

## References

- [Backstage descriptor format](https://backstage.io/docs/features/software-catalog/descriptor-format/)
- [Backstage system model](https://backstage.io/docs/features/software-catalog/system-model/)
- [Backstage well-known relations](https://backstage.io/docs/features/software-catalog/well-known-relations/)
- [Pinned Backstage catalog-model package](https://github.com/backstage/backstage/tree/4bfa231152c6e454a2728850f63c6feb3d396191/packages/catalog-model)
- [Pinned Backstage catalog-model metadata](https://github.com/backstage/backstage/blob/4bfa231152c6e454a2728850f63c6feb3d396191/packages/catalog-model/package.json)
