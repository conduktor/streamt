# Product direction

## Status and authority

Revised on 2026-09-05 following the product owner's clarification: streamt's
purpose is to simplify the creation, deployment, and evolution of streaming
applications. This replaces the earlier review-first positioning. Deployment
is part of the product's value, even when an external backend executes it.

The product promise, SQL-first authoring, inclusion of custom applications, and
external/managed distinction are agreed. A Kafka-only starting point must be
addressed; the owner approved a bounded Kafka Streams backend on 2026-09-05.
Strimzi is not a product priority. The earlier authorization covered declaration-only external
resources, custom-application metadata, and a bounded Kafka Streams proof in the
[developer experience execution plan](../plans/2026-09-04-developer-experience-execution.md).
The [topology/runtime plan](../plans/2026-09-05-topology-runtime-execution.md)
now governs implementation; the [support matrix](../reference/support-matrix.md)
governs claims about what works today.

## Product promise

The owner reaffirmed on 2026-09-05 that streamt must absorb an existing topology
or start a new one and maintain coherence through development and infrastructure
evolution. The approved [runtime implementation cycle](../plans/2026-09-05-topology-runtime-execution.md)
must prove both entry paths and their dependency/contract checks, not just a
working SQL runner.

streamt is a framework for developing, testing, deploying, and evolving
streaming data applications as versioned projects.

A project brings together transformation logic, resource configuration,
contracts, tests, and dependencies. Engineers can change the application through
one workflow instead of maintaining deployment scripts for each component.

The first authoring path is SQL, but the project must also represent custom
streaming applications and their inputs, outputs, and owners. Users include
data engineers and application developers. Platform teams provide environments,
deployment permissions, and shared rules. Representation, deployment of an
existing artifact, and compilation of application code are separate capabilities.
Deployment of custom images/JARs and source builds is deferred.

## Problem

A pipeline that filters orders and sends the result to a destination needs more
than its business SQL. Someone must connect topics, schemas, processing jobs,
connectors, credentials, environments, tests, and deployment procedures. Later,
another engineer needs to discover those relationships before changing it.

streamt should reduce this assembly work and preserve the application definition
as it changes. The intended benefits are:

- Less configuration and deployment code to write for a new pipeline.
- A repeatable way to test, review, deploy, and update an existing pipeline.
- Dependencies and ownership that remain discoverable alongside the code.

Git supplies version history and review. streamt supplies the application model,
compilation, validation, and supported deployment lifecycle. A useful diff alone
does not establish that the full development workflow works.

## Canonical workflow

1. Start a complete example, or describe a small part of existing infrastructure.
2. Write transformations and declare their inputs, outputs, and tests.
3. Validate the project and test the relevant behavior on controlled data.
4. Review the resource changes, dependencies, and runtime requirements.
5. Deploy through a supported backend and inspect actual output records.
6. Change the application, repeat the checks, and deploy the supported update.
7. Commit the definitions and reuse the workflow in another environment or CI.

Lineage, documentation, policy checks, and change-impact reports derive from
this same project. Catalog exports let other tools consume it. They support
the development workflow rather than define its first success criterion.

The [developer workflow specification](developer-workflow.md) defines the
first complete journey and its acceptance evidence. It describes planned work,
not additional supported commands or runtime guarantees.

## Project boundaries

There are two entry paths. A user can discover and import an existing system as
external declarations, or add definitions whose resources streamt will manage.
Both belong in the same dependency graph.

External means streamt does not own the implementation or resource lifecycle.
Import captures what a provider can actually report. Unknown SQL, source code,
or settings remain unknown; metadata discovery cannot reconstruct arbitrary
business logic. Import does not silently adopt a resource. Explicit adoption
requires a complete supported definition and the existing ownership checks.

External resources are declaration-only by default: local references remain
validated, but ordinary planning does not query their live state or drift.
`status --include-external` explicitly requests live observations. Managed
operations retain their own evidence and safety checks. See the
[ownership contract](deployment-safety-and-ownership.md#external-declaration-behavior)
for the command boundaries and shared-provider limitations.

An application or domain owns a coherent project. A monorepo is allowed, but
streamt must not require every team to move into one repository. Each managed
resource has one authoritative definition. External resources remain explicit.

Versioned dependencies between projects are a later capability. Until then,
external source and exposure declarations document the boundary; streamt must
not imply cross-repository discovery or deployment coordination.

Import, observation, adoption, and management remain distinct actions. A project
that describes two topics in a shared cluster must not claim or change the rest.

## Deployment and safety

Existing Flink SQL installations remain supported within their current limits.
A user with only Kafka can create managed projection/filter jobs through the
fixed Kafka Streams runner. SQL compilation is separate from execution; an
unsupported operation fails explicitly, without falling back to Flink or
claiming SQL equivalence between engines.

Docker is the first execution backend for this bounded runner: one container
per managed model on an explicit local daemon and network. Creation, no-op
repeat apply and one reviewed predicate-only replacement are implemented, with
explicit same-operation resume and read-only completion checks. Projection,
schema, identity, image and stateful changes, deletion and generic runner-failure
recovery remain blocked. Docker support does not extend to arbitrary images,
remote scheduling or deployment of custom application code.

Git-authored definitions and CI delivery do not imply a continuously reconciling
GitOps controller. An export is not an executed deployment. The current Strimzi
integration only exports managed `KafkaTopic` artifacts. Preserve that tested
support, but schedule no further Strimzi or Kubernetes work in this cycle.

Safety requirements survive this change in priority:

- Reject unknown configuration, uncertain ownership, and stale reviewed plans.
- Keep unrelated resources untouched and require explicit removal workflows.
- Preserve deployment-state locking, durable recovery, and secret redaction.
- Keep protected/shared-environment changes bound to their reviewed plan.
- Distinguish static facts, live observations, and missing evidence.
- Block unsupported Flink updates before mutation. A Git revert does not by
  itself restore streaming state, source offsets, or previously emitted data.

Even a projection-only SQL job has source progress and delivery behavior to
consider during replacement. Update support requires a backend-specific
contract and real execution tests; deleting state or recreating a cluster is
not an implementation of application updates.

## Integration choices

Prioritize an integration when it supplies an input, delivers an output, supports
the chosen deployment environment, or removes repeated setup from the reference
journey. First make one source-to-destination route work end to end.

Retain tested exports and existing integrations. Additional catalogs, transports,
clouds, and infrastructure backends wait until a concrete user workflow needs
them. Publication of an alpha has its own release gate and does not prevent
local onboarding work on an immutable repository revision.

## Out of scope for the next cycle

- An unbounded SQL engine or arbitrary custom-language build system. The approved
  fixed Kafka Streams runner keeps its typed projection/filter contract.
- Requiring Kubernetes, a catalog, or a commercial account to try the local path.
- A hosted service, visual pipeline editor, or new telemetry database.
- Mandatory monorepo migration or a cross-project package resolver.
- New cloud backends, Strimzi expansion, arbitrary connector coverage, or extra
  catalog publishers.
- Exactly-once, zero-downtime, or state-migration claims without target evidence.

The [Kafka Streams architecture decision](kafka-streams-execution-proof.md)
records the historical proof. The active topology/runtime plan authorizes its
bounded productization; material expansions require another product decision.

## Measures of success

Record time to first output and to the first verified update, manual steps,
prerequisite failures, and any intervention needed to finish the guide. Separate
cold installation/image downloads from runtime readiness and application work.
These are measurements to collect, not performance claims already achieved.

The first acceptance run must prove that an engineer can create a pipeline,
inspect its data, change its logic, and deploy the supported update without
editing generated artifacts or using provider consoles to finish missing steps.
The same project must explain its declared dependencies and pass the documented
checks from an installed distribution.

A handful of independent walkthroughs should then test whether this saves users
work on their own pipelines. The number of integrations or passing tests is not
a substitute for that product evidence.
