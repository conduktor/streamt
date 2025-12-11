# Test Cases Fonctionnels - streamt

Ce document décrit les scénarios de test pour valider l'implémentation de streamt.

---

## Organisation

Les tests sont organisés par commande CLI et par fonctionnalité:

1. **Parsing & Validation** (`streamt validate`)
2. **Compilation** (`streamt compile`)
3. **Planning** (`streamt plan`)
4. **Déploiement** (`streamt apply`)
5. **Tests de données** (`streamt test`)
6. **Lineage** (`streamt lineage`)
7. **Gouvernance** (Rules)
8. **Sécurité** (Masking, Access)
9. **Erreurs & Edge Cases**

---

## 1. Parsing & Validation

### TC-VAL-001: Projet minimal valide

**Given** un projet avec uniquement:
```yaml
# stream_project.yml
project:
  name: minimal-project

runtime:
  kafka:
    bootstrap_servers: localhost:9092
```

**When** `streamt validate`

**Then**
- Exit code: 0
- Output: "Project 'minimal-project' is valid"

---

### TC-VAL-002: Projet avec source valide

**Given**:
```yaml
# stream_project.yml
project:
  name: test-project
runtime:
  kafka:
    bootstrap_servers: localhost:9092

# sources/payments.yml
sources:
  - name: payments_raw
    topic: payments.raw.v1
```

**When** `streamt validate`

**Then**
- Exit code: 0
- Source 'payments_raw' reconnue

---

### TC-VAL-003: Source sans topic - Erreur

**Given**:
```yaml
sources:
  - name: payments_raw
    # topic manquant
```

**When** `streamt validate`

**Then**
- Exit code: 1
- Error: "Source 'payments_raw' missing required field 'topic'"

---

### TC-VAL-004: Model avec SQL inline

**Given**:
```yaml
models:
  - name: payments_clean
    sql: |
      SELECT * FROM {{ source("payments_raw") }}
```

**When** `streamt validate`

**Then**
- Exit code: 0
- Dépendance `payments_raw` détectée automatiquement

---

### TC-VAL-005: Model avec from explicite

**Given**:
```yaml
models:
  - name: payments_clean
    from:
      - source: payments_raw
    sql: |
      SELECT * FROM {{ source("payments_raw") }}
```

**When** `streamt validate`

**Then**
- Exit code: 0
- Dépendance validée (from == SQL)

---

### TC-VAL-006: Model from != SQL - Warning

**Given**:
```yaml
models:
  - name: payments_clean
    from:
      - source: payments_raw
      - source: customers_raw  # déclaré mais pas utilisé
    sql: |
      SELECT * FROM {{ source("payments_raw") }}
```

**When** `streamt validate`

**Then**
- Exit code: 0
- Warning: "Source 'customers_raw' declared in 'from' but not used in SQL"

---

### TC-VAL-007: Référence à source inexistante

**Given**:
```yaml
models:
  - name: payments_clean
    sql: |
      SELECT * FROM {{ source("nonexistent") }}
```

**When** `streamt validate`

**Then**
- Exit code: 1
- Error: "Source 'nonexistent' not found. Referenced in model 'payments_clean'"

---

### TC-VAL-008: Référence à model inexistant

**Given**:
```yaml
models:
  - name: payments_enriched
    sql: |
      SELECT * FROM {{ ref("payments_clean") }}
      # payments_clean n'existe pas
```

**When** `streamt validate`

**Then**
- Exit code: 1
- Error: "Model 'payments_clean' not found. Referenced in model 'payments_enriched'"

---

### TC-VAL-009: Cycle détecté dans le DAG

**Given**:
```yaml
models:
  - name: model_a
    sql: SELECT * FROM {{ ref("model_b") }}

  - name: model_b
    sql: SELECT * FROM {{ ref("model_a") }}
```

**When** `streamt validate`

**Then**
- Exit code: 1
- Error: "Cycle detected in DAG: model_a -> model_b -> model_a"

---

### TC-VAL-010: Variables d'environnement

**Given**:
```yaml
runtime:
  kafka:
    bootstrap_servers: ${KAFKA_BOOTSTRAP}
```

Et `KAFKA_BOOTSTRAP` n'est pas défini

**When** `streamt validate`

**Then**
- Exit code: 1
- Error: "Environment variable 'KAFKA_BOOTSTRAP' not set"

---

### TC-VAL-011: Variables d'environnement avec .env

**Given**:
```yaml
runtime:
  kafka:
    bootstrap_servers: ${KAFKA_BOOTSTRAP}
```

Et `.env`:
```
KAFKA_BOOTSTRAP=localhost:9092
```

**When** `streamt validate`

**Then**
- Exit code: 0

---

### TC-VAL-012: Fichier YAML unique (tout-en-un)

**Given** un seul fichier:
```yaml
# stream_project.yml
project:
  name: all-in-one

runtime:
  kafka:
    bootstrap_servers: localhost:9092

sources:
  - name: payments_raw
    topic: payments.raw.v1

models:
  - name: payments_clean
    sql: SELECT * FROM {{ source("payments_raw") }}

tests:
  - name: payments_test
    model: payments_clean
    type: schema
    assertions:
      - not_null:
          columns: [payment_id]
```

**When** `streamt validate`

**Then**
- Exit code: 0
- Tous les éléments reconnus

---

## 2. Compilation

### TC-COMP-001: Compilation model topic

**Given**:
```yaml
models:
  - name: payments_clean
    sql: |
      SELECT payment_id, amount
      FROM {{ source("payments_raw") }}
    advanced:
      topic:
        partitions: 12
        replication_factor: 3
```

**When** `streamt compile`

**Then**
- Fichier `generated/topics/payments_clean.json`:
```json
{
  "name": "payments_clean",
  "partitions": 12,
  "replication_factor": 3,
  "config": {}
}
```
- Fichier `generated/manifest.json` contient le model

---

### TC-COMP-002: Compilation model flink

**Given**:
```yaml
models:
  - name: customer_balance
    key: customer_id
    sql: |
      SELECT
        customer_id,
        SUM(amount) as total
      FROM {{ ref("payments_clean") }}
      GROUP BY customer_id, TUMBLE(event_time, INTERVAL '5' MINUTE)
```

**When** `streamt compile`

**Then**
- Fichier `generated/flink/customer_balance.sql` contient:
  - CREATE TABLE pour la source (payments_clean topic)
  - CREATE TABLE pour le sink (customer_balance topic)
  - INSERT INTO avec le SQL transformé

---

### TC-COMP-003: Compilation model sink (Connect)

**Given**:
```yaml
models:
  - name: payments_to_snowflake
    from:
      - ref: payments_clean
    advanced:
      sink:
        connector: snowflake-sink
        config:
          snowflake.database.name: ANALYTICS
          snowflake.schema.name: PUBLIC
          snowflake.url.name: ${SNOWFLAKE_URL}
```

**When** `streamt compile`

**Then**
- Fichier `generated/connect/payments_to_snowflake.json`:
```json
{
  "name": "payments_to_snowflake",
  "config": {
    "connector.class": "com.snowflake.kafka.connector.SnowflakeSinkConnector",
    "topics": "payments_clean",
    "snowflake.database.name": "ANALYTICS",
    "snowflake.schema.name": "PUBLIC",
    "snowflake.url.name": "${SNOWFLAKE_URL}"
  }
}
```
- Note: `${SNOWFLAKE_URL}` reste un placeholder (pas de secret)

---

### TC-COMP-004: Compilation model virtual_topic (Gateway)

**Given**:
```yaml
runtime:
  conduktor:
    gateway:
      url: http://gateway:8888

models:
  - name: payments_filtered
    sql: |
      SELECT * FROM {{ source("payments_raw") }}
      WHERE status = 'CAPTURED'
    advanced:
      materialized: virtual_topic
```

**When** `streamt compile`

**Then**
- Fichier `generated/gateway/payments_filtered.json` avec config Gateway

---

### TC-COMP-005: virtual_topic sans Gateway - Erreur

**Given**:
```yaml
runtime:
  kafka:
    bootstrap_servers: localhost:9092
  # pas de conduktor.gateway

models:
  - name: payments_filtered
    sql: SELECT * FROM {{ source("payments_raw") }}
    advanced:
      materialized: virtual_topic
```

**When** `streamt compile`

**Then**
- Exit code: 1
- Error: "Model 'payments_filtered' uses 'virtual_topic' but no Gateway configured"

---

### TC-COMP-006: Compilation avec masking (sink)

**Given**:
```yaml
models:
  - name: payments_clean
    sql: SELECT * FROM {{ source("payments_raw") }}
    security:
      policies:
        - mask:
            column: card_number
            method: partial
            for_roles: [support]
```

Sans Gateway configuré.

**When** `streamt compile`

**Then**
- Le SQL Flink du prochain model (ou sink) inclut la fonction de masking:
```sql
SELECT
  payment_id,
  MASK_PARTIAL(card_number) as card_number,
  ...
```

---

### TC-COMP-007: Compilation dry-run

**When** `streamt compile --dry-run`

**Then**
- Output affiche ce qui serait généré
- Aucun fichier créé dans `generated/`

---

### TC-COMP-008: Compilation avec output custom

**When** `streamt compile --output ./my-output/`

**Then**
- Fichiers générés dans `./my-output/` au lieu de `./generated/`

---

## 3. Planning

### TC-PLAN-001: Plan nouveau projet

**Given** un projet jamais déployé

**When** `streamt plan`

**Then**
- Output:
```
Plan: 3 to create, 0 to update, 0 to delete

+ topic: payments_clean (12 partitions)
+ flink_job: customer_balance
+ connector: payments_to_snowflake
```

---

### TC-PLAN-002: Plan avec changement de partitions

**Given**:
- Topic `payments_clean` existe avec 6 partitions
- Model demande 12 partitions

**When** `streamt plan`

**Then**
```
Plan: 0 to create, 1 to update, 0 to delete

~ topic: payments_clean
    partitions: 6 -> 12
```

---

### TC-PLAN-003: Plan sans changement

**Given** projet déjà déployé, aucune modification

**When** `streamt plan`

**Then**
```
Plan: 0 to create, 0 to update, 0 to delete

No changes detected.
```

---

### TC-PLAN-004: Plan avec model supprimé

**Given**:
- Model `old_model` existe dans le cluster
- Model supprimé du projet

**When** `streamt plan`

**Then**
```
Plan: 0 to create, 0 to update, 1 to delete

- topic: old_model
```

---

## 4. Déploiement

### TC-APPLY-001: Apply crée un topic

**Given** model `payments_clean` compilé

**When** `streamt apply`

**Then**
- Topic `payments_clean` créé dans Kafka
- Output: "Created topic 'payments_clean'"

---

### TC-APPLY-002: Apply soumet un job Flink

**Given** model `customer_balance` (flink) compilé

**When** `streamt apply`

**Then**
- Job soumis via Flink REST API
- Output: "Submitted Flink job 'customer_balance' (job_id: xxx)"

---

### TC-APPLY-003: Apply crée un connector

**Given** model `payments_to_snowflake` (sink) compilé

**When** `streamt apply`

**Then**
- Connector créé via Connect REST API
- Output: "Created connector 'payments_to_snowflake'"

---

### TC-APPLY-004: Apply avec --target

**Given** projet avec 5 models

**When** `streamt apply --target payments_clean`

**Then**
- Seul `payments_clean` et ses dépendances sont déployés

---

### TC-APPLY-005: Apply avec --select tag

**Given** models avec tags:
```yaml
models:
  - name: model_a
    tags: [payments]
  - name: model_b
    tags: [fraud]
```

**When** `streamt apply --select "tag:payments"`

**Then**
- Seul `model_a` est déployé

---

### TC-APPLY-006: Apply idempotent

**Given** topic `payments_clean` déjà existe avec même config

**When** `streamt apply`

**Then**
- Output: "Topic 'payments_clean' unchanged"
- Pas d'erreur

---

### TC-APPLY-007: Apply rollback on error

**Given** 3 models à déployer, le 2ème échoue

**When** `streamt apply`

**Then**
- Model 1: créé
- Model 2: erreur
- Model 3: non tenté
- Output suggère: "Run 'streamt apply' again to retry"

---

## 5. Tests de données

### TC-TEST-001: Test schema - succès

**Given**:
```yaml
tests:
  - name: payments_schema
    model: payments_clean
    type: schema
    assertions:
      - not_null:
          columns: [payment_id, amount]
      - accepted_types:
          payment_id: string
          amount: long
```

Et le schéma du topic correspond.

**When** `streamt test`

**Then**
- Exit code: 0
- Output: "PASS: payments_schema"

---

### TC-TEST-002: Test schema - échec

**Given** même test mais `payment_id` est nullable dans le schéma

**When** `streamt test`

**Then**
- Exit code: 1
- Output:
```
FAIL: payments_schema
  - not_null: column 'payment_id' is nullable in schema
```

---

### TC-TEST-003: Test sample - succès

**Given**:
```yaml
tests:
  - name: payments_sample
    model: payments_clean
    type: sample
    sample_size: 100
    assertions:
      - accepted_values:
          column: status
          values: ["AUTHORIZED", "CAPTURED"]
```

Et les 100 messages échantillonnés ont des statuts valides.

**When** `streamt test`

**Then**
- Exit code: 0
- Output: "PASS: payments_sample (100 messages sampled)"

---

### TC-TEST-004: Test sample - échec

**Given** même test mais certains messages ont `status: PENDING`

**When** `streamt test`

**Then**
- Exit code: 1
- Output:
```
FAIL: payments_sample
  - accepted_values: found 3 messages with invalid status
    Invalid values: ['PENDING']
```

---

### TC-TEST-005: Test continuous - déploiement

**Given**:
```yaml
tests:
  - name: payments_quality
    model: payments_clean
    type: continuous
    assertions:
      - unique_key:
          key: payment_id
          window: "15 minutes"
```

**When** `streamt test --deploy`

**Then**
- Job Flink de test déployé
- Output: "Deployed continuous test 'payments_quality'"

---

### TC-TEST-006: Test continuous sans Flink - Erreur

**Given** test `type: continuous` mais pas de Flink configuré

**When** `streamt test`

**Then**
- Exit code: 1
- Error: "Continuous test 'payments_quality' requires Flink. Configure runtime.flink"

---

### TC-TEST-007: Test avec DLQ action

**Given**:
```yaml
tests:
  - name: payments_quality
    model: payments_clean
    type: continuous
    on_failure:
      actions:
        - dlq:
            model: payments_clean_dlq

models:
  - name: payments_clean_dlq
    advanced:
      topic:
        name: dlq.payments.clean
```

**When** `streamt test --deploy`

**Then**
- Test configuré pour router les bad records vers `dlq.payments.clean`

---

### TC-TEST-008: Test filter par model

**When** `streamt test --model payments_clean`

**Then**
- Seuls les tests associés à `payments_clean` sont exécutés

---

### TC-TEST-009: Test filter par type

**When** `streamt test --type sample`

**Then**
- Seuls les tests de type `sample` sont exécutés

---

## 6. Lineage

### TC-LIN-001: Lineage complet

**Given** projet avec:
- source: payments_raw
- model: payments_clean (from: payments_raw)
- model: payments_enriched (from: payments_clean)
- exposure: fraud_service (consumes: payments_enriched)

**When** `streamt lineage`

**Then**
```
payments_raw (source)
    │
    ▼
payments_clean (model:topic)
    │
    ▼
payments_enriched (model:flink)
    │
    ▼
fraud_service (exposure:consumer)
```

---

### TC-LIN-002: Lineage focalisé sur un model

**When** `streamt lineage --model payments_enriched`

**Then**
- Affiche uniquement les ancêtres et descendants de `payments_enriched`

---

### TC-LIN-003: Lineage upstream only

**When** `streamt lineage --model payments_enriched --upstream`

**Then**
- Affiche uniquement les ancêtres (payments_raw, payments_clean)

---

### TC-LIN-004: Lineage downstream only

**When** `streamt lineage --model payments_clean --downstream`

**Then**
- Affiche uniquement les descendants (payments_enriched, fraud_service)

---

### TC-LIN-005: Lineage format JSON

**When** `streamt lineage --format json`

**Then**
- Output JSON avec nodes et edges

---

### TC-LIN-006: Lineage génération HTML

**When** `streamt docs generate`

**Then**
- Fichier `docs/index.html` généré
- DAG interactif visualisable dans un browser

---

## 7. Gouvernance (Rules)

### TC-RULE-001: Violation min_partitions

**Given**:
```yaml
rules:
  topics:
    min_partitions: 6

models:
  - name: payments_clean
    advanced:
      topic:
        partitions: 3  # < 6
```

**When** `streamt validate`

**Then**
- Exit code: 1
- Error: "Model 'payments_clean' violates rule 'topics.min_partitions': expected >= 6, got 3"

---

### TC-RULE-002: Violation naming_pattern

**Given**:
```yaml
rules:
  topics:
    naming_pattern: "^[a-z]+\\.[a-z]+\\.v[0-9]+$"

models:
  - name: PaymentsClean  # PascalCase
```

**When** `streamt validate`

**Then**
- Exit code: 1
- Error: "Topic name 'PaymentsClean' does not match pattern"

---

### TC-RULE-003: Violation require_description

**Given**:
```yaml
rules:
  models:
    require_description: true

models:
  - name: payments_clean
    # pas de description
```

**When** `streamt validate`

**Then**
- Exit code: 1
- Error: "Model 'payments_clean' missing required field 'description'"

---

### TC-RULE-004: Violation require_tests

**Given**:
```yaml
rules:
  models:
    require_tests: true

models:
  - name: payments_clean

# pas de tests pour payments_clean
```

**When** `streamt validate`

**Then**
- Exit code: 1
- Error: "Model 'payments_clean' has no tests defined"

---

### TC-RULE-005: Violation sensitive_columns_require_masking

**Given**:
```yaml
rules:
  security:
    sensitive_columns_require_masking: true

sources:
  - name: payments_raw
    columns:
      - name: card_number
        classification: highly_sensitive
```

Et aucun model n'applique de masking sur `card_number`.

**When** `streamt validate`

**Then**
- Exit code: 1
- Error: "Column 'card_number' classified as 'highly_sensitive' has no masking policy"

---

### TC-RULE-006: Rules passent

**Given** projet qui respecte toutes les rules

**When** `streamt validate`

**Then**
- Exit code: 0
- Output inclut: "All governance rules passed"

---

## 8. Sécurité

### TC-SEC-001: Masking avec Gateway

**Given**:
```yaml
runtime:
  conduktor:
    gateway:
      url: http://gateway:8888

models:
  - name: payments_clean
    security:
      policies:
        - mask:
            column: card_number
            method: partial
            for_roles: [support]
    advanced:
      materialized: virtual_topic
```

**When** `streamt compile`

**Then**
- Config Gateway inclut règle de masking pour `card_number`

---

### TC-SEC-002: Masking sans Gateway (au sink)

**Given**:
```yaml
runtime:
  kafka:
    bootstrap_servers: localhost:9092
  # pas de Gateway

models:
  - name: payments_clean
    security:
      policies:
        - mask:
            column: card_number
            method: hash
```

**When** `streamt compile`

**Then**
- Warning: "Masking will be applied at sink level (no Gateway)"
- Le SQL des consumers inclut la fonction de masking

---

### TC-SEC-003: Access control - group

**Given**:
```yaml
models:
  - name: payments_internal
    access: private
    group: finance

  - name: fraud_model
    # owner: team-fraud (pas dans groupe finance)
    sql: SELECT * FROM {{ ref("payments_internal") }}
```

**When** `streamt validate`

**Then**
- Exit code: 1
- Error: "Model 'fraud_model' cannot reference 'payments_internal' (restricted to group 'finance')"

---

### TC-SEC-004: Access control - public

**Given**:
```yaml
models:
  - name: payments_public
    access: public

  - name: any_model
    sql: SELECT * FROM {{ ref("payments_public") }}
```

**When** `streamt validate`

**Then**
- Exit code: 0 (public = accessible à tous)

---

## 9. Erreurs & Edge Cases

### TC-ERR-001: Fichier YAML invalide

**Given** fichier avec syntaxe YAML cassée

**When** `streamt validate`

**Then**
- Exit code: 1
- Error: "YAML parse error in 'models/broken.yml': ..."

---

### TC-ERR-002: Kafka non accessible

**Given** `bootstrap_servers` pointe vers un Kafka down

**When** `streamt apply`

**Then**
- Exit code: 1
- Error: "Cannot connect to Kafka at 'localhost:9092'"

---

### TC-ERR-003: Flink non accessible

**Given** model `flink` mais Flink REST inaccessible

**When** `streamt apply`

**Then**
- Exit code: 1
- Error: "Cannot connect to Flink at 'http://localhost:8081'"

---

### TC-ERR-004: Schema Registry non accessible

**Given**:
```yaml
sources:
  - name: payments_raw
    schema:
      registry: schema_registry
      subject: payments.raw.v1-value
```

Et Schema Registry down.

**When** `streamt validate --check-schemas`

**Then**
- Exit code: 1
- Error: "Cannot connect to Schema Registry"

---

### TC-ERR-005: Schéma introuvable

**Given** subject `payments.raw.v1-value` n'existe pas dans SR

**When** `streamt validate --check-schemas`

**Then**
- Exit code: 1
- Error: "Schema subject 'payments.raw.v1-value' not found in registry"

---

### TC-ERR-006: Topic existe avec config incompatible

**Given**:
- Topic `payments_clean` existe avec 24 partitions
- Model demande 12 partitions (réduction impossible)

**When** `streamt apply`

**Then**
- Exit code: 1
- Error: "Cannot reduce partitions for topic 'payments_clean' (24 -> 12)"

---

### TC-ERR-007: Connector en échec

**Given** connector mal configuré (credentials invalides)

**When** `streamt apply`

**Then**
- Exit code: 1
- Error: "Connector 'payments_to_snowflake' failed: Authentication error"

---

### TC-ERR-008: Nom de projet dupliqué

**Given** deux models avec le même nom

**When** `streamt validate`

**Then**
- Exit code: 1
- Error: "Duplicate model name 'payments_clean'"

---

### TC-ERR-009: SQL Jinja invalide

**Given**:
```yaml
models:
  - name: broken
    sql: SELECT * FROM {{ source("payments_raw" }}  # parenthèse manquante
```

**When** `streamt validate`

**Then**
- Exit code: 1
- Error: "Jinja syntax error in model 'broken': ..."

---

### TC-ERR-010: Materialized inconnu

**Given**:
```yaml
models:
  - name: test
    advanced:
      materialized: unknown_type
```

**When** `streamt validate`

**Then**
- Exit code: 1
- Error: "Invalid materialized type 'unknown_type'. Valid: topic, virtual_topic, flink, sink"

---

## Matrice de couverture

| Fonctionnalité | Tests |
|----------------|-------|
| Parsing YAML | TC-VAL-001 to TC-VAL-012 |
| Compilation topic | TC-COMP-001 |
| Compilation flink | TC-COMP-002 |
| Compilation sink | TC-COMP-003 |
| Compilation virtual_topic | TC-COMP-004, TC-COMP-005 |
| Compilation masking | TC-COMP-006 |
| Planning | TC-PLAN-001 to TC-PLAN-004 |
| Apply | TC-APPLY-001 to TC-APPLY-007 |
| Tests schema | TC-TEST-001, TC-TEST-002 |
| Tests sample | TC-TEST-003, TC-TEST-004 |
| Tests continuous | TC-TEST-005 to TC-TEST-007 |
| Lineage | TC-LIN-001 to TC-LIN-006 |
| Rules topics | TC-RULE-001, TC-RULE-002 |
| Rules models | TC-RULE-003, TC-RULE-004 |
| Rules security | TC-RULE-005 |
| Security masking | TC-SEC-001, TC-SEC-002 |
| Security access | TC-SEC-003, TC-SEC-004 |
| Error handling | TC-ERR-001 to TC-ERR-010 |
