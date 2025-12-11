# Concepts - streamt

Ce document définit formellement tous les concepts de streamt.

---

## Vue d'ensemble

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           streamt Project                                │
│                                                                          │
│  ┌──────────┐     ┌──────────┐     ┌──────────┐     ┌──────────┐       │
│  │ Sources  │────▶│  Models  │────▶│  Models  │────▶│ Exposures│       │
│  │ (inputs) │     │(transform)│    │  (sink)  │     │ (outputs)│       │
│  └──────────┘     └──────────┘     └──────────┘     └──────────┘       │
│       │                │                │                │              │
│       └────────────────┴────────────────┴────────────────┘              │
│                                │                                         │
│                          ┌─────┴─────┐                                  │
│                          │   Tests   │                                  │
│                          └───────────┘                                  │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 1. Project

Un **Project** est l'unité de base de streamt. Il contient toutes les déclarations pour un domaine ou une équipe.

### Propriétés

| Propriété | Type | Requis | Description |
|-----------|------|--------|-------------|
| `name` | string | Oui | Identifiant unique du projet |
| `version` | string | Non | Version sémantique du projet |
| `description` | string | Non | Description du projet |

### Fichier racine

```yaml
# stream_project.yml
project:
  name: fraud-detection
  version: "1.0.0"
  description: "Pipeline de détection de fraude"

runtime:
  # connexions aux systèmes externes

defaults:
  # valeurs par défaut

rules:
  # règles de gouvernance
```

### Structure de fichiers

```
my-project/
├── stream_project.yml    # Configuration principale
├── .env                  # Secrets (gitignored)
├── sources/              # Déclarations de sources
│   └── *.yml
├── models/               # Déclarations de modèles
│   └── *.yml
├── tests/                # Déclarations de tests
│   └── *.yml
├── exposures/            # Déclarations d'exposures
│   └── *.yml
└── generated/            # Artefacts générés (gitignored optionnel)
    └── ...
```

---

## 2. Source

Une **Source** représente un flux de données externe au projet - produit par une application, un CDC, ou un autre système.

### Caractéristiques

- **Externe**: Le projet ne contrôle pas la logique qui écrit dans la source
- **Point d'entrée**: Première étape du DAG
- **Déclaratif**: Décrit où est la donnée, pas comment la produire

### Propriétés

| Propriété | Type | Requis | Description |
|-----------|------|--------|-------------|
| `name` | string | Oui | Identifiant unique dans le projet |
| `description` | string | Non | Description de la source |
| `topic` | string | Oui | Nom du topic Kafka physique |
| `cluster` | string | Non | Cluster Kafka (défaut: default) |
| `schema` | object | Non | Référence ou définition du schéma |
| `owner` | string | Non | Équipe/personne responsable |
| `tags` | list[string] | Non | Tags pour filtrage |
| `columns` | list[Column] | Non | Classification des colonnes |
| `freshness` | object | Non | SLA de fraîcheur attendu |

### Exemple

```yaml
sources:
  - name: card_payments_raw
    description: "Événements de paiement bruts"
    topic: payments.raw.v1
    schema:
      registry: schema_registry
      subject: payments.raw.v1-value
    owner: team-payments
    columns:
      - name: card_number
        classification: highly_sensitive
    freshness:
      max_lag_seconds: 60
```

### Référence dans SQL

```sql
SELECT * FROM {{ source("card_payments_raw") }}
```

---

## 3. Model

Un **Model** représente une transformation qui produit un nouveau flux de données.

### Caractéristiques

- **Interne**: Le projet contrôle entièrement le modèle
- **Transforme**: Prend des sources/modèles en entrée, produit un output
- **Matérialisé**: Différentes stratégies selon le runtime

### Types de matérialisation (auto-inférés)

Le type de matérialisation est **automatiquement inféré** à partir des patterns SQL :

| Pattern SQL | Type inféré | Runtime | Output |
|-------------|-------------|---------|--------|
| Simple SELECT/WHERE | `topic` | Kafka | Topic Kafka réel |
| TUMBLE/HOP/SESSION | `flink` | Flink | Topic Kafka + job |
| GROUP BY (non-windowed) | `flink` | Flink | Topic Kafka + job |
| JOIN | `flink` | Flink | Topic Kafka + job |
| `from:` sans `sql:` | `sink` | Connect | Système externe |
| `gateway:` rules | `virtual_topic` | Gateway | Pas de stockage |

**Note:** Peut être surchargé via `advanced.materialized:`

### Propriétés communes (top-level)

| Propriété | Type | Requis | Description |
|-----------|------|--------|-------------|
| `name` | string | Oui | Identifiant unique |
| `description` | string | Non | Description |
| `owner` | string | Non | Équipe responsable |
| `tags` | list[string] | Non | Tags |
| `from` | list | Non | Dépendances (inféré du SQL si absent) |
| `sql` | string | Cond. | Transformation SQL |
| `key` | string | Non | Clé de partitionnement |
| `columns` | list[Column] | Non | Définition des colonnes |
| `access` | enum | Non | private/protected/public |
| `group` | string | Non | Groupe autorisé à référencer |
| `security` | object | Non | Politiques de sécurité |

**Note:** `materialized` n'est plus au top-level - il est auto-inféré ou défini dans `advanced:`

### Propriétés avancées (dans `advanced:`)

Toutes les configurations d'implémentation sont dans la section `advanced:`:

**Générique:**

| Propriété | Type | Description |
|-----------|------|-------------|
| `materialized` | enum | Override auto-inference |
| `cluster` | string | Cluster Kafka à utiliser |

**Topic (topic / virtual_topic):**

| Propriété | Type | Description |
|-----------|------|-------------|
| `topic.name` | string | Nom du topic (défaut: model name) |
| `topic.partitions` | int | Nombre de partitions |
| `topic.replication_factor` | int | Facteur de réplication |
| `topic.config` | object | Config Kafka (retention, etc.) |

**Flink:**

| Propriété | Type | Description |
|-----------|------|-------------|
| `flink_cluster` | string | Cluster Flink à utiliser |
| `flink.parallelism` | int | Parallélisme du job |
| `flink.checkpoint_interval_ms` | int | Intervalle checkpoints |
| `flink.state_backend` | string | Backend de state |

**Sink (Connect):**

| Propriété | Type | Description |
|-----------|------|-------------|
| `connect_cluster` | string | Cluster Connect à utiliser |
| `sink.connector` | string | Type de connector |
| `sink.config` | object | Configuration du connector |

**Gateway:**

| Propriété | Type | Description |
|-----------|------|-------------|
| `gateway.virtual_topic` | bool | Force virtual topic mode |
| `gateway.interceptors` | list | Interceptors Gateway |

### Exemples

```yaml
models:
  # Stateless - topic réel
  - name: payments_clean
    description: "Cleaned payment events"
    owner: team-payments
    key: payment_id

    sql: |
      SELECT payment_id, amount_cents, status
      FROM {{ source("card_payments_raw") }}
      WHERE status = 'CAPTURED'

    # materialized: topic (auto-inféré depuis simple SELECT/WHERE)

    advanced:
      topic:
        partitions: 12
        replication_factor: 3

  # Stateful - Flink
  - name: customer_balance_5m
    description: "5-minute customer balance"
    owner: team-analytics
    key: customer_id

    sql: |
      SELECT customer_id, SUM(amount) as total
      FROM {{ ref("payments_clean") }}
      GROUP BY customer_id, TUMBLE(event_time, INTERVAL '5' MINUTE)

    # materialized: flink (auto-inféré depuis TUMBLE)

    advanced:
      flink:
        parallelism: 8
        checkpoint_interval_ms: 30000

  # Sink - Connect
  - name: payments_to_warehouse
    description: "Export to data warehouse"
    owner: team-data-platform

    from:
      - ref: payments_clean

    # Pas de sql: donc materialized: sink (auto-inféré)

    advanced:
      connect_cluster: prod-connect
      sink:
        connector: snowflake-sink
        config:
          snowflake.database.name: ANALYTICS
```

### Référence dans SQL

```sql
SELECT * FROM {{ ref("payments_clean") }}
```

### Auto-inférence de la matérialisation

Le système analyse le SQL pour déterminer automatiquement le type de matérialisation optimal :

**Règles d'inférence:**

| Pattern SQL détecté | Type inféré | Raison |
|-------------------|-------------|---------|
| `TUMBLE()`, `HOP()`, `SESSION()` | `flink` | Fonctions de fenêtrage → traitement stateful |
| `JOIN` | `flink` | Jointures de streams → opérateurs Flink |
| `GROUP BY` (non-fenêtré) | `flink` | Agrégation stateful → Flink requis |
| Simple `SELECT`, `WHERE`, `CASE` | `topic` | Transformation stateless → topic-to-topic |
| `from:` présent, `sql:` absent | `sink` | Modèle sink-only → pas de transformation |
| `advanced.gateway.virtual_topic: true` | `virtual_topic` | Virtual topic Gateway explicite |

**Surcharge de l'inférence:**

```yaml
models:
  - name: ma_transformation
    sql: SELECT * FROM {{ source("raw") }} WHERE valid = true
    # Normalement inféré comme 'topic', mais on veut 'virtual_topic':

    advanced:
      materialized: virtual_topic  # Surcharge explicite
      gateway:
        virtual_topic: true
```

**Organisation du schéma:**

- **Top-level**: Décrit **quoi** (logique métier, documentation, sécurité)
- **`advanced:`**: Décrit **comment** (implémentation, runtime, configs techniques)

---

## 4. Test

Un **Test** valide la qualité des données dans un model ou une source.

### Caractéristiques

- **Assertions**: Conditions qui doivent être vraies
- **Exécution**: Compile-time, on-demand, ou continue
- **Actions**: Réactions automatiques en cas d'échec

### Types de tests

| Type | Exécution | Description |
|------|-----------|-------------|
| `schema` | Compile-time | Valide structure et types |
| `sample` | On-demand | Vérifie N messages |
| `continuous` | Runtime (Flink) | Monitoring temps réel |

### Propriétés

| Propriété | Type | Requis | Description |
|-----------|------|--------|-------------|
| `name` | string | Oui | Identifiant unique |
| `model` | string | Oui | Model ou source à tester |
| `type` | enum | Oui | schema/sample/continuous |
| `assertions` | list | Oui | Liste d'assertions |
| `sample_size` | int | Cond. | Taille échantillon (type: sample) |
| `flink_cluster` | string | Cond. | Cluster Flink (type: continuous) |
| `on_failure` | object | Non | Actions en cas d'échec |

### Types d'assertions

| Assertion | Description | Paramètres |
|-----------|-------------|------------|
| `not_null` | Colonnes non nulles | `columns: [col1, col2]` |
| `unique_key` | Clé unique dans une fenêtre | `key`, `window`, `tolerance` |
| `accepted_values` | Valeurs autorisées | `column`, `values` |
| `accepted_types` | Types de données | `column: type` |
| `range` | Valeur dans un intervalle | `column`, `min`, `max` |
| `max_lag` | Retard maximum | `column`, `max_seconds` |
| `throughput` | Débit min/max | `min_per_second`, `max_per_second` |
| `distribution` | Répartition par buckets | `column`, `buckets` |
| `foreign_key` | Clé étrangère existe | `column`, `ref_model`, `ref_key`, `window` |
| `custom_sql` | SQL personnalisé | `sql`, `expect` |

### Actions on_failure

| Action | Description |
|--------|-------------|
| `alert.slack` | Envoie alerte Slack |
| `alert.webhook` | Appelle webhook HTTP |
| `pause_model` | Stoppe le model upstream |
| `dlq` | Redirige vers DLQ (model) |
| `block_deploy` | Bloque les futurs déploiements |

### Exemple

```yaml
tests:
  - name: payments_quality
    model: payments_clean
    type: continuous
    assertions:
      - unique_key:
          key: payment_id
          window: "15 minutes"
      - max_lag:
          column: event_time
          max_seconds: 300
    on_failure:
      severity: error
      actions:
        - alert:
            type: slack
            channel: "#alerts"
        - dlq:
            model: payments_clean_dlq
```

---

## 5. Exposure

Une **Exposure** documente un consommateur ou producteur externe du DAG.

### Caractéristiques

- **Métadonnée**: Ne génère pas de runtime, sert au lineage
- **Contrats**: Définit les attentes (SLA, accès)
- **Impact analysis**: Permet de voir qui est affecté par un changement

### Types d'exposures

| Type | Description |
|------|-------------|
| `application` | Service/app qui produit ou consomme |
| `dashboard` | Dashboard BI |
| `ml_training` | Pipeline d'entraînement ML |
| `ml_inference` | Service d'inférence ML |
| `api` | API externe |

### Rôles (pour type: application)

| Rôle | Description |
|------|-------------|
| `producer` | Produit des données vers une source |
| `consumer` | Consomme des données depuis un model |
| `both` | Produit et consomme |

### Propriétés

| Propriété | Type | Requis | Description |
|-----------|------|--------|-------------|
| `name` | string | Oui | Identifiant unique |
| `type` | enum | Oui | Type d'exposure |
| `role` | enum | Cond. | producer/consumer/both |
| `description` | string | Non | Description |
| `owner` | string | Non | Équipe responsable |
| `url` | string | Non | URL (repo, dashboard, etc.) |
| `produces` | list | Cond. | Sources produites (role: producer) |
| `consumes` | list | Cond. | Models consommés (role: consumer) |
| `depends_on` | list | Cond. | Dépendances générales |
| `sla` | object | Non | Attentes de performance |
| `access` | object | Non | Contrôle d'accès |

### Exemple

```yaml
exposures:
  # Producer
  - name: payments_api
    type: application
    role: producer
    owner: team-payments
    produces:
      - source: card_payments_raw
    sla:
      max_produce_latency_ms: 50

  # Consumer
  - name: fraud_scoring
    type: application
    role: consumer
    owner: team-fraud
    consumes:
      - ref: payments_enriched
    sla:
      max_lag_messages: 10000
```

---

## 6. Security

La **Security** définit les politiques de protection des données.

### Niveaux de classification

| Classification | Description |
|----------------|-------------|
| `public` | Données publiques |
| `internal` | Usage interne uniquement |
| `confidential` | Accès restreint |
| `sensitive` | Données personnelles (PII) |
| `highly_sensitive` | Données critiques (cartes, mots de passe) |

### Politiques disponibles

| Politique | Description | Paramètres |
|-----------|-------------|------------|
| `mask` | Masque la valeur | `column`, `method`, `for_roles` |
| `allow` | Autorise l'accès | `roles`, `purpose` |
| `deny` | Interdit l'accès | `roles` |

### Méthodes de masquage

| Méthode | Résultat | Exemple |
|---------|----------|---------|
| `hash` | Hash SHA-256 | `abc123...` |
| `redact` | Remplacement fixe | `[REDACTED]` |
| `partial` | Masquage partiel | `****1234` |
| `tokenize` | Token réversible | `tok_abc123` |
| `null` | Valeur nulle | `null` |

### Exemple

```yaml
models:
  - name: payments_clean
    security:
      classification:
        card_number: highly_sensitive
        amount_cents: confidential
      policies:
        - mask:
            column: card_number
            method: partial
            for_roles: [support, analytics]
        - allow:
            roles: [fraud-engine]
            purpose: fraud_detection
```

---

## 7. Rules (Gouvernance)

Les **Rules** définissent les contraintes à respecter dans le projet.

### Catégories

| Catégorie | S'applique à | Description |
|-----------|--------------|-------------|
| `topics` | Topics créés | Contraintes Kafka |
| `models` | Tous les models | Qualité des définitions |
| `sources` | Toutes les sources | Exigences de documentation |
| `security` | Données sensibles | Obligations de protection |

### Règles topics

| Règle | Type | Description |
|-------|------|-------------|
| `min_partitions` | int | Partitions minimum |
| `max_partitions` | int | Partitions maximum |
| `min_replication_factor` | int | Réplication minimum |
| `required_config` | list | Configs obligatoires |
| `naming_pattern` | regex | Pattern de nommage |
| `forbidden_prefixes` | list | Préfixes interdits |

### Règles models

| Règle | Type | Description |
|-------|------|-------------|
| `require_description` | bool | Description obligatoire |
| `require_owner` | bool | Owner obligatoire |
| `require_tests` | bool | Au moins un test |
| `max_dependencies` | int | Dépendances max |

### Validation

Les rules sont validées à **compile-time**. Une violation = erreur de compilation.

```bash
$ streamt compile

ERROR: Model 'payments_clean' violates rule 'topics.min_partitions'
  Expected: >= 6
  Got: 3
  Location: models/payments.yml:12
```

---

## 8. DAG (Directed Acyclic Graph)

Le **DAG** est le graphe des dépendances entre sources, models et exposures.

### Construction

1. Parser tous les fichiers YAML
2. Pour chaque model:
   - Si `from` présent → dépendances explicites
   - Sinon → parser le SQL pour `source()` et `ref()`
3. Valider: pas de cycles, pas de références manquantes
4. Ordonner: exécution topologique

### Utilisations

| Commande | Utilisation du DAG |
|----------|-------------------|
| `streamt compile` | Ordre de génération |
| `streamt apply` | Ordre de déploiement |
| `streamt lineage` | Visualisation |
| `streamt plan` | Impact analysis |
| `streamt test` | Ordre des tests |

### Visualisation

```bash
$ streamt lineage --model payments_enriched

card_payments_raw (source)
    │
    ▼
payments_clean (model:topic)
    │
    ├──────────────────┐
    ▼                  ▼
customer_balance    payments_enriched (model:flink)
(model:flink)           │
                        ├────────────────┐
                        ▼                ▼
                   fraud_scoring    payments_to_warehouse
                   (exposure)       (model:sink)
```

---

## 9. Manifest

Le **Manifest** est l'état compilé du projet.

### Contenu

```json
{
  "version": "1.0.0",
  "project": "fraud-detection",
  "compiled_at": "2025-12-06T10:30:00Z",
  "sources": [...],
  "models": [...],
  "tests": [...],
  "exposures": [...],
  "dag": {
    "nodes": [...],
    "edges": [...]
  },
  "artifacts": {
    "topics": [...],
    "flink_jobs": [...],
    "connectors": [...]
  }
}
```

### Utilisations

- Comparaison avant/après pour `plan`
- Input pour `apply`
- Référence pour `lineage`
- Debug et audit

---

## 10. Runtime

Le **Runtime** est la configuration des systèmes externes.

### Composants

| Composant | Requis | Description |
|-----------|--------|-------------|
| `kafka` | Oui | Cluster(s) Kafka |
| `schema_registry` | Non | Registry de schémas |
| `flink` | Non | Cluster(s) Flink |
| `connect` | Non | Cluster(s) Connect |
| `conduktor` | Non | Gateway + Console |

### Mode standalone vs enhanced

| Feature | Standalone | Enhanced (Conduktor) |
|---------|------------|---------------------|
| Topics réels | ✅ | ✅ |
| Virtual topics | ❌ | ✅ |
| Flink jobs | ✅ | ✅ |
| Connect sinks | ✅ | ✅ |
| Masking at read | ❌ | ✅ |
| Masking at sink | ✅ | ✅ |
| ACLs Kafka | ✅ | ✅ |
| RBAC avancé | ❌ | ✅ |
| Runtime lineage | ❌ | ✅ |
