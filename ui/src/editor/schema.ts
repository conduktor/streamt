/**
 * Streamt YAML Schema Definition
 * Complete schema for autocompletion, validation, and documentation
 */

export interface SchemaField {
  name: string
  type: 'string' | 'number' | 'boolean' | 'array' | 'object' | 'enum' | 'sql' | 'jinja'
  required?: boolean
  description: string
  enum?: string[]
  default?: any
  children?: Record<string, SchemaField>
  itemSchema?: SchemaField
  examples?: string[]
  deprecated?: boolean
  since?: string
}

// Column types supported by Flink SQL
export const COLUMN_TYPES = [
  'STRING', 'VARCHAR', 'CHAR',
  'BOOLEAN',
  'TINYINT', 'SMALLINT', 'INT', 'INTEGER', 'BIGINT',
  'FLOAT', 'DOUBLE', 'DECIMAL',
  'DATE', 'TIME', 'TIMESTAMP', 'TIMESTAMP_LTZ',
  'INTERVAL', 'ARRAY', 'MAP', 'ROW', 'RAW',
  'BYTES', 'VARBINARY', 'BINARY',
] as const

// Materialization types
export const MATERIALIZED_TYPES = ['topic', 'flink', 'virtual_topic', 'sink'] as const

// Test types
export const TEST_TYPES = ['schema', 'sample', 'continuous'] as const

// Exposure types
export const EXPOSURE_TYPES = ['application', 'dashboard', 'ml_model', 'notebook', 'other'] as const

// Exposure roles
export const EXPOSURE_ROLES = ['producer', 'consumer', 'both'] as const

// Assertion types for tests
export const ASSERTION_TYPES = [
  'not_null', 'unique', 'accepted_values', 'relationships',
  'range', 'regex', 'custom_sql', 'freshness'
] as const

// Watermark strategies
export const WATERMARK_STRATEGIES = ['strict_ascending', 'bounded_out_of_order', 'custom'] as const

// Sink types
export const SINK_TYPES = ['jdbc', 'elasticsearch', 'filesystem', 'kafka', 'custom'] as const

// Format types
export const FORMAT_TYPES = ['json', 'avro', 'protobuf', 'csv', 'raw'] as const

// Cleanup policies
export const CLEANUP_POLICIES = ['delete', 'compact', 'delete,compact'] as const

// Complete schema definition
export const STREAMT_SCHEMA: Record<string, SchemaField> = {
  project: {
    name: 'project',
    type: 'object',
    required: true,
    description: 'Project metadata and configuration',
    children: {
      name: {
        name: 'name',
        type: 'string',
        required: true,
        description: 'Unique project identifier used for naming resources',
        examples: ['my-pipeline', 'payments-processing', 'user-analytics'],
      },
      version: {
        name: 'version',
        type: 'string',
        required: true,
        description: 'Semantic version of the project (e.g., "1.0.0")',
        examples: ['1.0.0', '2.1.3', '0.1.0-beta'],
      },
      description: {
        name: 'description',
        type: 'string',
        description: 'Human-readable description of the pipeline purpose',
        examples: ['Real-time payment fraud detection pipeline'],
      },
      author: {
        name: 'author',
        type: 'string',
        description: 'Project maintainer or team name',
        examples: ['data-team', 'platform@company.com'],
      },
      tags: {
        name: 'tags',
        type: 'array',
        description: 'Labels for categorizing and filtering projects',
        examples: ['["production", "payments", "fraud"]'],
      },
    },
  },

  runtime: {
    name: 'runtime',
    type: 'object',
    required: true,
    description: 'Infrastructure connection settings for Kafka, Flink, and other services',
    children: {
      kafka: {
        name: 'kafka',
        type: 'object',
        required: true,
        description: 'Apache Kafka cluster configuration',
        children: {
          bootstrap_servers: {
            name: 'bootstrap_servers',
            type: 'string',
            required: true,
            description: 'Comma-separated list of Kafka broker addresses',
            examples: ['localhost:9092', 'broker1:9092,broker2:9092,broker3:9092'],
          },
          security_protocol: {
            name: 'security_protocol',
            type: 'enum',
            description: 'Security protocol for Kafka connections',
            enum: ['PLAINTEXT', 'SSL', 'SASL_PLAINTEXT', 'SASL_SSL'],
            default: 'PLAINTEXT',
          },
          sasl_mechanism: {
            name: 'sasl_mechanism',
            type: 'enum',
            description: 'SASL authentication mechanism',
            enum: ['PLAIN', 'SCRAM-SHA-256', 'SCRAM-SHA-512', 'OAUTHBEARER', 'GSSAPI'],
          },
          sasl_username: {
            name: 'sasl_username',
            type: 'string',
            description: 'SASL username for authentication',
          },
          sasl_password: {
            name: 'sasl_password',
            type: 'string',
            description: 'SASL password (use environment variable reference)',
            examples: ['${KAFKA_PASSWORD}'],
          },
        },
      },
      schema_registry: {
        name: 'schema_registry',
        type: 'object',
        description: 'Confluent Schema Registry configuration',
        children: {
          url: {
            name: 'url',
            type: 'string',
            required: true,
            description: 'Schema Registry HTTP endpoint',
            examples: ['http://localhost:8081', 'https://schema-registry.company.com'],
          },
          basic_auth_user: {
            name: 'basic_auth_user',
            type: 'string',
            description: 'Username for basic authentication',
          },
          basic_auth_password: {
            name: 'basic_auth_password',
            type: 'string',
            description: 'Password for basic authentication',
          },
        },
      },
      flink: {
        name: 'flink',
        type: 'object',
        description: 'Apache Flink cluster configuration',
        children: {
          default: {
            name: 'default',
            type: 'string',
            description: 'Default cluster name to use for Flink jobs',
            examples: ['local', 'production', 'staging'],
          },
          clusters: {
            name: 'clusters',
            type: 'object',
            description: 'Named Flink cluster configurations',
            children: {
              _dynamic_: {
                name: 'cluster_name',
                type: 'object',
                description: 'Individual cluster configuration',
                children: {
                  rest_url: {
                    name: 'rest_url',
                    type: 'string',
                    required: true,
                    description: 'Flink REST API endpoint',
                    examples: ['http://localhost:8081'],
                  },
                  sql_gateway_url: {
                    name: 'sql_gateway_url',
                    type: 'string',
                    description: 'Flink SQL Gateway endpoint for SQL submissions',
                    examples: ['http://localhost:8083'],
                  },
                },
              },
            },
          },
        },
      },
      connect: {
        name: 'connect',
        type: 'object',
        description: 'Kafka Connect cluster configuration',
        children: {
          clusters: {
            name: 'clusters',
            type: 'object',
            description: 'Named Connect cluster configurations',
          },
        },
      },
      gateway: {
        name: 'gateway',
        type: 'object',
        description: 'Conduktor Gateway configuration for virtual topics',
        since: '0.2.0',
        children: {
          url: {
            name: 'url',
            type: 'string',
            required: true,
            description: 'Gateway admin API endpoint',
            examples: ['http://localhost:8888'],
          },
          virtual_cluster: {
            name: 'virtual_cluster',
            type: 'string',
            description: 'Virtual cluster name for topic aliasing',
            examples: ['main', 'passthrough'],
          },
          username: {
            name: 'username',
            type: 'string',
            description: 'Gateway admin username',
          },
          password: {
            name: 'password',
            type: 'string',
            description: 'Gateway admin password',
          },
        },
      },
    },
  },

  sources: {
    name: 'sources',
    type: 'array',
    description: 'External data sources that feed into the pipeline',
    itemSchema: {
      name: 'source',
      type: 'object',
      description: 'A single source definition',
      children: {
        name: {
          name: 'name',
          type: 'string',
          required: true,
          description: 'Unique identifier for referencing this source in SQL',
          examples: ['raw_events', 'user_clicks', 'payment_transactions'],
        },
        description: {
          name: 'description',
          type: 'string',
          description: 'Human-readable description of the data source',
        },
        topic: {
          name: 'topic',
          type: 'string',
          required: true,
          description: 'Kafka topic name to consume from',
          examples: ['events.raw.v1', 'clicks.user.v2'],
        },
        format: {
          name: 'format',
          type: 'enum',
          description: 'Message serialization format',
          enum: [...FORMAT_TYPES],
          default: 'json',
        },
        schema_subject: {
          name: 'schema_subject',
          type: 'string',
          description: 'Schema Registry subject name (defaults to topic-value)',
        },
        columns: {
          name: 'columns',
          type: 'array',
          required: true,
          description: 'Column definitions for the source schema',
          itemSchema: {
            name: 'column',
            type: 'object',
            description: 'Column specification',
            children: {
              name: {
                name: 'name',
                type: 'string',
                required: true,
                description: 'Column name',
              },
              type: {
                name: 'type',
                type: 'enum',
                required: true,
                description: 'Flink SQL data type',
                enum: [...COLUMN_TYPES],
              },
              nullable: {
                name: 'nullable',
                type: 'boolean',
                description: 'Whether the column can contain NULL values',
                default: true,
              },
              description: {
                name: 'description',
                type: 'string',
                description: 'Column documentation',
              },
            },
          },
        },
        watermark: {
          name: 'watermark',
          type: 'object',
          description: 'Watermark configuration for event-time processing',
          children: {
            column: {
              name: 'column',
              type: 'string',
              required: true,
              description: 'Timestamp column for watermark generation',
            },
            strategy: {
              name: 'strategy',
              type: 'enum',
              description: 'Watermark generation strategy',
              enum: [...WATERMARK_STRATEGIES],
              default: 'bounded_out_of_order',
            },
            delay: {
              name: 'delay',
              type: 'string',
              description: 'Maximum out-of-orderness for bounded strategy',
              examples: ['5 seconds', '1 minute', '30 seconds'],
            },
          },
        },
        freshness: {
          name: 'freshness',
          type: 'object',
          description: 'Data freshness SLA configuration',
          children: {
            warn_after_seconds: {
              name: 'warn_after_seconds',
              type: 'number',
              description: 'Warning threshold for stale data',
            },
            error_after_seconds: {
              name: 'error_after_seconds',
              type: 'number',
              description: 'Error threshold for stale data',
            },
          },
        },
        tags: {
          name: 'tags',
          type: 'array',
          description: 'Labels for categorization',
        },
      },
    },
  },

  models: {
    name: 'models',
    type: 'array',
    description: 'Data transformations and derived datasets',
    itemSchema: {
      name: 'model',
      type: 'object',
      description: 'A single model definition',
      children: {
        name: {
          name: 'name',
          type: 'string',
          required: true,
          description: 'Unique identifier for this model',
          examples: ['enriched_events', 'daily_aggregates', 'fraud_scores'],
        },
        description: {
          name: 'description',
          type: 'string',
          description: 'Human-readable description of the transformation',
        },
        materialized: {
          name: 'materialized',
          type: 'enum',
          required: true,
          description: 'How the model output is materialized',
          enum: [...MATERIALIZED_TYPES],
        },
        owner: {
          name: 'owner',
          type: 'string',
          description: 'Team or person responsible for this model',
        },
        sql: {
          name: 'sql',
          type: 'sql',
          required: true,
          description: 'Flink SQL transformation logic. Use {{ source("name") }} and {{ ref("model") }} for dependencies.',
          examples: [
            'SELECT * FROM {{ source("raw_events") }} WHERE event_type IS NOT NULL',
            'SELECT user_id, COUNT(*) as event_count FROM {{ ref("validated_events") }} GROUP BY user_id',
          ],
        },
        topic: {
          name: 'topic',
          type: 'object',
          description: 'Kafka topic configuration for topic/flink materialization',
          children: {
            name: {
              name: 'name',
              type: 'string',
              description: 'Override topic name (defaults to model name)',
            },
            partitions: {
              name: 'partitions',
              type: 'number',
              description: 'Number of partitions',
              default: 6,
            },
            replication_factor: {
              name: 'replication_factor',
              type: 'number',
              description: 'Replication factor for durability',
              default: 3,
            },
            cleanup_policy: {
              name: 'cleanup_policy',
              type: 'enum',
              description: 'Log cleanup policy',
              enum: [...CLEANUP_POLICIES],
              default: 'delete',
            },
            retention_ms: {
              name: 'retention_ms',
              type: 'number',
              description: 'Retention period in milliseconds',
              examples: ['604800000', '86400000'],
            },
          },
        },
        flink: {
          name: 'flink',
          type: 'object',
          description: 'Flink job configuration (for flink materialization)',
          children: {
            parallelism: {
              name: 'parallelism',
              type: 'number',
              description: 'Job parallelism level',
              default: 1,
            },
            checkpoint_interval_ms: {
              name: 'checkpoint_interval_ms',
              type: 'number',
              description: 'Checkpoint interval in milliseconds',
              default: 60000,
            },
            state_ttl_ms: {
              name: 'state_ttl_ms',
              type: 'number',
              description: 'State time-to-live in milliseconds',
              examples: ['3600000', '86400000'],
            },
            min_pause_between_checkpoints_ms: {
              name: 'min_pause_between_checkpoints_ms',
              type: 'number',
              description: 'Minimum pause between checkpoints',
            },
            savepoint_path: {
              name: 'savepoint_path',
              type: 'string',
              description: 'Path to restore from savepoint',
            },
          },
        },
        virtual_topic: {
          name: 'virtual_topic',
          type: 'object',
          description: 'Conduktor Gateway virtual topic configuration',
          children: {
            physical_topic: {
              name: 'physical_topic',
              type: 'string',
              required: true,
              description: 'Physical Kafka topic to filter',
            },
            filter: {
              name: 'filter',
              type: 'string',
              description: 'SQL WHERE clause for filtering',
              examples: ['status = "active"', 'amount > 1000'],
            },
          },
        },
        sink: {
          name: 'sink',
          type: 'object',
          description: 'Kafka Connect sink configuration',
          children: {
            type: {
              name: 'type',
              type: 'enum',
              required: true,
              description: 'Sink connector type',
              enum: [...SINK_TYPES],
            },
            config: {
              name: 'config',
              type: 'object',
              description: 'Connector-specific configuration',
            },
          },
        },
        depends_on: {
          name: 'depends_on',
          type: 'array',
          description: 'Explicit dependencies (usually inferred from SQL)',
        },
        tags: {
          name: 'tags',
          type: 'array',
          description: 'Labels for categorization',
        },
      },
    },
  },

  tests: {
    name: 'tests',
    type: 'array',
    description: 'Data quality tests and assertions',
    itemSchema: {
      name: 'test',
      type: 'object',
      description: 'A single test definition',
      children: {
        name: {
          name: 'name',
          type: 'string',
          required: true,
          description: 'Unique test identifier',
          examples: ['payments_not_null', 'unique_order_ids', 'valid_amounts'],
        },
        description: {
          name: 'description',
          type: 'string',
          description: 'What this test validates',
        },
        model: {
          name: 'model',
          type: 'string',
          required: true,
          description: 'Model or source to test (use ref() syntax)',
          examples: ['enriched_payments', 'raw_events'],
        },
        type: {
          name: 'type',
          type: 'enum',
          required: true,
          description: 'Test execution strategy',
          enum: [...TEST_TYPES],
        },
        sample_size: {
          name: 'sample_size',
          type: 'number',
          description: 'Number of messages to sample (for sample type)',
          default: 100,
        },
        timeout_seconds: {
          name: 'timeout_seconds',
          type: 'number',
          description: 'Maximum test execution time',
          default: 60,
        },
        assertions: {
          name: 'assertions',
          type: 'array',
          required: true,
          description: 'List of assertions to validate',
          itemSchema: {
            name: 'assertion',
            type: 'object',
            description: 'Single assertion. Key is assertion type, value is config.',
          },
        },
        on_failure: {
          name: 'on_failure',
          type: 'object',
          description: 'Actions to take when test fails',
          children: {
            actions: {
              name: 'actions',
              type: 'array',
              description: 'List of failure actions (alert, pause, log)',
            },
          },
        },
        tags: {
          name: 'tags',
          type: 'array',
          description: 'Labels for categorization',
        },
      },
    },
  },

  exposures: {
    name: 'exposures',
    type: 'array',
    description: 'Downstream consumers and applications using pipeline data',
    itemSchema: {
      name: 'exposure',
      type: 'object',
      description: 'A single exposure definition',
      children: {
        name: {
          name: 'name',
          type: 'string',
          required: true,
          description: 'Unique identifier for this exposure',
          examples: ['fraud_dashboard', 'ml_scoring_service', 'analytics_notebook'],
        },
        description: {
          name: 'description',
          type: 'string',
          description: 'What this exposure does with the data',
        },
        type: {
          name: 'type',
          type: 'enum',
          required: true,
          description: 'Type of downstream consumer',
          enum: [...EXPOSURE_TYPES],
        },
        role: {
          name: 'role',
          type: 'enum',
          description: 'Whether this exposure produces, consumes, or both',
          enum: [...EXPOSURE_ROLES],
          default: 'consumer',
        },
        owner: {
          name: 'owner',
          type: 'string',
          description: 'Team or person responsible for this exposure',
        },
        consumer_group: {
          name: 'consumer_group',
          type: 'string',
          description: 'Kafka consumer group ID',
        },
        depends_on: {
          name: 'depends_on',
          type: 'array',
          description: 'Models or sources this exposure reads from',
        },
        consumes: {
          name: 'consumes',
          type: 'array',
          description: 'What this exposure consumes',
        },
        produces: {
          name: 'produces',
          type: 'array',
          description: 'What this exposure produces (for producer role)',
        },
        sla: {
          name: 'sla',
          type: 'object',
          description: 'Service Level Agreement configuration',
          children: {
            max_latency_ms: {
              name: 'max_latency_ms',
              type: 'number',
              description: 'Maximum acceptable end-to-end latency',
            },
            availability_percent: {
              name: 'availability_percent',
              type: 'number',
              description: 'Required uptime percentage (e.g., 99.9)',
            },
          },
        },
        tags: {
          name: 'tags',
          type: 'array',
          description: 'Labels for categorization',
        },
      },
    },
  },

  rules: {
    name: 'rules',
    type: 'object',
    description: 'Global rules and conventions for the project',
    children: {
      naming: {
        name: 'naming',
        type: 'object',
        description: 'Naming convention rules',
        children: {
          topic_prefix: {
            name: 'topic_prefix',
            type: 'string',
            description: 'Prefix for all generated topics',
            examples: ['mycompany.', 'prod.payments.'],
          },
          topic_suffix: {
            name: 'topic_suffix',
            type: 'string',
            description: 'Suffix for all generated topics',
          },
        },
      },
      defaults: {
        name: 'defaults',
        type: 'object',
        description: 'Default values for models',
        children: {
          partitions: {
            name: 'partitions',
            type: 'number',
            description: 'Default partition count for topics',
          },
          replication_factor: {
            name: 'replication_factor',
            type: 'number',
            description: 'Default replication factor',
          },
        },
      },
    },
  },
}

// Documentation snippets for common patterns
export const DOCUMENTATION_SNIPPETS = {
  jinja_source: {
    syntax: '{{ source("source_name") }}',
    description: 'Reference an external source defined in the sources section',
    example: 'SELECT * FROM {{ source("raw_events") }}',
  },
  jinja_ref: {
    syntax: '{{ ref("model_name") }}',
    description: 'Reference another model, creating a dependency',
    example: 'SELECT * FROM {{ ref("validated_events") }}',
  },
  jinja_var: {
    syntax: '{{ var("variable_name", "default") }}',
    description: 'Reference a project variable with optional default',
    example: 'WHERE environment = {{ var("env", "production") }}',
  },
  watermark: {
    syntax: 'WATERMARK FOR column AS column - INTERVAL \'5\' SECOND',
    description: 'Define event-time watermark for streaming operations',
    example: 'WATERMARK FOR event_time AS event_time - INTERVAL \'5\' SECOND',
  },
}

// Common SQL snippets for Flink
export const SQL_SNIPPETS = [
  {
    label: 'SELECT FROM source',
    insertText: 'SELECT\n  $1\nFROM {{ source("$2") }}\nWHERE $3',
    description: 'Basic SELECT from a source',
  },
  {
    label: 'SELECT FROM ref',
    insertText: 'SELECT\n  $1\nFROM {{ ref("$2") }}\nWHERE $3',
    description: 'Basic SELECT from another model',
  },
  {
    label: 'Tumbling window aggregation',
    insertText: 'SELECT\n  window_start,\n  window_end,\n  $1,\n  COUNT(*) as cnt\nFROM TABLE(\n  TUMBLE(TABLE {{ ref("$2") }}, DESCRIPTOR($3), INTERVAL \'$4\' MINUTES)\n)\nGROUP BY window_start, window_end, $1',
    description: 'Tumbling window aggregation',
  },
  {
    label: 'Hopping window aggregation',
    insertText: 'SELECT\n  window_start,\n  window_end,\n  $1,\n  COUNT(*) as cnt\nFROM TABLE(\n  HOP(TABLE {{ ref("$2") }}, DESCRIPTOR($3), INTERVAL \'$4\' MINUTES, INTERVAL \'$5\' MINUTES)\n)\nGROUP BY window_start, window_end, $1',
    description: 'Hopping (sliding) window aggregation',
  },
  {
    label: 'Session window aggregation',
    insertText: 'SELECT\n  window_start,\n  window_end,\n  $1,\n  COUNT(*) as cnt\nFROM TABLE(\n  SESSION(TABLE {{ ref("$2") }}, DESCRIPTOR($3), INTERVAL \'$4\' MINUTES)\n)\nGROUP BY window_start, window_end, $1',
    description: 'Session window aggregation',
  },
  {
    label: 'JOIN two streams',
    insertText: 'SELECT\n  a.$1,\n  b.$2\nFROM {{ ref("$3") }} a\nJOIN {{ ref("$4") }} b\n  ON a.$5 = b.$6\nWHERE a.$7 BETWEEN b.$8 - INTERVAL \'$9\' MINUTES AND b.$8',
    description: 'Interval join between two streams',
  },
  {
    label: 'Deduplication',
    insertText: 'SELECT $1\nFROM (\n  SELECT *,\n    ROW_NUMBER() OVER (PARTITION BY $2 ORDER BY $3 DESC) as row_num\n  FROM {{ ref("$4") }}\n)\nWHERE row_num = 1',
    description: 'Deduplicate records by key',
  },
]
