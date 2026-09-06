/**
 * Monaco Hover Provider for Streamt YAML
 * Provides documentation on hover for keys, values, and Jinja templates
 */

import type { Monaco } from '@monaco-editor/react'
import type { languages, editor, Position } from 'monaco-editor'
import {
  STREAMT_SCHEMA,
  SchemaField,
  COLUMN_TYPES,
  MATERIALIZED_TYPES,
  TEST_TYPES,
} from './schema'

type Hover = languages.Hover

interface HoverContext {
  path: string[]
  key: string | null
  value: string | null
  isJinja: boolean
  jinjaFunction: string | null
  jinjaArg: string | null
}

/**
 * Parse the YAML context at hover position
 */
function getHoverContext(
  model: editor.ITextModel,
  position: Position
): HoverContext {
  const lines = model.getLinesContent()
  const currentLine = position.lineNumber - 1
  const lineContent = lines[currentLine] || ''
  const col = position.column

  // Check for Jinja template hover
  let isJinja = false
  let jinjaFunction: string | null = null
  let jinjaArg: string | null = null

  const jinjaMatch = lineContent.match(/\{\{\s*(\w+)\s*\(\s*["']([^"']+)["']\s*\)/)
  if (jinjaMatch) {
    const jinjaStart = lineContent.indexOf('{{')
    const jinjaEnd = lineContent.indexOf('}}') + 2
    if (col > jinjaStart && col < jinjaEnd) {
      isJinja = true
      jinjaFunction = jinjaMatch[1]
      jinjaArg = jinjaMatch[2]
    }
  }

  // Extract key and value from line
  let key: string | null = null
  let value: string | null = null

  const keyValueMatch = lineContent.match(/^\s*-?\s*(\w+):\s*(.*)$/)
  if (keyValueMatch) {
    key = keyValueMatch[1]
    value = keyValueMatch[2].trim().replace(/^["']|["']$/g, '')
  }

  // Build path by walking up
  const path: string[] = []
  const lineIndent = lineContent.search(/\S/)
  let currentIndent = lineIndent === -1 ? 0 : lineIndent

  for (let i = currentLine - 1; i >= 0; i--) {
    const line = lines[i]
    const indent = line.search(/\S/)

    if (indent === -1) continue

    if (indent < currentIndent) {
      const match = line.match(/^\s*-?\s*(\w+):/)
      if (match) {
        path.unshift(match[1])
        currentIndent = indent
      }
    }

    if (indent === 0 && path.length > 0) break
  }

  return { path, key, value, isJinja, jinjaFunction, jinjaArg }
}

/**
 * Get schema field for a path
 */
function getSchemaField(path: string[], key: string | null): SchemaField | null {
  if (path.length === 0 && key) {
    return STREAMT_SCHEMA[key] || null
  }

  let current: any = STREAMT_SCHEMA

  for (const segment of path) {
    if (current[segment]) {
      current = current[segment]
    } else if (current.children?.[segment]) {
      current = current.children[segment]
    } else if (current.itemSchema?.children?.[segment]) {
      current = current.itemSchema.children[segment]
    } else if (current.children?.['_dynamic_']) {
      current = current.children['_dynamic_']
    } else if (current.itemSchema) {
      current = current.itemSchema
    } else {
      return null
    }
  }

  if (key) {
    if (current.children?.[key]) {
      return current.children[key]
    } else if (current.itemSchema?.children?.[key]) {
      return current.itemSchema.children[key]
    }
  }

  return current as SchemaField
}

/**
 * Build markdown documentation for a schema field
 */
function buildSchemaDocumentation(field: SchemaField): string {
  const lines: string[] = []

  lines.push(`## ${field.name}`)
  lines.push('')
  lines.push(field.description)
  lines.push('')

  if (field.required) {
    lines.push('**Required**')
    lines.push('')
  }

  lines.push(`**Type:** \`${field.type}\``)

  if (field.enum && field.enum.length > 0) {
    lines.push('')
    lines.push('**Allowed values:**')
    field.enum.forEach(v => lines.push(`- \`${v}\``))
  }

  if (field.default !== undefined) {
    lines.push('')
    lines.push(`**Default:** \`${field.default}\``)
  }

  if (field.examples && field.examples.length > 0) {
    lines.push('')
    lines.push('**Examples:**')
    field.examples.forEach(ex => lines.push(`- \`${ex}\``))
  }

  if (field.since) {
    lines.push('')
    lines.push(`*Available since version ${field.since}*`)
  }

  if (field.deprecated) {
    lines.push('')
    lines.push('⚠️ **Deprecated** - This field may be removed in a future version')
  }

  return lines.join('\n')
}

/**
 * Build documentation for Jinja functions
 */
function buildJinjaDocumentation(func: string, arg: string | null): string {
  switch (func) {
    case 'source':
      return [
        '## {{ source("name") }}',
        '',
        'Reference an external data source defined in the `sources` section.',
        '',
        'The source function creates a dependency on the specified source and',
        'generates the appropriate Flink SQL table reference.',
        '',
        '**Example:**',
        '```sql',
        'SELECT * FROM {{ source("raw_events") }}',
        '```',
        '',
        arg ? `Current reference: **${arg}**` : '',
      ].join('\n')

    case 'ref':
      return [
        '## {{ ref("model") }}',
        '',
        'Reference another model, creating a dependency relationship.',
        '',
        'The ref function ensures that the referenced model is built',
        'before this model and generates the appropriate table reference.',
        '',
        '**Example:**',
        '```sql',
        'SELECT * FROM {{ ref("validated_events") }}',
        '```',
        '',
        arg ? `Current reference: **${arg}**` : '',
      ].join('\n')

    case 'var':
      return [
        '## {{ var("name", "default") }}',
        '',
        'Reference a project variable with an optional default value.',
        '',
        'Variables can be set at runtime to customize pipeline behavior',
        'without modifying the YAML configuration.',
        '',
        '**Example:**',
        '```sql',
        'WHERE environment = {{ var("env", "production") }}',
        '```',
      ].join('\n')

    default:
      return `Jinja function: ${func}`
  }
}

/**
 * Build documentation for specific values
 */
function buildValueDocumentation(key: string, value: string, path: string[]): string | null {
  // Materialization types
  if (key === 'materialized' && MATERIALIZED_TYPES.includes(value as any)) {
    const docs: Record<string, string> = {
      topic: [
        '## Materialization: topic',
        '',
        'Creates a Kafka topic with the transformation results.',
        '',
        '**Use case:** Stateless transformations, filtering, enrichment',
        '',
        '**Implementation:** Single Flink INSERT statement',
        '',
        '**Characteristics:**',
        '- Low latency',
        '- No state management overhead',
        '- Results immediately available downstream',
      ].join('\n'),
      flink: [
        '## Materialization: flink',
        '',
        'Creates a persistent Flink job for stateful processing.',
        '',
        '**Use case:** Aggregations, windowing, joins, complex event processing',
        '',
        '**Implementation:** Long-running Flink job with checkpointing',
        '',
        '**Characteristics:**',
        '- Stateful processing with exactly-once semantics',
        '- Automatic checkpoint and recovery',
        '- Configurable parallelism and state TTL',
      ].join('\n'),
      virtual_topic: [
        '## Materialization: virtual_topic',
        '',
        'Creates a Conduktor Gateway virtual topic with read-time filtering.',
        '',
        '**Use case:** Row-level access control, data filtering without duplication',
        '',
        '**Implementation:** Gateway interceptor with SQL filter',
        '',
        '**Characteristics:**',
        '- No data duplication',
        '- Filter applied at read time',
        '- Requires Conduktor Gateway',
      ].join('\n'),
      sink: [
        '## Materialization: sink',
        '',
        'Exports data to external systems via Kafka Connect.',
        '',
        '**Use case:** Data export to databases, data lakes, search engines',
        '',
        '**Implementation:** Kafka Connect sink connector',
        '',
        '**Supported sinks:**',
        '- JDBC (PostgreSQL, MySQL, etc.)',
        '- Elasticsearch',
        '- Filesystem (S3, HDFS)',
        '- And more via custom connectors',
      ].join('\n'),
    }
    return docs[value] || null
  }

  // Test types
  if (key === 'type' && path.includes('tests') && TEST_TYPES.includes(value as any)) {
    const docs: Record<string, string> = {
      schema: [
        '## Test Type: schema',
        '',
        'Validates data structure against Schema Registry.',
        '',
        '**Execution:** One-time validation at build time',
        '',
        '**Use case:** Ensure schema compatibility and field presence',
      ].join('\n'),
      sample: [
        '## Test Type: sample',
        '',
        'Consumes N messages and validates assertions.',
        '',
        '**Execution:** Samples `sample_size` messages from the topic',
        '',
        '**Use case:** Data quality checks on a subset of data',
        '',
        '**Configuration:**',
        '- `sample_size`: Number of messages to consume (default: 100)',
        '- `timeout_seconds`: Maximum execution time (default: 60)',
      ].join('\n'),
      continuous: [
        '## Test Type: continuous',
        '',
        'Long-running Flink job for continuous data quality monitoring.',
        '',
        '**Execution:** Persistent streaming job checking all records',
        '',
        '**Use case:** Real-time anomaly detection and quality monitoring',
        '',
        '**Features:**',
        '- Monitors all records in real-time',
        '- Can trigger alerts on failure',
        '- Stateful for tracking trends',
      ].join('\n'),
    }
    return docs[value] || null
  }

  // Column types
  if (key === 'type' && path.some(p => p === 'columns') && COLUMN_TYPES.includes(value as any)) {
    const docs: Record<string, string> = {
      STRING: 'Variable-length character string (equivalent to VARCHAR)',
      VARCHAR: 'Variable-length character string with optional max length',
      BOOLEAN: 'Boolean value (true/false)',
      INT: '32-bit signed integer (-2^31 to 2^31-1)',
      INTEGER: '32-bit signed integer (alias for INT)',
      BIGINT: '64-bit signed integer (-2^63 to 2^63-1)',
      FLOAT: '32-bit IEEE 754 floating point',
      DOUBLE: '64-bit IEEE 754 floating point',
      DECIMAL: 'Exact numeric with configurable precision and scale',
      TIMESTAMP: 'Timestamp without timezone (SQL standard)',
      TIMESTAMP_LTZ: 'Timestamp with local timezone',
      DATE: 'Date without time component (year-month-day)',
      TIME: 'Time without date component (hour:minute:second)',
      BYTES: 'Binary string (byte array)',
      ARRAY: 'Ordered collection of elements of the same type',
      MAP: 'Key-value pairs (associative array)',
      ROW: 'Structured type with named fields',
    }
    return docs[value] ? `## ${value}\n\nFlink SQL type: ${docs[value]}` : null
  }

  return null
}

/**
 * Register the hover provider with Monaco
 */
export function registerHoverProvider(monaco: Monaco): void {
  monaco.languages.registerHoverProvider('yaml', {
    provideHover(
      model: editor.ITextModel,
      position: Position
    ): Hover | null {
      const context = getHoverContext(model, position)

      // Jinja template hover
      if (context.isJinja && context.jinjaFunction) {
        return {
          contents: [
            {
              value: buildJinjaDocumentation(context.jinjaFunction, context.jinjaArg),
            },
          ],
        }
      }

      // Value documentation
      if (context.key && context.value) {
        const valueDoc = buildValueDocumentation(context.key, context.value, context.path)
        if (valueDoc) {
          return {
            contents: [{ value: valueDoc }],
          }
        }
      }

      // Key documentation
      if (context.key) {
        const field = getSchemaField(context.path, context.key)
        if (field) {
          return {
            contents: [
              {
                value: buildSchemaDocumentation(field),
              },
            ],
          }
        }
      }

      return null
    },
  })
}
