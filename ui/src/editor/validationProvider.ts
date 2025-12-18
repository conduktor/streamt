/**
 * Monaco Validation Provider for Streamt YAML
 * Provides real-time validation with diagnostics markers
 */

import type { Monaco } from '@monaco-editor/react'
import type { editor } from 'monaco-editor'
import YAML from 'yaml'
import {
  MATERIALIZED_TYPES,
  TEST_TYPES,
  EXPOSURE_TYPES,
  EXPOSURE_ROLES,
  COLUMN_TYPES,
  FORMAT_TYPES,
} from './schema'

type MarkerSeverity = typeof import('monaco-editor').MarkerSeverity
type IMarkerData = editor.IMarkerData

/**
 * Quick fix suggestion for validation errors
 */
export interface QuickFix {
  label: string
  description: string
  /** YAML snippet to insert/replace */
  snippet?: string
  /** Line to insert at (if inserting new content) */
  insertAtLine?: number
  /** Replace text from startLine to endLine with snippet */
  replaceRange?: {
    startLine: number
    endLine: number
  }
}

/**
 * Extended marker data with quick fix support
 */
export interface ValidationMarker extends IMarkerData {
  quickFixes?: QuickFix[]
}

export interface ValidationResult {
  errors: ValidationMarker[]
  warnings: ValidationMarker[]
  info: ValidationMarker[]
}

interface ParsedDocument {
  content: any
  sources: Set<string>
  models: Set<string>
  lineMap: Map<string, number>
}

/**
 * Parse YAML and build line number mapping
 */
function parseYAMLWithLineNumbers(yaml: string): ParsedDocument | null {
  const lineMap = new Map<string, number>()
  const sources = new Set<string>()
  const models = new Set<string>()

  try {
    // Parse with source mapping
    const doc = YAML.parseDocument(yaml)
    const content = doc.toJS()

    // Build line mapping by scanning the document
    const lines = yaml.split('\n')
    let currentPath: string[] = []
    let inSources = false
    let inModels = false
    let currentItemIndex = -1

    lines.forEach((line, lineNum) => {
      const indent = line.search(/\S/)
      if (indent === -1) return

      // Top-level keys
      const topKeyMatch = line.match(/^(\w+):/)
      if (topKeyMatch) {
        currentPath = [topKeyMatch[1]]
        lineMap.set(topKeyMatch[1], lineNum + 1)
        inSources = topKeyMatch[1] === 'sources'
        inModels = topKeyMatch[1] === 'models'
        currentItemIndex = -1
        return
      }

      // Array items
      if (line.trim().startsWith('-')) {
        currentItemIndex++
        const itemKeyMatch = line.match(/^\s+-\s*(\w+):\s*(.*)$/)
        if (itemKeyMatch) {
          const key = itemKeyMatch[1]
          const value = itemKeyMatch[2].trim().replace(/^["']|["']$/g, '')
          const path = [...currentPath, String(currentItemIndex), key].join('.')
          lineMap.set(path, lineNum + 1)

          if (key === 'name' && value) {
            if (inSources) sources.add(value)
            if (inModels) models.add(value)
          }
        }
      }

      // Nested keys
      const nestedKeyMatch = line.match(/^\s+(\w+):/)
      if (nestedKeyMatch && !line.trim().startsWith('-')) {
        // Determine path based on indentation
        // This is a simplified version - full implementation would track indent stack
      }
    })

    return { content, sources, models, lineMap }
  } catch (e) {
    return null
  }
}

/**
 * Validate the project section
 */
function validateProject(
  project: any,
  severity: MarkerSeverity,
  lineMap: Map<string, number>
): ValidationMarker[] {
  const markers: ValidationMarker[] = []
  const line = lineMap.get('project') || 1

  if (!project) {
    markers.push({
      severity: severity.Error,
      message: 'Missing required "project" section',
      startLineNumber: 1,
      startColumn: 1,
      endLineNumber: 1,
      endColumn: 1,
      quickFixes: [{
        label: 'Add project section',
        description: 'Add a minimal project configuration',
        snippet: `project:
  name: my-pipeline
  version: "1.0.0"
  description: "My streaming data pipeline"

`,
        insertAtLine: 1,
      }],
    })
    return markers
  }

  if (!project.name) {
    markers.push({
      severity: severity.Error,
      message: 'Missing required field: project.name',
      startLineNumber: line,
      startColumn: 1,
      endLineNumber: line,
      endColumn: 100,
      quickFixes: [{
        label: 'Add name field',
        description: 'Add: name: my-pipeline',
        snippet: '  name: my-pipeline',
        insertAtLine: line + 1,
      }],
    })
  } else if (!/^[a-z][a-z0-9-_]*$/i.test(project.name)) {
    const sanitized = project.name.replace(/[^a-z0-9-_]/gi, '-').toLowerCase()
    markers.push({
      severity: severity.Warning,
      message: 'Project name should be alphanumeric with dashes/underscores',
      startLineNumber: line,
      startColumn: 1,
      endLineNumber: line,
      endColumn: 100,
      quickFixes: [{
        label: `Rename to "${sanitized}"`,
        description: 'Sanitize the project name',
        snippet: `  name: ${sanitized}`,
      }],
    })
  }

  if (!project.version) {
    markers.push({
      severity: severity.Error,
      message: 'Missing required field: project.version',
      startLineNumber: line,
      startColumn: 1,
      endLineNumber: line,
      endColumn: 100,
      quickFixes: [{
        label: 'Add version field',
        description: 'Add: version: "1.0.0"',
        snippet: '  version: "1.0.0"',
        insertAtLine: line + 1,
      }],
    })
  } else if (!/^\d+\.\d+\.\d+/.test(project.version)) {
    markers.push({
      severity: severity.Warning,
      message: 'Version should follow semantic versioning (e.g., 1.0.0)',
      startLineNumber: line,
      startColumn: 1,
      endLineNumber: line,
      endColumn: 100,
      quickFixes: [{
        label: 'Use semantic version',
        description: 'Change to: version: "1.0.0"',
        snippet: '  version: "1.0.0"',
      }],
    })
  }

  return markers
}

/**
 * Validate the runtime section
 */
function validateRuntime(
  runtime: any,
  severity: MarkerSeverity,
  lineMap: Map<string, number>
): ValidationMarker[] {
  const markers: ValidationMarker[] = []
  const line = lineMap.get('runtime') || 1

  if (!runtime) {
    markers.push({
      severity: severity.Error,
      message: 'Missing required "runtime" section',
      startLineNumber: 1,
      startColumn: 1,
      endLineNumber: 1,
      endColumn: 1,
      quickFixes: [{
        label: 'Add runtime section',
        description: 'Add minimal runtime configuration',
        snippet: `runtime:
  kafka:
    bootstrap_servers: localhost:9092
  schema_registry:
    url: http://localhost:8081

`,
        insertAtLine: 1,
      }],
    })
    return markers
  }

  if (!runtime.kafka) {
    markers.push({
      severity: severity.Error,
      message: 'Missing required section: runtime.kafka',
      startLineNumber: line,
      startColumn: 1,
      endLineNumber: line,
      endColumn: 100,
      quickFixes: [{
        label: 'Add kafka configuration',
        description: 'Add kafka bootstrap_servers',
        snippet: `  kafka:
    bootstrap_servers: localhost:9092`,
        insertAtLine: line + 1,
      }],
    })
  } else if (!runtime.kafka.bootstrap_servers) {
    markers.push({
      severity: severity.Error,
      message: 'Missing required field: runtime.kafka.bootstrap_servers',
      startLineNumber: line,
      startColumn: 1,
      endLineNumber: line,
      endColumn: 100,
      quickFixes: [{
        label: 'Add bootstrap_servers',
        description: 'Add: bootstrap_servers: localhost:9092',
        snippet: '    bootstrap_servers: localhost:9092',
        insertAtLine: line + 1,
      }],
    })
  }

  // Validate Flink configuration
  if (runtime.flink) {
    if (runtime.flink.default && runtime.flink.clusters) {
      const clusterNames = Object.keys(runtime.flink.clusters)
      if (!clusterNames.includes(runtime.flink.default)) {
        markers.push({
          severity: severity.Error,
          message: `Flink default cluster "${runtime.flink.default}" not found in clusters. Available: ${clusterNames.join(', ')}`,
          startLineNumber: line,
          startColumn: 1,
          endLineNumber: line,
          endColumn: 100,
          quickFixes: clusterNames.map(name => ({
            label: `Use "${name}"`,
            description: `Change default to: ${name}`,
            snippet: `    default: ${name}`,
          })),
        })
      }
    }
  }

  return markers
}

/**
 * Validate sources
 */
function validateSources(
  sources: any[],
  severity: MarkerSeverity,
  lineMap: Map<string, number>
): ValidationMarker[] {
  const markers: ValidationMarker[] = []
  const names = new Set<string>()

  if (!sources || !Array.isArray(sources)) return markers

  sources.forEach((source, index) => {
    const line = lineMap.get(`sources.${index}.name`) || lineMap.get('sources') || 1

    if (!source.name) {
      markers.push({
        severity: severity.Error,
        message: `Source #${index + 1}: Missing required field "name"`,
        startLineNumber: line,
        startColumn: 1,
        endLineNumber: line,
        endColumn: 100,
        quickFixes: [{
          label: 'Add name field',
          description: `Add: name: source_${index + 1}`,
          snippet: `    name: source_${index + 1}`,
          insertAtLine: line,
        }],
      })
    } else {
      if (names.has(source.name)) {
        markers.push({
          severity: severity.Error,
          message: `Duplicate source name: "${source.name}"`,
          startLineNumber: line,
          startColumn: 1,
          endLineNumber: line,
          endColumn: 100,
          quickFixes: [{
            label: `Rename to "${source.name}_${index + 1}"`,
            description: 'Make the name unique by adding a suffix',
          }],
        })
      }
      names.add(source.name)
    }

    if (!source.topic) {
      const suggestedTopic = source.name ? `${source.name.replace(/_/g, '.')}.v1` : `topic_${index + 1}`
      markers.push({
        severity: severity.Error,
        message: `Source "${source.name || index + 1}": Missing required field "topic"`,
        startLineNumber: line,
        startColumn: 1,
        endLineNumber: line,
        endColumn: 100,
        quickFixes: [{
          label: 'Add topic field',
          description: `Add: topic: ${suggestedTopic}`,
          snippet: `    topic: ${suggestedTopic}`,
          insertAtLine: line + 1,
        }],
      })
    }

    if (!source.columns || source.columns.length === 0) {
      markers.push({
        severity: severity.Warning,
        message: `Source "${source.name || index + 1}": No columns defined`,
        startLineNumber: line,
        startColumn: 1,
        endLineNumber: line,
        endColumn: 100,
        quickFixes: [{
          label: 'Add sample columns',
          description: 'Add common column definitions',
          snippet: `    columns:
      - name: id
        type: STRING
      - name: timestamp
        type: TIMESTAMP`,
          insertAtLine: line + 1,
        }],
      })
    } else if (Array.isArray(source.columns)) {
      source.columns.forEach((col: any, colIndex: number) => {
        if (!col.name) {
          markers.push({
            severity: severity.Error,
            message: `Source "${source.name}": Column #${colIndex + 1} missing "name"`,
            startLineNumber: line,
            startColumn: 1,
            endLineNumber: line,
            endColumn: 100,
            quickFixes: [{
              label: 'Add column name',
              description: `Add: name: column_${colIndex + 1}`,
            }],
          })
        }
        if (!col.type) {
          markers.push({
            severity: severity.Error,
            message: `Source "${source.name}": Column "${col.name || colIndex + 1}" missing "type"`,
            startLineNumber: line,
            startColumn: 1,
            endLineNumber: line,
            endColumn: 100,
            quickFixes: [
              { label: 'Use STRING', description: 'Add: type: STRING' },
              { label: 'Use BIGINT', description: 'Add: type: BIGINT' },
              { label: 'Use TIMESTAMP', description: 'Add: type: TIMESTAMP' },
            ],
          })
        } else if (!COLUMN_TYPES.includes(col.type)) {
          // Find similar type
          const upperType = col.type.toUpperCase()
          const similarType = COLUMN_TYPES.find(t => t.startsWith(upperType.slice(0, 3)))
          markers.push({
            severity: severity.Warning,
            message: `Source "${source.name}": Unknown column type "${col.type}"`,
            startLineNumber: line,
            startColumn: 1,
            endLineNumber: line,
            endColumn: 100,
            quickFixes: similarType
              ? [{ label: `Use ${similarType}`, description: `Change to: type: ${similarType}` }]
              : COLUMN_TYPES.slice(0, 4).map(t => ({ label: `Use ${t}`, description: `Change to: type: ${t}` })),
          })
        }
      })
    }

    // Validate format
    if (source.format && !FORMAT_TYPES.includes(source.format)) {
      markers.push({
        severity: severity.Warning,
        message: `Source "${source.name}": Unknown format "${source.format}". Expected one of: ${FORMAT_TYPES.join(', ')}`,
        startLineNumber: line,
        startColumn: 1,
        endLineNumber: line,
        endColumn: 100,
        quickFixes: FORMAT_TYPES.map(f => ({
          label: `Use ${f}`,
          description: `Change to: format: ${f}`,
        })),
      })
    }
  })

  return markers
}

/**
 * Validate models
 */
function validateModels(
  models: any[],
  sources: Set<string>,
  severity: MarkerSeverity,
  lineMap: Map<string, number>
): ValidationMarker[] {
  const markers: ValidationMarker[] = []
  const names = new Set<string>()

  if (!models || !Array.isArray(models)) return markers

  models.forEach((model, index) => {
    const line = lineMap.get(`models.${index}.name`) || lineMap.get('models') || 1

    if (!model.name) {
      markers.push({
        severity: severity.Error,
        message: `Model #${index + 1}: Missing required field "name"`,
        startLineNumber: line,
        startColumn: 1,
        endLineNumber: line,
        endColumn: 100,
        quickFixes: [{
          label: 'Add name field',
          description: `Add: name: model_${index + 1}`,
          snippet: `    name: model_${index + 1}`,
          insertAtLine: line,
        }],
      })
    } else {
      if (names.has(model.name)) {
        markers.push({
          severity: severity.Error,
          message: `Duplicate model name: "${model.name}"`,
          startLineNumber: line,
          startColumn: 1,
          endLineNumber: line,
          endColumn: 100,
          quickFixes: [{
            label: `Rename to "${model.name}_${index + 1}"`,
            description: 'Make the name unique by adding a suffix',
          }],
        })
      }
      names.add(model.name)
    }

    // Validate materialized
    if (!model.materialized) {
      markers.push({
        severity: severity.Error,
        message: `Model "${model.name || index + 1}": Missing required field "materialized"`,
        startLineNumber: line,
        startColumn: 1,
        endLineNumber: line,
        endColumn: 100,
        quickFixes: MATERIALIZED_TYPES.map(t => ({
          label: `Use ${t}`,
          description: t === 'topic' ? 'Stateless transformation → Kafka topic' :
                       t === 'flink' ? 'Stateful processing with Flink job' :
                       t === 'virtual_topic' ? 'Read-time filter via Gateway' :
                       'Export to external system',
          snippet: `    materialized: ${t}`,
          insertAtLine: line + 1,
        })),
      })
    } else if (!MATERIALIZED_TYPES.includes(model.materialized)) {
      markers.push({
        severity: severity.Error,
        message: `Model "${model.name}": Invalid materialization "${model.materialized}". Expected one of: ${MATERIALIZED_TYPES.join(', ')}`,
        startLineNumber: line,
        startColumn: 1,
        endLineNumber: line,
        endColumn: 100,
        quickFixes: MATERIALIZED_TYPES.map(t => ({
          label: `Use ${t}`,
          description: `Change to: materialized: ${t}`,
        })),
      })
    }

    // Validate SQL (required for non-sink)
    if (model.materialized !== 'sink' && !model.sql) {
      const sourceName = sources.size > 0 ? Array.from(sources)[0] : 'your_source'
      markers.push({
        severity: severity.Error,
        message: `Model "${model.name || index + 1}": Missing required field "sql"`,
        startLineNumber: line,
        startColumn: 1,
        endLineNumber: line,
        endColumn: 100,
        quickFixes: [{
          label: 'Add SQL template',
          description: 'Add a basic SELECT statement',
          snippet: `    sql: |
      SELECT *
      FROM {{ source("${sourceName}") }}`,
          insertAtLine: line + 1,
        }],
      })
    }

    // Validate SQL references
    if (model.sql) {
      // Check source references
      const sourceRefs = model.sql.matchAll(/\{\{\s*source\s*\(\s*["'](\w+)["']\s*\)/g)
      for (const match of sourceRefs) {
        const refName = match[1]
        if (!sources.has(refName)) {
          const availableSources = Array.from(sources)
          markers.push({
            severity: severity.Error,
            message: `Model "${model.name}": References undefined source "${refName}"`,
            startLineNumber: line,
            startColumn: 1,
            endLineNumber: line,
            endColumn: 100,
            quickFixes: availableSources.length > 0
              ? availableSources.map(s => ({
                  label: `Use source("${s}")`,
                  description: `Change reference to existing source: ${s}`,
                }))
              : [{
                  label: 'Create source first',
                  description: `Add a source named "${refName}" to the sources section`,
                }],
          })
        }
      }

      // Check model references (forward references are allowed)
      const modelRefs = model.sql.matchAll(/\{\{\s*ref\s*\(\s*["'](\w+)["']\s*\)/g)
      for (const match of modelRefs) {
        const refName = match[1]
        const allModelNames = models.map(m => m.name).filter(Boolean)
        if (!allModelNames.includes(refName)) {
          markers.push({
            severity: severity.Error,
            message: `Model "${model.name}": References undefined model "${refName}"`,
            startLineNumber: line,
            startColumn: 1,
            endLineNumber: line,
            endColumn: 100,
            quickFixes: allModelNames.length > 0
              ? allModelNames.filter(n => n !== model.name).map(m => ({
                  label: `Use ref("${m}")`,
                  description: `Change reference to existing model: ${m}`,
                }))
              : [{
                  label: `Use source() instead`,
                  description: 'Reference a source if no upstream model exists',
                }],
          })
        }
        // Check for self-reference
        if (refName === model.name) {
          markers.push({
            severity: severity.Error,
            message: `Model "${model.name}": Self-reference detected - a model cannot reference itself`,
            startLineNumber: line,
            startColumn: 1,
            endLineNumber: line,
            endColumn: 100,
            quickFixes: [{
              label: 'Remove self-reference',
              description: 'Reference a different model or source instead',
            }],
          })
        }
      }
    }

    // Validate virtual_topic configuration
    if (model.materialized === 'virtual_topic') {
      if (!model.virtual_topic?.physical_topic) {
        markers.push({
          severity: severity.Error,
          message: `Model "${model.name}": virtual_topic materialization requires virtual_topic.physical_topic`,
          startLineNumber: line,
          startColumn: 1,
          endLineNumber: line,
          endColumn: 100,
          quickFixes: [{
            label: 'Add virtual_topic config',
            description: 'Add the required virtual_topic configuration',
            snippet: `    virtual_topic:
      physical_topic: your-physical-topic
      gateway: gateway-1`,
            insertAtLine: line + 1,
          }],
        })
      }
    }

    // Validate sink configuration
    if (model.materialized === 'sink') {
      if (!model.sink?.type) {
        markers.push({
          severity: severity.Error,
          message: `Model "${model.name}": sink materialization requires sink.type`,
          startLineNumber: line,
          startColumn: 1,
          endLineNumber: line,
          endColumn: 100,
          quickFixes: [
            {
              label: 'Add JDBC sink',
              description: 'Export to PostgreSQL/MySQL database',
              snippet: `    sink:
      type: jdbc
      url: jdbc:postgresql://localhost:5432/mydb
      table: ${model.name || 'my_table'}`,
              insertAtLine: line + 1,
            },
            {
              label: 'Add Elasticsearch sink',
              description: 'Export to Elasticsearch index',
              snippet: `    sink:
      type: elasticsearch
      hosts: http://localhost:9200
      index: ${model.name || 'my_index'}`,
              insertAtLine: line + 1,
            },
          ],
        })
      }
    }

    // Validate flink configuration
    if (model.flink) {
      if (model.flink.parallelism && (model.flink.parallelism < 1 || model.flink.parallelism > 1000)) {
        markers.push({
          severity: severity.Warning,
          message: `Model "${model.name}": Unusual parallelism value ${model.flink.parallelism}. Typical range is 1-100.`,
          startLineNumber: line,
          startColumn: 1,
          endLineNumber: line,
          endColumn: 100,
          quickFixes: [
            { label: 'Use parallelism: 1', description: 'Single task for low volume' },
            { label: 'Use parallelism: 4', description: 'Moderate parallelism' },
            { label: 'Use parallelism: 8', description: 'Higher parallelism for throughput' },
          ],
        })
      }
    }
  })

  return markers
}

/**
 * Validate tests
 */
function validateTests(
  tests: any[],
  models: Set<string>,
  sources: Set<string>,
  severity: MarkerSeverity,
  lineMap: Map<string, number>
): ValidationMarker[] {
  const markers: ValidationMarker[] = []
  const names = new Set<string>()

  if (!tests || !Array.isArray(tests)) return markers

  tests.forEach((test, index) => {
    const line = lineMap.get(`tests.${index}.name`) || lineMap.get('tests') || 1

    if (!test.name) {
      markers.push({
        severity: severity.Error,
        message: `Test #${index + 1}: Missing required field "name"`,
        startLineNumber: line,
        startColumn: 1,
        endLineNumber: line,
        endColumn: 100,
        quickFixes: [{
          label: 'Add name field',
          description: `Add: name: test_${index + 1}`,
          snippet: `    name: test_${index + 1}`,
          insertAtLine: line,
        }],
      })
    } else {
      if (names.has(test.name)) {
        markers.push({
          severity: severity.Error,
          message: `Duplicate test name: "${test.name}"`,
          startLineNumber: line,
          startColumn: 1,
          endLineNumber: line,
          endColumn: 100,
          quickFixes: [{
            label: `Rename to "${test.name}_${index + 1}"`,
            description: 'Make the name unique by adding a suffix',
          }],
        })
      }
      names.add(test.name)
    }

    if (!test.model) {
      const allTargets = [...Array.from(models), ...Array.from(sources)]
      markers.push({
        severity: severity.Error,
        message: `Test "${test.name || index + 1}": Missing required field "model"`,
        startLineNumber: line,
        startColumn: 1,
        endLineNumber: line,
        endColumn: 100,
        quickFixes: allTargets.length > 0
          ? allTargets.slice(0, 4).map(t => ({
              label: `Test ${t}`,
              description: `Add: model: ${t}`,
              snippet: `    model: ${t}`,
              insertAtLine: line + 1,
            }))
          : [{
              label: 'Add model field',
              description: 'Add: model: your_model',
              snippet: '    model: your_model',
              insertAtLine: line + 1,
            }],
      })
    } else if (!models.has(test.model) && !sources.has(test.model)) {
      const allTargets = [...Array.from(models), ...Array.from(sources)]
      markers.push({
        severity: severity.Warning,
        message: `Test "${test.name}": Model/source "${test.model}" not found`,
        startLineNumber: line,
        startColumn: 1,
        endLineNumber: line,
        endColumn: 100,
        quickFixes: allTargets.length > 0
          ? allTargets.map(t => ({
              label: `Use ${t}`,
              description: `Change to: model: ${t}`,
            }))
          : [{
              label: 'Create model/source first',
              description: `Define "${test.model}" in models or sources section`,
            }],
      })
    }

    if (!test.type) {
      markers.push({
        severity: severity.Error,
        message: `Test "${test.name || index + 1}": Missing required field "type"`,
        startLineNumber: line,
        startColumn: 1,
        endLineNumber: line,
        endColumn: 100,
        quickFixes: TEST_TYPES.map(t => ({
          label: `Use ${t}`,
          description: t === 'schema' ? 'One-time schema validation' :
                       t === 'sample' ? 'Sample N messages and check assertions' :
                       'Continuous Flink monitoring job',
          snippet: `    type: ${t}`,
          insertAtLine: line + 1,
        })),
      })
    } else if (!TEST_TYPES.includes(test.type)) {
      markers.push({
        severity: severity.Error,
        message: `Test "${test.name}": Invalid type "${test.type}". Expected one of: ${TEST_TYPES.join(', ')}`,
        startLineNumber: line,
        startColumn: 1,
        endLineNumber: line,
        endColumn: 100,
        quickFixes: TEST_TYPES.map(t => ({
          label: `Use ${t}`,
          description: `Change to: type: ${t}`,
        })),
      })
    }

    if (!test.assertions || test.assertions.length === 0) {
      markers.push({
        severity: severity.Warning,
        message: `Test "${test.name || index + 1}": No assertions defined`,
        startLineNumber: line,
        startColumn: 1,
        endLineNumber: line,
        endColumn: 100,
        quickFixes: [{
          label: 'Add not_null assertion',
          description: 'Check that columns are not null',
          snippet: `    assertions:
      - not_null:
          columns: [id]`,
          insertAtLine: line + 1,
        }, {
          label: 'Add unique assertion',
          description: 'Check that columns are unique',
          snippet: `    assertions:
      - unique:
          columns: [id]`,
          insertAtLine: line + 1,
        }],
      })
    }
  })

  return markers
}

/**
 * Validate exposures
 */
function validateExposures(
  exposures: any[],
  models: Set<string>,
  sources: Set<string>,
  severity: MarkerSeverity,
  lineMap: Map<string, number>
): ValidationMarker[] {
  const markers: ValidationMarker[] = []
  const names = new Set<string>()

  if (!exposures || !Array.isArray(exposures)) return markers

  exposures.forEach((exposure, index) => {
    const line = lineMap.get(`exposures.${index}.name`) || lineMap.get('exposures') || 1

    if (!exposure.name) {
      markers.push({
        severity: severity.Error,
        message: `Exposure #${index + 1}: Missing required field "name"`,
        startLineNumber: line,
        startColumn: 1,
        endLineNumber: line,
        endColumn: 100,
        quickFixes: [{
          label: 'Add name field',
          description: `Add: name: exposure_${index + 1}`,
          snippet: `    name: exposure_${index + 1}`,
          insertAtLine: line,
        }],
      })
    } else {
      if (names.has(exposure.name)) {
        markers.push({
          severity: severity.Error,
          message: `Duplicate exposure name: "${exposure.name}"`,
          startLineNumber: line,
          startColumn: 1,
          endLineNumber: line,
          endColumn: 100,
          quickFixes: [{
            label: `Rename to "${exposure.name}_${index + 1}"`,
            description: 'Make the name unique by adding a suffix',
          }],
        })
      }
      names.add(exposure.name)
    }

    if (!exposure.type) {
      markers.push({
        severity: severity.Error,
        message: `Exposure "${exposure.name || index + 1}": Missing required field "type"`,
        startLineNumber: line,
        startColumn: 1,
        endLineNumber: line,
        endColumn: 100,
        quickFixes: EXPOSURE_TYPES.map(t => ({
          label: `Use ${t}`,
          description: t === 'dashboard' ? 'Real-time visualization' :
                       t === 'application' ? 'Backend service consumer' :
                       t === 'ml_model' ? 'Machine learning pipeline' :
                       t === 'notebook' ? 'Analytics notebook' :
                       'Automated reporting',
          snippet: `    type: ${t}`,
          insertAtLine: line + 1,
        })),
      })
    } else if (!EXPOSURE_TYPES.includes(exposure.type)) {
      markers.push({
        severity: severity.Error,
        message: `Exposure "${exposure.name}": Invalid type "${exposure.type}". Expected one of: ${EXPOSURE_TYPES.join(', ')}`,
        startLineNumber: line,
        startColumn: 1,
        endLineNumber: line,
        endColumn: 100,
        quickFixes: EXPOSURE_TYPES.map(t => ({
          label: `Use ${t}`,
          description: `Change to: type: ${t}`,
        })),
      })
    }

    if (exposure.role && !EXPOSURE_ROLES.includes(exposure.role)) {
      markers.push({
        severity: severity.Warning,
        message: `Exposure "${exposure.name}": Invalid role "${exposure.role}". Expected one of: ${EXPOSURE_ROLES.join(', ')}`,
        startLineNumber: line,
        startColumn: 1,
        endLineNumber: line,
        endColumn: 100,
        quickFixes: EXPOSURE_ROLES.map(r => ({
          label: `Use ${r}`,
          description: `Change to: role: ${r}`,
        })),
      })
    }

    // Validate depends_on references
    if (exposure.depends_on && Array.isArray(exposure.depends_on)) {
      exposure.depends_on.forEach((dep: any) => {
        if (dep.ref && !models.has(dep.ref)) {
          const availableModels = Array.from(models)
          markers.push({
            severity: severity.Warning,
            message: `Exposure "${exposure.name}": References undefined model "${dep.ref}"`,
            startLineNumber: line,
            startColumn: 1,
            endLineNumber: line,
            endColumn: 100,
            quickFixes: availableModels.length > 0
              ? availableModels.map(m => ({
                  label: `Use ref: ${m}`,
                  description: `Change reference to: ${m}`,
                }))
              : [{
                  label: 'Create model first',
                  description: `Define "${dep.ref}" in the models section`,
                }],
          })
        }
        if (dep.source && !sources.has(dep.source)) {
          const availableSources = Array.from(sources)
          markers.push({
            severity: severity.Warning,
            message: `Exposure "${exposure.name}": References undefined source "${dep.source}"`,
            startLineNumber: line,
            startColumn: 1,
            endLineNumber: line,
            endColumn: 100,
            quickFixes: availableSources.length > 0
              ? availableSources.map(s => ({
                  label: `Use source: ${s}`,
                  description: `Change reference to: ${s}`,
                }))
              : [{
                  label: 'Create source first',
                  description: `Define "${dep.source}" in the sources section`,
                }],
          })
        }
      })
    }
  })

  return markers
}

/**
 * Main validation function
 */
export function validateYAML(
  yaml: string,
  monaco: Monaco
): ValidationResult {
  const result: ValidationResult = {
    errors: [],
    warnings: [],
    info: [],
  }

  const severity = monaco.MarkerSeverity

  // First try to parse YAML
  let parsed: any
  try {
    parsed = YAML.parse(yaml)
  } catch (e: any) {
    // YAML syntax error
    const match = e.message?.match(/at line (\d+)/)
    const line = match ? parseInt(match[1]) : 1

    result.errors.push({
      severity: severity.Error,
      message: `YAML syntax error: ${e.message}`,
      startLineNumber: line,
      startColumn: 1,
      endLineNumber: line,
      endColumn: 100,
    })
    return result
  }

  if (!parsed || typeof parsed !== 'object') {
    result.errors.push({
      severity: severity.Error,
      message: 'Invalid YAML: Document must be an object',
      startLineNumber: 1,
      startColumn: 1,
      endLineNumber: 1,
      endColumn: 1,
    })
    return result
  }

  // Parse with line numbers
  const doc = parseYAMLWithLineNumbers(yaml)
  const lineMap = doc?.lineMap || new Map()
  const definedSources = doc?.sources || new Set()
  const definedModels = doc?.models || new Set()

  // Validate each section
  result.errors.push(...validateProject(parsed.project, severity, lineMap))
  result.errors.push(...validateRuntime(parsed.runtime, severity, lineMap))
  result.errors.push(...validateSources(parsed.sources, severity, lineMap))
  result.errors.push(...validateModels(parsed.models, definedSources, severity, lineMap))
  result.errors.push(...validateTests(parsed.tests, definedModels, definedSources, severity, lineMap))
  result.errors.push(...validateExposures(parsed.exposures, definedModels, definedSources, severity, lineMap))

  // Separate warnings from errors
  result.warnings = result.errors.filter(m => m.severity === severity.Warning)
  result.errors = result.errors.filter(m => m.severity === severity.Error)

  // Add info markers for good practices
  if (parsed.sources?.length > 0 && parsed.tests?.length === 0) {
    result.info.push({
      severity: severity.Info,
      message: 'Consider adding tests to validate your data quality',
      startLineNumber: 1,
      startColumn: 1,
      endLineNumber: 1,
      endColumn: 1,
    })
  }

  return result
}

/**
 * Set up validation with debouncing
 */
export function setupValidation(
  monaco: Monaco,
  editor: editor.IStandaloneCodeEditor,
  onValidate?: (result: ValidationResult) => void
): () => void {
  let timeoutId: NodeJS.Timeout | null = null

  const validate = () => {
    const model = editor.getModel()
    if (!model) return

    const yaml = model.getValue()
    const result = validateYAML(yaml, monaco)

    // Set markers on the model
    const allMarkers = [
      ...result.errors,
      ...result.warnings,
      ...result.info,
    ]

    monaco.editor.setModelMarkers(model, 'streamt', allMarkers)

    // Callback for external handling
    if (onValidate) {
      onValidate(result)
    }
  }

  // Initial validation
  validate()

  // Subscribe to changes with debounce
  const disposable = editor.onDidChangeModelContent(() => {
    if (timeoutId) {
      clearTimeout(timeoutId)
    }
    timeoutId = setTimeout(validate, 300)
  })

  // Return cleanup function
  return () => {
    if (timeoutId) {
      clearTimeout(timeoutId)
    }
    disposable.dispose()
  }
}
