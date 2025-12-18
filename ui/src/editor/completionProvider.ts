/**
 * Monaco Completion Provider for Streamt YAML
 * Provides intelligent, context-aware autocompletion
 */

import type { Monaco } from '@monaco-editor/react'
import type { languages, editor, Position, IRange } from 'monaco-editor'
import * as YAML from 'yaml'
import { Parser } from 'node-sql-parser'
import {
  STREAMT_SCHEMA,
  COLUMN_TYPES,
  MATERIALIZED_TYPES,
  TEST_TYPES,
  EXPOSURE_TYPES,
  EXPOSURE_ROLES,
  FORMAT_TYPES,
  CLEANUP_POLICIES,
  SQL_SNIPPETS,
  SchemaField,
} from './schema'

type CompletionItem = languages.CompletionItem
type CompletionList = languages.CompletionList

interface SchemaDefinition {
  name: string
  columns: string[]
}

interface YAMLContext {
  path: string[]
  indent: number
  isValue: boolean
  isArrayItem: boolean
  currentKey: string | null
  lineContent: string
  sources: Map<string, SchemaDefinition>
  models: Map<string, SchemaDefinition>
}

// SQL Parser instance (reused for performance)
const sqlParser = new Parser()

/**
 * Extract column names from a SQL SELECT clause using proper SQL parser
 * Falls back to regex for Flink-specific syntax not supported by the parser
 */
function extractColumnsFromSql(sql: string): string[] {
  // First, preprocess SQL to handle Jinja templates
  // Replace {{ source("x") }} and {{ ref("x") }} with placeholder table names
  const preprocessedSql = sql
    .replace(/\{\{\s*source\s*\(\s*["']([^"']+)["']\s*\)\s*\}\}/gi, '_source_$1')
    .replace(/\{\{\s*ref\s*\(\s*["']([^"']+)["']\s*\)\s*\}\}/gi, '_ref_$1')

  try {
    // Try parsing with PostgreSQL dialect (closest to Flink SQL)
    const ast = sqlParser.astify(preprocessedSql, { database: 'postgresql' })

    // Handle single statement or array of statements
    const stmt = Array.isArray(ast) ? ast[0] : ast

    if (stmt && stmt.type === 'select' && stmt.columns) {
      return extractColumnsFromAst(stmt.columns)
    }
  } catch (e) {
    console.log('[SQL Parser] Failed to parse, falling back to regex:', e)
  }

  // Fallback to regex-based extraction for Flink-specific syntax
  return extractColumnsFromSqlRegex(sql)
}

/**
 * Extract column names from parsed AST columns array
 */
function extractColumnsFromAst(columns: unknown[]): string[] {
  const result: string[] = []

  console.log('[AST] Parsing columns:', JSON.stringify(columns, null, 2))

  for (const col of columns) {
    if (typeof col === 'string') {
      // SELECT * case
      if (col !== '*') result.push(col)
      continue
    }

    if (col === '*') {
      result.push('*')
      continue
    }

    const colObj = col as Record<string, unknown>

    // Check for alias first (AS clause)
    if (colObj.as) {
      result.push(String(colObj.as))
      continue
    }

    // Check for column reference
    if (colObj.expr) {
      const expr = colObj.expr as Record<string, unknown>

      // Simple column: { type: 'column_ref', column: 'name' }
      if (expr.type === 'column_ref') {
        // column can be a string or an object with expr
        if (typeof expr.column === 'string') {
          result.push(expr.column)
          continue
        }
        if (expr.column && typeof expr.column === 'object') {
          const colRef = expr.column as Record<string, unknown>
          if (colRef.expr && typeof (colRef.expr as Record<string, unknown>).value === 'string') {
            result.push((colRef.expr as Record<string, unknown>).value as string)
            continue
          }
        }
      }

      // Aggregation or function with implicit alias from column name
      if (expr.type === 'aggr_func' || expr.type === 'function') {
        // No alias - skip, can't determine column name
        continue
      }
    }

    // Direct column reference without expr wrapper
    if (colObj.type === 'column_ref') {
      if (typeof colObj.column === 'string') {
        result.push(colObj.column)
      }
    }
  }

  console.log('[AST] Extracted columns:', result)
  return result
}

/**
 * Fallback regex-based extraction for when parser fails
 * Handles Flink-specific syntax like TUMBLE, HOP, etc.
 */
function extractColumnsFromSqlRegex(sql: string): string[] {
  const columns: string[] = []
  const cleanSql = sql.replace(/\s+/g, ' ').trim()

  // Find SELECT ... FROM pattern
  const selectMatch = cleanSql.match(/SELECT\s+(.+?)\s+FROM/i)
  if (!selectMatch) return columns

  const selectClause = selectMatch[1]

  // Split by comma, handling nested parentheses
  const parts: string[] = []
  let current = ''
  let parenDepth = 0

  for (const char of selectClause) {
    if (char === '(') parenDepth++
    else if (char === ')') parenDepth--
    else if (char === ',' && parenDepth === 0) {
      parts.push(current.trim())
      current = ''
      continue
    }
    current += char
  }
  if (current.trim()) parts.push(current.trim())

  for (const part of parts) {
    if (part === '*' || part.endsWith('.*')) continue

    // AS alias
    const asMatch = part.match(/\s+[Aa][Ss]\s+(\w+)\s*$/)
    if (asMatch) {
      columns.push(asMatch[1])
      continue
    }

    // Implicit alias after function: SUM(x) total
    const implicitAliasMatch = part.match(/\)\s+(\w+)\s*$/)
    if (implicitAliasMatch) {
      columns.push(implicitAliasMatch[1])
      continue
    }

    // Simple column
    const simpleMatch = part.match(/^[\w.]+$/)
    if (simpleMatch) {
      const colName = part.split('.').pop()
      if (colName) columns.push(colName)
    }
  }

  return columns
}

/**
 * Extract source names referenced in SQL via {{ source("name") }}
 */
function extractSourceRefsFromSql(sql: string): string[] {
  const refs: string[] = []
  const regex = /\{\{\s*source\s*\(\s*["']([^"']+)["']\s*\)\s*\}\}/gi
  let match
  while ((match = regex.exec(sql)) !== null) {
    refs.push(match[1])
  }
  return refs
}

/**
 * Extract model names referenced in SQL via {{ ref("name") }}
 */
function extractModelRefsFromSql(sql: string): string[] {
  const refs: string[] = []
  const regex = /\{\{\s*ref\s*\(\s*["']([^"']+)["']\s*\)\s*\}\}/gi
  let match
  while ((match = regex.exec(sql)) !== null) {
    refs.push(match[1])
  }
  return refs
}

/**
 * Parse YAML document to extract context at cursor position
 * and build a lightweight schema map of defined sources and models
 */
function getYAMLContext(
  model: editor.ITextModel,
  position: Position
): YAMLContext {
  const lines = model.getLinesContent()
  const currentLine = position.lineNumber - 1
  const lineContent = lines[currentLine] || ''

  // Calculate indentation
  const indent = lineContent.search(/\S/)
  const effectiveIndent = indent === -1 ? 0 : indent

  // Check if we're in a value position (after colon + optional space)
  const colonIndex = lineContent.indexOf(':')
  // isValue is true if cursor is after "key: " (colon + at least one space or at end)
  const isValue = colonIndex !== -1 && position.column > colonIndex + 1

  // Check if this is an array item
  const isArrayItem = lineContent.trim().startsWith('-')

  // Get current key (if any) - handle both "key:" and "- key:" patterns
  let currentKey: string | null = null
  // First try "- key:" pattern for array items
  const arrayKeyMatch = lineContent.match(/^\s*-\s+(\w+):/)
  // Then try regular "key:" pattern
  const regularKeyMatch = lineContent.match(/^\s*(\w+):/)

  console.log('[Context] Key detection on line: "%s"', lineContent)
  console.log('[Context] arrayKeyMatch:', arrayKeyMatch ? arrayKeyMatch[0] : null, '→', arrayKeyMatch ? arrayKeyMatch[1] : null)
  console.log('[Context] regularKeyMatch:', regularKeyMatch ? regularKeyMatch[0] : null, '→', regularKeyMatch ? regularKeyMatch[1] : null)

  if (arrayKeyMatch) {
    currentKey = arrayKeyMatch[1]
  } else if (regularKeyMatch) {
    currentKey = regularKeyMatch[1]
  }
  console.log('[Context] Final currentKey:', currentKey)

  // Build path by walking up the document
  const path: string[] = []
  let currentIndent = effectiveIndent

  for (let i = currentLine - 1; i >= 0; i--) {
    const line = lines[i]
    const lineIndent = line.search(/\S/)

    if (lineIndent === -1) continue // Skip empty lines

    if (lineIndent < currentIndent) {
      const match = line.match(/^\s*-?\s*(\w+):/)
      if (match) {
        path.unshift(match[1])
        currentIndent = lineIndent
      }
    }

    if (lineIndent === 0 && path.length > 0) break
  }

  // Extract defined sources and models with their columns using proper YAML parsing
  const sources = new Map<string, SchemaDefinition>()
  const models = new Map<string, SchemaDefinition>()

  try {
    const yamlText = lines.join('\n')
    const parsed = YAML.parse(yamlText)

    // Extract sources - columns come from explicit definition or will be inferred
    if (parsed?.sources && Array.isArray(parsed.sources)) {
      for (const source of parsed.sources) {
        if (source?.name) {
          const columns: string[] = []
          if (source.columns && Array.isArray(source.columns)) {
            for (const col of source.columns) {
              if (col?.name) columns.push(col.name)
            }
          }
          sources.set(source.name, { name: source.name, columns })
        }
      }
    }

    // Extract models - columns from explicit definition OR inferred from SQL SELECT
    if (parsed?.models && Array.isArray(parsed.models)) {
      for (const model of parsed.models) {
        if (model?.name) {
          let columns: string[] = []

          // First try explicit columns
          if (model.columns && Array.isArray(model.columns)) {
            for (const col of model.columns) {
              // Handle various column definition formats
              if (typeof col === 'string') {
                columns.push(col)
              } else if (col?.name) {
                // col.name could be a string or an object
                if (typeof col.name === 'string') {
                  columns.push(col.name)
                } else if (typeof col.name === 'object' && col.name !== null) {
                  // Might be a complex structure, try to extract
                  console.log('[YAML] Complex column structure:', col)
                }
              }
            }
          }

          // If no explicit columns, infer from SQL SELECT clause
          if (columns.length === 0 && model.sql) {
            columns = extractColumnsFromSql(model.sql)
          }

          // Ensure all columns are strings (filter out any objects that slipped through)
          const stringColumns = columns
            .filter((c): c is string => typeof c === 'string' && c.length > 0)
            .map(c => c.trim())

          models.set(model.name, { name: model.name, columns: stringColumns })
        }
      }
    }

    // Second pass: infer source columns from models that SELECT FROM them
    if (parsed?.models && Array.isArray(parsed.models)) {
      for (const model of parsed.models) {
        if (model?.sql) {
          // Find source references in this SQL
          const sourceRefs = extractSourceRefsFromSql(model.sql)
          for (const sourceName of sourceRefs) {
            const source = sources.get(sourceName)
            if (source && source.columns.length === 0) {
              // Infer columns from the SELECT that uses this source
              source.columns = extractColumnsFromSql(model.sql)
            }
          }

          // Also propagate columns from upstream models via ref()
          const modelRefs = extractModelRefsFromSql(model.sql)
          const currentModel = models.get(model.name)
          if (currentModel && currentModel.columns.length === 0 && modelRefs.length > 0) {
            // If this model has no columns but references other models,
            // try to inherit columns from SELECT * or first referenced model
            for (const refName of modelRefs) {
              const upstreamModel = models.get(refName)
              if (upstreamModel && upstreamModel.columns.length > 0) {
                // Check if SQL is SELECT * FROM ref
                if (model.sql.match(/SELECT\s+\*\s+FROM/i)) {
                  currentModel.columns = [...upstreamModel.columns]
                  break
                }
              }
            }
          }
        }
      }
    }
  } catch {
    // YAML parsing failed (likely incomplete/invalid YAML), continue with empty maps
  }

  console.log('[Context] Parsed sources:', Array.from(sources.entries()).map(([k, v]) => `${k}: [${v.columns.join(', ')}]`))
  console.log('[Context] Parsed models:', Array.from(models.entries()).map(([k, v]) => `${k}: [${v.columns.join(', ')}]`))

  return {
    path,
    indent: effectiveIndent,
    isValue,
    isArrayItem,
    currentKey,
    lineContent,
    sources,
    models,
  }
}

/**
 * Get schema field for a given path
 */
function getSchemaForPath(path: string[]): SchemaField | null {
  if (path.length === 0) return null

  let current: unknown = STREAMT_SCHEMA

  for (const key of path) {
    // Safety check: current must be a non-null object (not a primitive)
    if (current === null || current === undefined || typeof current !== 'object' || Array.isArray(current)) {
      return null
    }

    const obj = current as Record<string, unknown>

    if ('children' in obj && obj.children && typeof obj.children === 'object') {
      const children = obj.children as Record<string, unknown>
      current = children[key] ?? children['_dynamic_'] ?? null
    } else if (key in obj) {
      current = obj[key]
    } else {
      return null
    }
  }

  // Final check: must be a non-null object
  if (current === null || current === undefined || typeof current !== 'object' || Array.isArray(current)) {
    return null
  }

  return current as SchemaField
}

/**
 * Create completion items for top-level keys
 */
function getTopLevelCompletions(monaco: Monaco, range: IRange): CompletionItem[] {
  return Object.entries(STREAMT_SCHEMA).map(([key, field]) => ({
    label: key,
    kind: monaco.languages.CompletionItemKind.Property,
    insertText: field.type === 'array' ? `${key}:\n  - ` : `${key}:\n  `,
    insertTextRules: monaco.languages.CompletionItemInsertTextRule.InsertAsSnippet,
    documentation: {
      value: `**${key}**\n\n${field.description}${field.required ? '\n\n*Required*' : ''}`,
    },
    detail: field.required ? '(required)' : '(optional)',
    range,
    sortText: field.required ? '0' + key : '1' + key,
  }))
}

/**
 * Create completion items for object children
 */
function getChildCompletions(
  monaco: Monaco,
  schema: SchemaField,
  range: IRange,
  indent: string
): CompletionItem[] {
  if (!schema.children) return []

  return Object.entries(schema.children)
    .filter(([key]) => key !== '_dynamic_')
    .map(([key, field]) => {
      let insertText = key + ': '

      if (field.type === 'object' && field.children) {
        insertText = `${key}:\n${indent}  `
      } else if (field.type === 'array') {
        insertText = `${key}:\n${indent}  - `
      } else if (field.type === 'enum' && field.enum) {
        insertText = `${key}: \${{1|${field.enum.join(',')}|}}`
      } else if (field.type === 'sql') {
        insertText = `${key}: |\n${indent}  SELECT\n${indent}    $1\n${indent}  FROM {{ source("$2") }}`
      } else if (field.type === 'boolean') {
        insertText = `${key}: \${{1|true,false|}}`
      } else if (field.default !== undefined) {
        insertText = `${key}: ${field.default}`
      }

      let docValue = `**${key}** *(${field.type})*\n\n${field.description}`;
      if (field.default !== undefined) {
        docValue += '\n\nDefault: `' + field.default + '`';
      }
      if (field.examples) {
        docValue += '\n\nExamples:\n' + field.examples.map(e => '- `' + e + '`').join('\n');
      }

      return {
        label: key,
        kind: monaco.languages.CompletionItemKind.Property,
        insertText,
        insertTextRules: monaco.languages.CompletionItemInsertTextRule.InsertAsSnippet,
        documentation: {
          value: docValue,
        },
        detail: field.required ? '(required)' : '(optional)',
        range,
        sortText: field.required ? '0' + key : '1' + key,
      }
    })
}

/**
 * Create completion items for enum values
 */
function getEnumCompletions(
  monaco: Monaco,
  values: readonly string[],
  range: IRange,
  description: string
): CompletionItem[] {
  return values.map((value, index) => ({
    label: value,
    kind: monaco.languages.CompletionItemKind.EnumMember,
    insertText: value,
    documentation: description,
    range,
    sortText: String(index).padStart(3, '0'),
  }))
}

/**
 * Detect if we're currently inside a SQL block (multiline after | or >)
 */
function isInSqlBlock(model: editor.ITextModel, position: Position): boolean {
  const lines = model.getLinesContent()
  const currentLineIndex = position.lineNumber - 1
  const currentLine = lines[currentLineIndex] || ''
  let currentIndent = currentLine.search(/\S/)

  // If current line is empty/whitespace, find indent from cursor position or nearby lines
  if (currentIndent === -1) {
    // Use cursor column as a hint for expected indentation
    currentIndent = position.column - 1

    // Also look at the next non-empty line to get context
    for (let i = currentLineIndex + 1; i < lines.length; i++) {
      const nextIndent = lines[i].search(/\S/)
      if (nextIndent !== -1) {
        currentIndent = Math.max(currentIndent, nextIndent)
        break
      }
    }
  }

  // Scan backwards to find the start of this block
  for (let i = currentLineIndex - 1; i >= 0; i--) {
    const line = lines[i]
    const lineIndent = line.search(/\S/)

    if (lineIndent === -1) continue // Skip empty lines

    // Check if this is a sql: | line
    if (line.match(/^\s*sql:\s*[|>]/)) {
      // We found sql: |, now check if cursor is inside the block
      // The SQL block content should be indented more than the sql: key
      const sqlKeyIndent = lineIndent
      if (currentIndent > sqlKeyIndent) {
        return true
      }
      return false
    }

    // If we hit another YAML key at root or lower indent than where we started looking, stop
    if (line.match(/^\s*\w+:/) && lineIndent < currentIndent) {
      // Check if this key is "sql:" - if so, we're in the block
      if (line.match(/^\s*sql:\s*[|>]/)) {
        return true
      }
      return false
    }
  }

  return false
}

/**
 * Create completion items for Jinja templates in SQL
 */
function getJinjaCompletions(
  monaco: Monaco,
  range: IRange,
  sources: Map<string, SchemaDefinition>,
  models: Map<string, SchemaDefinition>,
  textBeforeCursor: string = ''
): CompletionItem[] {
  const items: CompletionItem[] = []

  // Check if we're inside ref(" or source(" to provide name completions only
  const inRefQuote = /ref\s*\(\s*["']$/.test(textBeforeCursor)
  const inSourceQuote = /source\s*\(\s*["']$/.test(textBeforeCursor)

  if (inRefQuote) {
    // Only show model names
    Array.from(models.values()).forEach((model, i) => {
      const cols = model.columns.length > 0 ? `\n\nColumns: ${model.columns.join(', ')}` : ''
      items.push({
        label: model.name,
        kind: monaco.languages.CompletionItemKind.Reference,
        insertText: model.name,
        documentation: {
          value: `**${model.name}**\n\nReference this model in your SQL query.${cols}`,
        },
        detail: `Model (${model.columns.length} columns)`,
        range,
        sortText: String(i).padStart(3, '0'),
      })
    })
    return items
  }

  if (inSourceQuote) {
    // Only show source names
    Array.from(sources.values()).forEach((source, i) => {
      const cols = source.columns.length > 0 ? `\n\nColumns: ${source.columns.join(', ')}` : ''
      items.push({
        label: source.name,
        kind: monaco.languages.CompletionItemKind.Reference,
        insertText: source.name,
        documentation: {
          value: `**${source.name}**\n\nReference this source in your SQL query.${cols}`,
        },
        detail: `Source (${source.columns.length} columns)`,
        range,
        sortText: String(i).padStart(3, '0'),
      })
    })
    return items
  }

  // Full Jinja completions when inside {{ but not in ref/source quotes

  // source() completions with full syntax
  Array.from(sources.values()).forEach((source, i) => {
    const cols = source.columns.length > 0 ? `\n\nColumns: ${source.columns.join(', ')}` : ''
    items.push({
      label: `source("${source.name}")`,
      kind: monaco.languages.CompletionItemKind.Function,
      insertText: `source("${source.name}")`,
      documentation: {
        value: `Reference the **${source.name}** source\n\n\`\`\`sql\nSELECT * FROM {{ source("${source.name}") }}\n\`\`\`${cols}`,
      },
      detail: `Source (${source.columns.length} cols)`,
      range,
      sortText: '0' + String(i).padStart(3, '0'),
    })
  })

  // ref() completions with full syntax
  Array.from(models.values()).forEach((model, i) => {
    const cols = model.columns.length > 0 ? `\n\nColumns: ${model.columns.join(', ')}` : ''
    items.push({
      label: `ref("${model.name}")`,
      kind: monaco.languages.CompletionItemKind.Function,
      insertText: `ref("${model.name}")`,
      documentation: {
        value: `Reference the **${model.name}** model\n\n\`\`\`sql\nSELECT * FROM {{ ref("${model.name}") }}\n\`\`\`${cols}`,
      },
      detail: `Model (${model.columns.length} cols)`,
      range,
      sortText: '1' + String(i).padStart(3, '0'),
    })
  })

  // Generic Jinja templates
  items.push({
    label: 'source("...")',
    kind: monaco.languages.CompletionItemKind.Snippet,
    insertText: 'source("${1:source_name}")',
    insertTextRules: monaco.languages.CompletionItemInsertTextRule.InsertAsSnippet,
    documentation: {
      value: 'Reference an external source\n\n```sql\nSELECT * FROM {{ source("raw_events") }}\n```',
    },
    detail: 'Source template',
    range,
    sortText: '2000',
  })

  items.push({
    label: 'ref("...")',
    kind: monaco.languages.CompletionItemKind.Snippet,
    insertText: 'ref("${1:model_name}")',
    insertTextRules: monaco.languages.CompletionItemInsertTextRule.InsertAsSnippet,
    documentation: {
      value: 'Reference another model\n\n```sql\nSELECT * FROM {{ ref("enriched_events") }}\n```',
    },
    detail: 'Model reference template',
    range,
    sortText: '2001',
  })

  items.push({
    label: 'var("...", default)',
    kind: monaco.languages.CompletionItemKind.Snippet,
    insertText: 'var("${1:variable_name}", "${2:default_value}")',
    insertTextRules: monaco.languages.CompletionItemInsertTextRule.InsertAsSnippet,
    documentation: {
      value: 'Reference a project variable with optional default\n\n```sql\nWHERE env = {{ var("environment", "production") }}\n```',
    },
    detail: 'Variable template',
    range,
    sortText: '2002',
  })

  // Additional Jinja functions
  items.push({
    label: 'config(...)',
    kind: monaco.languages.CompletionItemKind.Snippet,
    insertText: 'config(${1:key}="${2:value}")',
    insertTextRules: monaco.languages.CompletionItemInsertTextRule.InsertAsSnippet,
    documentation: {
      value: 'Set model configuration\n\n```jinja\n{{ config(materialized="topic", tags=["realtime"]) }}\n```',
    },
    detail: 'Config block',
    range,
    sortText: '2003',
  })

  items.push({
    label: 'env_var("...")',
    kind: monaco.languages.CompletionItemKind.Snippet,
    insertText: 'env_var("${1:ENV_VAR_NAME}", "${2:default}")',
    insertTextRules: monaco.languages.CompletionItemInsertTextRule.InsertAsSnippet,
    documentation: {
      value: 'Reference an environment variable\n\n```jinja\n{{ env_var("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092") }}\n```',
    },
    detail: 'Environment variable',
    range,
    sortText: '2004',
  })

  return items
}

interface SqlReference {
  type: 'source' | 'model'
  name: string
  alias?: string
}

/**
 * Extract all ref() and source() references from SQL, with optional aliases
 */
function getSqlReferences(sql: string): SqlReference[] {
  const refs: SqlReference[] = []
  const cleanSql = sql.replace(/\s+/g, ' ')

  // Match source() calls - with optional AS alias or just alias
  const sourceRegex = /\{\{\s*source\s*\(\s*["']([^"']+)["']\s*\)\s*\}\}(?:\s+(?:AS\s+)?(\w+))?/gi
  let match
  while ((match = sourceRegex.exec(cleanSql)) !== null) {
    refs.push({ type: 'source', name: match[1], alias: match[2] || undefined })
  }

  // Match ref() calls
  const refRegex = /\{\{\s*ref\s*\(\s*["']([^"']+)["']\s*\)\s*\}\}(?:\s+(?:AS\s+)?(\w+))?/gi
  while ((match = refRegex.exec(cleanSql)) !== null) {
    refs.push({ type: 'model', name: match[1], alias: match[2] || undefined })
  }

  return refs
}

/**
 * Get the full SQL block content (from sql: | to end of block)
 */
function getFullSqlBlock(lines: string[], startSearchLine: number): { content: string, startLine: number } {
  let sqlStartLine = -1
  let sqlIndent = 0

  // Find sql: | or sql: > line
  for (let i = startSearchLine; i >= 0; i--) {
    const match = lines[i].match(/^(\s*)sql:\s*[|>]/)
    if (match) {
      sqlStartLine = i
      sqlIndent = match[1].length + 2
      break
    }
  }

  if (sqlStartLine === -1) {
    return { content: '', startLine: 0 }
  }

  // Collect SQL content until indentation decreases
  const sqlLines: string[] = []
  for (let i = sqlStartLine + 1; i < lines.length; i++) {
    const line = lines[i]
    const lineIndent = line.search(/\S/)
    if (lineIndent === -1) {
      sqlLines.push(line)
      continue
    }
    if (lineIndent < sqlIndent) {
      break
    }
    sqlLines.push(line)
  }

  return { content: sqlLines.join('\n'), startLine: sqlStartLine }
}

/**
 * Create completion items for SQL snippets and semantic column suggestions
 */
function getSQLCompletions(
  monaco: Monaco,
  range: IRange,
  model: editor.ITextModel,
  position: Position,
  sources: Map<string, SchemaDefinition>,
  models: Map<string, SchemaDefinition>
): CompletionItem[] {
  const suggestions: CompletionItem[] = []
  const lines = model.getLinesContent()
  const lineContent = lines[position.lineNumber - 1]
  const textBeforeCursor = lineContent.substring(0, position.column - 1)

  // Get the full SQL block (including content after cursor for ref detection)
  const { content: sqlContent } = getFullSqlBlock(lines, position.lineNumber - 1)
  const sqlRefs = getSqlReferences(sqlContent)

  console.log('[SQL] Found refs in SQL block:', sqlRefs)

  // Build alias map and collect all referenced columns
  const aliasToRef = new Map<string, SqlReference>()
  const allColumns: { col: string; ref: SqlReference; schemaDef: SchemaDefinition }[] = []

  for (const ref of sqlRefs) {
    const schemaDef = ref.type === 'source' ? sources.get(ref.name) : models.get(ref.name)
    if (schemaDef) {
      // Map alias to reference
      if (ref.alias) {
        aliasToRef.set(ref.alias, ref)
      }
      // Collect columns
      for (const col of schemaDef.columns) {
        allColumns.push({ col, ref, schemaDef })
      }
    }
  }

  console.log('[SQL] Alias map:', Array.from(aliasToRef.entries()))
  console.log('[SQL] All columns count:', allColumns.length)

  // Check if user is typing "alias." pattern
  const aliasMatch = textBeforeCursor.match(/(\w+)\.$/)

  if (aliasMatch) {
    // User typed "alias." - show only columns from that alias
    const alias = aliasMatch[1]
    const ref = aliasToRef.get(alias)
    console.log('[SQL] Alias completion for "%s" -> ref:', alias, ref)

    if (ref) {
      const schemaDef = ref.type === 'source' ? sources.get(ref.name) : models.get(ref.name)
      if (schemaDef) {
        schemaDef.columns.forEach((col, i) => {
          suggestions.push({
            label: col,
            kind: monaco.languages.CompletionItemKind.Field,
            insertText: col,
            documentation: {
              value: `**${col}**\n\nColumn from ${ref.type} \`${ref.name}\``,
            },
            detail: `${ref.name}.${col}`,
            range,
            sortText: '0' + String(i).padStart(3, '0'),
          })
        })
      }
    }
    // Return early - only show alias columns, not snippets
    return suggestions
  }

  // General SQL context - show all columns from all referenced models/sources
  if (allColumns.length > 0) {
    // Deduplicate: only show each column name once (unqualified)
    // For columns that appear in multiple refs, show them with qualified versions only
    const columnCounts = new Map<string, number>()
    for (const { col } of allColumns) {
      columnCounts.set(col, (columnCounts.get(col) || 0) + 1)
    }

    // Track which unqualified columns we've already added
    const addedUnqualified = new Set<string>()

    allColumns.forEach(({ col, ref, schemaDef }, i) => {
      const isDuplicate = columnCounts.get(col)! > 1

      // Add unqualified column name only once per unique column name
      if (!addedUnqualified.has(col)) {
        addedUnqualified.add(col)
        suggestions.push({
          label: col,
          kind: monaco.languages.CompletionItemKind.Field,
          insertText: col,
          documentation: {
            value: `**${col}**\n\nColumn from ${ref.type} \`${ref.name}\`${isDuplicate ? `\n\n*Also in other tables - use alias.${col} to disambiguate*` : ''}`,
          },
          detail: schemaDef.name,
          range,
          sortText: '1' + String(i).padStart(3, '0'),
        })
      }

      // If there's an alias, also offer qualified version (always useful for disambiguation)
      if (ref.alias) {
        suggestions.push({
          label: `${ref.alias}.${col}`,
          kind: monaco.languages.CompletionItemKind.Field,
          insertText: `${ref.alias}.${col}`,
          documentation: {
            value: `**${ref.alias}.${col}**\n\nQualified column from ${ref.type} \`${ref.name}\``,
          },
          detail: schemaDef.name,
          range,
          sortText: '2' + String(i).padStart(3, '0'),
        })
      }
    })
  }

  // Add SQL snippets (lower priority than columns)
  suggestions.push(...SQL_SNIPPETS.map((snippet, i) => ({
    label: snippet.label,
    kind: monaco.languages.CompletionItemKind.Snippet,
    insertText: snippet.insertText,
    insertTextRules: monaco.languages.CompletionItemInsertTextRule.InsertAsSnippet,
    documentation: {
      value: '**' + snippet.label + '**\n\n' + snippet.description + '\n\n```sql\n' + snippet.insertText.replace(/\$\d/g, '...').replace(/\${.*?}/g, '...') + '\n```',
    },
    detail: 'SQL Snippet',
    range,
    sortText: '3' + String(i).padStart(3, '0'),
  })))

  return suggestions
}

/**
 * Create completion items for assertion types
 */
function getAssertionCompletions(
  monaco: Monaco,
  range: IRange,
  indent: string
): CompletionItem[] {
  const assertions: { type: string; template: string; description: string }[] = [
    {
      type: 'not_null',
      template: `not_null:\n${indent}  columns: [\${1:column_name}]`,
      description: 'Assert that specified columns have no NULL values',
    },
    {
      type: 'unique',
      template: `unique:\n${indent}  columns: [\${1:column_name}]`,
      description: 'Assert that specified columns are unique',
    },
    {
      type: 'accepted_values',
      template: `accepted_values:\n${indent}  column: \${1:column_name}\n${indent}  values: [\${2:value1, value2}]`,
      description: 'Assert that column values are within an allowed set',
    },
    {
      type: 'range',
      template: `range:\n${indent}  column: \${1:column_name}\n${indent}  min: \${2:0}\n${indent}  max: \${3:100}`,
      description: 'Assert that numeric values are within a range',
    },
    {
      type: 'regex',
      template: `regex:\n${indent}  column: \${1:column_name}\n${indent}  pattern: "\${2:^[A-Z]+$}"`,
      description: 'Assert that string values match a regex pattern',
    },
    {
      type: 'relationships',
      template: `relationships:\n${indent}  column: \${1:column_name}\n${indent}  to: ref("\${2:other_model}")\n${indent}  field: \${3:id}`,
      description: 'Assert referential integrity between models',
    },
    {
      type: 'custom_sql',
      template: `custom_sql:\n${indent}  sql: "SELECT COUNT(*) = 0 FROM {{ ref('$1') }} WHERE \${2:condition}"`,
      description: 'Custom SQL assertion (should return 0 for pass)',
    },
    {
      type: 'freshness',
      template: `freshness:\n${indent}  column: \${1:timestamp_column}\n${indent}  max_age_minutes: \${2:60}`,
      description: 'Assert that data is not stale',
    },
  ]

  return assertions.map((assertion, i) => ({
    label: assertion.type,
    kind: monaco.languages.CompletionItemKind.Struct,
    insertText: assertion.template,
    insertTextRules: monaco.languages.CompletionItemInsertTextRule.InsertAsSnippet,
    documentation: {
      value: `**${assertion.type}**\n\n${assertion.description}`,
    },
    detail: 'Assertion type',
    range,
    sortText: String(i).padStart(3, '0'),
  }))
}

/**
 * Register the completion provider with Monaco
 */
export function registerCompletionProvider(monaco: Monaco): void {
  monaco.languages.registerCompletionItemProvider('yaml', {
    triggerCharacters: ['.', ':', ' ', '-', '"', "'", '{', '(', '>'],

    provideCompletionItems(
      model: editor.ITextModel,
      position: Position
    ): CompletionList {
      const lineContent = model.getLineContent(position.lineNumber)
      console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━')
      console.log('[Completion] TRIGGERED at L%d C%d', position.lineNumber, position.column)
      console.log('[Completion] Line content: "%s"', lineContent)
      console.log('[Completion] Cursor position marked: "%s|%s"',
        lineContent.substring(0, position.column - 1),
        lineContent.substring(position.column - 1))

      const context = getYAMLContext(model, position)
      console.log('[Completion] Context detected:', {
        isValue: context.isValue,
        currentKey: context.currentKey,
        isArrayItem: context.isArrayItem,
        path: context.path,
        indent: context.indent,
        sourcesCount: context.sources.size,
        modelsCount: context.models.size,
        sourceNames: Array.from(context.sources.keys()),
        modelNames: Array.from(context.models.keys()),
      })

      const word = model.getWordUntilPosition(position)
      const range: IRange = {
        startLineNumber: position.lineNumber,
        startColumn: word.startColumn,
        endLineNumber: position.lineNumber,
        endColumn: word.endColumn,
      }

      const indent = ' '.repeat(context.indent + 2)
      const suggestions: CompletionItem[] = []

      // Check if we're inside a Jinja template {{ }}
      const ctxLineContent = context.lineContent
      const beforeCursor = ctxLineContent.substring(0, position.column - 1)
      const inJinja = beforeCursor.includes('{{') && !beforeCursor.includes('}}')

      if (inJinja) {
        console.log('[Completion] → Early return: inJinja detected')
        suggestions.push(...getJinjaCompletions(monaco, range, context.sources, context.models, beforeCursor))
        console.log('[Completion] Returning %d Jinja suggestions', suggestions.length)
        return { suggestions }
      }

      // Check if we're in an SQL block (multiline or inline)
      const inSqlBlock = isInSqlBlock(model, position)
      const inSQL = context.path.includes('sql') || context.currentKey === 'sql' || inSqlBlock
      console.log('[Completion] inSQL check: path.includes(sql)=%s, currentKey===sql: %s, isInSqlBlock=%s → inSQL=%s',
        context.path.includes('sql'), context.currentKey === 'sql', inSqlBlock, inSQL)
      // For multiline SQL blocks, we don't require isValue (the line won't have a colon)
      if (inSQL && (context.isValue || inSqlBlock)) {
        console.log('[Completion] → Early return: inSQL detected (isValue=%s, inSqlBlock=%s)', context.isValue, inSqlBlock)
        suggestions.push(...getSQLCompletions(monaco, range, model, position, context.sources, context.models))
        // Only add Jinja completions if we're starting a Jinja block (typing {{ or inside one)
        const isStartingJinja = beforeCursor.endsWith('{') || beforeCursor.endsWith('{{')
        if (isStartingJinja || inJinja) {
          suggestions.push(...getJinjaCompletions(monaco, range, context.sources, context.models, beforeCursor))
        }
        console.log('[Completion] Returning %d SQL suggestions', suggestions.length)
        return { suggestions }
      }

      // Top level completions
      if (context.path.length === 0 && !context.isValue) {
        console.log('[Completion] → Early return: top-level completions')
        suggestions.push(...getTopLevelCompletions(monaco, range))
        console.log('[Completion] Returning %d top-level suggestions', suggestions.length)
        return { suggestions }
      }

      // Get schema for current path
      const schema = getSchemaForPath(context.path)

      // Value completions based on current key
      console.log('[Completion] Checking value completion: isValue=%s, currentKey=%s', context.isValue, context.currentKey)
      if (context.isValue && context.currentKey) {
        console.log('[Completion] ✓ Entering value completion switch for key: "%s"', context.currentKey)
        // Specific enum completions
        switch (context.currentKey) {
          case 'type':
            if (context.path.includes('tests')) {
              suggestions.push(...getEnumCompletions(monaco, TEST_TYPES, range, 'Test execution type'))
            } else if (context.path.includes('exposures')) {
              suggestions.push(...getEnumCompletions(monaco, EXPOSURE_TYPES, range, 'Exposure type'))
            } else if (context.path.includes('columns')) {
              suggestions.push(...getEnumCompletions(monaco, COLUMN_TYPES, range, 'Flink SQL column type'))
            }
            break
          case 'materialized':
            suggestions.push(...getEnumCompletions(monaco, MATERIALIZED_TYPES, range, 'Materialization strategy'))
            break
          case 'role':
            suggestions.push(...getEnumCompletions(monaco, EXPOSURE_ROLES, range, 'Exposure role'))
            break
          case 'format':
            suggestions.push(...getEnumCompletions(monaco, FORMAT_TYPES, range, 'Message format'))
            break
          case 'cleanup_policy':
            suggestions.push(...getEnumCompletions(monaco, CLEANUP_POLICIES, range, 'Topic cleanup policy'))
            break

          // Reference completions for depends_on, produces, etc.
          case 'ref':
            console.log('[Completion] ✓✓ MATCHED case "ref" - adding %d model suggestions', context.models.size)
            // Suggest defined models for ref: value
            Array.from(context.models.values()).forEach((m, i) => {
              console.log('[Completion]   Adding model: %s', m.name)
              const cols = m.columns.length > 0 ? `\n\nColumns: ${m.columns.join(', ')}` : ''
              suggestions.push({
                label: m.name,
                kind: monaco.languages.CompletionItemKind.Reference,
                insertText: m.name,
                documentation: {
                  value: `**${m.name}**\n\nReference this model as a dependency.${cols}`,
                },
                detail: `Model (${m.columns.length} columns)`,
                range,
                sortText: String(i).padStart(3, '0'),
              })
            })
            console.log('[Completion] After ref case, suggestions count: %d', suggestions.length)
            break

          case 'source':
            // Suggest defined sources for source: value (in produces, depends_on, etc.)
            Array.from(context.sources.values()).forEach((s, i) => {
              const cols = s.columns.length > 0 ? `\n\nColumns: ${s.columns.join(', ')}` : ''
              suggestions.push({
                label: s.name,
                kind: monaco.languages.CompletionItemKind.Reference,
                insertText: s.name,
                documentation: {
                  value: `**${s.name}**\n\nReference this source.${cols}`,
                },
                detail: `Source (${s.columns.length} columns)`,
                range,
                sortText: String(i).padStart(3, '0'),
              })
            })
            break

          case 'model':
            // Suggest defined models for test.model
            Array.from(context.models.values()).forEach((m, i) => {
              const cols = m.columns.length > 0 ? `\n\nColumns: ${m.columns.join(', ')}` : ''
              suggestions.push({
                label: m.name,
                kind: monaco.languages.CompletionItemKind.Reference,
                insertText: m.name,
                documentation: {
                  value: `**${m.name}**\n\nReference this model for testing.${cols}`,
                },
                detail: `Model (${m.columns.length} columns)`,
                range,
                sortText: String(i).padStart(3, '0'),
              })
            })
            break

          case 'to':
            // For relationships assertion: to: ref("other_model")
            // Suggest models
            Array.from(context.models.values()).forEach((m, i) => {
              suggestions.push({
                label: `ref("${m.name}")`,
                kind: monaco.languages.CompletionItemKind.Reference,
                insertText: `ref("${m.name}")`,
                documentation: {
                  value: `Reference **${m.name}** for relationship check`,
                },
                detail: 'Model reference',
                range,
                sortText: String(i).padStart(3, '0'),
              })
            })
            break

          case 'column':
          case 'field':
            // Suggest columns from sources and models in context
            // This is a heuristic - we show all known columns
            const allColumns = new Set<string>()
            context.sources.forEach(s => s.columns.forEach(c => allColumns.add(c)))
            context.models.forEach(m => m.columns.forEach(c => allColumns.add(c)))
            Array.from(allColumns).forEach((col, i) => {
              suggestions.push({
                label: col,
                kind: monaco.languages.CompletionItemKind.Field,
                insertText: col,
                documentation: { value: `Column: ${col}` },
                detail: 'Column',
                range,
                sortText: String(i).padStart(3, '0'),
              })
            })
            break

          default:
            console.log('[Completion] No matching case for key: "%s"', context.currentKey)
        }

        console.log('[Completion] Returning from value completion with %d suggestions', suggestions.length)
        return { suggestions }
      }

      console.log('[Completion] Did not enter value completion block')

      // Key completions for current context
      if (schema && schema.children && !context.isValue) {
        suggestions.push(...getChildCompletions(monaco, schema, range, indent))
      }

      // Array item completions
      if (context.isArrayItem && schema && schema.itemSchema && schema.itemSchema.children) {
        suggestions.push(...getChildCompletions(monaco, schema.itemSchema, range, indent))
      }

      // Assertion completions in tests
      if (context.path.includes('assertions') && context.isArrayItem) {
        suggestions.push(...getAssertionCompletions(monaco, range, indent))
      }

      // Source/model item completions
      if (context.path[0] === 'sources' && context.isArrayItem && !context.currentKey) {
        const sourceSchema = STREAMT_SCHEMA.sources.itemSchema
        if (sourceSchema) {
          suggestions.push(...getChildCompletions(monaco, sourceSchema, range, indent))
        }
      }

      if (context.path[0] === 'models' && context.isArrayItem && !context.currentKey) {
        const modelSchema = STREAMT_SCHEMA.models.itemSchema
        if (modelSchema) {
          suggestions.push(...getChildCompletions(monaco, modelSchema, range, indent))
        }
      }

      if (context.path[0] === 'tests' && context.isArrayItem && !context.currentKey) {
        const testSchema = STREAMT_SCHEMA.tests.itemSchema
        if (testSchema) {
          suggestions.push(...getChildCompletions(monaco, testSchema, range, indent))
        }
      }

      if (context.path[0] === 'exposures' && context.isArrayItem && !context.currentKey) {
        const exposureSchema = STREAMT_SCHEMA.exposures.itemSchema
        if (exposureSchema) {
          suggestions.push(...getChildCompletions(monaco, exposureSchema, range, indent))
        }
      }

      console.log('[Completion] FINAL return with %d suggestions:', suggestions.length, suggestions.map(s => s.label))
      return { suggestions }
    },
  })
}
