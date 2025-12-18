import { useRef, useEffect, useCallback } from 'react'
import Editor, { Monaco, OnMount, BeforeMount } from '@monaco-editor/react'
import { RefreshCw } from 'lucide-react'
import type { editor, languages } from 'monaco-editor'
import { SQL_SNIPPETS } from '@/editor/schema'
import { useProjectStore } from '@/stores/projectStore'
import { initializeEditor } from '@/editor'

interface SqlEditorProps {
  value: string
  onChange: (value: string) => void
  height?: string
  className?: string
  placeholder?: string
}

// Flink SQL keywords
const FLINK_KEYWORDS = [
  'SELECT', 'FROM', 'WHERE', 'GROUP BY', 'HAVING', 'ORDER BY', 'LIMIT',
  'JOIN', 'LEFT JOIN', 'RIGHT JOIN', 'FULL JOIN', 'INNER JOIN', 'CROSS JOIN',
  'ON', 'AS', 'AND', 'OR', 'NOT', 'IN', 'EXISTS', 'BETWEEN', 'LIKE', 'IS NULL', 'IS NOT NULL',
  'CASE', 'WHEN', 'THEN', 'ELSE', 'END',
  'UNION', 'UNION ALL', 'INTERSECT', 'EXCEPT',
  'INSERT INTO', 'VALUES', 'CREATE TABLE', 'DROP TABLE', 'ALTER TABLE',
  'PARTITION BY', 'OVER', 'WINDOW', 'ROWS', 'RANGE',
  'TUMBLE', 'HOP', 'SESSION', 'CUMULATE',
  'PROCTIME', 'ROWTIME', 'WATERMARK',
  'DISTINCT', 'ALL', 'TRUE', 'FALSE', 'NULL',
  'WITH', 'RECURSIVE', 'LATERAL', 'UNNEST',
  'MATCH_RECOGNIZE', 'PATTERN', 'DEFINE', 'MEASURES', 'AFTER MATCH',
]

// Flink SQL functions
const FLINK_FUNCTIONS = [
  // Aggregate functions
  'COUNT', 'SUM', 'AVG', 'MIN', 'MAX', 'FIRST_VALUE', 'LAST_VALUE',
  'COLLECT', 'LISTAGG', 'STDDEV_POP', 'STDDEV_SAMP', 'VAR_POP', 'VAR_SAMP',
  // String functions
  'UPPER', 'LOWER', 'TRIM', 'LTRIM', 'RTRIM', 'CONCAT', 'SUBSTRING', 'REPLACE',
  'REGEXP_REPLACE', 'REGEXP_EXTRACT', 'SPLIT_INDEX', 'CHAR_LENGTH', 'LENGTH',
  'POSITION', 'LOCATE', 'OVERLAY', 'LPAD', 'RPAD', 'REVERSE', 'INITCAP',
  // Date/time functions
  'CURRENT_DATE', 'CURRENT_TIME', 'CURRENT_TIMESTAMP', 'LOCALTIME', 'LOCALTIMESTAMP',
  'NOW', 'TO_DATE', 'TO_TIMESTAMP', 'TO_TIMESTAMP_LTZ', 'DATE_FORMAT',
  'TIMESTAMPDIFF', 'TIMESTAMPADD', 'EXTRACT', 'YEAR', 'QUARTER', 'MONTH', 'WEEK',
  'DAYOFYEAR', 'DAYOFMONTH', 'DAYOFWEEK', 'HOUR', 'MINUTE', 'SECOND',
  // Type conversion
  'CAST', 'TRY_CAST', 'TYPEOF',
  // Conditional
  'IF', 'IFNULL', 'NULLIF', 'COALESCE', 'NVL', 'GREATEST', 'LEAST',
  // Math functions
  'ABS', 'CEIL', 'FLOOR', 'ROUND', 'TRUNCATE', 'MOD', 'POWER', 'SQRT', 'EXP', 'LN', 'LOG',
  'SIN', 'COS', 'TAN', 'ASIN', 'ACOS', 'ATAN', 'ATAN2', 'SINH', 'COSH', 'TANH',
  'RAND', 'RAND_INTEGER', 'UUID', 'BIN', 'HEX',
  // JSON functions
  'JSON_VALUE', 'JSON_QUERY', 'JSON_EXISTS', 'JSON_OBJECT', 'JSON_ARRAY', 'JSON_STRING',
  // Array functions
  'CARDINALITY', 'ELEMENT', 'ARRAY', 'ARRAY_CONCAT', 'ARRAY_CONTAINS', 'ARRAY_DISTINCT',
  'ARRAY_POSITION', 'ARRAY_REMOVE', 'ARRAY_REVERSE', 'ARRAY_UNION', 'ARRAY_SLICE',
  // Map functions
  'MAP', 'MAP_KEYS', 'MAP_VALUES', 'MAP_FROM_ARRAYS', 'MAP_ENTRIES',
  // Window functions
  'ROW_NUMBER', 'RANK', 'DENSE_RANK', 'NTILE', 'LAG', 'LEAD', 'CUME_DIST', 'PERCENT_RANK',
]

export function SqlEditor({
  value,
  onChange,
  height = '300px',
  className = '',
  placeholder,
}: SqlEditorProps) {
  const editorRef = useRef<editor.IStandaloneCodeEditor | null>(null)
  const monacoRef = useRef<Monaco | null>(null)
  const disposablesRef = useRef<{ dispose: () => void }[]>([])

  // Get sources and models from the store
  const { project } = useProjectStore()
  const availableSources = project?.sources.map(s => s.name) ?? []
  const availableModels = project?.models.map(m => m.name) ?? []

  // Initialize theme and language BEFORE editor mounts
  const handleEditorWillMount: BeforeMount = (monaco) => {
    initializeEditor(monaco)
  }

  // Handle editor mount
  const handleEditorDidMount: OnMount = (editorInstance, monaco) => {
    editorRef.current = editorInstance
    monacoRef.current = monaco

    // Explicitly set theme to ensure it's applied on remount
    monaco.editor.setTheme('streamt-dark')

    // Register completion provider for SQL with Jinja support
    const completionDisposable = monaco.languages.registerCompletionItemProvider('sql', {
      triggerCharacters: ['.', ' ', '(', '{', '"', "'"],
      provideCompletionItems: (model: editor.ITextModel, position: { lineNumber: number; column: number }) => {
        const lineContent = model.getLineContent(position.lineNumber)
        const textBeforeCursor = lineContent.substring(0, position.column - 1)
        const word = model.getWordUntilPosition(position)
        const range = {
          startLineNumber: position.lineNumber,
          startColumn: word.startColumn,
          endLineNumber: position.lineNumber,
          endColumn: word.endColumn,
        }

        const suggestions: languages.CompletionItem[] = []

        // Check if we're inside Jinja braces {{ }}
        const inJinja = textBeforeCursor.includes('{{') && !textBeforeCursor.includes('}}')

        // Check if typing source/ref name
        const inRefQuote = /ref\s*\(\s*["']$/.test(textBeforeCursor)
        const inSourceQuote = /source\s*\(\s*["']$/.test(textBeforeCursor)

        if (inRefQuote) {
          // Suggest model names
          availableModels.forEach((modelName, i) => {
            suggestions.push({
              label: modelName,
              kind: monaco.languages.CompletionItemKind.Reference,
              insertText: modelName,
              documentation: `Reference model: ${modelName}`,
              detail: 'Model',
              range,
              sortText: String(i).padStart(3, '0'),
            })
          })
          return { suggestions }
        }

        if (inSourceQuote) {
          // Suggest source names
          availableSources.forEach((sourceName, i) => {
            suggestions.push({
              label: sourceName,
              kind: monaco.languages.CompletionItemKind.Reference,
              insertText: sourceName,
              documentation: `Reference source: ${sourceName}`,
              detail: 'Source',
              range,
              sortText: String(i).padStart(3, '0'),
            })
          })
          return { suggestions }
        }

        if (inJinja) {
          // Jinja completions
          availableSources.forEach((sourceName, i) => {
            suggestions.push({
              label: `source("${sourceName}")`,
              kind: monaco.languages.CompletionItemKind.Function,
              insertText: `source("${sourceName}")`,
              documentation: `Reference the ${sourceName} source`,
              detail: 'Source',
              range,
              sortText: '0' + String(i).padStart(3, '0'),
            })
          })

          availableModels.forEach((modelName, i) => {
            suggestions.push({
              label: `ref("${modelName}")`,
              kind: monaco.languages.CompletionItemKind.Function,
              insertText: `ref("${modelName}")`,
              documentation: `Reference the ${modelName} model`,
              detail: 'Model',
              range,
              sortText: '1' + String(i).padStart(3, '0'),
            })
          })

          suggestions.push({
            label: 'source("...")',
            kind: monaco.languages.CompletionItemKind.Snippet,
            insertText: 'source("${1:source_name}")',
            insertTextRules: monaco.languages.CompletionItemInsertTextRule.InsertAsSnippet,
            documentation: 'Reference an external source',
            detail: 'Source template',
            range,
            sortText: '2000',
          })

          suggestions.push({
            label: 'ref("...")',
            kind: monaco.languages.CompletionItemKind.Snippet,
            insertText: 'ref("${1:model_name}")',
            insertTextRules: monaco.languages.CompletionItemInsertTextRule.InsertAsSnippet,
            documentation: 'Reference another model',
            detail: 'Model reference template',
            range,
            sortText: '2001',
          })

          suggestions.push({
            label: 'var("...", default)',
            kind: monaco.languages.CompletionItemKind.Snippet,
            insertText: 'var("${1:variable_name}", "${2:default_value}")',
            insertTextRules: monaco.languages.CompletionItemInsertTextRule.InsertAsSnippet,
            documentation: 'Reference a project variable',
            detail: 'Variable template',
            range,
            sortText: '2002',
          })

          return { suggestions }
        }

        // SQL keywords
        FLINK_KEYWORDS.forEach((keyword, i) => {
          suggestions.push({
            label: keyword,
            kind: monaco.languages.CompletionItemKind.Keyword,
            insertText: keyword,
            detail: 'SQL Keyword',
            range,
            sortText: '1' + String(i).padStart(3, '0'),
          })
        })

        // SQL functions
        FLINK_FUNCTIONS.forEach((func, i) => {
          suggestions.push({
            label: func,
            kind: monaco.languages.CompletionItemKind.Function,
            insertText: func + '($0)',
            insertTextRules: monaco.languages.CompletionItemInsertTextRule.InsertAsSnippet,
            detail: 'Flink SQL Function',
            range,
            sortText: '2' + String(i).padStart(3, '0'),
          })
        })

        // SQL snippets
        SQL_SNIPPETS.forEach((snippet, i) => {
          suggestions.push({
            label: snippet.label,
            kind: monaco.languages.CompletionItemKind.Snippet,
            insertText: snippet.insertText,
            insertTextRules: monaco.languages.CompletionItemInsertTextRule.InsertAsSnippet,
            documentation: snippet.description,
            detail: 'SQL Snippet',
            range,
            sortText: '3' + String(i).padStart(3, '0'),
          })
        })

        // Jinja template starter
        suggestions.push({
          label: '{{ }}',
          kind: monaco.languages.CompletionItemKind.Snippet,
          insertText: '{{ ${1} }}',
          insertTextRules: monaco.languages.CompletionItemInsertTextRule.InsertAsSnippet,
          documentation: 'Insert a Jinja template block',
          detail: 'Jinja template',
          range,
          sortText: '4000',
        })

        return { suggestions }
      },
    })

    disposablesRef.current.push(completionDisposable)

    // Focus editor
    editorInstance.focus()
  }

  // Cleanup on unmount
  useEffect(() => {
    return () => {
      disposablesRef.current.forEach(d => d.dispose())
    }
  }, [])

  // Update completions when sources/models change
  useEffect(() => {
    // The completion provider captures these via closure,
    // so we don't need to do anything special here
  }, [availableSources, availableModels])

  // Handle SQL changes
  const handleEditorChange = useCallback((newValue: string | undefined) => {
    onChange(newValue || '')
  }, [onChange])

  return (
    <div className={`rounded-lg border border-slate-700 ${className}`}>
      <Editor
        height={height}
        defaultLanguage="sql"
        theme="streamt-dark"
        value={value}
        onChange={handleEditorChange}
        beforeMount={handleEditorWillMount}
        onMount={handleEditorDidMount}
        options={{
          fontSize: 14,
          fontFamily: "'JetBrains Mono', 'Fira Code', 'Cascadia Code', Menlo, Monaco, monospace",
          fontLigatures: true,
          minimap: { enabled: false },
          lineNumbers: 'on',
          scrollBeyondLastLine: false,
          automaticLayout: true,
          tabSize: 2,
          wordWrap: 'on',
          padding: { top: 16, bottom: 16 },
          renderLineHighlight: 'all',
          cursorBlinking: 'smooth',
          cursorSmoothCaretAnimation: 'on',
          smoothScrolling: true,
          suggest: {
            showKeywords: true,
            showSnippets: true,
            showFunctions: true,
            snippetsPreventQuickSuggestions: false,
          },
          quickSuggestions: {
            other: true,
            comments: false,
            strings: true,
          },
          bracketPairColorization: { enabled: true },
          guides: {
            bracketPairs: true,
            indentation: true,
            highlightActiveIndentation: true,
          },
        }}
        loading={
          <div className="h-full flex items-center justify-center bg-slate-900" style={{ height }}>
            <RefreshCw className="w-8 h-8 text-slate-600 animate-spin" />
          </div>
        }
      />
    </div>
  )
}
