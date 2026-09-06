/**
 * Custom Monaco Theme for Streamt YAML Editor
 * Provides syntax highlighting for YAML, SQL, and Jinja templates
 */

import type { Monaco } from '@monaco-editor/react'
import type { editor } from 'monaco-editor'

/**
 * Streamt dark theme - optimized for YAML with Jinja
 */
export const STREAMT_THEME: editor.IStandaloneThemeData = {
  base: 'vs-dark',
  inherit: true,
  rules: [
    // ═══════════════════════════════════════════════════════════════
    // YAML Tokens
    // ═══════════════════════════════════════════════════════════════

    // YAML keys
    { token: 'type.yaml', foreground: '7dd3fc' }, // sky-300

    // YAML values
    { token: 'string.yaml', foreground: 'a5f3fc' }, // cyan-200
    { token: 'number.yaml', foreground: 'fcd34d' }, // amber-300
    { token: 'keyword.yaml', foreground: 'c4b5fd' }, // violet-300

    // YAML anchors/aliases
    { token: 'variable.anchor', foreground: 'a78bfa' }, // violet-400
    { token: 'variable.alias', foreground: 'a78bfa', fontStyle: 'italic' },

    // YAML tags
    { token: 'metatag.tag', foreground: '94a3b8' }, // slate-400

    // ═══════════════════════════════════════════════════════════════
    // SQL Tokens - Pink/Magenta theme for SQL keywords
    // ═══════════════════════════════════════════════════════════════

    // SQL keywords (SELECT, FROM, WHERE, etc.) - Bold pink
    { token: 'keyword.sql', foreground: 'f472b6', fontStyle: 'bold' }, // pink-400

    // SQL strings ('value') - Light green
    { token: 'string.sql', foreground: '86efac' }, // green-300

    // SQL numbers
    { token: 'number.sql', foreground: 'fcd34d' }, // amber-300

    // SQL operators (=, <>, etc.)
    { token: 'operator.sql', foreground: '94a3b8' }, // slate-400

    // SQL delimiters (parentheses, commas)
    { token: 'delimiter.sql', foreground: '94a3b8' }, // slate-400

    // SQL function calls (COUNT, SUM, custom functions)
    { token: 'support.function.sql', foreground: '22d3ee' }, // cyan-400

    // SQL identifiers (column names, table names)
    { token: 'identifier.sql', foreground: 'e2e8f0' }, // slate-200

    // SQL comments
    { token: 'comment.sql', foreground: '64748b', fontStyle: 'italic' }, // slate-500

    // ═══════════════════════════════════════════════════════════════
    // Jinja Tokens - Amber/Orange theme for templating
    // ═══════════════════════════════════════════════════════════════

    // Jinja delimiters {{ }} - Bold amber
    { token: 'delimiter.bracket.jinja', foreground: 'fbbf24', fontStyle: 'bold' }, // amber-400

    // Jinja block tags {% %} - Bold amber
    { token: 'metatag.jinja', foreground: 'fbbf24', fontStyle: 'bold' }, // amber-400

    // Jinja control keywords (if, for, endif, etc.)
    { token: 'keyword.control.jinja', foreground: 'fb923c', fontStyle: 'bold' }, // orange-400

    // Jinja built-in functions (ref, source, var)
    { token: 'support.function.jinja', foreground: '22d3ee', fontStyle: 'bold' }, // cyan-400

    // Jinja filters (default, upper, etc.)
    { token: 'support.function.filter', foreground: '2dd4bf' }, // teal-400

    // Jinja strings inside templates
    { token: 'string.jinja', foreground: '86efac' }, // green-300

    // Jinja numbers
    { token: 'number.jinja', foreground: 'fcd34d' }, // amber-300

    // Jinja operators (and, or, not, |, etc.)
    { token: 'operator.jinja', foreground: 'fb923c' }, // orange-400

    // Jinja variables
    { token: 'variable.jinja', foreground: 'a78bfa' }, // violet-400

    // Jinja constants (True, False, None)
    { token: 'constant.jinja', foreground: 'fb923c' }, // orange-400

    // Jinja delimiters (parentheses, dots, etc.)
    { token: 'delimiter.jinja', foreground: 'fbbf24' }, // amber-400

    // Jinja comments {# #}
    { token: 'comment.jinja', foreground: '64748b', fontStyle: 'italic' }, // slate-500

    // ═══════════════════════════════════════════════════════════════
    // General Tokens
    // ═══════════════════════════════════════════════════════════════

    // Comments
    { token: 'comment', foreground: '64748b', fontStyle: 'italic' }, // slate-500

    // Strings
    { token: 'string', foreground: '86efac' }, // green-300
    { token: 'string.quote', foreground: '86efac' }, // green-300
    { token: 'string.escape', foreground: 'fcd34d' }, // amber-300

    // Operators
    { token: 'operator', foreground: '94a3b8' }, // slate-400
    { token: 'delimiter', foreground: '94a3b8' },
    { token: 'delimiter.bracket', foreground: '94a3b8' },

    // Special values
    { token: 'constant', foreground: 'fb923c' }, // orange-400

    // Variables
    { token: 'variable', foreground: 'a78bfa' }, // violet-400

    // Functions
    { token: 'support.function', foreground: '22d3ee' }, // cyan-400

    // Invalid
    { token: 'invalid', foreground: 'ef4444', fontStyle: 'underline' }, // red-500
  ],
  colors: {
    // Editor background
    'editor.background': '#0f172a', // slate-900
    'editor.foreground': '#e2e8f0', // slate-200

    // Selection
    'editor.selectionBackground': '#334155', // slate-700
    'editor.selectionHighlightBackground': '#475569', // slate-600

    // Line highlight
    'editor.lineHighlightBackground': '#1e293b', // slate-800
    'editor.lineHighlightBorder': '#334155', // slate-700

    // Cursor
    'editorCursor.foreground': '#7c3aed', // violet-600

    // Line numbers
    'editorLineNumber.foreground': '#475569', // slate-600
    'editorLineNumber.activeForeground': '#94a3b8', // slate-400

    // Indentation guides
    'editorIndentGuide.background': '#1e293b', // slate-800
    'editorIndentGuide.activeBackground': '#334155', // slate-700

    // Whitespace
    'editorWhitespace.foreground': '#334155', // slate-700

    // Brackets
    'editorBracketMatch.background': '#7c3aed33', // violet-600 with opacity
    'editorBracketMatch.border': '#7c3aed', // violet-600

    // Minimap
    'minimap.background': '#0f172a', // slate-900
    'minimapSlider.background': '#33415566',
    'minimapSlider.hoverBackground': '#47556988',
    'minimapSlider.activeBackground': '#475569aa',

    // Scrollbar
    'scrollbar.shadow': '#00000000',
    'scrollbarSlider.background': '#33415566',
    'scrollbarSlider.hoverBackground': '#47556988',
    'scrollbarSlider.activeBackground': '#475569aa',

    // Find/Replace
    'editor.findMatchBackground': '#fbbf2444',
    'editor.findMatchHighlightBackground': '#fbbf2422',

    // Word highlight
    'editor.wordHighlightBackground': '#7c3aed22',
    'editor.wordHighlightStrongBackground': '#7c3aed44',

    // Errors and warnings
    'editorError.foreground': '#ef4444', // red-500
    'editorWarning.foreground': '#fbbf24', // amber-400
    'editorInfo.foreground': '#38bdf8', // sky-400

    // Gutter
    'editorGutter.background': '#0f172a',
    'editorGutter.modifiedBackground': '#38bdf8', // sky-400
    'editorGutter.addedBackground': '#22c55e', // green-500
    'editorGutter.deletedBackground': '#ef4444', // red-500

    // Widgets
    'editorWidget.background': '#1e293b', // slate-800
    'editorWidget.border': '#334155', // slate-700
    'editorSuggestWidget.background': '#1e293b',
    'editorSuggestWidget.border': '#334155',
    'editorSuggestWidget.selectedBackground': '#334155',
    'editorSuggestWidget.highlightForeground': '#7c3aed',

    // Hover widget
    'editorHoverWidget.background': '#1e293b',
    'editorHoverWidget.border': '#334155',

    // Peek view
    'peekView.border': '#7c3aed',
    'peekViewEditor.background': '#0f172a',
    'peekViewResult.background': '#1e293b',
    'peekViewTitle.background': '#1e293b',
  },
}

/**
 * SQL keywords for highlighting
 */
const SQL_KEYWORDS = [
  // DML
  'SELECT', 'FROM', 'WHERE', 'AND', 'OR', 'NOT', 'IN', 'IS', 'NULL', 'AS',
  'JOIN', 'LEFT', 'RIGHT', 'INNER', 'OUTER', 'CROSS', 'FULL', 'ON', 'USING',
  'GROUP', 'BY', 'ORDER', 'HAVING', 'LIMIT', 'OFFSET', 'FETCH', 'FIRST', 'NEXT', 'ROWS', 'ONLY',
  'UNION', 'INTERSECT', 'EXCEPT', 'ALL',
  'INSERT', 'INTO', 'VALUES', 'UPDATE', 'SET', 'DELETE', 'MERGE', 'UPSERT',
  // DDL
  'CREATE', 'TABLE', 'VIEW', 'INDEX', 'DROP', 'ALTER', 'TRUNCATE', 'RENAME',
  'PRIMARY', 'KEY', 'FOREIGN', 'REFERENCES', 'CONSTRAINT', 'UNIQUE', 'CHECK', 'DEFAULT',
  // Query clauses
  'DISTINCT', 'TOP', 'PERCENT', 'WITH', 'RECURSIVE', 'LATERAL',
  // Expressions
  'CASE', 'WHEN', 'THEN', 'ELSE', 'END', 'IF', 'NULLIF', 'IFNULL',
  'COALESCE', 'CAST', 'CONVERT', 'TRY_CAST',
  // Window functions
  'OVER', 'PARTITION', 'WINDOW', 'ROWS', 'RANGE', 'UNBOUNDED', 'PRECEDING', 'FOLLOWING', 'CURRENT', 'ROW',
  // Operators
  'BETWEEN', 'LIKE', 'ILIKE', 'SIMILAR', 'REGEXP', 'RLIKE', 'GLOB', 'ESCAPE',
  'ANY', 'SOME', 'EXISTS',
  // Data types
  'INT', 'INTEGER', 'BIGINT', 'SMALLINT', 'TINYINT', 'DECIMAL', 'NUMERIC', 'FLOAT', 'DOUBLE', 'REAL',
  'VARCHAR', 'CHAR', 'TEXT', 'STRING', 'BOOLEAN', 'BOOL', 'DATE', 'TIME', 'DATETIME', 'TIMESTAMP',
  'INTERVAL', 'BINARY', 'VARBINARY', 'BLOB', 'CLOB', 'JSON', 'JSONB', 'UUID', 'ARRAY', 'MAP', 'ROW', 'STRUCT',
  // Boolean values
  'TRUE', 'FALSE',
  // Aggregates
  'COUNT', 'SUM', 'AVG', 'MIN', 'MAX', 'FIRST_VALUE', 'LAST_VALUE', 'NTH_VALUE',
  'ROW_NUMBER', 'RANK', 'DENSE_RANK', 'NTILE', 'LAG', 'LEAD', 'PERCENT_RANK', 'CUME_DIST',
  'LISTAGG', 'STRING_AGG', 'GROUP_CONCAT', 'ARRAY_AGG', 'COLLECT_LIST', 'COLLECT_SET',
  // Common functions
  'CONCAT', 'SUBSTRING', 'SUBSTR', 'TRIM', 'LTRIM', 'RTRIM', 'UPPER', 'LOWER', 'LENGTH', 'LEN',
  'REPLACE', 'SPLIT', 'REGEXP_EXTRACT', 'REGEXP_REPLACE', 'PARSE_JSON', 'TO_JSON', 'JSON_VALUE',
  'EXTRACT', 'DATE_TRUNC', 'DATE_ADD', 'DATE_SUB', 'DATEDIFF', 'NOW', 'CURRENT_TIMESTAMP', 'CURRENT_DATE',
  'ROUND', 'FLOOR', 'CEIL', 'CEILING', 'ABS', 'MOD', 'POWER', 'SQRT', 'LOG', 'LN', 'EXP',
  // Table expressions
  'TABLESAMPLE', 'PIVOT', 'UNPIVOT', 'LATERAL', 'UNNEST', 'EXPLODE',
  // Transaction/other
  'BEGIN', 'COMMIT', 'ROLLBACK', 'SAVEPOINT', 'GRANT', 'REVOKE',
  // Flink-specific
  'TUMBLE', 'HOP', 'SESSION', 'CUMULATE', 'WATERMARK', 'PROCTIME', 'ROWTIME',
  'MATCH_RECOGNIZE', 'PATTERN', 'DEFINE', 'MEASURES', 'ONE', 'PER', 'MATCH',
  'AFTER', 'SKIP', 'PAST', 'LAST', 'EVENT', 'PERMUTE',
].join('|')

/**
 * Configure YAML language with Jinja and SQL highlighting
 */
export function configureLanguage(monaco: Monaco): void {
  // Register custom Monarch tokenizer for YAML with embedded SQL and Jinja
  monaco.languages.setMonarchTokensProvider('yaml', {
    defaultToken: '',
    ignoreCase: false,

    // SQL keyword regex (case insensitive)
    sqlKeywords: new RegExp(`\\b(${SQL_KEYWORDS})\\b`, 'i'),

    tokenizer: {
      root: [
        // Jinja block tags {% %}
        [/\{%/, { token: 'metatag.jinja', next: '@jinjaBlock' }],

        // Jinja expression {{ }}
        [/\{\{/, { token: 'delimiter.bracket.jinja', next: '@jinjaExpr' }],

        // Jinja comments {# #}
        [/\{#/, { token: 'comment.jinja', next: '@jinjaComment' }],

        // YAML Comments
        [/^\s*#.*$/, 'comment'],
        [/\s+#.*$/, 'comment'],

        // YAML key with multiline indicator (sql: |) - MUST match the full pattern
        // and transition to SQL highlighting. This rule MUST come before generic key rules.
        [/^(\s*[\w][\w-]*\s*:\s*)([|>][-+]?\s*)$/, ['type.yaml', { token: 'keyword.yaml', next: '@multilineSql' }]],
        [/^(\s*-\s*[\w][\w-]*\s*:\s*)([|>][-+]?\s*)$/, ['type.yaml', { token: 'keyword.yaml', next: '@multilineSql' }]],

        // YAML keys without multiline indicator
        [/^\s*[\w][\w-]*\s*:/, 'type.yaml'],
        [/^\s*-\s*[\w][\w-]*\s*:/, 'type.yaml'],

        // List item marker
        [/^(\s*)(-)(\s+)/, ['white', 'delimiter', 'white']],

        // Double-quoted strings with potential SQL
        [/"/, { token: 'string.quote', next: '@stringDouble' }],

        // Single-quoted strings with potential SQL
        [/'/, { token: 'string.quote', next: '@stringSingle' }],

        // Numbers
        [/-?(\d+\.?\d*|\.\d+)(e[-+]?\d+)?/, 'number.yaml'],

        // Booleans and null
        [/\b(true|false|yes|no|null|~)\b/i, 'keyword.yaml'],

        // Flow arrays
        [/\[/, { token: 'delimiter.bracket', next: '@flowArray' }],

        // Flow objects
        [/\{/, { token: 'delimiter.bracket', next: '@flowObject' }],

        // Anchors and aliases
        [/&[\w-]+/, 'variable.anchor'],
        [/\*[\w-]+/, 'variable.alias'],

        // Tags
        [/![\w!.-]+/, 'metatag.tag'],

        // Whitespace
        [/\s+/, 'white'],
      ],

      // Jinja expression: {{ ... }}
      jinjaExpr: [
        [/\}\}/, { token: 'delimiter.bracket.jinja', next: '@pop' }],

        // Jinja built-in functions (streamt specific)
        [/\b(source|ref|var|config|env_var|run_query|log|exceptions|return|target|this)\b/, 'support.function.jinja'],

        // Jinja filters
        [/\|/, { token: 'operator.jinja', next: '@jinjaFilter' }],

        // Strings in Jinja
        [/"[^"]*"/, 'string.jinja'],
        [/'[^']*'/, 'string.jinja'],

        // Numbers
        [/-?\d+(\.\d+)?/, 'number.jinja'],

        // Operators
        [/[+\-*/%=<>!&|~^]/, 'operator.jinja'],
        [/\b(and|or|not|in|is)\b/, 'operator.jinja'],

        // Boolean/None
        [/\b(True|False|None|true|false|none)\b/, 'constant.jinja'],

        // Identifiers (variables)
        [/[a-zA-Z_]\w*/, 'variable.jinja'],

        // Punctuation
        [/[()[\],.:?]/, 'delimiter.jinja'],

        // Whitespace
        [/\s+/, 'white'],
      ],

      // Jinja filter after |
      jinjaFilter: [
        [/\b(default|d|first|last|length|lower|upper|title|capitalize|trim|striptags|safe|join|sort|reverse|unique|map|select|reject|selectattr|rejectattr|groupby|batch|slice|round|int|float|string|list|dictsort|abs|random|tojson|indent|wordwrap|truncate|center|replace|format|escape|e|forceescape|urlencode|urlize)\b/, { token: 'support.function.filter', next: '@pop' }],
        [/[a-zA-Z_]\w*/, { token: 'support.function.filter', next: '@pop' }],
        // Fallback: if no filter name found, just pop back
        [/./, { token: '@rematch', next: '@pop' }],
      ],

      // Jinja block: {% ... %}
      jinjaBlock: [
        [/%\}/, { token: 'metatag.jinja', next: '@pop' }],

        // Block keywords
        [/\b(if|elif|else|endif|for|endfor|block|endblock|extends|include|import|from|macro|endmacro|call|endcall|filter|endfilter|set|endset|raw|endraw|with|endwith|autoescape|endautoescape|do|continue|break)\b/, 'keyword.control.jinja'],

        // Same as jinjaExpr for the rest
        [/\b(source|ref|var|config|env_var|run_query|log|exceptions|return|target|this)\b/, 'support.function.jinja'],
        [/"[^"]*"/, 'string.jinja'],
        [/'[^']*'/, 'string.jinja'],
        [/-?\d+(\.\d+)?/, 'number.jinja'],
        [/[+\-*/%=<>!&|~^]/, 'operator.jinja'],
        [/\b(and|or|not|in|is)\b/, 'operator.jinja'],
        [/\b(True|False|None|true|false|none)\b/, 'constant.jinja'],
        [/[a-zA-Z_]\w*/, 'variable.jinja'],
        [/[()[\],.:?]/, 'delimiter.jinja'],
        [/\s+/, 'white'],
      ],

      // Jinja comment: {# ... #}
      jinjaComment: [
        [/#\}/, { token: 'comment.jinja', next: '@pop' }],
        [/./, 'comment.jinja'],
      ],

      // Double-quoted string with SQL and Jinja support
      stringDouble: [
        [/"/, { token: 'string.quote', next: '@pop' }],

        // Jinja inside strings
        [/\{\{/, { token: 'delimiter.bracket.jinja', next: '@jinjaExpr' }],
        [/\{%/, { token: 'metatag.jinja', next: '@jinjaBlock' }],

        // SQL keywords (highlight within strings)
        [/@sqlKeywords/, 'keyword.sql'],

        // SQL strings inside the string (nested quotes)
        [/'[^']*'/, 'string.sql'],

        // SQL operators
        [/[<>=!]+/, 'operator.sql'],

        // SQL numbers
        [/\b\d+(\.\d+)?\b/, 'number.sql'],

        // SQL identifiers/columns
        [/\b[a-zA-Z_]\w*\s*(?=\()/, 'support.function.sql'],

        // Everything else is string
        [/[^\\"{}]+/, 'string'],
        [/\\./, 'string.escape'],
      ],

      // Single-quoted string with SQL and Jinja support
      stringSingle: [
        [/'/, { token: 'string.quote', next: '@pop' }],

        // Jinja inside strings
        [/\{\{/, { token: 'delimiter.bracket.jinja', next: '@jinjaExpr' }],
        [/\{%/, { token: 'metatag.jinja', next: '@jinjaBlock' }],

        // SQL keywords (highlight within strings)
        [/@sqlKeywords/, 'keyword.sql'],

        // SQL double-quoted identifiers inside single-quoted string
        [/"[^"]*"/, 'string.sql'],

        // SQL operators
        [/[<>=!]+/, 'operator.sql'],

        // SQL numbers
        [/\b\d+(\.\d+)?\b/, 'number.sql'],

        // SQL identifiers/columns
        [/\b[a-zA-Z_]\w*\s*(?=\()/, 'support.function.sql'],

        // Everything else is string
        [/[^\\'{}]+/, 'string'],
        [/\\./, 'string.escape'],
      ],

      // Multiline block (after | or >) - treat as SQL
      multilineSql: [
        // Exit multiline when we see a YAML structure that indicates end of SQL block
        // We look for unindented or less-indented YAML keys/list items
        // Using conservative patterns to avoid false positives in SQL
        [/^[a-zA-Z_][\w-]*:/, { token: '@rematch', next: '@pop' }],  // Unindented key
        [/^- /, { token: '@rematch', next: '@pop' }],  // Unindented list item
        [/^  [a-zA-Z_][\w-]*:/, { token: '@rematch', next: '@pop' }],  // 2-space indented key
        [/^  - /, { token: '@rematch', next: '@pop' }],  // 2-space indented list item

        // Jinja templates
        [/\{\{/, { token: 'delimiter.bracket.jinja', next: '@jinjaExpr' }],
        [/\{%/, { token: 'metatag.jinja', next: '@jinjaBlock' }],
        [/\{#/, { token: 'comment.jinja', next: '@jinjaComment' }],

        // SQL single-line comments
        [/--.*$/, 'comment.sql'],

        // SQL keywords - must come before identifier rule
        [/@sqlKeywords/, 'keyword.sql'],

        // SQL strings
        [/'[^']*'/, 'string.sql'],
        [/"[^"]*"/, 'string.sql'],

        // SQL numbers (integers, decimals, scientific notation)
        [/-?\d+(\.\d+)?([eE][-+]?\d+)?/, 'number.sql'],

        // SQL operators
        [/[<>=!]+|::|->|->>|\|\|/, 'operator.sql'],
        [/[(),;*]/, 'delimiter.sql'],

        // SQL function calls (word followed by open paren)
        [/\b[a-zA-Z_]\w*(?=\s*\()/, 'support.function.sql'],

        // SQL identifiers/column names (anything else that's a word)
        [/\b[a-zA-Z_]\w*\b/, 'identifier.sql'],

        // Dot for qualified names
        [/\./, 'delimiter.sql'],

        // Whitespace
        [/\s+/, 'white'],

        // Anything else
        [/./, 'identifier.sql'],
      ],

      // Flow array [...]
      flowArray: [
        [/\]/, { token: 'delimiter.bracket', next: '@pop' }],
        [/,/, 'delimiter'],
        [/\{\{/, { token: 'delimiter.bracket.jinja', next: '@jinjaExpr' }],
        [/"/, { token: 'string.quote', next: '@stringDouble' }],
        [/'/, { token: 'string.quote', next: '@stringSingle' }],
        [/-?(\d+\.?\d*|\.\d+)(e[-+]?\d+)?/, 'number.yaml'],
        [/\b(true|false|yes|no|null|~)\b/i, 'keyword.yaml'],
        [/[a-zA-Z_][\w-]*/, 'string'],
        [/\s+/, 'white'],
      ],

      // Flow object {...}
      flowObject: [
        [/\}/, { token: 'delimiter.bracket', next: '@pop' }],
        [/,/, 'delimiter'],
        [/:/, 'delimiter'],
        [/\{\{/, { token: 'delimiter.bracket.jinja', next: '@jinjaExpr' }],
        [/"/, { token: 'string.quote', next: '@stringDouble' }],
        [/'/, { token: 'string.quote', next: '@stringSingle' }],
        [/-?(\d+\.?\d*|\.\d+)(e[-+]?\d+)?/, 'number.yaml'],
        [/\b(true|false|yes|no|null|~)\b/i, 'keyword.yaml'],
        [/[a-zA-Z_][\w-]*/, 'type.yaml'],
        [/\s+/, 'white'],
      ],
    },
  })
}

/**
 * Configure SQL language with Jinja highlighting
 */
export function configureSqlLanguage(monaco: Monaco): void {
  monaco.languages.setMonarchTokensProvider('sql', {
    defaultToken: '',
    ignoreCase: true,

    // SQL keyword regex
    sqlKeywords: new RegExp(`\\b(${SQL_KEYWORDS})\\b`, 'i'),

    tokenizer: {
      root: [
        // Jinja expression {{ }}
        [/\{\{/, { token: 'delimiter.bracket.jinja', next: '@jinjaExpr' }],

        // Jinja block tags {% %}
        [/\{%/, { token: 'metatag.jinja', next: '@jinjaBlock' }],

        // Jinja comments {# #}
        [/\{#/, { token: 'comment.jinja', next: '@jinjaComment' }],

        // SQL single-line comments
        [/--.*$/, 'comment.sql'],

        // SQL multi-line comments
        [/\/\*/, { token: 'comment.sql', next: '@sqlComment' }],

        // SQL keywords
        [/@sqlKeywords/, 'keyword.sql'],

        // SQL strings
        [/'[^']*'/, 'string.sql'],
        [/"[^"]*"/, 'string.sql'],

        // SQL numbers
        [/-?\d+(\.\d+)?([eE][-+]?\d+)?/, 'number.sql'],

        // SQL operators
        [/[<>=!]+|::|->|->>|\|\|/, 'operator.sql'],
        [/[(),;*]/, 'delimiter.sql'],

        // SQL function calls
        [/\b[a-zA-Z_]\w*(?=\s*\()/, 'support.function.sql'],

        // SQL identifiers
        [/\b[a-zA-Z_]\w*\b/, 'identifier.sql'],

        // Dot for qualified names
        [/\./, 'delimiter.sql'],

        // Whitespace
        [/\s+/, 'white'],
      ],

      // SQL multi-line comment
      sqlComment: [
        [/\*\//, { token: 'comment.sql', next: '@pop' }],
        [/./, 'comment.sql'],
      ],

      // Jinja expression: {{ ... }}
      jinjaExpr: [
        [/\}\}/, { token: 'delimiter.bracket.jinja', next: '@pop' }],

        // Jinja built-in functions (streamt specific)
        [/\b(source|ref|var|config|env_var|run_query|log|exceptions|return|target|this)\b/, 'support.function.jinja'],

        // Jinja filters
        [/\|/, { token: 'operator.jinja', next: '@jinjaFilter' }],

        // Strings in Jinja
        [/"[^"]*"/, 'string.jinja'],
        [/'[^']*'/, 'string.jinja'],

        // Numbers
        [/-?\d+(\.\d+)?/, 'number.jinja'],

        // Operators
        [/[+\-*/%=<>!&|~^]/, 'operator.jinja'],
        [/\b(and|or|not|in|is)\b/, 'operator.jinja'],

        // Boolean/None
        [/\b(True|False|None|true|false|none)\b/, 'constant.jinja'],

        // Identifiers (variables)
        [/[a-zA-Z_]\w*/, 'variable.jinja'],

        // Punctuation
        [/[()[\],.:?]/, 'delimiter.jinja'],

        // Whitespace
        [/\s+/, 'white'],
      ],

      // Jinja filter after |
      jinjaFilter: [
        [/\b(default|d|first|last|length|lower|upper|title|capitalize|trim|striptags|safe|join|sort|reverse|unique|map|select|reject|selectattr|rejectattr|groupby|batch|slice|round|int|float|string|list|dictsort|abs|random|tojson|indent|wordwrap|truncate|center|replace|format|escape|e|forceescape|urlencode|urlize)\b/, { token: 'support.function.filter', next: '@pop' }],
        [/[a-zA-Z_]\w*/, { token: 'support.function.filter', next: '@pop' }],
        [/./, { token: '@rematch', next: '@pop' }],
      ],

      // Jinja block: {% ... %}
      jinjaBlock: [
        [/%\}/, { token: 'metatag.jinja', next: '@pop' }],

        // Block keywords
        [/\b(if|elif|else|endif|for|endfor|block|endblock|extends|include|import|from|macro|endmacro|call|endcall|filter|endfilter|set|endset|raw|endraw|with|endwith|autoescape|endautoescape|do|continue|break)\b/, 'keyword.control.jinja'],

        // Same as jinjaExpr for the rest
        [/\b(source|ref|var|config|env_var|run_query|log|exceptions|return|target|this)\b/, 'support.function.jinja'],
        [/"[^"]*"/, 'string.jinja'],
        [/'[^']*'/, 'string.jinja'],
        [/-?\d+(\.\d+)?/, 'number.jinja'],
        [/[+\-*/%=<>!&|~^]/, 'operator.jinja'],
        [/\b(and|or|not|in|is)\b/, 'operator.jinja'],
        [/\b(True|False|None|true|false|none)\b/, 'constant.jinja'],
        [/[a-zA-Z_]\w*/, 'variable.jinja'],
        [/[()[\],.:?]/, 'delimiter.jinja'],
        [/\s+/, 'white'],
      ],

      // Jinja comment: {# ... #}
      jinjaComment: [
        [/#\}/, { token: 'comment.jinja', next: '@pop' }],
        [/./, 'comment.jinja'],
      ],
    },
  })
}

/**
 * Register the theme with Monaco
 */
export function registerTheme(monaco: Monaco): void {
  monaco.editor.defineTheme('streamt-dark', STREAMT_THEME)
}

/**
 * Configure editor options for optimal YAML editing
 */
export function getEditorOptions(): editor.IStandaloneEditorConstructionOptions {
  return {
    theme: 'streamt-dark',
    language: 'yaml',

    // Font
    fontSize: 14,
    fontFamily: "'JetBrains Mono', 'Fira Code', 'Cascadia Code', Menlo, Monaco, monospace",
    fontLigatures: true,
    fontWeight: '400',

    // Layout
    lineNumbers: 'on',
    lineNumbersMinChars: 3,
    renderLineHighlight: 'all',
    renderWhitespace: 'selection',
    rulers: [80, 120],

    // Editing
    tabSize: 2,
    insertSpaces: true,
    autoIndent: 'full',
    formatOnPaste: true,
    formatOnType: true,

    // Scrolling
    smoothScrolling: true,
    scrollBeyondLastLine: false,
    scrollbar: {
      vertical: 'visible',
      horizontal: 'visible',
      verticalScrollbarSize: 10,
      horizontalScrollbarSize: 10,
    },

    // Minimap
    minimap: {
      enabled: true,
      maxColumn: 80,
      renderCharacters: false,
      scale: 2,
      showSlider: 'mouseover',
    },

    // Suggestions
    quickSuggestions: {
      other: true,
      comments: false,
      strings: true,
    },
    suggestOnTriggerCharacters: true,
    acceptSuggestionOnEnter: 'on',
    tabCompletion: 'on',
    wordBasedSuggestions: 'off', // We provide our own

    // Brackets
    bracketPairColorization: {
      enabled: true,
    },
    guides: {
      bracketPairs: true,
      indentation: true,
      highlightActiveIndentation: true,
    },

    // Folding
    folding: true,
    foldingStrategy: 'indentation',
    showFoldingControls: 'mouseover',

    // Word wrap
    wordWrap: 'on',
    wordWrapColumn: 120,
    wrappingIndent: 'same',

    // Cursor
    cursorStyle: 'line',
    cursorBlinking: 'smooth',
    cursorSmoothCaretAnimation: 'on',

    // Inlay hints
    inlayHints: {
      enabled: 'on',
    },

    // Other
    automaticLayout: true,
    links: true,
    contextmenu: true,
    mouseWheelZoom: true,
    padding: { top: 16, bottom: 16 },
    stickyScroll: {
      enabled: true,
    },
  }
}
