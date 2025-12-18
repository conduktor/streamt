/**
 * SQL Syntax Highlighter Component
 * Provides syntax highlighting for SQL code with Jinja template support
 * Includes clickable links for ref() and source() references
 */

import { useMemo } from 'react'
import { useNavigate } from 'react-router-dom'

// SQL keywords (same list as in editor/theme.ts)
const SQL_KEYWORDS = new Set([
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
  'OVER', 'PARTITION', 'WINDOW', 'RANGE', 'UNBOUNDED', 'PRECEDING', 'FOLLOWING', 'CURRENT', 'ROW',
  // Operators
  'BETWEEN', 'LIKE', 'ILIKE', 'SIMILAR', 'REGEXP', 'RLIKE', 'GLOB', 'ESCAPE',
  'ANY', 'SOME', 'EXISTS',
  // Data types
  'INT', 'INTEGER', 'BIGINT', 'SMALLINT', 'TINYINT', 'DECIMAL', 'NUMERIC', 'FLOAT', 'DOUBLE', 'REAL',
  'VARCHAR', 'CHAR', 'TEXT', 'STRING', 'BOOLEAN', 'BOOL', 'DATE', 'TIME', 'DATETIME', 'TIMESTAMP',
  'INTERVAL', 'BINARY', 'VARBINARY', 'BLOB', 'CLOB', 'JSON', 'JSONB', 'UUID', 'ARRAY', 'MAP', 'STRUCT',
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
  'TABLESAMPLE', 'PIVOT', 'UNPIVOT', 'UNNEST', 'EXPLODE',
  // Transaction/other
  'BEGIN', 'COMMIT', 'ROLLBACK', 'SAVEPOINT', 'GRANT', 'REVOKE',
  // Flink-specific
  'TUMBLE', 'HOP', 'SESSION', 'CUMULATE', 'WATERMARK', 'PROCTIME', 'ROWTIME',
  'MATCH_RECOGNIZE', 'PATTERN', 'DEFINE', 'MEASURES', 'ONE', 'PER', 'MATCH',
  'AFTER', 'SKIP', 'PAST', 'LAST', 'EVENT', 'PERMUTE', 'DESCRIPTOR',
])

interface Token {
  type: 'keyword' | 'string' | 'number' | 'comment' | 'jinja-delim' | 'jinja-func' | 'jinja-string' | 'jinja-link' | 'operator' | 'identifier' | 'whitespace'
  value: string
  linkType?: 'ref' | 'source'
  linkTarget?: string
}

function tokenize(sql: string): Token[] {
  const tokens: Token[] = []
  let i = 0

  while (i < sql.length) {
    // Jinja expression {{ ... }}
    if (sql.slice(i, i + 2) === '{{') {
      i += 2
      tokens.push({ type: 'jinja-delim', value: '{{' })

      // Track if we're in a ref() or source() call
      let currentFunc: 'ref' | 'source' | null = null

      // Parse inside Jinja
      while (i < sql.length && sql.slice(i, i + 2) !== '}}') {
        // Skip whitespace
        if (/\s/.test(sql[i])) {
          let ws = ''
          while (i < sql.length && /\s/.test(sql[i])) {
            ws += sql[i++]
          }
          tokens.push({ type: 'whitespace', value: ws })
          continue
        }

        // Jinja string - check if it's inside a ref/source call
        if (sql[i] === '"' || sql[i] === "'") {
          const quote = sql[i]
          let str = quote
          i++
          let innerValue = ''
          while (i < sql.length && sql[i] !== quote) {
            innerValue += sql[i]
            str += sql[i++]
          }
          if (i < sql.length) str += sql[i++]

          // If we're in a ref or source function, make it a clickable link
          if (currentFunc) {
            tokens.push({
              type: 'jinja-link',
              value: str,
              linkType: currentFunc,
              linkTarget: innerValue
            })
          } else {
            tokens.push({ type: 'jinja-string', value: str })
          }
          continue
        }

        // Jinja function or identifier
        if (/[a-zA-Z_]/.test(sql[i])) {
          let word = ''
          while (i < sql.length && /[\w]/.test(sql[i])) {
            word += sql[i++]
          }
          if (word === 'ref' || word === 'source') {
            currentFunc = word as 'ref' | 'source'
            tokens.push({ type: 'jinja-func', value: word })
          } else if (['var', 'config', 'env_var'].includes(word)) {
            currentFunc = null
            tokens.push({ type: 'jinja-func', value: word })
          } else {
            tokens.push({ type: 'identifier', value: word })
          }
          continue
        }

        // Closing parenthesis resets the current function context
        if (sql[i] === ')') {
          currentFunc = null
        }

        // Other Jinja characters (parentheses, etc.)
        tokens.push({ type: 'jinja-delim', value: sql[i++] })
      }

      if (sql.slice(i, i + 2) === '}}') {
        tokens.push({ type: 'jinja-delim', value: '}}' })
        i += 2
      }
      continue
    }

    // SQL comment --
    if (sql.slice(i, i + 2) === '--') {
      let comment = ''
      while (i < sql.length && sql[i] !== '\n') {
        comment += sql[i++]
      }
      tokens.push({ type: 'comment', value: comment })
      continue
    }

    // SQL string
    if (sql[i] === "'") {
      let str = "'"
      i++
      while (i < sql.length && sql[i] !== "'") {
        str += sql[i++]
      }
      if (i < sql.length) str += sql[i++]
      tokens.push({ type: 'string', value: str })
      continue
    }

    // Number
    if (/\d/.test(sql[i]) || (sql[i] === '-' && /\d/.test(sql[i + 1]))) {
      let num = ''
      if (sql[i] === '-') num += sql[i++]
      while (i < sql.length && /[\d.]/.test(sql[i])) {
        num += sql[i++]
      }
      tokens.push({ type: 'number', value: num })
      continue
    }

    // Word (keyword or identifier)
    if (/[a-zA-Z_]/.test(sql[i])) {
      let word = ''
      while (i < sql.length && /[\w]/.test(sql[i])) {
        word += sql[i++]
      }
      if (SQL_KEYWORDS.has(word.toUpperCase())) {
        tokens.push({ type: 'keyword', value: word })
      } else {
        tokens.push({ type: 'identifier', value: word })
      }
      continue
    }

    // Operators
    if (/[<>=!+\-*/%]/.test(sql[i])) {
      let op = sql[i++]
      while (i < sql.length && /[<>=!]/.test(sql[i])) {
        op += sql[i++]
      }
      tokens.push({ type: 'operator', value: op })
      continue
    }

    // Whitespace
    if (/\s/.test(sql[i])) {
      let ws = ''
      while (i < sql.length && /\s/.test(sql[i])) {
        ws += sql[i++]
      }
      tokens.push({ type: 'whitespace', value: ws })
      continue
    }

    // Any other character
    tokens.push({ type: 'identifier', value: sql[i++] })
  }

  return tokens
}

const tokenStyles: Record<Token['type'], string> = {
  keyword: 'text-pink-400 font-semibold',
  string: 'text-green-300',
  number: 'text-amber-300',
  comment: 'text-slate-500 italic',
  'jinja-delim': 'text-amber-400 font-bold',
  'jinja-func': 'text-cyan-400 font-bold',
  'jinja-string': 'text-green-300',
  'jinja-link': 'text-green-300 underline decoration-dotted cursor-pointer hover:text-green-200 hover:decoration-solid',
  operator: 'text-slate-400',
  identifier: 'text-slate-200',
  whitespace: '',
}

interface SqlHighlighterProps {
  sql: string
  className?: string
}

export function SqlHighlighter({ sql, className = '' }: SqlHighlighterProps) {
  const navigate = useNavigate()

  const handleLinkClick = (linkType: 'ref' | 'source', target: string) => {
    if (linkType === 'ref') {
      navigate(`/models/${target}`)
    } else {
      navigate(`/sources/${target}`)
    }
  }

  const highlighted = useMemo(() => {
    const tokens = tokenize(sql)
    return tokens.map((token, i) => {
      if (token.type === 'whitespace') {
        return <span key={i}>{token.value}</span>
      }
      if (token.type === 'jinja-link' && token.linkType && token.linkTarget) {
        return (
          <span
            key={i}
            className={tokenStyles[token.type]}
            onClick={() => handleLinkClick(token.linkType!, token.linkTarget!)}
            title={`Go to ${token.linkType === 'ref' ? 'model' : 'source'}: ${token.linkTarget}`}
          >
            {token.value}
          </span>
        )
      }
      return (
        <span key={i} className={tokenStyles[token.type]}>
          {token.value}
        </span>
      )
    })
  }, [sql])

  return (
    <pre className={`text-sm bg-slate-800 p-4 rounded-lg overflow-x-auto ${className}`}>
      <code>{highlighted}</code>
    </pre>
  )
}
