/**
 * Connectivity Provider for Streamt YAML Editor
 * Checks availability of external services (Kafka, Flink, Gateway, etc.)
 * and displays status markers next to the configuration.
 */

import type { Monaco } from '@monaco-editor/react'
import type { editor } from 'monaco-editor'
import { api } from '@/utils/api'

// Keys that contain URLs or connection strings we want to check
const CONNECTIVITY_KEYS = [
  'url',
  'rest_url',
  'sql_gateway_url',
  'bootstrap_servers',
  'hosts', // elasticsearch
]

interface ConnectivityStatus {
  line: number
  column: number
  endColumn: number
  url: string
  key: string
  status: 'pending' | 'online' | 'offline'
}

/**
 * Scan document for connectivity configuration
 */
function scanForConnectivity(model: editor.ITextModel): ConnectivityStatus[] {
  const lines = model.getLinesContent()
  const items: ConnectivityStatus[] = []

  lines.forEach((line, index) => {
    const match = line.match(/^(\s*)(\w+):\s*(.+)$/)
    if (match) {
      // const indent = match[1].length  // Available for future hierarchical parsing
      const key = match[2]
      const value = match[3].trim().replace(/^["']|["']$/g, '') // strip quotes

      if (CONNECTIVITY_KEYS.includes(key) && value.length > 0 && !value.includes('${')) { // skip env vars
        // Normalize URL if needed (e.g. bootstrap_servers might be list)
        // For simple check, we take the first item if comma separated
        const urlToCheck = value.split(',')[0].trim()

        // Ensure it looks like a URL or host:port
        if (urlToCheck.startsWith('http') || urlToCheck.match(/^[\w.-]+:\d+$/)) {
           // If host:port, assume http for checking
           let finalUrl = urlToCheck
           if (!finalUrl.startsWith('http')) {
             finalUrl = `http://${finalUrl}`
           }

           // Calculate column positions for the URL value
           const valueStart = line.indexOf(value) + 1
           const valueEnd = valueStart + value.length

           items.push({
             line: index + 1,
             column: valueStart,
             endColumn: valueEnd,
             url: finalUrl,
             key,
             status: 'pending'
           })
        }
      }
    }
  })

  return items
}

/**
 * Setup connectivity checks with Monaco markers
 */
export function setupConnectivity(
  monaco: Monaco,
  editor: editor.IStandaloneCodeEditor
): () => void {
  let decorations: string[] = []
  let timeoutId: NodeJS.Timeout | null = null
  let items: ConnectivityStatus[] = []
  let isChecking = false

  const updateMarkers = () => {
    const model = editor.getModel()
    if (!model) return

    // Create markers for offline URLs
    const markers: editor.IMarkerData[] = items
      .filter(item => item.status === 'offline')
      .map(item => ({
        severity: monaco.MarkerSeverity.Warning,
        message: `Unreachable: ${item.url} - service may be down or not started`,
        startLineNumber: item.line,
        startColumn: item.column,
        endLineNumber: item.line,
        endColumn: item.endColumn,
        source: 'connectivity',
        tags: [monaco.MarkerTag.Unnecessary], // Shows as faded
      }))

    // Set markers on the model (separate owner from validation)
    monaco.editor.setModelMarkers(model, 'connectivity', markers)
  }

  const updateDecorations = () => {
    const newDecorations: editor.IModelDeltaDecoration[] = items.map(item => {
      let content = ''
      let className = ''

      if (item.status === 'online') {
        content = ' ✓'
        className = 'connectivity-online'
      } else if (item.status === 'offline') {
        content = ' ✗ unreachable'
        className = 'connectivity-offline'
      } else {
        content = ' ⋯'
        className = 'connectivity-pending'
      }

      return {
        range: new monaco.Range(item.line, item.endColumn, item.line, item.endColumn),
        options: {
          after: {
            content,
            inlineClassName: className,
          },
          // Also add glyph margin icon for better visibility
          glyphMarginClassName: item.status === 'offline' ? 'connectivity-glyph-offline' :
                                 item.status === 'online' ? 'connectivity-glyph-online' : undefined,
        },
      }
    })

    decorations = editor.deltaDecorations(decorations, newDecorations)
  }

  const checkAll = async () => {
    if (isChecking) return
    isChecking = true

    const model = editor.getModel()
    if (!model) {
      isChecking = false
      return
    }

    items = scanForConnectivity(model)

    if (items.length === 0) {
      // Clear old markers and decorations
      monaco.editor.setModelMarkers(model, 'connectivity', [])
      decorations = editor.deltaDecorations(decorations, [])
      isChecking = false
      return
    }

    updateDecorations() // Show pending

    // Check each URL in parallel
    await Promise.all(items.map(async (item) => {
      try {
        const isOnline = await api.checkConnectivity(item.url)
        item.status = isOnline ? 'online' : 'offline'
      } catch {
        item.status = 'offline'
      }
    }))

    updateDecorations() // Update with final status
    updateMarkers()     // Set squiggly lines for offline URLs

    isChecking = false
  }

  // Add CSS styles for connectivity decorations
  const styleId = 'streamt-connectivity-styles'
  if (!document.getElementById(styleId)) {
    const style = document.createElement('style')
    style.id = styleId
    style.textContent = `
      .connectivity-online {
        color: #4ade80 !important;
        font-style: italic;
      }
      .connectivity-offline {
        color: #f87171 !important;
        font-weight: bold;
      }
      .connectivity-pending {
        color: #94a3b8 !important;
        font-style: italic;
      }
      .connectivity-glyph-offline {
        background: url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='16' height='16' viewBox='0 0 24 24' fill='none' stroke='%23f87171' stroke-width='2'%3E%3Ccircle cx='12' cy='12' r='10'/%3E%3Cline x1='15' y1='9' x2='9' y2='15'/%3E%3Cline x1='9' y1='9' x2='15' y2='15'/%3E%3C/svg%3E") center center no-repeat;
      }
      .connectivity-glyph-online {
        background: url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='16' height='16' viewBox='0 0 24 24' fill='none' stroke='%234ade80' stroke-width='2'%3E%3Cpath d='M22 11.08V12a10 10 0 1 1-5.93-9.14'/%3E%3Cpolyline points='22 4 12 14.01 9 11.01'/%3E%3C/svg%3E") center center no-repeat;
      }
    `
    document.head.appendChild(style)
  }

  // Initial check after a small delay to let editor settle
  setTimeout(checkAll, 500)

  // Re-check on change with debounce
  const disposable = editor.onDidChangeModelContent(() => {
    if (timeoutId) clearTimeout(timeoutId)
    timeoutId = setTimeout(checkAll, 2000) // Don't spam checks while typing
  })

  return () => {
    if (timeoutId) clearTimeout(timeoutId)
    disposable.dispose()
    decorations = editor.deltaDecorations(decorations, [])
    const model = editor.getModel()
    if (model) {
      monaco.editor.setModelMarkers(model, 'connectivity', [])
    }
  }
}
