import { useState, useEffect, useCallback, useRef } from 'react'
import Editor, { Monaco, OnMount } from '@monaco-editor/react'
import { motion, AnimatePresence } from 'framer-motion'
import {
  Play,
  CheckCircle2,
  XCircle,
  AlertTriangle,
  Copy,
  Download,
  Upload,
  FileText,
  Info,
  Sparkles,
  Keyboard,
  BookOpen,
  Lightbulb,
  ChevronDown,
  ChevronRight,
  RefreshCw,
  Wrench,
  Zap,
} from 'lucide-react'
import { Card, CardContent, CardHeader, CardTitle, Badge, Button } from '@/components/ui'
import { useProjectStore } from '@/stores/projectStore'
import toast from 'react-hot-toast'
import YAML from 'yaml'
import {
  initializeEditor,
  setupEditorInstance,
  getEditorOptions,
} from '@/editor'
import type { ValidationResult, ValidationMarker, QuickFix } from '@/editor'

const defaultYaml = `# Streamt Pipeline Configuration
# Press Ctrl+Space for autocompletion, hover for documentation

project:
  name: my-pipeline
  version: "1.0.0"
  description: "My streaming data pipeline"

runtime:
  kafka:
    bootstrap_servers: localhost:9092
  schema_registry:
    url: http://localhost:8081
  flink:
    default: local
    clusters:
      local:
        rest_url: http://localhost:8081
        sql_gateway_url: http://localhost:8083

sources:
  - name: raw_events
    description: "Raw events from external system"
    topic: events.raw.v1
    format: json
    columns:
      - name: event_id
        type: STRING
      - name: user_id
        type: STRING
      - name: event_type
        type: STRING
      - name: payload
        type: STRING
      - name: timestamp
        type: TIMESTAMP

models:
  - name: validated_events
    description: "Events with validation and enrichment"
    materialized: topic
    topic:
      partitions: 6
      replication_factor: 3
    sql: |
      SELECT
        event_id,
        user_id,
        event_type,
        payload,
        timestamp
      FROM {{ source("raw_events") }}
      WHERE event_id IS NOT NULL
        AND user_id IS NOT NULL

  - name: event_counts
    description: "Event counts per user (5-minute windows)"
    materialized: flink
    flink:
      parallelism: 4
      checkpoint_interval_ms: 60000
      state_ttl_ms: 86400000
    sql: |
      SELECT
        window_start,
        window_end,
        user_id,
        COUNT(*) as event_count
      FROM TABLE(
        TUMBLE(TABLE {{ ref("validated_events") }}, DESCRIPTOR(timestamp), INTERVAL '5' MINUTES)
      )
      GROUP BY window_start, window_end, user_id

tests:
  - name: events_not_null
    description: "Ensure key fields are never null"
    model: validated_events
    type: sample
    sample_size: 100
    assertions:
      - not_null:
          columns: [event_id, user_id, timestamp]
      - accepted_values:
          column: event_type
          values: [click, view, purchase, signup]

exposures:
  - name: analytics_dashboard
    description: "Real-time analytics dashboard"
    type: dashboard
    role: consumer
    owner: analytics-team
    depends_on:
      - ref: event_counts
    sla:
      max_latency_ms: 5000
      availability_percent: 99.9
`

export function EditorPage() {
  const { project } = useProjectStore()
  const [yaml, setYaml] = useState(defaultYaml)
  const [validationResult, setValidationResult] = useState<ValidationResult | null>(null)
  const [isValidating, setIsValidating] = useState(false)
  const [hasChanges, setHasChanges] = useState(false)
  const [showQuickReference, setShowQuickReference] = useState(true)
  const [showShortcuts, setShowShortcuts] = useState(false)
  const [expandedError, setExpandedError] = useState<number | null>(null)
  const [expandedWarning, setExpandedWarning] = useState<number | null>(null)
  const editorRef = useRef<any>(null)
  const monacoRef = useRef<Monaco | null>(null)
  const cleanupRef = useRef<(() => void) | null>(null)

  // Initialize with project YAML if available
  useEffect(() => {
    if (project) {
      try {
        const yamlContent = YAML.stringify(project, { indent: 2 })
        setYaml(yamlContent)
      } catch {
        // Keep default
      }
    }
  }, [project])

  // Handle editor mount
  const handleEditorDidMount: OnMount = (editor, monaco) => {
    editorRef.current = editor
    monacoRef.current = monaco

    // Initialize custom features
    initializeEditor(monaco)

    // Setup validation with callback
    cleanupRef.current = setupEditorInstance(monaco, editor, (result) => {
      setValidationResult(result)
    })

    // Add keyboard shortcuts
    editor.addCommand(monaco.KeyMod.CtrlCmd | monaco.KeyCode.KeyS, () => {
      handleSave()
    })

    editor.addCommand(monaco.KeyMod.CtrlCmd | monaco.KeyCode.Enter, () => {
      handleValidate()
    })

    // Focus editor
    editor.focus()
  }

  // Cleanup on unmount
  useEffect(() => {
    return () => {
      if (cleanupRef.current) {
        cleanupRef.current()
      }
    }
  }, [])

  // Handle YAML changes
  const handleEditorChange = useCallback((value: string | undefined) => {
    setYaml(value || '')
    setHasChanges(true)
  }, [])

  // Manual validation
  const handleValidate = async () => {
    setIsValidating(true)

    // Validation happens automatically via the provider
    // This just triggers UI feedback
    await new Promise((resolve) => setTimeout(resolve, 500))

    setIsValidating(false)

    if (validationResult) {
      if (validationResult.errors.length === 0) {
        toast.success('Configuration is valid!')
      } else {
        toast.error(`Found ${validationResult.errors.length} error(s)`)
      }
    }
  }

  // Save (placeholder - would connect to backend)
  const handleSave = () => {
    setHasChanges(false)
    toast.success('Configuration saved!')
  }

  // Copy to clipboard
  const handleCopy = () => {
    navigator.clipboard.writeText(yaml)
    toast.success('Copied to clipboard!')
  }

  // Download YAML
  const handleDownload = () => {
    const blob = new Blob([yaml], { type: 'text/yaml' })
    const url = URL.createObjectURL(blob)
    const a = document.createElement('a')
    a.href = url
    a.download = 'stream_project.yml'
    a.click()
    URL.revokeObjectURL(url)
    toast.success('Downloaded stream_project.yml')
  }

  // Upload YAML
  const handleUpload = () => {
    const input = document.createElement('input')
    input.type = 'file'
    input.accept = '.yml,.yaml'
    input.onchange = async (e) => {
      const file = (e.target as HTMLInputElement).files?.[0]
      if (file) {
        const text = await file.text()
        setYaml(text)
        setHasChanges(true)
        toast.success(`Loaded ${file.name}`)
      }
    }
    input.click()
  }

  // Go to line with error
  const goToLine = (line: number) => {
    if (editorRef.current) {
      editorRef.current.revealLineInCenter(line)
      editorRef.current.setPosition({ lineNumber: line, column: 1 })
      editorRef.current.focus()
    }
  }

  // Apply a quick fix
  const applyQuickFix = (fix: QuickFix, marker: ValidationMarker) => {
    if (!editorRef.current || !fix.snippet) return

    const editor = editorRef.current
    const model = editor.getModel()
    if (!model) return

    const lines = model.getLinesContent()

    if (fix.insertAtLine) {
      // Insert at specific line
      const lineNum = Math.min(fix.insertAtLine, lines.length + 1)
      const insertText = fix.snippet + '\n'

      editor.executeEdits('quickfix', [{
        range: {
          startLineNumber: lineNum,
          startColumn: 1,
          endLineNumber: lineNum,
          endColumn: 1,
        },
        text: insertText,
      }])

      toast.success(`Applied: ${fix.label}`)
    } else if (marker.startLineNumber) {
      // Replace the line where the error is
      const lineNum = marker.startLineNumber
      const lineContent = lines[lineNum - 1] || ''
      const indent = lineContent.match(/^(\s*)/)?.[1] || ''

      // Try to find and replace the relevant part
      editor.executeEdits('quickfix', [{
        range: {
          startLineNumber: lineNum,
          startColumn: 1,
          endLineNumber: lineNum,
          endColumn: lineContent.length + 1,
        },
        text: indent + fix.snippet.trim(),
      }])

      toast.success(`Applied: ${fix.label}`)
    }

    setHasChanges(true)
    setExpandedError(null)
    setExpandedWarning(null)
  }

  const errorCount = validationResult?.errors.length || 0
  const warningCount = validationResult?.warnings.length || 0
  const infoCount = validationResult?.info.length || 0

  return (
    <div className="h-[calc(100vh-8rem)] flex gap-4">
      {/* Editor */}
      <div className="flex-1 flex flex-col">
        <Card className="flex-1 flex flex-col overflow-hidden">
          <CardHeader className="flex flex-row items-center justify-between py-3 border-b border-slate-800">
            <div className="flex items-center gap-3">
              <div className="flex items-center gap-2">
                <FileText className="w-5 h-5 text-streamt-400" />
                <CardTitle className="text-base">stream_project.yml</CardTitle>
              </div>
              {hasChanges && (
                <Badge variant="warning" size="sm">Unsaved</Badge>
              )}
              {validationResult && errorCount === 0 && (
                <Badge variant="success" size="sm">
                  <CheckCircle2 className="w-3 h-3 mr-1" />
                  Valid
                </Badge>
              )}
            </div>
            <div className="flex items-center gap-2">
              <Button
                variant="ghost"
                size="sm"
                onClick={handleUpload}
                icon={<Upload className="w-4 h-4" />}
              >
                Load
              </Button>
              <Button
                variant="ghost"
                size="sm"
                onClick={handleCopy}
                icon={<Copy className="w-4 h-4" />}
              >
                Copy
              </Button>
              <Button
                variant="ghost"
                size="sm"
                onClick={handleDownload}
                icon={<Download className="w-4 h-4" />}
              >
                Download
              </Button>
              <div className="w-px h-6 bg-slate-700" />
              <Button
                variant="secondary"
                size="sm"
                onClick={handleValidate}
                loading={isValidating}
                icon={<Play className="w-4 h-4" />}
              >
                Validate
              </Button>
            </div>
          </CardHeader>
          <CardContent className="flex-1 p-0 overflow-hidden">
            <Editor
              height="100%"
              defaultLanguage="yaml"
              value={yaml}
              onChange={handleEditorChange}
              onMount={handleEditorDidMount}
              options={getEditorOptions()}
              loading={
                <div className="h-full flex items-center justify-center bg-slate-900">
                  <RefreshCw className="w-8 h-8 text-slate-600 animate-spin" />
                </div>
              }
            />
          </CardContent>
        </Card>
      </div>

      {/* Sidebar */}
      <div className="w-80 space-y-4 overflow-y-auto">
        {/* Validation Status */}
        <Card>
          <CardHeader className="py-3">
            <CardTitle className="text-sm flex items-center gap-2">
              {errorCount > 0 ? (
                <XCircle className="w-4 h-4 text-red-400" />
              ) : warningCount > 0 ? (
                <AlertTriangle className="w-4 h-4 text-yellow-400" />
              ) : (
                <CheckCircle2 className="w-4 h-4 text-emerald-400" />
              )}
              Validation Status
            </CardTitle>
          </CardHeader>
          <CardContent className="py-3">
            <div className="flex items-center gap-4 mb-3">
              <div className="flex items-center gap-1.5">
                <XCircle className="w-4 h-4 text-red-400" />
                <span className="text-sm text-slate-300">{errorCount}</span>
              </div>
              <div className="flex items-center gap-1.5">
                <AlertTriangle className="w-4 h-4 text-yellow-400" />
                <span className="text-sm text-slate-300">{warningCount}</span>
              </div>
              <div className="flex items-center gap-1.5">
                <Info className="w-4 h-4 text-blue-400" />
                <span className="text-sm text-slate-300">{infoCount}</span>
              </div>
            </div>

            {errorCount === 0 && warningCount === 0 ? (
              <div className="flex items-center gap-2 p-3 rounded-lg bg-emerald-900/20 border border-emerald-800/50">
                <Sparkles className="w-5 h-5 text-emerald-400" />
                <span className="text-sm text-emerald-400">All checks passed!</span>
              </div>
            ) : null}
          </CardContent>
        </Card>

        {/* Errors */}
        <AnimatePresence>
          {errorCount > 0 && (
            <motion.div
              initial={{ opacity: 0, height: 0 }}
              animate={{ opacity: 1, height: 'auto' }}
              exit={{ opacity: 0, height: 0 }}
            >
              <Card>
                <CardHeader className="py-3">
                  <CardTitle className="text-sm text-red-400 flex items-center gap-2">
                    <XCircle className="w-4 h-4" />
                    Errors ({errorCount})
                  </CardTitle>
                </CardHeader>
                <CardContent className="py-3 space-y-2 max-h-64 overflow-y-auto">
                  {validationResult?.errors.map((error: ValidationMarker, i: number) => (
                    <motion.div
                      key={i}
                      initial={{ opacity: 0, x: -10 }}
                      animate={{ opacity: 1, x: 0 }}
                      transition={{ delay: i * 0.05 }}
                      className="rounded bg-red-900/20 border border-red-900/30 overflow-hidden"
                    >
                      <button
                        onClick={() => {
                          if (error.quickFixes && error.quickFixes.length > 0) {
                            setExpandedError(expandedError === i ? null : i)
                          }
                          error.startLineNumber && goToLine(error.startLineNumber)
                        }}
                        className="w-full text-left p-2 text-sm text-red-300 hover:bg-red-900/30 transition-colors"
                      >
                        <div className="flex items-start gap-2">
                          <span className="text-red-500 font-mono text-xs shrink-0">
                            {error.startLineNumber ? `L${error.startLineNumber}` : ''}
                          </span>
                          <span className="flex-1">{error.message}</span>
                          {error.quickFixes && error.quickFixes.length > 0 && (
                            <Wrench className="w-3.5 h-3.5 text-red-400 shrink-0" />
                          )}
                        </div>
                      </button>
                      <AnimatePresence>
                        {expandedError === i && error.quickFixes && error.quickFixes.length > 0 && (
                          <motion.div
                            initial={{ height: 0, opacity: 0 }}
                            animate={{ height: 'auto', opacity: 1 }}
                            exit={{ height: 0, opacity: 0 }}
                            className="border-t border-red-900/30 bg-red-950/30"
                          >
                            <div className="p-2 space-y-1.5">
                              <p className="text-xs text-red-400 font-medium flex items-center gap-1">
                                <Zap className="w-3 h-3" />
                                Quick Fixes
                              </p>
                              {error.quickFixes.map((fix: QuickFix, j: number) => (
                                <button
                                  key={j}
                                  onClick={(e) => {
                                    e.stopPropagation()
                                    applyQuickFix(fix, error)
                                  }}
                                  className="w-full text-left p-1.5 rounded bg-red-900/30 hover:bg-red-900/50 transition-colors text-xs"
                                >
                                  <span className="text-red-200 font-medium">{fix.label}</span>
                                  {fix.description && (
                                    <span className="text-red-400 ml-2">{fix.description}</span>
                                  )}
                                </button>
                              ))}
                            </div>
                          </motion.div>
                        )}
                      </AnimatePresence>
                    </motion.div>
                  ))}
                </CardContent>
              </Card>
            </motion.div>
          )}
        </AnimatePresence>

        {/* Warnings */}
        <AnimatePresence>
          {warningCount > 0 && (
            <motion.div
              initial={{ opacity: 0, height: 0 }}
              animate={{ opacity: 1, height: 'auto' }}
              exit={{ opacity: 0, height: 0 }}
            >
              <Card>
                <CardHeader className="py-3">
                  <CardTitle className="text-sm text-yellow-400 flex items-center gap-2">
                    <AlertTriangle className="w-4 h-4" />
                    Warnings ({warningCount})
                  </CardTitle>
                </CardHeader>
                <CardContent className="py-3 space-y-2 max-h-64 overflow-y-auto">
                  {validationResult?.warnings.map((warning: ValidationMarker, i: number) => (
                    <motion.div
                      key={i}
                      initial={{ opacity: 0, x: -10 }}
                      animate={{ opacity: 1, x: 0 }}
                      transition={{ delay: i * 0.05 }}
                      className="rounded bg-yellow-900/20 border border-yellow-900/30 overflow-hidden"
                    >
                      <button
                        onClick={() => {
                          if (warning.quickFixes && warning.quickFixes.length > 0) {
                            setExpandedWarning(expandedWarning === i ? null : i)
                          }
                          warning.startLineNumber && goToLine(warning.startLineNumber)
                        }}
                        className="w-full text-left p-2 text-sm text-yellow-300 hover:bg-yellow-900/30 transition-colors"
                      >
                        <div className="flex items-start gap-2">
                          <span className="text-yellow-500 font-mono text-xs shrink-0">
                            {warning.startLineNumber ? `L${warning.startLineNumber}` : ''}
                          </span>
                          <span className="flex-1">{warning.message}</span>
                          {warning.quickFixes && warning.quickFixes.length > 0 && (
                            <Wrench className="w-3.5 h-3.5 text-yellow-400 shrink-0" />
                          )}
                        </div>
                      </button>
                      <AnimatePresence>
                        {expandedWarning === i && warning.quickFixes && warning.quickFixes.length > 0 && (
                          <motion.div
                            initial={{ height: 0, opacity: 0 }}
                            animate={{ height: 'auto', opacity: 1 }}
                            exit={{ height: 0, opacity: 0 }}
                            className="border-t border-yellow-900/30 bg-yellow-950/30"
                          >
                            <div className="p-2 space-y-1.5">
                              <p className="text-xs text-yellow-400 font-medium flex items-center gap-1">
                                <Zap className="w-3 h-3" />
                                Quick Fixes
                              </p>
                              {warning.quickFixes.map((fix: QuickFix, j: number) => (
                                <button
                                  key={j}
                                  onClick={(e) => {
                                    e.stopPropagation()
                                    applyQuickFix(fix, warning)
                                  }}
                                  className="w-full text-left p-1.5 rounded bg-yellow-900/30 hover:bg-yellow-900/50 transition-colors text-xs"
                                >
                                  <span className="text-yellow-200 font-medium">{fix.label}</span>
                                  {fix.description && (
                                    <span className="text-yellow-400 ml-2">{fix.description}</span>
                                  )}
                                </button>
                              ))}
                            </div>
                          </motion.div>
                        )}
                      </AnimatePresence>
                    </motion.div>
                  ))}
                </CardContent>
              </Card>
            </motion.div>
          )}
        </AnimatePresence>

        {/* Quick Reference */}
        <Card>
          <div
            className="py-3 px-4 cursor-pointer hover:bg-slate-800/50 transition-colors border-b border-slate-800"
            onClick={() => setShowQuickReference(!showQuickReference)}
          >
            <div className="text-sm font-semibold flex items-center justify-between">
              <span className="flex items-center gap-2">
                <BookOpen className="w-4 h-4 text-streamt-400" />
                Quick Reference
              </span>
              {showQuickReference ? (
                <ChevronDown className="w-4 h-4 text-slate-500" />
              ) : (
                <ChevronRight className="w-4 h-4 text-slate-500" />
              )}
            </div>
          </div>
          <AnimatePresence>
            {showQuickReference && (
              <motion.div
                initial={{ height: 0, opacity: 0 }}
                animate={{ height: 'auto', opacity: 1 }}
                exit={{ height: 0, opacity: 0 }}
              >
                <CardContent className="py-3 space-y-4 text-xs">
                  <div>
                    <p className="font-medium text-slate-300 mb-2 flex items-center gap-1">
                      <Lightbulb className="w-3 h-3" />
                      Jinja Templates
                    </p>
                    <div className="space-y-1.5 text-slate-500">
                      <p><code className="text-cyan-400">{'{{ source("name") }}'}</code> - Reference source</p>
                      <p><code className="text-cyan-400">{'{{ ref("model") }}'}</code> - Reference model</p>
                      <p><code className="text-cyan-400">{'{{ var("name", "default") }}'}</code> - Variable</p>
                    </div>
                  </div>

                  <div>
                    <p className="font-medium text-slate-300 mb-2">Materializations</p>
                    <div className="space-y-1.5 text-slate-500">
                      <p><code className="text-purple-400">topic</code> - Stateless transform</p>
                      <p><code className="text-orange-400">flink</code> - Stateful processing</p>
                      <p><code className="text-cyan-400">virtual_topic</code> - Read-time filter</p>
                      <p><code className="text-pink-400">sink</code> - Export to external</p>
                    </div>
                  </div>

                  <div>
                    <p className="font-medium text-slate-300 mb-2">Test Types</p>
                    <div className="space-y-1.5 text-slate-500">
                      <p><code className="text-blue-400">schema</code> - Structure validation</p>
                      <p><code className="text-yellow-400">sample</code> - Sample N messages</p>
                      <p><code className="text-green-400">continuous</code> - Flink monitoring</p>
                    </div>
                  </div>

                  <div>
                    <p className="font-medium text-slate-300 mb-2">Common Assertions</p>
                    <div className="space-y-1.5 text-slate-500">
                      <p><code className="text-streamt-400">not_null</code> - No NULL values</p>
                      <p><code className="text-streamt-400">unique</code> - Unique values</p>
                      <p><code className="text-streamt-400">accepted_values</code> - Value whitelist</p>
                      <p><code className="text-streamt-400">range</code> - Numeric range</p>
                    </div>
                  </div>
                </CardContent>
              </motion.div>
            )}
          </AnimatePresence>
        </Card>

        {/* Keyboard Shortcuts */}
        <Card>
          <div
            className="py-3 px-4 cursor-pointer hover:bg-slate-800/50 transition-colors border-b border-slate-800"
            onClick={() => setShowShortcuts(!showShortcuts)}
          >
            <div className="text-sm font-semibold flex items-center justify-between">
              <span className="flex items-center gap-2">
                <Keyboard className="w-4 h-4 text-streamt-400" />
                Keyboard Shortcuts
              </span>
              {showShortcuts ? (
                <ChevronDown className="w-4 h-4 text-slate-500" />
              ) : (
                <ChevronRight className="w-4 h-4 text-slate-500" />
              )}
            </div>
          </div>
          <AnimatePresence>
            {showShortcuts && (
              <motion.div
                initial={{ height: 0, opacity: 0 }}
                animate={{ height: 'auto', opacity: 1 }}
                exit={{ height: 0, opacity: 0 }}
              >
                <CardContent className="py-3 space-y-2">
                  {[
                    { keys: ['Ctrl', 'Space'], desc: 'Trigger autocomplete' },
                    { keys: ['Ctrl', 'S'], desc: 'Save' },
                    { keys: ['Ctrl', 'Enter'], desc: 'Validate' },
                    { keys: ['Ctrl', '/'], desc: 'Toggle comment' },
                    { keys: ['Ctrl', 'D'], desc: 'Duplicate line' },
                    { keys: ['Alt', '↑↓'], desc: 'Move line' },
                    { keys: ['Ctrl', 'Shift', 'K'], desc: 'Delete line' },
                    { keys: ['F1'], desc: 'Command palette' },
                  ].map((shortcut, i) => (
                    <div key={i} className="flex items-center justify-between text-xs">
                      <span className="text-slate-400">{shortcut.desc}</span>
                      <div className="flex items-center gap-1">
                        {shortcut.keys.map((key, j) => (
                          <kbd
                            key={j}
                            className="px-1.5 py-0.5 bg-slate-800 text-slate-300 rounded border border-slate-700 font-mono"
                          >
                            {key}
                          </kbd>
                        ))}
                      </div>
                    </div>
                  ))}
                </CardContent>
              </motion.div>
            )}
          </AnimatePresence>
        </Card>

        {/* Pro Tips */}
        <Card className="border-streamt-800/50 bg-gradient-to-br from-streamt-900/10 to-transparent">
          <CardContent className="py-4">
            <div className="flex items-start gap-3">
              <Sparkles className="w-5 h-5 text-streamt-400 flex-shrink-0" />
              <div className="text-xs">
                <p className="font-medium text-streamt-400 mb-1">Pro Tip</p>
                <p className="text-slate-400">
                  Type <code className="text-slate-300">{'{{ '}</code> inside SQL blocks to get
                  intelligent suggestions for source and model references.
                </p>
              </div>
            </div>
          </CardContent>
        </Card>
      </div>
    </div>
  )
}
