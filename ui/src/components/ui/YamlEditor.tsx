import { useRef, useEffect, useCallback } from 'react'
import Editor, { Monaco, OnMount, BeforeMount } from '@monaco-editor/react'
import { RefreshCw } from 'lucide-react'
import {
  initializeEditor,
  setupEditorInstance,
  getEditorOptions,
} from '@/editor'
import type { ValidationResult } from '@/editor'

interface YamlEditorProps {
  value: string
  onChange: (value: string) => void
  onValidate?: (result: ValidationResult) => void
  height?: string
  className?: string
}

export function YamlEditor({
  value,
  onChange,
  onValidate,
  height = '400px',
  className = '',
}: YamlEditorProps) {
  const editorRef = useRef<any>(null)
  const monacoRef = useRef<Monaco | null>(null)
  const cleanupRef = useRef<(() => void) | null>(null)

  // Initialize theme and language BEFORE editor mounts
  const handleEditorWillMount: BeforeMount = (monaco) => {
    initializeEditor(monaco)
  }

  // Handle editor mount
  const handleEditorDidMount: OnMount = (editor, monaco) => {
    editorRef.current = editor
    monacoRef.current = monaco

    // Explicitly set theme to ensure it's applied on remount
    monaco.editor.setTheme('streamt-dark')

    // Setup validation with callback
    cleanupRef.current = setupEditorInstance(monaco, editor, (result) => {
      onValidate?.(result)
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
  const handleEditorChange = useCallback((newValue: string | undefined) => {
    onChange(newValue || '')
  }, [onChange])

  return (
    <div className={`rounded-lg border border-slate-700 ${className}`}>
      <Editor
        height={height}
        defaultLanguage="yaml"
        theme="streamt-dark"
        value={value}
        onChange={handleEditorChange}
        beforeMount={handleEditorWillMount}
        onMount={handleEditorDidMount}
        options={getEditorOptions()}
        loading={
          <div className="h-full flex items-center justify-center bg-slate-900" style={{ height }}>
            <RefreshCw className="w-8 h-8 text-slate-600 animate-spin" />
          </div>
        }
      />
    </div>
  )
}
