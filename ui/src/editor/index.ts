/**
 * Streamt YAML Editor Module
 * Exports all editor functionality for Monaco integration
 */

export * from './schema'
export * from './completionProvider'
export * from './hoverProvider'
export * from './validationProvider'
export * from './connectivityProvider'
export * from './theme'

import type { Monaco } from '@monaco-editor/react'
import type { editor } from 'monaco-editor'
import { registerCompletionProvider } from './completionProvider'
import { registerHoverProvider } from './hoverProvider'
import { setupValidation, type ValidationResult } from './validationProvider'
import { setupConnectivity } from './connectivityProvider'
export type { ValidationResult, ValidationMarker, QuickFix } from './validationProvider'
import { registerTheme, configureLanguage, configureSqlLanguage, getEditorOptions } from './theme'

/**
 * Initialize all Monaco enhancements for Streamt YAML editing
 */
export function initializeEditor(monaco: Monaco): void {
  // Register custom theme
  registerTheme(monaco)

  // Configure YAML language tokenizer
  configureLanguage(monaco)

  // Configure SQL language tokenizer with Jinja support
  configureSqlLanguage(monaco)

  // Register completion provider
  registerCompletionProvider(monaco)

  // Register hover provider
  registerHoverProvider(monaco)
}

/**
 * Set up the editor instance with validation and connectivity checks
 */
export function setupEditorInstance(
  monaco: Monaco,
  editor: editor.IStandaloneCodeEditor,
  onValidate?: (result: ValidationResult) => void
): () => void {
  const cleanupValidation = setupValidation(monaco, editor, onValidate)
  const cleanupConnectivity = setupConnectivity(monaco, editor)

  return () => {
    cleanupValidation()
    cleanupConnectivity()
  }
}

/**
 * Get recommended editor options
 */
export { getEditorOptions }
