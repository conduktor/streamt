import { useState, useEffect } from 'react'
import { useParams, useNavigate, useSearchParams } from 'react-router-dom'
import { ArrowLeft, Save, Plus, Trash2, Code, FormInput } from 'lucide-react'
import yaml from 'js-yaml'
import { Card, CardContent, Button, YamlEditor } from '@/components/ui'
import { FormField, Input, Textarea, Select } from '@/components/ui/FormField'
import { useProjectStore } from '@/stores/projectStore'
import type { Source, Column, Classification } from '@/types'

const classificationOptions = [
  { value: '', label: 'None' },
  { value: 'public', label: 'Public' },
  { value: 'internal', label: 'Internal' },
  { value: 'confidential', label: 'Confidential' },
  { value: 'sensitive', label: 'Sensitive' },
  { value: 'highly_sensitive', label: 'Highly Sensitive' },
]

const columnTypeOptions = [
  { value: 'STRING', label: 'STRING' },
  { value: 'INT', label: 'INT' },
  { value: 'BIGINT', label: 'BIGINT' },
  { value: 'DOUBLE', label: 'DOUBLE' },
  { value: 'DECIMAL(10,2)', label: 'DECIMAL(10,2)' },
  { value: 'BOOLEAN', label: 'BOOLEAN' },
  { value: 'TIMESTAMP', label: 'TIMESTAMP' },
  { value: 'DATE', label: 'DATE' },
  { value: 'ARRAY<STRING>', label: 'ARRAY<STRING>' },
  { value: 'MAP<STRING,STRING>', label: 'MAP<STRING,STRING>' },
]

const emptyColumn: Column = { name: '', type: 'STRING' }

export function SourceEditPage() {
  const { name } = useParams()
  const navigate = useNavigate()
  const [searchParams, setSearchParams] = useSearchParams()
  const { project, addSource, updateSource } = useProjectStore()

  const isNew = name === 'new'
  const existingSource = !isNew ? project?.sources.find((s) => s.name === name) : null
  const existingNames = project?.sources.map((s) => s.name) ?? []

  const [viewMode, setViewMode] = useState<'form' | 'yaml'>(
    searchParams.get('view') === 'yaml' ? 'yaml' : 'form'
  )
  const [saving, setSaving] = useState(false)
  const [errors, setErrors] = useState<Record<string, string>>({})

  // Form state
  const [sourceName, setSourceName] = useState('')
  const [topic, setTopic] = useState('')
  const [description, setDescription] = useState('')
  const [columns, setColumns] = useState<Column[]>([{ ...emptyColumn }])
  const [warnAfter, setWarnAfter] = useState('')
  const [errorAfter, setErrorAfter] = useState('')

  // YAML state
  const [yamlContent, setYamlContent] = useState('')
  const [yamlError, setYamlError] = useState('')
  const [yamlKey, setYamlKey] = useState(0)

  // Initialize form from existing source
  useEffect(() => {
    if (existingSource) {
      setSourceName(existingSource.name)
      setTopic(existingSource.topic)
      setDescription(existingSource.description || '')
      setColumns(existingSource.columns.length > 0 ? existingSource.columns : [{ ...emptyColumn }])
      setWarnAfter(existingSource.freshness?.warn_after_minutes?.toString() || '')
      setErrorAfter(existingSource.freshness?.error_after_minutes?.toString() || '')
    }
  }, [existingSource])

  // Sync YAML when switching to YAML view or when form data changes in YAML mode
  useEffect(() => {
    if (viewMode === 'yaml') {
      const source = buildSourceFromForm()
      if (source) {
        setYamlContent(yaml.dump(source, { indent: 2, lineWidth: -1 }))
        setYamlError('')
      }
    }
  }, [viewMode, sourceName, topic, description, columns, warnAfter, errorAfter])

  const buildSourceFromForm = (): Source | null => {
    const validColumns = columns.filter((c) => c.name.trim())
    return {
      name: sourceName.trim(),
      topic: topic.trim(),
      description: description.trim() || undefined,
      columns: validColumns.map((c) => ({
        name: c.name.trim(),
        type: c.type || 'STRING',
        description: c.description?.trim() || undefined,
        classification: c.classification || undefined,
      })),
      freshness:
        warnAfter || errorAfter
          ? {
              warn_after_minutes: warnAfter ? parseInt(warnAfter) : undefined,
              error_after_minutes: errorAfter ? parseInt(errorAfter) : undefined,
            }
          : undefined,
    }
  }

  const parseYamlToForm = () => {
    try {
      const parsed = yaml.load(yamlContent) as Source
      if (parsed) {
        setSourceName(parsed.name || '')
        setTopic(parsed.topic || '')
        setDescription(parsed.description || '')
        setColumns(parsed.columns?.length > 0 ? parsed.columns : [{ ...emptyColumn }])
        setWarnAfter(parsed.freshness?.warn_after_minutes?.toString() || '')
        setErrorAfter(parsed.freshness?.error_after_minutes?.toString() || '')
        setYamlError('')
        return true
      }
    } catch (err) {
      setYamlError((err as Error).message)
    }
    return false
  }

  const updateSearchParams = (updates: Record<string, string | null>) => {
    const newParams = new URLSearchParams(searchParams)
    Object.entries(updates).forEach(([key, value]) => {
      if (value === null) {
        newParams.delete(key)
      } else {
        newParams.set(key, value)
      }
    })
    setSearchParams(newParams, { replace: true })
  }

  const handleViewModeChange = (mode: 'form' | 'yaml') => {
    if (mode === 'form' && viewMode === 'yaml') {
      // Parse YAML back to form
      if (!parseYamlToForm()) return
    } else if (mode === 'yaml') {
      // Generate YAML from form immediately
      const source = buildSourceFromForm()
      if (source) {
        setYamlContent(yaml.dump(source, { indent: 2, lineWidth: -1 }))
        setYamlError('')
        setYamlKey(k => k + 1)
      }
    }
    setViewMode(mode)
    updateSearchParams({ view: mode === 'yaml' ? 'yaml' : null })
  }

  const validate = (): boolean => {
    const newErrors: Record<string, string> = {}

    if (!sourceName.trim()) {
      newErrors.name = 'Name is required'
    } else if (isNew && existingNames.includes(sourceName.trim())) {
      newErrors.name = 'A source with this name already exists'
    } else if (!/^[a-z][a-z0-9_]*$/.test(sourceName.trim())) {
      newErrors.name = 'Name must start with a letter and contain only lowercase letters, numbers, and underscores'
    }

    if (!topic.trim()) {
      newErrors.topic = 'Topic is required'
    }

    const validColumns = columns.filter((c) => c.name.trim())
    if (validColumns.length === 0) {
      newErrors.columns = 'At least one column is required'
    }

    const colNames = validColumns.map((c) => c.name.trim())
    const duplicates = colNames.filter((n, i) => colNames.indexOf(n) !== i)
    if (duplicates.length > 0) {
      newErrors.columns = `Duplicate column names: ${duplicates.join(', ')}`
    }

    setErrors(newErrors)
    return Object.keys(newErrors).length === 0
  }

  const handleSave = async () => {
    // If in YAML mode, parse first
    if (viewMode === 'yaml') {
      if (!parseYamlToForm()) return
    }

    if (!validate()) return

    setSaving(true)
    try {
      const source = buildSourceFromForm()!
      if (isNew) {
        addSource(source)
      } else {
        updateSource(name!, source)
      }
      navigate(`/sources/${source.name}`)
    } finally {
      setSaving(false)
    }
  }

  const addColumn = () => {
    setColumns([...columns, { ...emptyColumn }])
  }

  const removeColumn = (index: number) => {
    setColumns(columns.filter((_, i) => i !== index))
  }

  const updateColumn = (index: number, field: keyof Column, value: string) => {
    setColumns(
      columns.map((col, i) =>
        i === index ? { ...col, [field]: value || undefined } : col
      )
    )
  }

  if (!isNew && !existingSource) {
    return (
      <div className="p-8 text-center">
        <p className="text-slate-400">Source not found</p>
        <Button variant="ghost" onClick={() => navigate('/sources')} className="mt-4">
          Back to Sources
        </Button>
      </div>
    )
  }

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-4">
          <Button variant="ghost" size="icon" onClick={() => navigate(-1)}>
            <ArrowLeft className="w-5 h-5" />
          </Button>
          <div>
            <h2 className="text-2xl font-bold text-white">
              {isNew ? 'Create Source' : `Edit Source: ${name}`}
            </h2>
            <p className="text-slate-400 mt-1">
              {isNew ? 'Define a new Kafka topic source' : 'Modify the source configuration'}
            </p>
          </div>
        </div>
        <div className="flex items-center gap-3">
          {/* View mode toggle */}
          <div className="flex items-center bg-slate-800 rounded-lg p-1">
            <button
              onClick={() => handleViewModeChange('form')}
              className={`flex items-center gap-2 px-3 py-1.5 rounded-md text-sm transition-colors ${
                viewMode === 'form'
                  ? 'bg-streamt-600 text-white'
                  : 'text-slate-400 hover:text-white'
              }`}
            >
              <FormInput className="w-4 h-4" />
              Form
            </button>
            <button
              onClick={() => handleViewModeChange('yaml')}
              className={`flex items-center gap-2 px-3 py-1.5 rounded-md text-sm transition-colors ${
                viewMode === 'yaml'
                  ? 'bg-streamt-600 text-white'
                  : 'text-slate-400 hover:text-white'
              }`}
            >
              <Code className="w-4 h-4" />
              YAML
            </button>
          </div>
          <Button onClick={handleSave} loading={saving} icon={<Save className="w-4 h-4" />}>
            {isNew ? 'Create' : 'Save'}
          </Button>
        </div>
      </div>

      {/* Content */}
      {viewMode === 'form' ? (
        <Card>
          <CardContent className="p-6 space-y-6">
            {/* Basic Info */}
            <div className="grid grid-cols-2 gap-4">
              <FormField label="Name" required error={errors.name}>
                <Input
                  value={sourceName}
                  onChange={(e) => setSourceName(e.target.value)}
                  placeholder="e.g., payments_raw"
                  disabled={!isNew}
                  error={!!errors.name}
                />
              </FormField>
              <FormField label="Topic" required error={errors.topic}>
                <Input
                  value={topic}
                  onChange={(e) => setTopic(e.target.value)}
                  placeholder="e.g., payments.raw.v1"
                  error={!!errors.topic}
                />
              </FormField>
            </div>

            <FormField label="Description">
              <Textarea
                value={description}
                onChange={(e) => setDescription(e.target.value)}
                placeholder="Brief description of this source..."
                rows={2}
              />
            </FormField>

            {/* Columns */}
            <div>
              <div className="flex items-center justify-between mb-3">
                <label className="text-sm font-medium text-slate-300">
                  Columns <span className="text-red-400">*</span>
                </label>
                <Button variant="ghost" size="sm" onClick={addColumn} icon={<Plus className="w-4 h-4" />}>
                  Add Column
                </Button>
              </div>
              {errors.columns && <p className="text-sm text-red-400 mb-2">{errors.columns}</p>}
              <div className="space-y-2">
                {columns.map((col, i) => (
                  <div key={i} className="flex items-start gap-2 p-3 bg-slate-800/50 rounded-lg">
                    <div className="flex-1 grid grid-cols-4 gap-2">
                      <Input
                        value={col.name}
                        onChange={(e) => updateColumn(i, 'name', e.target.value)}
                        placeholder="Column name"
                      />
                      <Select
                        value={col.type || 'STRING'}
                        onChange={(e) => updateColumn(i, 'type', e.target.value)}
                        options={columnTypeOptions}
                      />
                      <Select
                        value={col.classification || ''}
                        onChange={(e) => updateColumn(i, 'classification', e.target.value as Classification)}
                        options={classificationOptions}
                      />
                      <Input
                        value={col.description || ''}
                        onChange={(e) => updateColumn(i, 'description', e.target.value)}
                        placeholder="Description"
                      />
                    </div>
                    <Button
                      variant="ghost"
                      size="icon"
                      onClick={() => removeColumn(i)}
                      disabled={columns.length === 1}
                      className="text-slate-500 hover:text-red-400"
                    >
                      <Trash2 className="w-4 h-4" />
                    </Button>
                  </div>
                ))}
              </div>
            </div>

            {/* Freshness SLA */}
            <div>
              <label className="block text-sm font-medium text-slate-300 mb-3">
                Freshness SLA (optional)
              </label>
              <div className="grid grid-cols-2 gap-4">
                <FormField label="Warn After (minutes)">
                  <Input
                    type="number"
                    value={warnAfter}
                    onChange={(e) => setWarnAfter(e.target.value)}
                    placeholder="e.g., 5"
                    min={1}
                  />
                </FormField>
                <FormField label="Error After (minutes)">
                  <Input
                    type="number"
                    value={errorAfter}
                    onChange={(e) => setErrorAfter(e.target.value)}
                    placeholder="e.g., 15"
                    min={1}
                  />
                </FormField>
              </div>
            </div>
          </CardContent>
        </Card>
      ) : (
        <Card>
          <CardContent className="p-6">
            {yamlError && (
              <div className="mb-4 p-3 bg-red-900/20 border border-red-800/50 rounded-lg">
                <p className="text-sm text-red-400">YAML Error: {yamlError}</p>
              </div>
            )}
            <YamlEditor
              key={yamlKey}
              value={yamlContent}
              onChange={setYamlContent}
              height="500px"
            />
          </CardContent>
        </Card>
      )}
    </div>
  )
}
