import { useState, useEffect } from 'react'
import { Plus, Trash2 } from 'lucide-react'
import { Dialog, DialogFooter } from '@/components/ui/Dialog'
import { FormField, Input, Textarea, Select } from '@/components/ui/FormField'
import { Button } from '@/components/ui/Button'
import type { Source, Column, Classification } from '@/types'

interface SourceFormProps {
  open: boolean
  onClose: () => void
  onSave: (source: Source) => Promise<void>
  source?: Source | null // null = create mode, Source = edit mode
  existingNames: string[]
}

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

export function SourceForm({ open, onClose, onSave, source, existingNames }: SourceFormProps) {
  const isEdit = !!source
  const [saving, setSaving] = useState(false)
  const [errors, setErrors] = useState<Record<string, string>>({})

  // Form state
  const [name, setName] = useState('')
  const [topic, setTopic] = useState('')
  const [description, setDescription] = useState('')
  const [columns, setColumns] = useState<Column[]>([{ ...emptyColumn }])
  const [warnAfter, setWarnAfter] = useState('')
  const [errorAfter, setErrorAfter] = useState('')

  // Reset form when source changes
  useEffect(() => {
    if (source) {
      setName(source.name)
      setTopic(source.topic)
      setDescription(source.description || '')
      setColumns(source.columns.length > 0 ? source.columns : [{ ...emptyColumn }])
      setWarnAfter(source.freshness?.warn_after_minutes?.toString() || '')
      setErrorAfter(source.freshness?.error_after_minutes?.toString() || '')
    } else {
      setName('')
      setTopic('')
      setDescription('')
      setColumns([{ ...emptyColumn }])
      setWarnAfter('')
      setErrorAfter('')
    }
    setErrors({})
  }, [source, open])

  const validate = (): boolean => {
    const newErrors: Record<string, string> = {}

    if (!name.trim()) {
      newErrors.name = 'Name is required'
    } else if (!isEdit && existingNames.includes(name.trim())) {
      newErrors.name = 'A source with this name already exists'
    } else if (!/^[a-z][a-z0-9_]*$/.test(name.trim())) {
      newErrors.name = 'Name must start with a letter and contain only lowercase letters, numbers, and underscores'
    }

    if (!topic.trim()) {
      newErrors.topic = 'Topic is required'
    }

    const validColumns = columns.filter((c) => c.name.trim())
    if (validColumns.length === 0) {
      newErrors.columns = 'At least one column is required'
    }

    // Check for duplicate column names
    const colNames = validColumns.map((c) => c.name.trim())
    const duplicates = colNames.filter((n, i) => colNames.indexOf(n) !== i)
    if (duplicates.length > 0) {
      newErrors.columns = `Duplicate column names: ${duplicates.join(', ')}`
    }

    setErrors(newErrors)
    return Object.keys(newErrors).length === 0
  }

  const handleSave = async () => {
    if (!validate()) return

    setSaving(true)
    try {
      const validColumns = columns.filter((c) => c.name.trim())
      const newSource: Source = {
        name: name.trim(),
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
      await onSave(newSource)
      onClose()
    } catch (err) {
      setErrors({ submit: (err as Error).message })
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

  return (
    <Dialog
      open={open}
      onClose={onClose}
      title={isEdit ? `Edit Source: ${source.name}` : 'Create Source'}
      description={isEdit ? 'Update the source configuration' : 'Define a new Kafka topic source'}
      size="lg"
    >
      <div className="space-y-6">
        {/* Basic Info */}
        <div className="grid grid-cols-2 gap-4">
          <FormField label="Name" required error={errors.name}>
            <Input
              value={name}
              onChange={(e) => setName(e.target.value)}
              placeholder="e.g., payments_raw"
              disabled={isEdit}
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

        {errors.submit && (
          <div className="p-3 bg-red-900/20 border border-red-800/50 rounded-lg">
            <p className="text-sm text-red-400">{errors.submit}</p>
          </div>
        )}
      </div>

      <DialogFooter>
        <Button variant="ghost" onClick={onClose} disabled={saving}>
          Cancel
        </Button>
        <Button onClick={handleSave} loading={saving}>
          {isEdit ? 'Update Source' : 'Create Source'}
        </Button>
      </DialogFooter>
    </Dialog>
  )
}
