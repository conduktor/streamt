import { useState, useEffect } from 'react'
import { Plus, Trash2 } from 'lucide-react'
import { Dialog, DialogFooter } from '@/components/ui/Dialog'
import { FormField, Input, Textarea, Select } from '@/components/ui/FormField'
import { Button } from '@/components/ui/Button'
import type { DataTest, TestType, Assertion } from '@/types'

interface TestFormProps {
  open: boolean
  onClose: () => void
  onSave: (test: DataTest) => Promise<void>
  test?: DataTest | null
  existingNames: string[]
  availableModels: string[]
}

const testTypeOptions = [
  { value: 'schema', label: 'Schema' },
  { value: 'sample', label: 'Sample' },
  { value: 'continuous', label: 'Continuous' },
]

const assertionTypeOptions = [
  { value: 'not_null', label: 'Not Null' },
  { value: 'unique', label: 'Unique' },
  { value: 'accepted_values', label: 'Accepted Values' },
  { value: 'in_range', label: 'In Range' },
  { value: 'referential_integrity', label: 'Referential Integrity' },
]

interface AssertionFormState {
  type: string
  columns?: string
  column?: string
  values?: string
  min?: string
  max?: string
  key?: string
}

const emptyAssertion: AssertionFormState = { type: 'not_null', columns: '' }

export function TestForm({
  open,
  onClose,
  onSave,
  test,
  existingNames,
  availableModels,
}: TestFormProps) {
  const isEdit = !!test
  const [saving, setSaving] = useState(false)
  const [errors, setErrors] = useState<Record<string, string>>({})

  // Basic fields
  const [name, setName] = useState('')
  const [model, setModel] = useState('')
  const [type, setType] = useState<TestType>('sample')
  const [description, setDescription] = useState('')
  const [sampleSize, setSampleSize] = useState('')
  const [timeoutSeconds, setTimeoutSeconds] = useState('')

  // Assertions
  const [assertions, setAssertions] = useState<AssertionFormState[]>([{ ...emptyAssertion }])

  // Reset form when test changes
  useEffect(() => {
    if (test) {
      setName(test.name)
      setModel(test.model)
      setType(test.type)
      setDescription(test.description || '')
      setSampleSize(test.sample_size?.toString() || '')
      setTimeoutSeconds(test.timeout_seconds?.toString() || '')

      // Convert assertions to form state
      const formAssertions: AssertionFormState[] = test.assertions.map((assertion) => {
        const assertionType = Object.keys(assertion)[0]
        const config = assertion[assertionType]
        return {
          type: assertionType,
          columns: config.columns?.join(', ') || '',
          column: config.column || '',
          values: config.values?.join(', ') || '',
          min: config.min?.toString() || '',
          max: config.max?.toString() || '',
          key: config.key || '',
        }
      })
      setAssertions(formAssertions.length > 0 ? formAssertions : [{ ...emptyAssertion }])
    } else {
      setName('')
      setModel(availableModels[0] || '')
      setType('sample')
      setDescription('')
      setSampleSize('100')
      setTimeoutSeconds('60')
      setAssertions([{ ...emptyAssertion }])
    }
    setErrors({})
  }, [test, open, availableModels])

  const validate = (): boolean => {
    const newErrors: Record<string, string> = {}

    if (!name.trim()) {
      newErrors.name = 'Name is required'
    } else if (!isEdit && existingNames.includes(name.trim())) {
      newErrors.name = 'A test with this name already exists'
    } else if (!/^[a-z][a-z0-9_]*$/.test(name.trim())) {
      newErrors.name = 'Name must start with a letter and contain only lowercase letters, numbers, and underscores'
    }

    if (!model) {
      newErrors.model = 'Model is required'
    }

    const validAssertions = assertions.filter((a) => a.type)
    if (validAssertions.length === 0) {
      newErrors.assertions = 'At least one assertion is required'
    }

    setErrors(newErrors)
    return Object.keys(newErrors).length === 0
  }

  const handleSave = async () => {
    if (!validate()) return

    setSaving(true)
    try {
      // Convert form assertions to API format
      const apiAssertions: Assertion[] = assertions
        .filter((a) => a.type)
        .map((a) => {
          const config: Record<string, unknown> = {}
          if (a.columns?.trim()) {
            config.columns = a.columns.split(',').map((c) => c.trim()).filter(Boolean)
          }
          if (a.column?.trim()) {
            config.column = a.column.trim()
          }
          if (a.values?.trim()) {
            config.values = a.values.split(',').map((v) => v.trim()).filter(Boolean)
          }
          if (a.min?.trim()) {
            config.min = parseFloat(a.min)
          }
          if (a.max?.trim()) {
            config.max = parseFloat(a.max)
          }
          if (a.key?.trim()) {
            config.key = a.key.trim()
          }
          return { [a.type]: config }
        })

      const newTest: DataTest = {
        name: name.trim(),
        model,
        type,
        description: description.trim() || undefined,
        assertions: apiAssertions,
        sample_size: type === 'sample' && sampleSize ? parseInt(sampleSize) : undefined,
        timeout_seconds: timeoutSeconds ? parseInt(timeoutSeconds) : undefined,
      }

      await onSave(newTest)
      onClose()
    } catch (err) {
      setErrors({ submit: (err as Error).message })
    } finally {
      setSaving(false)
    }
  }

  const addAssertion = () => {
    setAssertions([...assertions, { ...emptyAssertion }])
  }

  const removeAssertion = (index: number) => {
    setAssertions(assertions.filter((_, i) => i !== index))
  }

  const updateAssertion = (index: number, field: keyof AssertionFormState, value: string) => {
    setAssertions(
      assertions.map((a, i) =>
        i === index ? { ...a, [field]: value } : a
      )
    )
  }

  const getAssertionFields = (assertionType: string) => {
    switch (assertionType) {
      case 'not_null':
      case 'unique':
        return ['columns']
      case 'accepted_values':
        return ['column', 'values']
      case 'in_range':
        return ['column', 'min', 'max']
      case 'referential_integrity':
        return ['column', 'key']
      default:
        return ['columns']
    }
  }

  return (
    <Dialog
      open={open}
      onClose={onClose}
      title={isEdit ? `Edit Test: ${test.name}` : 'Create Test'}
      description={isEdit ? 'Update the test configuration' : 'Define a new data quality test'}
      size="lg"
    >
      <div className="space-y-6">
        {/* Basic Info */}
        <div className="grid grid-cols-2 gap-4">
          <FormField label="Name" required error={errors.name}>
            <Input
              value={name}
              onChange={(e) => setName(e.target.value)}
              placeholder="e.g., test_payments_not_null"
              disabled={isEdit}
              error={!!errors.name}
            />
          </FormField>
          <FormField label="Model" required error={errors.model}>
            <Select
              value={model}
              onChange={(e) => setModel(e.target.value)}
              options={[
                { value: '', label: 'Select model...' },
                ...availableModels.map((m) => ({ value: m, label: m })),
              ]}
              error={!!errors.model}
            />
          </FormField>
        </div>

        <div className="grid grid-cols-3 gap-4">
          <FormField label="Test Type" required>
            <Select
              value={type}
              onChange={(e) => setType(e.target.value as TestType)}
              options={testTypeOptions}
            />
          </FormField>
          {type === 'sample' && (
            <FormField label="Sample Size">
              <Input
                type="number"
                value={sampleSize}
                onChange={(e) => setSampleSize(e.target.value)}
                placeholder="e.g., 100"
                min={1}
              />
            </FormField>
          )}
          <FormField label="Timeout (seconds)">
            <Input
              type="number"
              value={timeoutSeconds}
              onChange={(e) => setTimeoutSeconds(e.target.value)}
              placeholder="e.g., 60"
              min={1}
            />
          </FormField>
        </div>

        <FormField label="Description">
          <Textarea
            value={description}
            onChange={(e) => setDescription(e.target.value)}
            placeholder="Brief description of this test..."
            rows={2}
          />
        </FormField>

        {/* Assertions */}
        <div>
          <div className="flex items-center justify-between mb-3">
            <label className="text-sm font-medium text-slate-300">
              Assertions <span className="text-red-400">*</span>
            </label>
            <Button variant="ghost" size="sm" onClick={addAssertion} icon={<Plus className="w-4 h-4" />}>
              Add Assertion
            </Button>
          </div>
          {errors.assertions && <p className="text-sm text-red-400 mb-2">{errors.assertions}</p>}
          <div className="space-y-3">
            {assertions.map((assertion, i) => {
              const fields = getAssertionFields(assertion.type)
              return (
                <div key={i} className="p-4 bg-slate-800/50 rounded-lg space-y-3">
                  <div className="flex items-center gap-3">
                    <Select
                      value={assertion.type}
                      onChange={(e) => updateAssertion(i, 'type', e.target.value)}
                      options={assertionTypeOptions}
                      className="w-48"
                    />
                    <Button
                      variant="ghost"
                      size="icon"
                      onClick={() => removeAssertion(i)}
                      disabled={assertions.length === 1}
                      className="text-slate-500 hover:text-red-400 ml-auto"
                    >
                      <Trash2 className="w-4 h-4" />
                    </Button>
                  </div>
                  <div className="grid grid-cols-2 gap-3">
                    {fields.includes('columns') && (
                      <FormField label="Columns (comma-separated)">
                        <Input
                          value={assertion.columns || ''}
                          onChange={(e) => updateAssertion(i, 'columns', e.target.value)}
                          placeholder="e.g., payment_id, customer_id"
                        />
                      </FormField>
                    )}
                    {fields.includes('column') && (
                      <FormField label="Column">
                        <Input
                          value={assertion.column || ''}
                          onChange={(e) => updateAssertion(i, 'column', e.target.value)}
                          placeholder="e.g., status"
                        />
                      </FormField>
                    )}
                    {fields.includes('values') && (
                      <FormField label="Values (comma-separated)">
                        <Input
                          value={assertion.values || ''}
                          onChange={(e) => updateAssertion(i, 'values', e.target.value)}
                          placeholder="e.g., pending, completed, failed"
                        />
                      </FormField>
                    )}
                    {fields.includes('min') && (
                      <FormField label="Min Value">
                        <Input
                          type="number"
                          value={assertion.min || ''}
                          onChange={(e) => updateAssertion(i, 'min', e.target.value)}
                          placeholder="e.g., 0"
                        />
                      </FormField>
                    )}
                    {fields.includes('max') && (
                      <FormField label="Max Value">
                        <Input
                          type="number"
                          value={assertion.max || ''}
                          onChange={(e) => updateAssertion(i, 'max', e.target.value)}
                          placeholder="e.g., 1000000"
                        />
                      </FormField>
                    )}
                    {fields.includes('key') && (
                      <FormField label="Reference Key">
                        <Input
                          value={assertion.key || ''}
                          onChange={(e) => updateAssertion(i, 'key', e.target.value)}
                          placeholder="e.g., customers.customer_id"
                        />
                      </FormField>
                    )}
                  </div>
                </div>
              )
            })}
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
          {isEdit ? 'Update Test' : 'Create Test'}
        </Button>
      </DialogFooter>
    </Dialog>
  )
}
