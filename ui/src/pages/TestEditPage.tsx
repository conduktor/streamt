import { useState, useEffect } from 'react'
import { useParams, useNavigate, useSearchParams } from 'react-router-dom'
import { ArrowLeft, Save, Plus, Trash2, Code, FormInput } from 'lucide-react'
import yaml from 'js-yaml'
import { Card, CardContent, Button, YamlEditor } from '@/components/ui'
import { FormField, Input, Textarea, Select } from '@/components/ui/FormField'
import { useProjectStore } from '@/stores/projectStore'
import type { DataTest, TestType, Assertion } from '@/types'

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

export function TestEditPage() {
  const { name } = useParams()
  const navigate = useNavigate()
  const [searchParams, setSearchParams] = useSearchParams()
  const { project, addTest, updateTest } = useProjectStore()

  const isNew = name === 'new'
  const existingTest = !isNew ? project?.tests.find((t) => t.name === name) : null
  const existingNames = project?.tests.map((t) => t.name) ?? []
  const availableModels = project?.models.map((m) => m.name) ?? []

  const [viewMode, setViewMode] = useState<'form' | 'yaml'>(
    searchParams.get('view') === 'yaml' ? 'yaml' : 'form'
  )
  const [saving, setSaving] = useState(false)
  const [errors, setErrors] = useState<Record<string, string>>({})

  // Form state
  const [testName, setTestName] = useState('')
  const [model, setModel] = useState('')
  const [type, setType] = useState<TestType>('sample')
  const [description, setDescription] = useState('')
  const [sampleSize, setSampleSize] = useState('100')
  const [timeoutSeconds, setTimeoutSeconds] = useState('60')
  const [assertions, setAssertions] = useState<AssertionFormState[]>([{ ...emptyAssertion }])

  // YAML state
  const [yamlContent, setYamlContent] = useState('')
  const [yamlError, setYamlError] = useState('')
  const [yamlKey, setYamlKey] = useState(0)

  // Initialize form from existing test
  useEffect(() => {
    if (existingTest) {
      setTestName(existingTest.name)
      setModel(existingTest.model)
      setType(existingTest.type)
      setDescription(existingTest.description || '')
      setSampleSize(existingTest.sample_size?.toString() || '100')
      setTimeoutSeconds(existingTest.timeout_seconds?.toString() || '60')

      const formAssertions: AssertionFormState[] = existingTest.assertions.map((assertion) => {
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
      setModel(availableModels[0] || '')
    }
  }, [existingTest, availableModels])

  // Sync YAML when switching to YAML view or when form data changes in YAML mode
  useEffect(() => {
    if (viewMode === 'yaml') {
      const test = buildTestFromForm()
      if (test) {
        setYamlContent(yaml.dump(test, { indent: 2, lineWidth: -1 }))
        setYamlError('')
      }
    }
  }, [viewMode, testName, model, type, description, sampleSize, timeoutSeconds, assertions])

  const buildTestFromForm = (): DataTest | null => {
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

    return {
      name: testName.trim(),
      model,
      type,
      description: description.trim() || undefined,
      assertions: apiAssertions,
      sample_size: type === 'sample' && sampleSize ? parseInt(sampleSize) : undefined,
      timeout_seconds: timeoutSeconds ? parseInt(timeoutSeconds) : undefined,
    }
  }

  const parseYamlToForm = () => {
    try {
      const parsed = yaml.load(yamlContent) as DataTest
      if (parsed) {
        setTestName(parsed.name || '')
        setModel(parsed.model || '')
        setType(parsed.type || 'sample')
        setDescription(parsed.description || '')
        setSampleSize(parsed.sample_size?.toString() || '100')
        setTimeoutSeconds(parsed.timeout_seconds?.toString() || '60')

        const formAssertions: AssertionFormState[] = (parsed.assertions || []).map((assertion) => {
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
      if (!parseYamlToForm()) return
    } else if (mode === 'yaml') {
      // Generate YAML from form immediately
      const test = buildTestFromForm()
      if (test) {
        setYamlContent(yaml.dump(test, { indent: 2, lineWidth: -1 }))
        setYamlError('')
        setYamlKey(k => k + 1)
      }
    }
    setViewMode(mode)
    updateSearchParams({ view: mode === 'yaml' ? 'yaml' : null })
  }

  const validate = (): boolean => {
    const newErrors: Record<string, string> = {}

    if (!testName.trim()) {
      newErrors.name = 'Name is required'
    } else if (isNew && existingNames.includes(testName.trim())) {
      newErrors.name = 'A test with this name already exists'
    } else if (!/^[a-z][a-z0-9_]*$/.test(testName.trim())) {
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
    if (viewMode === 'yaml') {
      if (!parseYamlToForm()) return
    }

    if (!validate()) return

    setSaving(true)
    try {
      const test = buildTestFromForm()!
      if (isNew) {
        addTest(test)
      } else {
        updateTest(name!, test)
      }
      navigate(`/tests/${test.name}`)
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

  if (!isNew && !existingTest) {
    return (
      <div className="p-8 text-center">
        <p className="text-slate-400">Test not found</p>
        <Button variant="ghost" onClick={() => navigate('/tests')} className="mt-4">
          Back to Tests
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
              {isNew ? 'Create Test' : `Edit Test: ${name}`}
            </h2>
            <p className="text-slate-400 mt-1">
              {isNew ? 'Define a new data quality test' : 'Modify the test configuration'}
            </p>
          </div>
        </div>
        <div className="flex items-center gap-3">
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
                  value={testName}
                  onChange={(e) => setTestName(e.target.value)}
                  placeholder="e.g., test_payments_not_null"
                  disabled={!isNew}
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
