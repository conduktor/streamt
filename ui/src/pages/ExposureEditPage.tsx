import { useState, useEffect } from 'react'
import { useParams, useNavigate, useSearchParams } from 'react-router-dom'
import { ArrowLeft, Save, Plus, Trash2, Code, FormInput } from 'lucide-react'
import yaml from 'js-yaml'
import { Card, CardContent, Button, YamlEditor } from '@/components/ui'
import { FormField, Input, Textarea, Select } from '@/components/ui/FormField'
import { useProjectStore } from '@/stores/projectStore'
import type { Exposure, ExposureType, ExposureRole, ExposureRef } from '@/types'

const exposureTypeOptions = [
  { value: 'application', label: 'Application' },
  { value: 'dashboard', label: 'Dashboard' },
  { value: 'ml_model', label: 'ML Model' },
  { value: 'notebook', label: 'Notebook' },
  { value: 'other', label: 'Other' },
]

const roleOptions = [
  { value: '', label: 'None' },
  { value: 'producer', label: 'Producer' },
  { value: 'consumer', label: 'Consumer' },
  { value: 'both', label: 'Both' },
]

interface RefFormState {
  type: 'source' | 'ref'
  value: string
}

export function ExposureEditPage() {
  const { name } = useParams()
  const navigate = useNavigate()
  const [searchParams, setSearchParams] = useSearchParams()
  const { project, addExposure, updateExposure } = useProjectStore()

  const isNew = name === 'new'
  const existingExposure = !isNew ? project?.exposures.find((e) => e.name === name) : null
  const existingNames = project?.exposures.map((e) => e.name) ?? []
  const availableSources = project?.sources.map((s) => s.name) ?? []
  const availableModels = project?.models.map((m) => m.name) ?? []

  const [viewMode, setViewMode] = useState<'form' | 'yaml'>(
    searchParams.get('view') === 'yaml' ? 'yaml' : 'form'
  )
  const initialTab = searchParams.get('tab') as 'basic' | 'refs' | 'sla' | null
  const [activeTab, setActiveTab] = useState<'basic' | 'refs' | 'sla'>(
    initialTab && ['basic', 'refs', 'sla'].includes(initialTab) ? initialTab : 'basic'
  )

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

  const handleTabChange = (tab: 'basic' | 'refs' | 'sla') => {
    setActiveTab(tab)
    updateSearchParams({ tab: tab === 'basic' ? null : tab })
  }
  const [saving, setSaving] = useState(false)
  const [errors, setErrors] = useState<Record<string, string>>({})

  // Basic fields
  const [exposureName, setExposureName] = useState('')
  const [type, setType] = useState<ExposureType>('application')
  const [role, setRole] = useState<ExposureRole | ''>('')
  const [description, setDescription] = useState('')
  const [owner, setOwner] = useState('')
  const [consumerGroup, setConsumerGroup] = useState('')

  // References
  const [produces, setProduces] = useState<RefFormState[]>([])
  const [consumes, setConsumes] = useState<RefFormState[]>([])
  const [dependsOn, setDependsOn] = useState<RefFormState[]>([])

  // SLA
  const [maxLatencyMs, setMaxLatencyMs] = useState('')
  const [availabilityPercent, setAvailabilityPercent] = useState('')

  // YAML state
  const [yamlContent, setYamlContent] = useState('')
  const [yamlError, setYamlError] = useState('')
  const [yamlKey, setYamlKey] = useState(0)

  // Initialize form from existing exposure
  useEffect(() => {
    if (existingExposure) {
      setExposureName(existingExposure.name)
      setType(existingExposure.type)
      setRole(existingExposure.role || '')
      setDescription(existingExposure.description || '')
      setOwner(existingExposure.owner || '')
      setConsumerGroup(existingExposure.consumer_group || '')

      setProduces(
        (existingExposure.produces || [])
          .filter((p) => p.ref || p.source)
          .map((p) => ({
            type: p.source ? 'source' : 'ref',
            value: p.source || p.ref || '',
          }))
      )
      setConsumes(
        (existingExposure.consumes || [])
          .filter((c) => c.ref || c.source)
          .map((c) => ({
            type: c.source ? 'source' : 'ref',
            value: c.source || c.ref || '',
          }))
      )
      setDependsOn(
        (existingExposure.depends_on || [])
          .filter((d) => d.ref || d.source)
          .map((d) => ({
            type: d.source ? 'source' : 'ref',
            value: d.source || d.ref || '',
          }))
      )

      setMaxLatencyMs(existingExposure.sla?.max_latency_ms?.toString() || '')
      setAvailabilityPercent(existingExposure.sla?.availability_percent?.toString() || '')
    }
  }, [existingExposure])

  // Sync YAML when switching to YAML view or when form data changes in YAML mode
  useEffect(() => {
    if (viewMode === 'yaml') {
      const exposure = buildExposureFromForm()
      if (exposure) {
        setYamlContent(yaml.dump(exposure, { indent: 2, lineWidth: -1 }))
        setYamlError('')
      }
    }
  }, [viewMode, exposureName, type, role, description, owner, consumerGroup, produces, consumes, dependsOn, maxLatencyMs, availabilityPercent])

  const buildExposureFromForm = (): Exposure | null => {
    const convertRefs = (refs: RefFormState[]): ExposureRef[] =>
      refs.filter((r) => r.value).map((r) =>
        r.type === 'source' ? { source: r.value } : { ref: r.value }
      )

    return {
      name: exposureName.trim(),
      type,
      role: role || undefined,
      description: description.trim() || undefined,
      owner: owner.trim() || undefined,
      produces: convertRefs(produces),
      consumes: convertRefs(consumes),
      depends_on: convertRefs(dependsOn),
      consumer_group: consumerGroup.trim() || undefined,
      sla:
        maxLatencyMs || availabilityPercent
          ? {
              max_latency_ms: maxLatencyMs ? parseInt(maxLatencyMs) : undefined,
              availability_percent: availabilityPercent ? parseFloat(availabilityPercent) : undefined,
            }
          : undefined,
    }
  }

  const parseYamlToForm = () => {
    try {
      const parsed = yaml.load(yamlContent) as Exposure
      if (parsed) {
        setExposureName(parsed.name || '')
        setType(parsed.type || 'application')
        setRole(parsed.role || '')
        setDescription(parsed.description || '')
        setOwner(parsed.owner || '')
        setConsumerGroup(parsed.consumer_group || '')

        setProduces(
          (parsed.produces || [])
            .filter((p) => p.ref || p.source)
            .map((p) => ({
              type: p.source ? 'source' : 'ref',
              value: p.source || p.ref || '',
            }))
        )
        setConsumes(
          (parsed.consumes || [])
            .filter((c) => c.ref || c.source)
            .map((c) => ({
              type: c.source ? 'source' : 'ref',
              value: c.source || c.ref || '',
            }))
        )
        setDependsOn(
          (parsed.depends_on || [])
            .filter((d) => d.ref || d.source)
            .map((d) => ({
              type: d.source ? 'source' : 'ref',
              value: d.source || d.ref || '',
            }))
        )

        setMaxLatencyMs(parsed.sla?.max_latency_ms?.toString() || '')
        setAvailabilityPercent(parsed.sla?.availability_percent?.toString() || '')

        setYamlError('')
        return true
      }
    } catch (err) {
      setYamlError((err as Error).message)
    }
    return false
  }

  const handleViewModeChange = (mode: 'form' | 'yaml') => {
    if (mode === 'form' && viewMode === 'yaml') {
      if (!parseYamlToForm()) return
    } else if (mode === 'yaml') {
      // Generate YAML from form immediately
      const exposure = buildExposureFromForm()
      if (exposure) {
        setYamlContent(yaml.dump(exposure, { indent: 2, lineWidth: -1 }))
        setYamlError('')
        setYamlKey(k => k + 1)
      }
    }
    setViewMode(mode)
    updateSearchParams({ view: mode === 'yaml' ? 'yaml' : null })
  }

  const validate = (): boolean => {
    const newErrors: Record<string, string> = {}

    if (!exposureName.trim()) {
      newErrors.name = 'Name is required'
    } else if (isNew && existingNames.includes(exposureName.trim())) {
      newErrors.name = 'An exposure with this name already exists'
    } else if (!/^[a-z][a-z0-9_]*$/.test(exposureName.trim())) {
      newErrors.name = 'Name must start with a letter and contain only lowercase letters, numbers, and underscores'
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
      const exposure = buildExposureFromForm()!
      if (isNew) {
        addExposure(exposure)
      } else {
        updateExposure(name!, exposure)
      }
      navigate(`/exposures/${exposure.name}`)
    } finally {
      setSaving(false)
    }
  }

  const addRef = (setter: React.Dispatch<React.SetStateAction<RefFormState[]>>) => {
    setter((prev) => [...prev, { type: 'ref', value: '' }])
  }

  const removeRef = (setter: React.Dispatch<React.SetStateAction<RefFormState[]>>, index: number) => {
    setter((prev) => prev.filter((_, i) => i !== index))
  }

  const updateRef = (
    setter: React.Dispatch<React.SetStateAction<RefFormState[]>>,
    index: number,
    field: 'type' | 'value',
    value: string
  ) => {
    setter((prev) =>
      prev.map((ref, i) =>
        i === index ? { ...ref, [field]: value } : ref
      )
    )
  }

  const RefList = ({
    title,
    refs,
    setter,
  }: {
    title: string
    refs: RefFormState[]
    setter: React.Dispatch<React.SetStateAction<RefFormState[]>>
  }) => (
    <div>
      <div className="flex items-center justify-between mb-2">
        <label className="text-sm font-medium text-slate-300">{title}</label>
        <Button variant="ghost" size="sm" onClick={() => addRef(setter)} icon={<Plus className="w-4 h-4" />}>
          Add
        </Button>
      </div>
      {refs.length > 0 && (
        <div className="space-y-2">
          {refs.map((ref, i) => (
            <div key={i} className="flex items-center gap-2 p-2 bg-slate-800/50 rounded-lg">
              <Select
                value={ref.type}
                onChange={(e) => updateRef(setter, i, 'type', e.target.value)}
                options={[
                  { value: 'source', label: 'Source' },
                  { value: 'ref', label: 'Model' },
                ]}
                className="w-28"
              />
              <Select
                value={ref.value}
                onChange={(e) => updateRef(setter, i, 'value', e.target.value)}
                options={[
                  { value: '', label: 'Select...' },
                  ...(ref.type === 'source'
                    ? availableSources.map((s) => ({ value: s, label: s }))
                    : availableModels.map((m) => ({ value: m, label: m }))),
                ]}
                className="flex-1"
              />
              <Button
                variant="ghost"
                size="icon"
                onClick={() => removeRef(setter, i)}
                className="text-slate-500 hover:text-red-400"
              >
                <Trash2 className="w-4 h-4" />
              </Button>
            </div>
          ))}
        </div>
      )}
    </div>
  )

  if (!isNew && !existingExposure) {
    return (
      <div className="p-8 text-center">
        <p className="text-slate-400">Exposure not found</p>
        <Button variant="ghost" onClick={() => navigate('/exposures')} className="mt-4">
          Back to Exposures
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
              {isNew ? 'Create Exposure' : `Edit Exposure: ${name}`}
            </h2>
            <p className="text-slate-400 mt-1">
              {isNew ? 'Define a new downstream exposure' : 'Modify the exposure configuration'}
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
          {/* Tabs */}
          <div className="flex gap-1 px-6 pt-4 border-b border-slate-700">
            {(['basic', 'refs', 'sla'] as const).map((tab) => (
              <button
                key={tab}
                onClick={() => handleTabChange(tab)}
                className={`px-4 py-2 text-sm font-medium transition-colors border-b-2 -mb-px ${
                  activeTab === tab
                    ? 'text-streamt-400 border-streamt-400'
                    : 'text-slate-400 border-transparent hover:text-slate-300'
                }`}
              >
                {tab === 'basic' && 'Basic Info'}
                {tab === 'refs' && 'References'}
                {tab === 'sla' && 'SLA'}
              </button>
            ))}
          </div>

          <CardContent className="p-6 space-y-6">
            {/* Basic Tab */}
            {activeTab === 'basic' && (
              <>
                <div className="grid grid-cols-2 gap-4">
                  <FormField label="Name" required error={errors.name}>
                    <Input
                      value={exposureName}
                      onChange={(e) => setExposureName(e.target.value)}
                      placeholder="e.g., payment_dashboard"
                      disabled={!isNew}
                      error={!!errors.name}
                    />
                  </FormField>
                  <FormField label="Type" required>
                    <Select
                      value={type}
                      onChange={(e) => setType(e.target.value as ExposureType)}
                      options={exposureTypeOptions}
                    />
                  </FormField>
                </div>

                <FormField label="Description">
                  <Textarea
                    value={description}
                    onChange={(e) => setDescription(e.target.value)}
                    placeholder="Brief description of this exposure..."
                    rows={2}
                  />
                </FormField>

                <div className="grid grid-cols-3 gap-4">
                  <FormField label="Role">
                    <Select
                      value={role}
                      onChange={(e) => setRole(e.target.value as ExposureRole | '')}
                      options={roleOptions}
                    />
                  </FormField>
                  <FormField label="Owner">
                    <Input
                      value={owner}
                      onChange={(e) => setOwner(e.target.value)}
                      placeholder="e.g., analytics-team"
                    />
                  </FormField>
                  <FormField label="Consumer Group">
                    <Input
                      value={consumerGroup}
                      onChange={(e) => setConsumerGroup(e.target.value)}
                      placeholder="e.g., dashboard-cg"
                    />
                  </FormField>
                </div>
              </>
            )}

            {/* References Tab */}
            {activeTab === 'refs' && (
              <div className="space-y-6">
                <RefList title="Produces (writes to)" refs={produces} setter={setProduces} />
                <RefList title="Consumes (reads from)" refs={consumes} setter={setConsumes} />
                <RefList title="Depends On" refs={dependsOn} setter={setDependsOn} />
              </div>
            )}

            {/* SLA Tab */}
            {activeTab === 'sla' && (
              <div className="space-y-4">
                <p className="text-sm text-slate-400">
                  Define service level agreements for this exposure.
                </p>
                <div className="grid grid-cols-2 gap-4">
                  <FormField label="Max Latency (ms)">
                    <Input
                      type="number"
                      value={maxLatencyMs}
                      onChange={(e) => setMaxLatencyMs(e.target.value)}
                      placeholder="e.g., 1000"
                      min={0}
                    />
                  </FormField>
                  <FormField label="Availability (%)">
                    <Input
                      type="number"
                      value={availabilityPercent}
                      onChange={(e) => setAvailabilityPercent(e.target.value)}
                      placeholder="e.g., 99.9"
                      min={0}
                      max={100}
                      step={0.1}
                    />
                  </FormField>
                </div>
              </div>
            )}
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
