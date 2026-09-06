import { useState, useEffect } from 'react'
import { Plus, Trash2 } from 'lucide-react'
import { Dialog, DialogFooter } from '@/components/ui/Dialog'
import { FormField, Input, Textarea, Select } from '@/components/ui/FormField'
import { Button } from '@/components/ui/Button'
import type { Exposure, ExposureType, ExposureRole, ExposureRef } from '@/types'

interface ExposureFormProps {
  open: boolean
  onClose: () => void
  onSave: (exposure: Exposure) => Promise<void>
  exposure?: Exposure | null
  existingNames: string[]
  availableSources: string[]
  availableModels: string[]
}

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

export function ExposureForm({
  open,
  onClose,
  onSave,
  exposure,
  existingNames,
  availableSources,
  availableModels,
}: ExposureFormProps) {
  const isEdit = !!exposure
  const [saving, setSaving] = useState(false)
  const [errors, setErrors] = useState<Record<string, string>>({})
  const [activeTab, setActiveTab] = useState<'basic' | 'refs' | 'sla'>('basic')

  // Basic fields
  const [name, setName] = useState('')
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

  // Reset form when exposure changes
  useEffect(() => {
    if (exposure) {
      setName(exposure.name)
      setType(exposure.type)
      setRole(exposure.role || '')
      setDescription(exposure.description || '')
      setOwner(exposure.owner || '')
      setConsumerGroup(exposure.consumer_group || '')

      // Convert refs to form state
      setProduces(
        (exposure.produces || [])
          .filter((p) => p.ref || p.source)
          .map((p) => ({
            type: p.source ? 'source' : 'ref',
            value: p.source || p.ref || '',
          }))
      )
      setConsumes(
        (exposure.consumes || [])
          .filter((c) => c.ref || c.source)
          .map((c) => ({
            type: c.source ? 'source' : 'ref',
            value: c.source || c.ref || '',
          }))
      )
      setDependsOn(
        (exposure.depends_on || [])
          .filter((d) => d.ref || d.source)
          .map((d) => ({
            type: d.source ? 'source' : 'ref',
            value: d.source || d.ref || '',
          }))
      )

      // SLA
      setMaxLatencyMs(exposure.sla?.max_latency_ms?.toString() || '')
      setAvailabilityPercent(exposure.sla?.availability_percent?.toString() || '')
    } else {
      setName('')
      setType('application')
      setRole('')
      setDescription('')
      setOwner('')
      setConsumerGroup('')
      setProduces([])
      setConsumes([])
      setDependsOn([])
      setMaxLatencyMs('')
      setAvailabilityPercent('')
    }
    setErrors({})
    setActiveTab('basic')
  }, [exposure, open])

  const validate = (): boolean => {
    const newErrors: Record<string, string> = {}

    if (!name.trim()) {
      newErrors.name = 'Name is required'
    } else if (!isEdit && existingNames.includes(name.trim())) {
      newErrors.name = 'An exposure with this name already exists'
    } else if (!/^[a-z][a-z0-9_]*$/.test(name.trim())) {
      newErrors.name = 'Name must start with a letter and contain only lowercase letters, numbers, and underscores'
    }

    setErrors(newErrors)
    return Object.keys(newErrors).length === 0
  }

  const handleSave = async () => {
    if (!validate()) return

    setSaving(true)
    try {
      // Convert refs to API format
      const convertRefs = (refs: RefFormState[]): ExposureRef[] =>
        refs.filter((r) => r.value).map((r) =>
          r.type === 'source' ? { source: r.value } : { ref: r.value }
        )

      const newExposure: Exposure = {
        name: name.trim(),
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

      await onSave(newExposure)
      onClose()
    } catch (err) {
      setErrors({ submit: (err as Error).message })
    } finally {
      setSaving(false)
    }
  }

  const addRef = (
    setter: React.Dispatch<React.SetStateAction<RefFormState[]>>
  ) => {
    setter((prev) => [...prev, { type: 'ref', value: '' }])
  }

  const removeRef = (
    setter: React.Dispatch<React.SetStateAction<RefFormState[]>>,
    index: number
  ) => {
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
        <Button
          variant="ghost"
          size="sm"
          onClick={() => addRef(setter)}
          icon={<Plus className="w-4 h-4" />}
        >
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

  return (
    <Dialog
      open={open}
      onClose={onClose}
      title={isEdit ? `Edit Exposure: ${exposure.name}` : 'Create Exposure'}
      description={isEdit ? 'Update the exposure configuration' : 'Define a new downstream exposure'}
      size="lg"
    >
      {/* Tabs */}
      <div className="flex gap-1 mb-6 border-b border-slate-700">
        {(['basic', 'refs', 'sla'] as const).map((tab) => (
          <button
            key={tab}
            onClick={() => setActiveTab(tab)}
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

      <div className="space-y-6 min-h-[350px]">
        {/* Basic Tab */}
        {activeTab === 'basic' && (
          <>
            <div className="grid grid-cols-2 gap-4">
              <FormField label="Name" required error={errors.name}>
                <Input
                  value={name}
                  onChange={(e) => setName(e.target.value)}
                  placeholder="e.g., payment_dashboard"
                  disabled={isEdit}
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
          {isEdit ? 'Update Exposure' : 'Create Exposure'}
        </Button>
      </DialogFooter>
    </Dialog>
  )
}
