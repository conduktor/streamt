import { useState, useEffect } from 'react'
import { Plus, Trash2 } from 'lucide-react'
import { Dialog, DialogFooter } from '@/components/ui/Dialog'
import { FormField, Input, Textarea, Select } from '@/components/ui/FormField'
import { Button } from '@/components/ui/Button'
import { SqlHighlighter } from '@/components/ui/SqlHighlighter'
import type { Model, MaterializedType, AccessLevel, TopicConfig, FlinkConfig, SinkConfig } from '@/types'

interface ModelFormProps {
  open: boolean
  onClose: () => void
  onSave: (model: Model) => Promise<void>
  model?: Model | null
  existingNames: string[]
  availableSources: string[]
  availableModels: string[]
}

const materializationOptions = [
  { value: 'topic', label: 'Topic' },
  { value: 'flink', label: 'Flink Job' },
  { value: 'virtual_topic', label: 'Virtual Topic' },
  { value: 'sink', label: 'Sink' },
]

const accessOptions = [
  { value: '', label: 'None' },
  { value: 'public', label: 'Public' },
  { value: 'protected', label: 'Protected' },
  { value: 'private', label: 'Private' },
]

const sinkConnectorOptions = [
  { value: 'jdbc', label: 'JDBC' },
  { value: 'elasticsearch', label: 'Elasticsearch' },
  { value: 's3', label: 'S3' },
  { value: 'hdfs', label: 'HDFS' },
  { value: 'http', label: 'HTTP' },
]

export function ModelForm({
  open,
  onClose,
  onSave,
  model,
  existingNames,
  availableSources,
  availableModels,
}: ModelFormProps) {
  const isEdit = !!model
  const [saving, setSaving] = useState(false)
  const [errors, setErrors] = useState<Record<string, string>>({})
  const [activeTab, setActiveTab] = useState<'basic' | 'sql' | 'config'>('basic')

  // Basic fields
  const [name, setName] = useState('')
  const [description, setDescription] = useState('')
  const [materialized, setMaterialized] = useState<MaterializedType>('topic')
  const [sql, setSql] = useState('')
  const [access, setAccess] = useState<AccessLevel | ''>('')
  const [owner, setOwner] = useState('')
  const [group, setGroup] = useState('')

  // From references
  const [fromRefs, setFromRefs] = useState<Array<{ type: 'source' | 'ref'; value: string }>>([])

  // Topic config
  const [topicPartitions, setTopicPartitions] = useState('')
  const [topicReplication, setTopicReplication] = useState('')
  const [topicRetention, setTopicRetention] = useState('')

  // Flink config
  const [flinkParallelism, setFlinkParallelism] = useState('')
  const [flinkCheckpoint, setFlinkCheckpoint] = useState('')
  const [flinkStateTtl, setFlinkStateTtl] = useState('')
  const [flinkWatermark, setFlinkWatermark] = useState('')

  // Sink config
  const [sinkConnector, setSinkConnector] = useState('')
  const [sinkConfig, setSinkConfig] = useState<Array<{ key: string; value: string }>>([])

  // Reset form when model changes
  useEffect(() => {
    if (model) {
      setName(model.name)
      setDescription(model.description || '')
      setMaterialized(model.materialized)
      setSql(model.sql || '')
      setAccess(model.access || '')
      setOwner(model.owner || '')
      setGroup(model.group || '')

      // From references
      const refs: Array<{ type: 'source' | 'ref'; value: string }> = []
      model.from_?.forEach((f) => {
        if (f.source) refs.push({ type: 'source', value: f.source })
        if (f.ref) refs.push({ type: 'ref', value: f.ref })
      })
      setFromRefs(refs)

      // Topic config
      setTopicPartitions(model.topic?.partitions?.toString() || '')
      setTopicReplication(model.topic?.replication_factor?.toString() || '')
      setTopicRetention(model.topic?.retention_ms ? (model.topic.retention_ms / 86400000).toString() : '')

      // Flink config
      setFlinkParallelism(model.flink?.parallelism?.toString() || '')
      setFlinkCheckpoint(model.flink?.checkpoint_interval_ms ? (model.flink.checkpoint_interval_ms / 1000).toString() : '')
      setFlinkStateTtl(model.flink?.state_ttl_ms ? (model.flink.state_ttl_ms / 3600000).toString() : '')
      setFlinkWatermark(model.flink?.watermark_delay_ms ? (model.flink.watermark_delay_ms / 1000).toString() : '')

      // Sink config
      setSinkConnector(model.sink?.connector || '')
      setSinkConfig(
        model.sink?.config
          ? Object.entries(model.sink.config).map(([key, value]) => ({ key, value }))
          : []
      )
    } else {
      setName('')
      setDescription('')
      setMaterialized('topic')
      setSql('')
      setAccess('')
      setOwner('')
      setGroup('')
      setFromRefs([])
      setTopicPartitions('')
      setTopicReplication('')
      setTopicRetention('')
      setFlinkParallelism('')
      setFlinkCheckpoint('')
      setFlinkStateTtl('')
      setFlinkWatermark('')
      setSinkConnector('')
      setSinkConfig([])
    }
    setErrors({})
    setActiveTab('basic')
  }, [model, open])

  const validate = (): boolean => {
    const newErrors: Record<string, string> = {}

    if (!name.trim()) {
      newErrors.name = 'Name is required'
    } else if (!isEdit && existingNames.includes(name.trim())) {
      newErrors.name = 'A model with this name already exists'
    } else if (!/^[a-z][a-z0-9_]*$/.test(name.trim())) {
      newErrors.name = 'Name must start with a letter and contain only lowercase letters, numbers, and underscores'
    }

    if (materialized === 'sink' && !sinkConnector) {
      newErrors.sink = 'Sink connector is required for sink models'
    }

    setErrors(newErrors)
    return Object.keys(newErrors).length === 0
  }

  const handleSave = async () => {
    if (!validate()) return

    setSaving(true)
    try {
      // Build topic config
      let topic: TopicConfig | undefined
      if (topicPartitions || topicReplication || topicRetention) {
        topic = {
          partitions: topicPartitions ? parseInt(topicPartitions) : undefined,
          replication_factor: topicReplication ? parseInt(topicReplication) : undefined,
          retention_ms: topicRetention ? parseInt(topicRetention) * 86400000 : undefined,
        }
      }

      // Build flink config
      let flink: FlinkConfig | undefined
      if (flinkParallelism || flinkCheckpoint || flinkStateTtl || flinkWatermark) {
        flink = {
          parallelism: flinkParallelism ? parseInt(flinkParallelism) : undefined,
          checkpoint_interval_ms: flinkCheckpoint ? parseInt(flinkCheckpoint) * 1000 : undefined,
          state_ttl_ms: flinkStateTtl ? parseInt(flinkStateTtl) * 3600000 : undefined,
          watermark_delay_ms: flinkWatermark ? parseInt(flinkWatermark) * 1000 : undefined,
        }
      }

      // Build sink config
      let sink: SinkConfig | undefined
      if (materialized === 'sink' && sinkConnector) {
        sink = {
          connector: sinkConnector,
          config: Object.fromEntries(
            sinkConfig.filter((c) => c.key.trim()).map((c) => [c.key.trim(), c.value])
          ),
        }
      }

      // Build from_ array
      const from_ = fromRefs.length > 0
        ? fromRefs.map((f) => f.type === 'source' ? { source: f.value } : { ref: f.value })
        : undefined

      const newModel: Model = {
        name: name.trim(),
        description: description.trim() || undefined,
        materialized,
        sql: sql.trim() || undefined,
        topic,
        flink,
        sink,
        access: access || undefined,
        owner: owner.trim() || undefined,
        group: group.trim() || undefined,
        from_,
      }

      await onSave(newModel)
      onClose()
    } catch (err) {
      setErrors({ submit: (err as Error).message })
    } finally {
      setSaving(false)
    }
  }

  const addFromRef = () => {
    setFromRefs([...fromRefs, { type: 'source', value: '' }])
  }

  const removeFromRef = (index: number) => {
    setFromRefs(fromRefs.filter((_, i) => i !== index))
  }

  const updateFromRef = (index: number, field: 'type' | 'value', value: string) => {
    setFromRefs(
      fromRefs.map((ref, i) =>
        i === index ? { ...ref, [field]: value } : ref
      )
    )
  }

  const addSinkConfigEntry = () => {
    setSinkConfig([...sinkConfig, { key: '', value: '' }])
  }

  const removeSinkConfigEntry = (index: number) => {
    setSinkConfig(sinkConfig.filter((_, i) => i !== index))
  }

  const updateSinkConfig = (index: number, field: 'key' | 'value', value: string) => {
    setSinkConfig(
      sinkConfig.map((entry, i) =>
        i === index ? { ...entry, [field]: value } : entry
      )
    )
  }

  return (
    <Dialog
      open={open}
      onClose={onClose}
      title={isEdit ? `Edit Model: ${model.name}` : 'Create Model'}
      description={isEdit ? 'Update the model configuration' : 'Define a new data transformation model'}
      size="xl"
    >
      {/* Tabs */}
      <div className="flex gap-1 mb-6 border-b border-slate-700">
        {(['basic', 'sql', 'config'] as const).map((tab) => (
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
            {tab === 'sql' && 'SQL'}
            {tab === 'config' && 'Configuration'}
          </button>
        ))}
      </div>

      <div className="space-y-6 min-h-[400px]">
        {/* Basic Tab */}
        {activeTab === 'basic' && (
          <>
            <div className="grid grid-cols-2 gap-4">
              <FormField label="Name" required error={errors.name}>
                <Input
                  value={name}
                  onChange={(e) => setName(e.target.value)}
                  placeholder="e.g., payments_enriched"
                  disabled={isEdit}
                  error={!!errors.name}
                />
              </FormField>
              <FormField label="Materialization" required>
                <Select
                  value={materialized}
                  onChange={(e) => setMaterialized(e.target.value as MaterializedType)}
                  options={materializationOptions}
                />
              </FormField>
            </div>

            <FormField label="Description">
              <Textarea
                value={description}
                onChange={(e) => setDescription(e.target.value)}
                placeholder="Brief description of this model..."
                rows={2}
              />
            </FormField>

            <div className="grid grid-cols-3 gap-4">
              <FormField label="Access Level">
                <Select
                  value={access}
                  onChange={(e) => setAccess(e.target.value as AccessLevel | '')}
                  options={accessOptions}
                />
              </FormField>
              <FormField label="Owner">
                <Input
                  value={owner}
                  onChange={(e) => setOwner(e.target.value)}
                  placeholder="e.g., data-team"
                />
              </FormField>
              <FormField label="Group">
                <Input
                  value={group}
                  onChange={(e) => setGroup(e.target.value)}
                  placeholder="e.g., core-transformations"
                />
              </FormField>
            </div>

            {/* From References */}
            <div>
              <div className="flex items-center justify-between mb-3">
                <label className="text-sm font-medium text-slate-300">
                  Dependencies (from)
                </label>
                <Button variant="ghost" size="sm" onClick={addFromRef} icon={<Plus className="w-4 h-4" />}>
                  Add Dependency
                </Button>
              </div>
              {fromRefs.length > 0 && (
                <div className="space-y-2">
                  {fromRefs.map((ref, i) => (
                    <div key={i} className="flex items-center gap-2 p-3 bg-slate-800/50 rounded-lg">
                      <Select
                        value={ref.type}
                        onChange={(e) => updateFromRef(i, 'type', e.target.value)}
                        options={[
                          { value: 'source', label: 'Source' },
                          { value: 'ref', label: 'Model' },
                        ]}
                        className="w-32"
                      />
                      <Select
                        value={ref.value}
                        onChange={(e) => updateFromRef(i, 'value', e.target.value)}
                        options={[
                          { value: '', label: 'Select...' },
                          ...(ref.type === 'source'
                            ? availableSources.map((s) => ({ value: s, label: s }))
                            : availableModels.filter((m) => m !== name).map((m) => ({ value: m, label: m }))),
                        ]}
                        className="flex-1"
                      />
                      <Button
                        variant="ghost"
                        size="icon"
                        onClick={() => removeFromRef(i)}
                        className="text-slate-500 hover:text-red-400"
                      >
                        <Trash2 className="w-4 h-4" />
                      </Button>
                    </div>
                  ))}
                </div>
              )}
            </div>
          </>
        )}

        {/* SQL Tab */}
        {activeTab === 'sql' && (
          <div className="space-y-4">
            <FormField label="SQL Transformation">
              <Textarea
                value={sql}
                onChange={(e) => setSql(e.target.value)}
                placeholder={`SELECT
  payment_id,
  customer_id,
  amount,
  currency,
  PROCTIME() as processing_time
FROM source('payments_raw')
WHERE status = 'completed'`}
                rows={16}
                className="font-mono text-sm"
              />
            </FormField>
            {sql && (
              <div>
                <label className="block text-sm font-medium text-slate-300 mb-2">Preview</label>
                <SqlHighlighter sql={sql} className="max-h-[200px]" />
              </div>
            )}
          </div>
        )}

        {/* Config Tab */}
        {activeTab === 'config' && (
          <div className="space-y-6">
            {/* Topic Configuration */}
            {(materialized === 'topic' || materialized === 'flink') && (
              <div>
                <label className="block text-sm font-medium text-slate-300 mb-3">Topic Configuration</label>
                <div className="grid grid-cols-3 gap-4">
                  <FormField label="Partitions">
                    <Input
                      type="number"
                      value={topicPartitions}
                      onChange={(e) => setTopicPartitions(e.target.value)}
                      placeholder="e.g., 6"
                      min={1}
                    />
                  </FormField>
                  <FormField label="Replication Factor">
                    <Input
                      type="number"
                      value={topicReplication}
                      onChange={(e) => setTopicReplication(e.target.value)}
                      placeholder="e.g., 3"
                      min={1}
                    />
                  </FormField>
                  <FormField label="Retention (days)">
                    <Input
                      type="number"
                      value={topicRetention}
                      onChange={(e) => setTopicRetention(e.target.value)}
                      placeholder="e.g., 7"
                      min={1}
                    />
                  </FormField>
                </div>
              </div>
            )}

            {/* Flink Configuration */}
            {materialized === 'flink' && (
              <div>
                <label className="block text-sm font-medium text-slate-300 mb-3">Flink Configuration</label>
                <div className="grid grid-cols-2 gap-4">
                  <FormField label="Parallelism">
                    <Input
                      type="number"
                      value={flinkParallelism}
                      onChange={(e) => setFlinkParallelism(e.target.value)}
                      placeholder="e.g., 4"
                      min={1}
                    />
                  </FormField>
                  <FormField label="Checkpoint Interval (seconds)">
                    <Input
                      type="number"
                      value={flinkCheckpoint}
                      onChange={(e) => setFlinkCheckpoint(e.target.value)}
                      placeholder="e.g., 60"
                      min={1}
                    />
                  </FormField>
                  <FormField label="State TTL (hours)">
                    <Input
                      type="number"
                      value={flinkStateTtl}
                      onChange={(e) => setFlinkStateTtl(e.target.value)}
                      placeholder="e.g., 24"
                      min={1}
                    />
                  </FormField>
                  <FormField label="Watermark Delay (seconds)">
                    <Input
                      type="number"
                      value={flinkWatermark}
                      onChange={(e) => setFlinkWatermark(e.target.value)}
                      placeholder="e.g., 5"
                      min={0}
                    />
                  </FormField>
                </div>
              </div>
            )}

            {/* Sink Configuration */}
            {materialized === 'sink' && (
              <div>
                <label className="block text-sm font-medium text-slate-300 mb-3">Sink Configuration</label>
                {errors.sink && <p className="text-sm text-red-400 mb-2">{errors.sink}</p>}
                <div className="space-y-4">
                  <FormField label="Connector" required>
                    <Select
                      value={sinkConnector}
                      onChange={(e) => setSinkConnector(e.target.value)}
                      options={[{ value: '', label: 'Select connector...' }, ...sinkConnectorOptions]}
                      error={!!errors.sink}
                    />
                  </FormField>
                  <div>
                    <div className="flex items-center justify-between mb-2">
                      <label className="text-sm font-medium text-slate-300">Connector Properties</label>
                      <Button variant="ghost" size="sm" onClick={addSinkConfigEntry} icon={<Plus className="w-4 h-4" />}>
                        Add Property
                      </Button>
                    </div>
                    {sinkConfig.length > 0 && (
                      <div className="space-y-2">
                        {sinkConfig.map((entry, i) => (
                          <div key={i} className="flex items-center gap-2 p-2 bg-slate-800/50 rounded-lg">
                            <Input
                              value={entry.key}
                              onChange={(e) => updateSinkConfig(i, 'key', e.target.value)}
                              placeholder="Property name"
                              className="flex-1"
                            />
                            <Input
                              value={entry.value}
                              onChange={(e) => updateSinkConfig(i, 'value', e.target.value)}
                              placeholder="Value"
                              className="flex-1"
                            />
                            <Button
                              variant="ghost"
                              size="icon"
                              onClick={() => removeSinkConfigEntry(i)}
                              className="text-slate-500 hover:text-red-400"
                            >
                              <Trash2 className="w-4 h-4" />
                            </Button>
                          </div>
                        ))}
                      </div>
                    )}
                  </div>
                </div>
              </div>
            )}

            {materialized === 'virtual_topic' && (
              <div className="p-4 bg-slate-800/50 rounded-lg text-slate-400">
                Virtual topics use Conduktor Gateway for routing. No additional configuration required.
              </div>
            )}
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
          {isEdit ? 'Update Model' : 'Create Model'}
        </Button>
      </DialogFooter>
    </Dialog>
  )
}
