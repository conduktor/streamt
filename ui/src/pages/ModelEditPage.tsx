import { useState, useEffect } from 'react'
import { useParams, useNavigate, useSearchParams } from 'react-router-dom'
import { ArrowLeft, Save, Plus, Trash2, Code, FormInput } from 'lucide-react'
import yaml from 'js-yaml'
import { Card, CardContent, Button, YamlEditor, SqlEditor } from '@/components/ui'
import { FormField, Input, Textarea, Select } from '@/components/ui/FormField'
import { useProjectStore } from '@/stores/projectStore'
import type { Model, MaterializedType, AccessLevel, TopicConfig, FlinkConfig, SinkConfig } from '@/types'

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

export function ModelEditPage() {
  const { name } = useParams()
  const navigate = useNavigate()
  const [searchParams, setSearchParams] = useSearchParams()
  const { project, addModel, updateModel } = useProjectStore()

  const isNew = name === 'new'
  const existingModel = !isNew ? project?.models.find((m) => m.name === name) : null
  const existingNames = project?.models.map((m) => m.name) ?? []

  const [viewMode, setViewMode] = useState<'form' | 'yaml'>(
    searchParams.get('view') === 'yaml' ? 'yaml' : 'form'
  )
  const initialTab = searchParams.get('tab') as 'basic' | 'sql' | 'config' | null
  const [activeTab, setActiveTab] = useState<'basic' | 'sql' | 'config'>(
    initialTab && ['basic', 'sql', 'config'].includes(initialTab) ? initialTab : 'basic'
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

  const handleTabChange = (tab: 'basic' | 'sql' | 'config') => {
    setActiveTab(tab)
    updateSearchParams({ tab: tab === 'basic' ? null : tab })
  }
  const [saving, setSaving] = useState(false)
  const [errors, setErrors] = useState<Record<string, string>>({})

  // Basic fields
  const [modelName, setModelName] = useState('')
  const [description, setDescription] = useState('')
  const [materialized, setMaterialized] = useState<MaterializedType>('topic')
  const [sql, setSql] = useState('')
  const [access, setAccess] = useState<AccessLevel | ''>('')
  const [owner, setOwner] = useState('')
  const [group, setGroup] = useState('')

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

  // YAML state
  const [yamlContent, setYamlContent] = useState('')
  const [yamlError, setYamlError] = useState('')
  const [yamlKey, setYamlKey] = useState(0)

  // Initialize form from existing model
  useEffect(() => {
    if (existingModel) {
      setModelName(existingModel.name)
      setDescription(existingModel.description || '')
      setMaterialized(existingModel.materialized)
      setSql(existingModel.sql || '')
      setAccess(existingModel.access || '')
      setOwner(existingModel.owner || '')
      setGroup(existingModel.group || '')

      setTopicPartitions(existingModel.topic?.partitions?.toString() || '')
      setTopicReplication(existingModel.topic?.replication_factor?.toString() || '')
      setTopicRetention(existingModel.topic?.retention_ms ? (existingModel.topic.retention_ms / 86400000).toString() : '')

      setFlinkParallelism(existingModel.flink?.parallelism?.toString() || '')
      setFlinkCheckpoint(existingModel.flink?.checkpoint_interval_ms ? (existingModel.flink.checkpoint_interval_ms / 1000).toString() : '')
      setFlinkStateTtl(existingModel.flink?.state_ttl_ms ? (existingModel.flink.state_ttl_ms / 3600000).toString() : '')
      setFlinkWatermark(existingModel.flink?.watermark_delay_ms ? (existingModel.flink.watermark_delay_ms / 1000).toString() : '')

      setSinkConnector(existingModel.sink?.connector || '')
      setSinkConfig(
        existingModel.sink?.config
          ? Object.entries(existingModel.sink.config).map(([key, value]) => ({ key, value }))
          : []
      )
    }
  }, [existingModel])

  // Sync YAML when switching to YAML view or when form data changes in YAML mode
  useEffect(() => {
    if (viewMode === 'yaml') {
      const model = buildModelFromForm()
      if (model) {
        setYamlContent(yaml.dump(model, { indent: 2, lineWidth: -1 }))
        setYamlError('')
      }
    }
  }, [viewMode, modelName, description, materialized, sql, access, owner, group, topicPartitions, topicReplication, topicRetention, flinkParallelism, flinkCheckpoint, flinkStateTtl, flinkWatermark, sinkConnector, sinkConfig])

  const buildModelFromForm = (): Model | null => {
    let topic: TopicConfig | undefined
    if (topicPartitions || topicReplication || topicRetention) {
      topic = {
        partitions: topicPartitions ? parseInt(topicPartitions) : undefined,
        replication_factor: topicReplication ? parseInt(topicReplication) : undefined,
        retention_ms: topicRetention ? parseInt(topicRetention) * 86400000 : undefined,
      }
    }

    let flink: FlinkConfig | undefined
    if (flinkParallelism || flinkCheckpoint || flinkStateTtl || flinkWatermark) {
      flink = {
        parallelism: flinkParallelism ? parseInt(flinkParallelism) : undefined,
        checkpoint_interval_ms: flinkCheckpoint ? parseInt(flinkCheckpoint) * 1000 : undefined,
        state_ttl_ms: flinkStateTtl ? parseInt(flinkStateTtl) * 3600000 : undefined,
        watermark_delay_ms: flinkWatermark ? parseInt(flinkWatermark) * 1000 : undefined,
      }
    }

    let sink: SinkConfig | undefined
    if (materialized === 'sink' && sinkConnector) {
      sink = {
        connector: sinkConnector,
        config: Object.fromEntries(
          sinkConfig.filter((c) => c.key.trim()).map((c) => [c.key.trim(), c.value])
        ),
      }
    }

    // Note: from_ (dependencies) are automatically discovered from SQL
    // via {{ source("...") }} and {{ ref("...") }} references

    return {
      name: modelName.trim(),
      description: description.trim() || undefined,
      materialized,
      sql: sql.trim() || undefined,
      topic,
      flink,
      sink,
      access: access || undefined,
      owner: owner.trim() || undefined,
      group: group.trim() || undefined,
    }
  }

  const parseYamlToForm = () => {
    try {
      const parsed = yaml.load(yamlContent) as Model
      if (parsed) {
        setModelName(parsed.name || '')
        setDescription(parsed.description || '')
        setMaterialized(parsed.materialized || 'topic')
        setSql(parsed.sql || '')
        setAccess(parsed.access || '')
        setOwner(parsed.owner || '')
        setGroup(parsed.group || '')

        setTopicPartitions(parsed.topic?.partitions?.toString() || '')
        setTopicReplication(parsed.topic?.replication_factor?.toString() || '')
        setTopicRetention(parsed.topic?.retention_ms ? (parsed.topic.retention_ms / 86400000).toString() : '')

        setFlinkParallelism(parsed.flink?.parallelism?.toString() || '')
        setFlinkCheckpoint(parsed.flink?.checkpoint_interval_ms ? (parsed.flink.checkpoint_interval_ms / 1000).toString() : '')
        setFlinkStateTtl(parsed.flink?.state_ttl_ms ? (parsed.flink.state_ttl_ms / 3600000).toString() : '')
        setFlinkWatermark(parsed.flink?.watermark_delay_ms ? (parsed.flink.watermark_delay_ms / 1000).toString() : '')

        setSinkConnector(parsed.sink?.connector || '')
        setSinkConfig(
          parsed.sink?.config
            ? Object.entries(parsed.sink.config).map(([key, value]) => ({ key, value }))
            : []
        )

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
      const model = buildModelFromForm()
      if (model) {
        setYamlContent(yaml.dump(model, { indent: 2, lineWidth: -1 }))
        setYamlError('')
        setYamlKey(k => k + 1)
      }
    }
    setViewMode(mode)
    updateSearchParams({ view: mode === 'yaml' ? 'yaml' : null })
  }

  const validate = (): boolean => {
    const newErrors: Record<string, string> = {}

    if (!modelName.trim()) {
      newErrors.name = 'Name is required'
    } else if (isNew && existingNames.includes(modelName.trim())) {
      newErrors.name = 'A model with this name already exists'
    } else if (!/^[a-z][a-z0-9_]*$/.test(modelName.trim())) {
      newErrors.name = 'Name must start with a letter and contain only lowercase letters, numbers, and underscores'
    }

    if (materialized === 'sink' && !sinkConnector) {
      newErrors.sink = 'Sink connector is required for sink models'
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
      const model = buildModelFromForm()!
      if (isNew) {
        addModel(model)
      } else {
        updateModel(name!, model)
      }
      navigate(`/models/${model.name}`)
    } finally {
      setSaving(false)
    }
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

  if (!isNew && !existingModel) {
    return (
      <div className="p-8 text-center">
        <p className="text-slate-400">Model not found</p>
        <Button variant="ghost" onClick={() => navigate('/models')} className="mt-4">
          Back to Models
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
              {isNew ? 'Create Model' : `Edit Model: ${name}`}
            </h2>
            <p className="text-slate-400 mt-1">
              {isNew ? 'Define a new data transformation model' : 'Modify the model configuration'}
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
            {(['basic', 'sql', 'config'] as const).map((tab) => (
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
                {tab === 'sql' && 'SQL'}
                {tab === 'config' && 'Configuration'}
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
                      value={modelName}
                      onChange={(e) => setModelName(e.target.value)}
                      placeholder="e.g., payments_enriched"
                      disabled={!isNew}
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

              </>
            )}

            {/* SQL Tab */}
            {activeTab === 'sql' && (
              <div>
                <label className="block text-sm font-medium text-slate-300 mb-3">
                  SQL Transformation
                </label>
                <p className="text-xs text-slate-500 mb-3">
                  Use <code className="bg-slate-800 px-1 py-0.5 rounded">{'{{ source("name") }}'}</code> and{' '}
                  <code className="bg-slate-800 px-1 py-0.5 rounded">{'{{ ref("name") }}'}</code> to reference sources and models.
                  Dependencies are automatically discovered.
                </p>
                <SqlEditor
                  value={sql}
                  onChange={setSql}
                  height="400px"
                  placeholder="SELECT * FROM {{ source('payments_raw') }}"
                />
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
