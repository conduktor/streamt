import { useState, useMemo, useEffect } from 'react'
import { useParams, useNavigate } from 'react-router-dom'
import { motion } from 'framer-motion'
import {
  Boxes,
  Search,
  Zap,
  Cloud,
  Upload,
  User,
  FileCode,
  ExternalLink,
  Activity,
  Plus,
  Pencil,
  Trash2,
  ChevronRight,
  Code,
} from 'lucide-react'
import { Card, CardContent, CardHeader, CardTitle, Badge, StatusIndicator, SqlHighlighter, RelationshipLinks, DependencyTree, Button, CascadeDeleteDialog } from '@/components/ui'
import { useProjectStore } from '@/stores/projectStore'
import { cn } from '@/utils/cn'
import type { MaterializedType } from '@/types'

const materializationConfig: Record<MaterializedType, { icon: typeof Boxes; color: string; label: string }> = {
  topic: { icon: Boxes, color: 'model', label: 'Topic' },
  flink: { icon: Zap, color: 'warning', label: 'Flink' },
  virtual_topic: { icon: Cloud, color: 'info', label: 'Virtual' },
  sink: { icon: Upload, color: 'default', label: 'Sink' },
}

export function ModelsPage() {
  const { name } = useParams()
  const navigate = useNavigate()
  const { project, status, deleteModel } = useProjectStore()
  const [searchQuery, setSearchQuery] = useState('')
  const [filterMaterialization, setFilterMaterialization] = useState<MaterializedType | 'all'>('all')

  // Delete dialog state
  const [deleteDialogOpen, setDeleteDialogOpen] = useState(false)
  const [modelToDelete, setModelToDelete] = useState<string | null>(null)
  const [saving, setSaving] = useState(false)

  const sources = project?.sources ?? []
  const models = project?.models ?? []
  const tests = project?.tests ?? []
  const exposures = project?.exposures ?? []
  const filteredModels = models.filter((m) => {
    const matchesSearch =
      m.name.toLowerCase().includes(searchQuery.toLowerCase()) ||
      m.description?.toLowerCase().includes(searchQuery.toLowerCase())
    const matchesFilter = filterMaterialization === 'all' || m.materialized === filterMaterialization
    return matchesSearch && matchesFilter
  })

  const selectedModel = name ? models.find((m) => m.name === name) : null
  const selectedModelStatus = selectedModel
    ? status?.flink_jobs.find((j) => j.name.includes(selectedModel.name))
    : null

  // Auto-select last viewed item or first item if none selected
  useEffect(() => {
    if (!name && models.length > 0) {
      const lastViewed = localStorage.getItem('lastModel')
      const targetModel = lastViewed && models.find(m => m.name === lastViewed)
        ? lastViewed
        : models[0].name
      navigate(`/models/${targetModel}`, { replace: true })
    }
  }, [name, models, navigate])

  // Remember last viewed item
  useEffect(() => {
    if (name && models.find(m => m.name === name)) {
      localStorage.setItem('lastModel', name)
    }
  }, [name, models])

  // Build node map for dependency tree
  const nodeMap = useMemo(() => {
    const map = new Map<string, { type: 'source' | 'model' | 'exposure' | 'test'; upstream: string[]; downstream: string[] }>()

    // Add sources
    sources.forEach((s) => {
      map.set(s.name, { type: 'source', upstream: [], downstream: [] })
    })

    // Add models
    models.forEach((m) => {
      const upstream: string[] = []
      // Find upstream from SQL or from_ field
      sources.forEach((s) => {
        if (m.sql?.includes(`source("${s.name}")`) || m.sql?.includes(`source('${s.name}')`)) {
          upstream.push(s.name)
        }
        if (m.from_?.some((f) => f.source === s.name)) {
          upstream.push(s.name)
        }
      })
      models.forEach((other) => {
        if (other.name !== m.name) {
          if (m.sql?.includes(`ref("${other.name}")`) || m.sql?.includes(`ref('${other.name}')`)) {
            upstream.push(other.name)
          }
          if (m.from_?.some((f) => f.ref === other.name)) {
            upstream.push(other.name)
          }
        }
      })
      map.set(m.name, { type: 'model', upstream: [...new Set(upstream)], downstream: [] })
    })

    // Add exposures
    exposures.forEach((e) => {
      const upstream: string[] = []
      e.consumes?.forEach((c) => {
        if (c.ref) upstream.push(c.ref)
        if (c.source) upstream.push(c.source)
      })
      e.depends_on?.forEach((d) => {
        if (d.ref) upstream.push(d.ref)
        if (d.source) upstream.push(d.source)
      })
      map.set(e.name, { type: 'exposure', upstream: [...new Set(upstream)], downstream: [] })
    })

    // Add tests
    tests.forEach((t) => {
      map.set(t.name, { type: 'test', upstream: [t.model], downstream: [] })
    })

    // Compute downstream from upstream
    map.forEach((node, nodeName) => {
      node.upstream.forEach((upstreamName) => {
        const upstreamNode = map.get(upstreamName)
        if (upstreamNode) {
          upstreamNode.downstream.push(nodeName)
        }
      })
    })

    return map
  }, [sources, models, exposures, tests])

  // Calculate dependents for cascade delete
  const getDependents = (modelName: string): Array<{ type: string; name: string }> => {
    const dependents: Array<{ type: string; name: string }> = []
    const visited = new Set<string>()

    const collectDependents = (nodeName: string) => {
      const node = nodeMap.get(nodeName)
      if (!node) return

      node.downstream.forEach((depName) => {
        if (!visited.has(depName)) {
          visited.add(depName)
          const depNode = nodeMap.get(depName)
          if (depNode) {
            dependents.push({ type: depNode.type, name: depName })
            collectDependents(depName)
          }
        }
      })
    }

    collectDependents(modelName)
    return dependents
  }

  const handleDeleteClick = (modelName: string) => {
    setModelToDelete(modelName)
    setDeleteDialogOpen(true)
  }

  const handleDeleteConfirm = async () => {
    if (!modelToDelete) return
    setSaving(true)
    try {
      deleteModel(modelToDelete)
      setDeleteDialogOpen(false)
      setModelToDelete(null)
      navigate('/models')
    } finally {
      setSaving(false)
    }
  }

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-2xl font-bold text-white">Models</h2>
          <p className="text-slate-400 mt-1">
            Data transformations and materializations
          </p>
        </div>
        <div className="flex items-center gap-3">
          <Badge variant="model">{models.length} models</Badge>
          <Button onClick={() => navigate('/models/new/edit')} icon={<Plus className="w-4 h-4" />}>
            Add Model
          </Button>
        </div>
      </div>

      {/* Filters */}
      <div className="flex items-center gap-4">
        <div className="relative flex-1">
          <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-5 h-5 text-slate-500" />
          <input
            type="text"
            placeholder="Search models..."
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            className="input w-full pl-10"
          />
        </div>
        <div className="flex items-center gap-2">
          {(['all', 'topic', 'flink', 'virtual_topic', 'sink'] as const).map((mat) => (
            <button
              key={mat}
              onClick={() => setFilterMaterialization(mat)}
              className={cn(
                'px-3 py-1.5 text-sm rounded-lg transition-colors',
                filterMaterialization === mat
                  ? 'bg-streamt-600 text-white'
                  : 'bg-slate-800 text-slate-400 hover:bg-slate-700'
              )}
            >
              {mat === 'all' ? 'All' : materializationConfig[mat].label}
            </button>
          ))}
        </div>
      </div>

      {/* Content */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        {/* Model list */}
        <div className="lg:col-span-1 space-y-3">
          {filteredModels.length === 0 ? (
            <Card>
              <CardContent className="py-12 text-center">
                <Boxes className="w-12 h-12 text-slate-600 mx-auto mb-4" />
                <p className="text-slate-400">No models found</p>
              </CardContent>
            </Card>
          ) : (
            filteredModels.map((model) => {
              const config = materializationConfig[model.materialized]
              const Icon = config.icon
              const jobStatus = status?.flink_jobs.find((j) => j.name.includes(model.name))

              return (
                <motion.div
                  key={model.name}
                  initial={{ opacity: 0, y: 10 }}
                  animate={{ opacity: 1, y: 0 }}
                  transition={{ duration: 0.2 }}
                >
                  <Card
                    hover
                    onClick={() => navigate(`/models/${model.name}`)}
                    className={cn(
                      'cursor-pointer',
                      selectedModel?.name === model.name && 'ring-2 ring-model'
                    )}
                  >
                    <CardContent className="p-4">
                      <div className="flex items-start justify-between">
                        <div className="flex items-center gap-3">
                          <div className={cn('p-2 rounded-lg', `bg-${config.color}/20`)}>
                            <Icon className={cn('w-5 h-5', `text-${config.color}`)} />
                          </div>
                          <div>
                            <div className="flex items-center gap-2">
                              <h3 className="font-medium text-white">{model.name}</h3>
                              {jobStatus?.status === 'RUNNING' && (
                                <StatusIndicator status="running" />
                              )}
                            </div>
                            <Badge variant={config.color as any} size="sm" className="mt-1">
                              {config.label}
                            </Badge>
                          </div>
                        </div>
                        <ChevronRight className="w-5 h-5 text-slate-500" />
                      </div>
                      {model.description && (
                        <p className="text-sm text-slate-400 mt-3 line-clamp-2">
                          {model.description}
                        </p>
                      )}
                    </CardContent>
                  </Card>
                </motion.div>
              )
            })
          )}
        </div>

        {/* Model details */}
        <div className="lg:col-span-2">
          {selectedModel ? (
            <Card>
              <CardHeader className="flex flex-row items-start justify-between">
                <div className="flex items-center gap-3">
                  <div className={cn('p-2 rounded-lg', `bg-${materializationConfig[selectedModel.materialized].color}/20`)}>
                    {(() => {
                      const Icon = materializationConfig[selectedModel.materialized].icon
                      return <Icon className={cn('w-6 h-6', `text-${materializationConfig[selectedModel.materialized].color}`)} />
                    })()}
                  </div>
                  <div>
                    <CardTitle>{selectedModel.name}</CardTitle>
                    <div className="flex items-center gap-2 mt-1">
                      <Badge variant={materializationConfig[selectedModel.materialized].color as any}>
                        {materializationConfig[selectedModel.materialized].label}
                      </Badge>
                      {selectedModelStatus?.status && (
                        <StatusIndicator status={selectedModelStatus.status.toLowerCase() as any} showLabel />
                      )}
                    </div>
                  </div>
                </div>
                <div className="flex items-center gap-4">
                  {selectedModel.owner && (
                    <div className="flex items-center gap-2 text-slate-400">
                      <User className="w-4 h-4" />
                      <span className="text-sm">{selectedModel.owner}</span>
                    </div>
                  )}
                  <div className="flex items-center gap-2">
                    <Button
                      variant="ghost"
                      size="icon"
                      onClick={() => navigate(`/models/${selectedModel.name}/edit`)}
                      title="Edit model"
                    >
                      <Pencil className="w-4 h-4" />
                    </Button>
                    <Button
                      variant="ghost"
                      size="icon"
                      onClick={() => navigate(`/models/${selectedModel.name}/edit?view=yaml`)}
                      title="Edit as YAML"
                    >
                      <Code className="w-4 h-4" />
                    </Button>
                    <Button
                      variant="ghost"
                      size="icon"
                      onClick={() => handleDeleteClick(selectedModel.name)}
                      title="Delete model"
                      className="text-slate-400 hover:text-red-400"
                    >
                      <Trash2 className="w-4 h-4" />
                    </Button>
                    <Button
                      variant="ghost"
                      size="icon"
                      onClick={() => navigate(`/dag?focus=${selectedModel.name}`)}
                      title="View in DAG"
                    >
                      <ExternalLink className="w-4 h-4" />
                    </Button>
                  </div>
                </div>
              </CardHeader>
              <CardContent className="space-y-6">
                {/* Description */}
                {selectedModel.description && (
                  <div>
                    <h4 className="text-sm font-medium text-slate-400 mb-2">Description</h4>
                    <p className="text-slate-300">{selectedModel.description}</p>
                  </div>
                )}

                {/* Flink Configuration */}
                {selectedModel.flink && (
                  <div>
                    <h4 className="text-sm font-medium text-slate-400 mb-3 flex items-center gap-2">
                      <Zap className="w-4 h-4" />
                      Flink Configuration
                    </h4>
                    <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                      {selectedModel.flink.parallelism && (
                        <div className="p-3 rounded-lg bg-slate-800/50">
                          <p className="text-xs text-slate-500">Parallelism</p>
                          <p className="text-lg font-semibold text-white">
                            {selectedModel.flink.parallelism}
                          </p>
                        </div>
                      )}
                      {selectedModel.flink.checkpoint_interval_ms && (
                        <div className="p-3 rounded-lg bg-slate-800/50">
                          <p className="text-xs text-slate-500">Checkpoint</p>
                          <p className="text-lg font-semibold text-white">
                            {selectedModel.flink.checkpoint_interval_ms / 1000}s
                          </p>
                        </div>
                      )}
                      {selectedModel.flink.state_ttl_ms && (
                        <div className="p-3 rounded-lg bg-slate-800/50">
                          <p className="text-xs text-slate-500">State TTL</p>
                          <p className="text-lg font-semibold text-white">
                            {(selectedModel.flink.state_ttl_ms / 3600000).toFixed(0)}h
                          </p>
                        </div>
                      )}
                      {selectedModel.flink.watermark_delay_ms && (
                        <div className="p-3 rounded-lg bg-slate-800/50">
                          <p className="text-xs text-slate-500">Watermark</p>
                          <p className="text-lg font-semibold text-white">
                            {selectedModel.flink.watermark_delay_ms / 1000}s
                          </p>
                        </div>
                      )}
                    </div>
                  </div>
                )}

                {/* Topic Configuration */}
                {selectedModel.topic && (
                  <div>
                    <h4 className="text-sm font-medium text-slate-400 mb-3 flex items-center gap-2">
                      <Activity className="w-4 h-4" />
                      Topic Configuration
                    </h4>
                    <div className="grid grid-cols-2 md:grid-cols-3 gap-4">
                      {selectedModel.topic.partitions && (
                        <div className="p-3 rounded-lg bg-slate-800/50">
                          <p className="text-xs text-slate-500">Partitions</p>
                          <p className="text-lg font-semibold text-white">
                            {selectedModel.topic.partitions}
                          </p>
                        </div>
                      )}
                      {selectedModel.topic.replication_factor && (
                        <div className="p-3 rounded-lg bg-slate-800/50">
                          <p className="text-xs text-slate-500">Replication</p>
                          <p className="text-lg font-semibold text-white">
                            {selectedModel.topic.replication_factor}
                          </p>
                        </div>
                      )}
                      {selectedModel.topic.retention_ms && (
                        <div className="p-3 rounded-lg bg-slate-800/50">
                          <p className="text-xs text-slate-500">Retention</p>
                          <p className="text-lg font-semibold text-white">
                            {(selectedModel.topic.retention_ms / 86400000).toFixed(0)}d
                          </p>
                        </div>
                      )}
                    </div>
                  </div>
                )}

                {/* SQL */}
                {selectedModel.sql && (
                  <div>
                    <h4 className="text-sm font-medium text-slate-400 mb-3 flex items-center gap-2">
                      <FileCode className="w-4 h-4" />
                      SQL Transformation
                    </h4>
                    <SqlHighlighter sql={selectedModel.sql} />
                  </div>
                )}

                {/* Sink Configuration */}
                {selectedModel.sink && (
                  <div>
                    <h4 className="text-sm font-medium text-slate-400 mb-3 flex items-center gap-2">
                      <Upload className="w-4 h-4" />
                      Sink Configuration
                    </h4>
                    <div className="p-4 rounded-lg bg-slate-800/50 space-y-3">
                      <div className="flex justify-between">
                        <span className="text-slate-500">Connector</span>
                        <code className="text-streamt-400">{selectedModel.sink.connector}</code>
                      </div>
                      {Object.entries(selectedModel.sink.config).map(([key, value]) => (
                        <div key={key} className="flex justify-between">
                          <span className="text-slate-500 text-sm">{key}</span>
                          <code className="text-slate-300 text-sm">{value}</code>
                        </div>
                      ))}
                    </div>
                  </div>
                )}

                {/* Dependency Trees */}
                <div className="space-y-4">
                  <DependencyTree
                    title="Upstream Dependencies"
                    rootName={selectedModel.name}
                    nodes={nodeMap}
                    direction="upstream"
                  />
                  <DependencyTree
                    title="Downstream Dependents"
                    rootName={selectedModel.name}
                    nodes={nodeMap}
                    direction="downstream"
                  />
                  {tests.filter((t) => t.model === selectedModel.name).length > 0 && (
                    <RelationshipLinks
                      title="Tests"
                      icon="related"
                      items={tests.filter((t) => t.model === selectedModel.name).map((t) => ({ type: 'test', name: t.name }))}
                    />
                  )}
                </div>
              </CardContent>
            </Card>
          ) : (
            <Card>
              <CardContent className="py-24 text-center">
                <Boxes className="w-16 h-16 text-slate-700 mx-auto mb-4" />
                <h3 className="text-lg font-medium text-slate-400 mb-2">
                  Select a model
                </h3>
                <p className="text-sm text-slate-500">
                  Choose a model from the list to view its details
                </p>
              </CardContent>
            </Card>
          )}
        </div>
      </div>

      {/* Cascade Delete Dialog */}
      <CascadeDeleteDialog
        open={deleteDialogOpen}
        onClose={() => {
          setDeleteDialogOpen(false)
          setModelToDelete(null)
        }}
        onConfirm={handleDeleteConfirm}
        entityName={modelToDelete || ''}
        entityType="model"
        dependents={modelToDelete ? getDependents(modelToDelete) : []}
        loading={saving}
      />
    </div>
  )
}
