import { useState, useMemo, useEffect } from 'react'
import { useParams, useNavigate } from 'react-router-dom'
import { motion } from 'framer-motion'
import {
  Radio,
  Search,
  AppWindow,
  BarChart3,
  Brain,
  FileCode,
  User,
  Clock,
  Shield,
  ArrowDownToLine,
  ArrowUpFromLine,
  ExternalLink,
  Plus,
  Pencil,
  Trash2,
  ChevronRight,
  Code,
} from 'lucide-react'
import { Card, CardContent, CardHeader, CardTitle, Badge, RelationshipLinks, DependencyTree, Button, CascadeDeleteDialog } from '@/components/ui'
import { useProjectStore } from '@/stores/projectStore'
import { cn } from '@/utils/cn'
import type { ExposureType, ExposureRole } from '@/types'

const exposureTypeConfig: Record<ExposureType, { icon: typeof Radio; color: string; label: string }> = {
  application: { icon: AppWindow, color: 'exposure', label: 'Application' },
  dashboard: { icon: BarChart3, color: 'info', label: 'Dashboard' },
  ml_model: { icon: Brain, color: 'warning', label: 'ML Model' },
  notebook: { icon: FileCode, color: 'default', label: 'Notebook' },
  other: { icon: Radio, color: 'default', label: 'Other' },
}

const roleConfig: Record<ExposureRole, { icon: typeof Radio; label: string }> = {
  producer: { icon: ArrowUpFromLine, label: 'Producer' },
  consumer: { icon: ArrowDownToLine, label: 'Consumer' },
  both: { icon: Radio, label: 'Both' },
}

export function ExposuresPage() {
  const { name } = useParams()
  const navigate = useNavigate()
  const { project, deleteExposure } = useProjectStore()
  const [searchQuery, setSearchQuery] = useState('')

  // Delete dialog state
  const [deleteDialogOpen, setDeleteDialogOpen] = useState(false)
  const [exposureToDelete, setExposureToDelete] = useState<string | null>(null)
  const [saving, setSaving] = useState(false)

  const sources = project?.sources ?? []
  const models = project?.models ?? []
  const exposures = project?.exposures ?? []
  const tests = project?.tests ?? []

  const filteredExposures = exposures.filter(
    (e) =>
      e.name.toLowerCase().includes(searchQuery.toLowerCase()) ||
      e.description?.toLowerCase().includes(searchQuery.toLowerCase())
  )

  const selectedExposure = name ? exposures.find((e) => e.name === name) : null

  // Auto-select last viewed item or first item if none selected
  useEffect(() => {
    if (!name && exposures.length > 0) {
      const lastViewed = localStorage.getItem('lastExposure')
      const targetExposure = lastViewed && exposures.find(e => e.name === lastViewed)
        ? lastViewed
        : exposures[0].name
      navigate(`/exposures/${targetExposure}`, { replace: true })
    }
  }, [name, exposures, navigate])

  // Remember last viewed item
  useEffect(() => {
    if (name && exposures.find(e => e.name === name)) {
      localStorage.setItem('lastExposure', name)
    }
  }, [name, exposures])

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

  const handleDeleteClick = (exposureName: string) => {
    setExposureToDelete(exposureName)
    setDeleteDialogOpen(true)
  }

  const handleDeleteConfirm = async () => {
    if (!exposureToDelete) return
    setSaving(true)
    try {
      deleteExposure(exposureToDelete)
      setDeleteDialogOpen(false)
      setExposureToDelete(null)
      navigate('/exposures')
    } finally {
      setSaving(false)
    }
  }

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-2xl font-bold text-white">Exposures</h2>
          <p className="text-slate-400 mt-1">
            Downstream applications and services consuming your pipeline
          </p>
        </div>
        <div className="flex items-center gap-3">
          <Badge variant="exposure">{exposures.length} exposures</Badge>
          <Button onClick={() => navigate('/exposures/new/edit')} icon={<Plus className="w-4 h-4" />}>
            Add Exposure
          </Button>
        </div>
      </div>

      {/* Search */}
      <div className="relative">
        <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-5 h-5 text-slate-500" />
        <input
          type="text"
          placeholder="Search exposures..."
          value={searchQuery}
          onChange={(e) => setSearchQuery(e.target.value)}
          className="input w-full pl-10"
        />
      </div>

      {/* Content */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        {/* Exposure list */}
        <div className="lg:col-span-1 space-y-3">
          {filteredExposures.length === 0 ? (
            <Card>
              <CardContent className="py-12 text-center">
                <Radio className="w-12 h-12 text-slate-600 mx-auto mb-4" />
                <p className="text-slate-400">No exposures found</p>
              </CardContent>
            </Card>
          ) : (
            filteredExposures.map((exposure) => {
              const config = exposureTypeConfig[exposure.type]
              const Icon = config.icon

              return (
                <motion.div
                  key={exposure.name}
                  initial={{ opacity: 0, y: 10 }}
                  animate={{ opacity: 1, y: 0 }}
                  transition={{ duration: 0.2 }}
                >
                  <Card
                    hover
                    onClick={() => navigate(`/exposures/${exposure.name}`)}
                    className={cn(
                      'cursor-pointer',
                      selectedExposure?.name === exposure.name && 'ring-2 ring-exposure'
                    )}
                  >
                    <CardContent className="p-4">
                      <div className="flex items-start justify-between">
                        <div className="flex items-center gap-3">
                          <div className="p-2 rounded-lg bg-exposure/20">
                            <Icon className="w-5 h-5 text-exposure" />
                          </div>
                          <div>
                            <h3 className="font-medium text-white">{exposure.name}</h3>
                            {exposure.role && (
                              <p className="text-xs text-slate-500 mt-0.5 capitalize">
                                {exposure.role}
                              </p>
                            )}
                          </div>
                        </div>
                        <ChevronRight className="w-5 h-5 text-slate-500" />
                      </div>
                      {exposure.description && (
                        <p className="text-sm text-slate-400 mt-3 line-clamp-2">
                          {exposure.description}
                        </p>
                      )}
                      <div className="flex items-center gap-2 mt-3">
                        <Badge variant={config.color as any} size="sm">
                          {config.label}
                        </Badge>
                      </div>
                    </CardContent>
                  </Card>
                </motion.div>
              )
            })
          )}
        </div>

        {/* Exposure details */}
        <div className="lg:col-span-2">
          {selectedExposure ? (
            <Card>
              <CardHeader className="flex flex-row items-center justify-between">
                <div className="flex items-center gap-3">
                  <div className="p-2 rounded-lg bg-exposure/20">
                    {(() => {
                      const Icon = exposureTypeConfig[selectedExposure.type].icon
                      return <Icon className="w-6 h-6 text-exposure" />
                    })()}
                  </div>
                  <div>
                    <CardTitle>{selectedExposure.name}</CardTitle>
                    <div className="flex items-center gap-2 mt-1">
                      <Badge variant={exposureTypeConfig[selectedExposure.type].color as any}>
                        {exposureTypeConfig[selectedExposure.type].label}
                      </Badge>
                      {selectedExposure.role && (
                        <Badge variant="default">
                          {roleConfig[selectedExposure.role].label}
                        </Badge>
                      )}
                    </div>
                  </div>
                </div>
                <div className="flex items-center gap-2">
                  <Button
                    variant="ghost"
                    size="icon"
                    onClick={() => navigate(`/exposures/${selectedExposure.name}/edit`)}
                    title="Edit exposure"
                  >
                    <Pencil className="w-4 h-4" />
                  </Button>
                  <Button
                    variant="ghost"
                    size="icon"
                    onClick={() => navigate(`/exposures/${selectedExposure.name}/edit?view=yaml`)}
                    title="Edit as YAML"
                  >
                    <Code className="w-4 h-4" />
                  </Button>
                  <Button
                    variant="ghost"
                    size="icon"
                    onClick={() => handleDeleteClick(selectedExposure.name)}
                    title="Delete exposure"
                    className="text-slate-400 hover:text-red-400"
                  >
                    <Trash2 className="w-4 h-4" />
                  </Button>
                  <Button
                    variant="ghost"
                    size="icon"
                    onClick={() => navigate(`/dag?focus=${selectedExposure.name}`)}
                    title="View in DAG"
                  >
                    <ExternalLink className="w-4 h-4" />
                  </Button>
                </div>
              </CardHeader>
              <CardContent className="space-y-6">
                {/* Description */}
                {selectedExposure.description && (
                  <div>
                    <h4 className="text-sm font-medium text-slate-400 mb-2">Description</h4>
                    <p className="text-slate-300">{selectedExposure.description}</p>
                  </div>
                )}

                {/* Owner */}
                {selectedExposure.owner && (
                  <div className="flex items-center gap-2">
                    <User className="w-4 h-4 text-slate-500" />
                    <span className="text-sm text-slate-300">{selectedExposure.owner}</span>
                  </div>
                )}

                {/* Consumer Group */}
                {selectedExposure.consumer_group && (
                  <div>
                    <h4 className="text-sm font-medium text-slate-400 mb-2">Consumer Group</h4>
                    <code className="text-sm text-streamt-400 bg-slate-800 px-3 py-1.5 rounded-lg">
                      {selectedExposure.consumer_group}
                    </code>
                  </div>
                )}

                {/* SLA */}
                {selectedExposure.sla && (
                  <div>
                    <h4 className="text-sm font-medium text-slate-400 mb-3 flex items-center gap-2">
                      <Shield className="w-4 h-4" />
                      Service Level Agreement
                    </h4>
                    <div className="grid grid-cols-2 gap-4">
                      {selectedExposure.sla.max_latency_ms && (
                        <div className="p-4 rounded-lg bg-slate-800/50">
                          <div className="flex items-center gap-2 mb-2">
                            <Clock className="w-4 h-4 text-slate-500" />
                            <p className="text-xs text-slate-500">Max Latency</p>
                          </div>
                          <p className="text-2xl font-semibold text-white">
                            {selectedExposure.sla.max_latency_ms}
                            <span className="text-sm text-slate-500 ml-1">ms</span>
                          </p>
                        </div>
                      )}
                      {selectedExposure.sla.availability_percent && (
                        <div className="p-4 rounded-lg bg-slate-800/50">
                          <div className="flex items-center gap-2 mb-2">
                            <Shield className="w-4 h-4 text-slate-500" />
                            <p className="text-xs text-slate-500">Availability</p>
                          </div>
                          <p className="text-2xl font-semibold text-white">
                            {selectedExposure.sla.availability_percent}
                            <span className="text-sm text-slate-500 ml-1">%</span>
                          </p>
                        </div>
                      )}
                    </div>
                  </div>
                )}

                {/* Dependency Tree */}
                <div className="space-y-4">
                  <DependencyTree
                    title="Upstream Dependencies"
                    rootName={selectedExposure.name}
                    nodes={nodeMap}
                    direction="upstream"
                  />
                  {selectedExposure.produces.filter((p) => p.ref || p.source).length > 0 && (
                    <RelationshipLinks
                      title="Produces"
                      icon="downstream"
                      items={selectedExposure.produces
                        .filter((p) => p.ref || p.source)
                        .map((p) => p.source
                          ? { type: 'source' as const, name: p.source }
                          : { type: 'model' as const, name: p.ref! }
                        )}
                    />
                  )}
                </div>
              </CardContent>
            </Card>
          ) : (
            <Card>
              <CardContent className="py-24 text-center">
                <Radio className="w-16 h-16 text-slate-700 mx-auto mb-4" />
                <h3 className="text-lg font-medium text-slate-400 mb-2">
                  Select an exposure
                </h3>
                <p className="text-sm text-slate-500">
                  Choose an exposure from the list to view its details
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
          setExposureToDelete(null)
        }}
        onConfirm={handleDeleteConfirm}
        entityName={exposureToDelete || ''}
        entityType="exposure"
        dependents={[]}
        loading={saving}
      />
    </div>
  )
}
