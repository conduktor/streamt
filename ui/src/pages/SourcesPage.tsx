import { useState, useMemo, useEffect } from 'react'
import { useParams, useNavigate } from 'react-router-dom'
import { motion } from 'framer-motion'
import {
  Database,
  Search,
  Clock,
  Shield,
  Table,
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

export function SourcesPage() {
  const { name } = useParams()
  const navigate = useNavigate()
  const { project, deleteSource } = useProjectStore()
  const [searchQuery, setSearchQuery] = useState('')

  // Delete dialog state
  const [deleteDialogOpen, setDeleteDialogOpen] = useState(false)
  const [sourceToDelete, setSourceToDelete] = useState<string | null>(null)
  const [saving, setSaving] = useState(false)

  const sources = project?.sources ?? []
  const models = project?.models ?? []
  const exposures = project?.exposures ?? []
  const tests = project?.tests ?? []

  const filteredSources = sources.filter(
    (s) =>
      s.name.toLowerCase().includes(searchQuery.toLowerCase()) ||
      s.description?.toLowerCase().includes(searchQuery.toLowerCase()) ||
      s.topic.toLowerCase().includes(searchQuery.toLowerCase())
  )

  const selectedSource = name ? sources.find((s) => s.name === name) : null

  // Auto-select last viewed item or first item if none selected
  useEffect(() => {
    if (!name && sources.length > 0) {
      const lastViewed = localStorage.getItem('lastSource')
      const targetSource = lastViewed && sources.find(s => s.name === lastViewed)
        ? lastViewed
        : sources[0].name
      navigate(`/sources/${targetSource}`, { replace: true })
    }
  }, [name, sources, navigate])

  // Remember last viewed item
  useEffect(() => {
    if (name && sources.find(s => s.name === name)) {
      localStorage.setItem('lastSource', name)
    }
  }, [name, sources])

  // Build node map for dependency tree
  const nodeMap = useMemo(() => {
    const map = new Map<string, { type: 'source' | 'model' | 'exposure' | 'test'; upstream: string[]; downstream: string[] }>()

    sources.forEach((s) => {
      map.set(s.name, { type: 'source', upstream: [], downstream: [] })
    })

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

    tests.forEach((t) => {
      map.set(t.name, { type: 'test', upstream: [t.model], downstream: [] })
    })

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
  const getDependents = (sourceName: string): Array<{ type: string; name: string }> => {
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

    collectDependents(sourceName)
    return dependents
  }

  const handleDeleteClick = (sourceName: string) => {
    setSourceToDelete(sourceName)
    setDeleteDialogOpen(true)
  }

  const handleDeleteConfirm = async () => {
    if (!sourceToDelete) return
    setSaving(true)
    try {
      deleteSource(sourceToDelete)
      setDeleteDialogOpen(false)
      setSourceToDelete(null)
      navigate('/sources')
    } finally {
      setSaving(false)
    }
  }

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-2xl font-bold text-white">Sources</h2>
          <p className="text-slate-400 mt-1">
            External Kafka topics that feed your pipeline
          </p>
        </div>
        <div className="flex items-center gap-3">
          <Badge variant="source">{sources.length} sources</Badge>
          <Button onClick={() => navigate('/sources/new/edit')} icon={<Plus className="w-4 h-4" />}>
            Add Source
          </Button>
        </div>
      </div>

      {/* Search */}
      <div className="relative">
        <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-5 h-5 text-slate-500" />
        <input
          type="text"
          placeholder="Search sources..."
          value={searchQuery}
          onChange={(e) => setSearchQuery(e.target.value)}
          className="input w-full pl-10"
        />
      </div>

      {/* Content */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        {/* Source list */}
        <div className="lg:col-span-1 space-y-3">
          {filteredSources.length === 0 ? (
            <Card>
              <CardContent className="py-12 text-center">
                <Database className="w-12 h-12 text-slate-600 mx-auto mb-4" />
                <p className="text-slate-400">No sources found</p>
              </CardContent>
            </Card>
          ) : (
            filteredSources.map((source) => (
              <motion.div
                key={source.name}
                initial={{ opacity: 0, y: 10 }}
                animate={{ opacity: 1, y: 0 }}
                transition={{ duration: 0.2 }}
              >
                <Card
                  hover
                  onClick={() => navigate(`/sources/${source.name}`)}
                  className={cn(
                    'cursor-pointer',
                    selectedSource?.name === source.name && 'ring-2 ring-source'
                  )}
                >
                  <CardContent className="p-4">
                    <div className="flex items-start justify-between">
                      <div className="flex items-center gap-3">
                        <div className="p-2 rounded-lg bg-source/20">
                          <Database className="w-5 h-5 text-source" />
                        </div>
                        <div>
                          <h3 className="font-medium text-white">{source.name}</h3>
                          <p className="text-xs text-slate-500 font-mono mt-0.5">
                            {source.topic}
                          </p>
                        </div>
                      </div>
                      <ChevronRight className="w-5 h-5 text-slate-500" />
                    </div>
                    {source.description && (
                      <p className="text-sm text-slate-400 mt-3 line-clamp-2">
                        {source.description}
                      </p>
                    )}
                    <div className="flex items-center gap-2 mt-3">
                      <Badge variant="default" size="sm">
                        <Table className="w-3 h-3" />
                        {source.columns.length} columns
                      </Badge>
                    </div>
                  </CardContent>
                </Card>
              </motion.div>
            ))
          )}
        </div>

        {/* Source details */}
        <div className="lg:col-span-2">
          {selectedSource ? (
            <Card>
              <CardHeader className="flex flex-row items-center justify-between">
                <div className="flex items-center gap-3">
                  <div className="p-2 rounded-lg bg-source/20">
                    <Database className="w-6 h-6 text-source" />
                  </div>
                  <div>
                    <CardTitle>{selectedSource.name}</CardTitle>
                    <code className="text-sm text-slate-500">{selectedSource.topic}</code>
                  </div>
                </div>
                <div className="flex items-center gap-2">
                  <Button
                    variant="ghost"
                    size="icon"
                    onClick={() => navigate(`/sources/${selectedSource.name}/edit`)}
                    title="Edit source"
                  >
                    <Pencil className="w-4 h-4" />
                  </Button>
                  <Button
                    variant="ghost"
                    size="icon"
                    onClick={() => navigate(`/sources/${selectedSource.name}/edit?view=yaml`)}
                    title="Edit as YAML"
                  >
                    <Code className="w-4 h-4" />
                  </Button>
                  <Button
                    variant="ghost"
                    size="icon"
                    onClick={() => handleDeleteClick(selectedSource.name)}
                    title="Delete source"
                    className="text-slate-400 hover:text-red-400"
                  >
                    <Trash2 className="w-4 h-4" />
                  </Button>
                  <Button
                    variant="ghost"
                    size="icon"
                    onClick={() => navigate(`/dag?focus=${selectedSource.name}`)}
                    title="View in DAG"
                  >
                    <ExternalLink className="w-4 h-4" />
                  </Button>
                </div>
              </CardHeader>
              <CardContent className="space-y-6">
                {/* Description */}
                {selectedSource.description && (
                  <div>
                    <h4 className="text-sm font-medium text-slate-400 mb-2">Description</h4>
                    <p className="text-slate-300">{selectedSource.description}</p>
                  </div>
                )}

                {/* Freshness */}
                {selectedSource.freshness && (
                  <div>
                    <h4 className="text-sm font-medium text-slate-400 mb-2 flex items-center gap-2">
                      <Clock className="w-4 h-4" />
                      Freshness SLA
                    </h4>
                    <div className="grid grid-cols-2 gap-4">
                      {selectedSource.freshness.warn_after_minutes && (
                        <div className="p-3 rounded-lg bg-yellow-900/20 border border-yellow-800/50">
                          <p className="text-xs text-yellow-400 mb-1">Warn After</p>
                          <p className="text-lg font-semibold text-white">
                            {selectedSource.freshness.warn_after_minutes} min
                          </p>
                        </div>
                      )}
                      {selectedSource.freshness.error_after_minutes && (
                        <div className="p-3 rounded-lg bg-red-900/20 border border-red-800/50">
                          <p className="text-xs text-red-400 mb-1">Error After</p>
                          <p className="text-lg font-semibold text-white">
                            {selectedSource.freshness.error_after_minutes} min
                          </p>
                        </div>
                      )}
                    </div>
                  </div>
                )}

                {/* Schema */}
                <div>
                  <h4 className="text-sm font-medium text-slate-400 mb-3 flex items-center gap-2">
                    <Table className="w-4 h-4" />
                    Schema ({selectedSource.columns.length} columns)
                  </h4>
                  <div className="rounded-lg border border-slate-800 overflow-hidden">
                    <table className="w-full">
                      <thead>
                        <tr className="bg-slate-800/50">
                          <th className="text-left text-xs font-medium text-slate-400 px-4 py-2">
                            Column
                          </th>
                          <th className="text-left text-xs font-medium text-slate-400 px-4 py-2">
                            Type
                          </th>
                          <th className="text-left text-xs font-medium text-slate-400 px-4 py-2">
                            Classification
                          </th>
                          <th className="text-left text-xs font-medium text-slate-400 px-4 py-2">
                            Description
                          </th>
                        </tr>
                      </thead>
                      <tbody>
                        {selectedSource.columns.map((col, i) => (
                          <tr
                            key={col.name}
                            className={cn(
                              'border-t border-slate-800',
                              i % 2 === 0 ? 'bg-slate-900/30' : ''
                            )}
                          >
                            <td className="px-4 py-2">
                              <code className="text-sm text-white">{col.name}</code>
                            </td>
                            <td className="px-4 py-2">
                              <code className="text-sm text-slate-400">{col.type || 'STRING'}</code>
                            </td>
                            <td className="px-4 py-2">
                              {col.classification ? (
                                <Badge
                                  variant={
                                    col.classification === 'sensitive' ||
                                    col.classification === 'highly_sensitive'
                                      ? 'warning'
                                      : 'default'
                                  }
                                  size="sm"
                                >
                                  {col.classification === 'highly_sensitive' && (
                                    <Shield className="w-3 h-3" />
                                  )}
                                  {col.classification}
                                </Badge>
                              ) : (
                                <span className="text-slate-600">-</span>
                              )}
                            </td>
                            <td className="px-4 py-2 text-sm text-slate-500">
                              {col.description || '-'}
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </div>

                {/* Dependency Tree */}
                <div className="space-y-4">
                  <DependencyTree
                    title="Downstream Dependents"
                    rootName={selectedSource.name}
                    nodes={nodeMap}
                    direction="downstream"
                  />
                  {exposures.filter((e) => e.produces?.some((p) => p.source === selectedSource.name)).length > 0 && (
                    <RelationshipLinks
                      title="Produced by"
                      icon="upstream"
                      items={exposures.filter((e) => e.produces?.some((p) => p.source === selectedSource.name)).map((e) => ({ type: 'exposure', name: e.name }))}
                    />
                  )}
                </div>
              </CardContent>
            </Card>
          ) : (
            <Card>
              <CardContent className="py-24 text-center">
                <Database className="w-16 h-16 text-slate-700 mx-auto mb-4" />
                <h3 className="text-lg font-medium text-slate-400 mb-2">
                  Select a source
                </h3>
                <p className="text-sm text-slate-500">
                  Choose a source from the list to view its details
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
          setSourceToDelete(null)
        }}
        onConfirm={handleDeleteConfirm}
        entityName={sourceToDelete || ''}
        entityType="source"
        dependents={sourceToDelete ? getDependents(sourceToDelete) : []}
        loading={saving}
      />
    </div>
  )
}
