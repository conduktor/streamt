import { useState, useEffect } from 'react'
import { useNavigate, useSearchParams } from 'react-router-dom'
import { motion, AnimatePresence } from 'framer-motion'
import {
  X,
  Database,
  Boxes,
  Radio,
  ExternalLink,
  GitBranch,
  User,
  FileCode,
  ChevronRight,
  Zap,
  Cloud,
  Upload,
} from 'lucide-react'
import { DAGVisualization } from '@/components/dag'
import { Card, CardContent, CardHeader, CardTitle, Badge, StatusIndicator, SqlHighlighter } from '@/components/ui'
import { useProjectStore } from '@/stores/projectStore'
import type { MaterializedType } from '@/types'

const materializationIcons: Record<MaterializedType, typeof Boxes> = {
  topic: Boxes,
  flink: Zap,
  virtual_topic: Cloud,
  sink: Upload,
}

export function DAGPage() {
  const navigate = useNavigate()
  const [searchParams, setSearchParams] = useSearchParams()
  const { dag, selectedNode, selectNode, getSource, getModel, getExposure, status } = useProjectStore()
  const [detailsOpen, setDetailsOpen] = useState(true)
  const [focusNode, setFocusNode] = useState<string | null>(null)

  // Handle focus query parameter
  useEffect(() => {
    const focus = searchParams.get('focus')
    if (focus && dag?.nodes.some((n) => n.name === focus)) {
      selectNode(focus)
      setFocusNode(focus)
      setDetailsOpen(true)
      // Clear the query param after focusing
      setSearchParams({}, { replace: true })
    }
  }, [searchParams, dag, selectNode, setSearchParams])

  // Get selected node details
  const dagNode = dag?.nodes.find((n) => n.name === selectedNode)
  const source = dagNode?.type === 'source' ? getSource(selectedNode!) : undefined
  const model = dagNode?.type === 'model' ? getModel(selectedNode!) : undefined
  const exposure = dagNode?.type === 'exposure' ? getExposure(selectedNode!) : undefined

  const nodeDetails = source || model || exposure

  // Get status for selected node
  const nodeStatus = selectedNode
    ? status?.flink_jobs.find((j) => j.name.includes(selectedNode))?.status ||
      (status?.topics.find((t) => t.name === selectedNode)?.exists ? 'running' : 'unknown')
    : null

  const handleCloseDetails = () => {
    selectNode(null)
    setDetailsOpen(false)
  }

  const handleNodeClick = () => {
    setDetailsOpen(true)
  }

  const navigateToEntity = () => {
    if (!dagNode) return
    const routes: Record<string, string> = {
      source: '/sources',
      model: '/models',
      exposure: '/exposures',
    }
    navigate(`${routes[dagNode.type]}/${selectedNode}`)
  }

  return (
    <div className="h-[calc(100vh-8rem)] flex gap-4">
      {/* Main DAG area */}
      <div className="flex-1 rounded-xl border border-slate-800 bg-slate-900/50 overflow-hidden">
        <DAGVisualization dag={dag} onNodeClick={handleNodeClick} focusNode={focusNode} onFocusComplete={() => setFocusNode(null)} />
      </div>

      {/* Details sidebar */}
      <AnimatePresence>
        {selectedNode && detailsOpen && (
          <motion.div
            initial={{ opacity: 0, x: 20, width: 0 }}
            animate={{ opacity: 1, x: 0, width: 400 }}
            exit={{ opacity: 0, x: 20, width: 0 }}
            transition={{ duration: 0.2 }}
            className="overflow-hidden"
          >
            <Card className="h-full w-[400px] flex flex-col">
              <CardHeader className="flex flex-row items-center justify-between py-3">
                <div className="flex items-center gap-2">
                  {dagNode?.type === 'source' && (
                    <div className="p-1.5 rounded-lg bg-source/20">
                      <Database className="w-4 h-4 text-source" />
                    </div>
                  )}
                  {dagNode?.type === 'model' && (
                    <div className="p-1.5 rounded-lg bg-model/20">
                      <Boxes className="w-4 h-4 text-model" />
                    </div>
                  )}
                  {dagNode?.type === 'exposure' && (
                    <div className="p-1.5 rounded-lg bg-exposure/20">
                      <Radio className="w-4 h-4 text-exposure" />
                    </div>
                  )}
                  <div>
                    <CardTitle className="text-base">{selectedNode}</CardTitle>
                    <p className="text-xs text-slate-500 capitalize">{dagNode?.type}</p>
                  </div>
                </div>
                <div className="flex items-center gap-2">
                  <button
                    onClick={navigateToEntity}
                    className="p-1.5 text-slate-400 hover:text-white hover:bg-slate-800 rounded-lg transition-colors"
                  >
                    <ExternalLink className="w-4 h-4" />
                  </button>
                  <button
                    onClick={handleCloseDetails}
                    className="p-1.5 text-slate-400 hover:text-white hover:bg-slate-800 rounded-lg transition-colors"
                  >
                    <X className="w-4 h-4" />
                  </button>
                </div>
              </CardHeader>

              <CardContent className="flex-1 overflow-y-auto space-y-4">
                {/* Status */}
                {nodeStatus && (
                  <div className="flex items-center justify-between p-3 rounded-lg bg-slate-800/50">
                    <span className="text-sm text-slate-400">Status</span>
                    <StatusIndicator status={nodeStatus as any} showLabel />
                  </div>
                )}

                {/* Description */}
                {nodeDetails && 'description' in nodeDetails && nodeDetails.description && (
                  <div>
                    <h4 className="text-sm font-medium text-slate-400 mb-2">Description</h4>
                    <p className="text-sm text-slate-300">{nodeDetails.description}</p>
                  </div>
                )}

                {/* Source details */}
                {source && (
                  <>
                    <div>
                      <h4 className="text-sm font-medium text-slate-400 mb-2">Topic</h4>
                      <code className="text-sm text-streamt-400 bg-slate-800 px-2 py-1 rounded">
                        {source.topic}
                      </code>
                    </div>
                    {source.columns.length > 0 && (
                      <div>
                        <h4 className="text-sm font-medium text-slate-400 mb-2">
                          Columns ({source.columns.length})
                        </h4>
                        <div className="space-y-1 max-h-48 overflow-y-auto">
                          {source.columns.map((col) => (
                            <div
                              key={col.name}
                              className="flex items-center justify-between p-2 rounded bg-slate-800/50"
                            >
                              <span className="text-sm text-white font-mono">{col.name}</span>
                              <span className="text-xs text-slate-500">{col.type}</span>
                            </div>
                          ))}
                        </div>
                      </div>
                    )}
                  </>
                )}

                {/* Model details */}
                {model && (
                  <>
                    <div>
                      <h4 className="text-sm font-medium text-slate-400 mb-2">Materialization</h4>
                      <Badge variant={model.materialized === 'flink' ? 'warning' : 'model'}>
                        {materializationIcons[model.materialized] && (
                          <span className="w-3 h-3">
                            {(() => {
                              const Icon = materializationIcons[model.materialized]
                              return <Icon className="w-3 h-3" />
                            })()}
                          </span>
                        )}
                        {model.materialized}
                      </Badge>
                    </div>

                    {model.owner && (
                      <div className="flex items-center gap-2">
                        <User className="w-4 h-4 text-slate-500" />
                        <span className="text-sm text-slate-300">{model.owner}</span>
                      </div>
                    )}

                    {model.sql && (
                      <div>
                        <h4 className="text-sm font-medium text-slate-400 mb-2 flex items-center gap-2">
                          <FileCode className="w-4 h-4" />
                          SQL
                        </h4>
                        <SqlHighlighter sql={model.sql} className="text-xs max-h-48" />
                      </div>
                    )}

                    {model.flink && (
                      <div>
                        <h4 className="text-sm font-medium text-slate-400 mb-2">Flink Config</h4>
                        <div className="space-y-2">
                          {model.flink.parallelism && (
                            <div className="flex justify-between text-sm">
                              <span className="text-slate-500">Parallelism</span>
                              <span className="text-white">{model.flink.parallelism}</span>
                            </div>
                          )}
                          {model.flink.checkpoint_interval_ms && (
                            <div className="flex justify-between text-sm">
                              <span className="text-slate-500">Checkpoint Interval</span>
                              <span className="text-white">{model.flink.checkpoint_interval_ms}ms</span>
                            </div>
                          )}
                          {model.flink.state_ttl_ms && (
                            <div className="flex justify-between text-sm">
                              <span className="text-slate-500">State TTL</span>
                              <span className="text-white">
                                {(model.flink.state_ttl_ms / 3600000).toFixed(1)}h
                              </span>
                            </div>
                          )}
                        </div>
                      </div>
                    )}
                  </>
                )}

                {/* Exposure details */}
                {exposure && (
                  <>
                    <div className="flex gap-2">
                      <Badge variant="exposure">{exposure.type}</Badge>
                      {exposure.role && <Badge variant="default">{exposure.role}</Badge>}
                    </div>

                    {exposure.owner && (
                      <div className="flex items-center gap-2">
                        <User className="w-4 h-4 text-slate-500" />
                        <span className="text-sm text-slate-300">{exposure.owner}</span>
                      </div>
                    )}

                    {exposure.consumer_group && (
                      <div>
                        <h4 className="text-sm font-medium text-slate-400 mb-1">Consumer Group</h4>
                        <code className="text-sm text-streamt-400 bg-slate-800 px-2 py-1 rounded">
                          {exposure.consumer_group}
                        </code>
                      </div>
                    )}

                    {exposure.sla && (
                      <div>
                        <h4 className="text-sm font-medium text-slate-400 mb-2">SLA</h4>
                        <div className="space-y-2">
                          {exposure.sla.max_latency_ms && (
                            <div className="flex justify-between text-sm">
                              <span className="text-slate-500">Max Latency</span>
                              <span className="text-white">{exposure.sla.max_latency_ms}ms</span>
                            </div>
                          )}
                          {exposure.sla.availability_percent && (
                            <div className="flex justify-between text-sm">
                              <span className="text-slate-500">Availability</span>
                              <span className="text-white">{exposure.sla.availability_percent}%</span>
                            </div>
                          )}
                        </div>
                      </div>
                    )}
                  </>
                )}

                {/* Dependencies */}
                {dagNode && (
                  <>
                    {dagNode.upstream.length > 0 && (
                      <div>
                        <h4 className="text-sm font-medium text-slate-400 mb-2">
                          Upstream ({dagNode.upstream.length})
                        </h4>
                        <div className="space-y-1">
                          {dagNode.upstream.map((name) => (
                            <button
                              key={name}
                              onClick={() => selectNode(name)}
                              className="w-full flex items-center gap-2 p-2 rounded-lg bg-slate-800/50 hover:bg-slate-800 transition-colors text-left"
                            >
                              <GitBranch className="w-4 h-4 text-slate-500" />
                              <span className="text-sm text-slate-300">{name}</span>
                              <ChevronRight className="w-4 h-4 text-slate-500 ml-auto" />
                            </button>
                          ))}
                        </div>
                      </div>
                    )}

                    {dagNode.downstream.length > 0 && (
                      <div>
                        <h4 className="text-sm font-medium text-slate-400 mb-2">
                          Downstream ({dagNode.downstream.length})
                        </h4>
                        <div className="space-y-1">
                          {dagNode.downstream.map((name) => (
                            <button
                              key={name}
                              onClick={() => selectNode(name)}
                              className="w-full flex items-center gap-2 p-2 rounded-lg bg-slate-800/50 hover:bg-slate-800 transition-colors text-left"
                            >
                              <GitBranch className="w-4 h-4 text-slate-500 rotate-180" />
                              <span className="text-sm text-slate-300">{name}</span>
                              <ChevronRight className="w-4 h-4 text-slate-500 ml-auto" />
                            </button>
                          ))}
                        </div>
                      </div>
                    )}
                  </>
                )}
              </CardContent>
            </Card>
          </motion.div>
        )}
      </AnimatePresence>
    </div>
  )
}
