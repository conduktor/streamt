import { useCallback, useEffect, useMemo, useRef } from 'react'
import {
  ReactFlow,
  Controls,
  MiniMap,
  Background,
  useNodesState,
  useEdgesState,
  useReactFlow,
  ReactFlowProvider,
  BackgroundVariant,
  ConnectionLineType,
  MarkerType,
  Node,
  Edge,
  Panel,
} from '@xyflow/react'
import dagre from 'dagre'
import '@xyflow/react/dist/style.css'

import { SourceNode } from './nodes/SourceNode'
import { ModelNode } from './nodes/ModelNode'
import { ExposureNode } from './nodes/ExposureNode'
import { useProjectStore } from '@/stores/projectStore'
import type { DAG, DAGNode as DAGNodeType } from '@/types'

// Node types mapping - use type assertion for ReactFlow compatibility
const nodeTypes = {
  source: SourceNode,
  model: ModelNode,
  exposure: ExposureNode,
  test: ModelNode, // Reuse model node for tests
} as const

// Layout configuration
const dagreGraph = new dagre.graphlib.Graph()
dagreGraph.setDefaultEdgeLabel(() => ({}))

const nodeWidth = 280
const nodeHeight = 100

// Apply dagre layout
function getLayoutedElements(dag: DAG) {
  dagreGraph.setGraph({ rankdir: 'LR', ranksep: 100, nodesep: 50 })

  dag.nodes.forEach((node) => {
    dagreGraph.setNode(node.name, { width: nodeWidth, height: nodeHeight })
  })

  dag.edges.forEach((edge) => {
    dagreGraph.setEdge(edge.from, edge.to)
  })

  dagre.layout(dagreGraph)

  const nodes: Node[] = dag.nodes.map((node) => {
    const nodeWithPosition = dagreGraph.node(node.name)
    return {
      id: node.name,
      type: node.type,
      position: {
        x: nodeWithPosition.x - nodeWidth / 2,
        y: nodeWithPosition.y - nodeHeight / 2,
      },
      data: {
        ...node,
        label: node.name,
      },
    }
  })

  const edges: Edge[] = dag.edges.map((edge, index) => ({
    id: `${edge.from}-${edge.to}-${index}`,
    source: edge.from,
    target: edge.to,
    type: 'smoothstep',
    animated: true,
    style: {
      stroke: '#64748b',
      strokeWidth: 2,
    },
    markerEnd: {
      type: MarkerType.ArrowClosed,
      color: '#64748b',
    },
  }))

  return { nodes, edges }
}

// Edge color based on source node type
function getEdgeStyle(sourceNode: DAGNodeType | undefined) {
  if (!sourceNode) return { stroke: '#64748b' }

  const colors: Record<string, string> = {
    source: '#10b981',
    model: '#6366f1',
    exposure: '#ec4899',
    test: '#f59e0b',
  }

  return { stroke: colors[sourceNode.type] || '#64748b' }
}

interface DAGVisualizationProps {
  dag: DAG | null
  onNodeClick?: (nodeId: string) => void
  focusNode?: string | null
  onFocusComplete?: () => void
  className?: string
}

function DAGVisualizationInner({
  dag,
  onNodeClick,
  focusNode,
  onFocusComplete,
  className,
}: DAGVisualizationProps) {
  const { selectedNode, selectNode } = useProjectStore()
  const { setCenter, getNode } = useReactFlow()
  const hasFocused = useRef(false)

  // Get layouted elements
  const { nodes: layoutedNodes, edges: layoutedEdges } = useMemo(() => {
    if (!dag) return { nodes: [], edges: [] }
    return getLayoutedElements(dag)
  }, [dag])

  const [nodes, setNodes, onNodesChange] = useNodesState(layoutedNodes)
  const [edges, setEdges, onEdgesChange] = useEdgesState(layoutedEdges)

  // Update nodes when dag changes
  useEffect(() => {
    if (dag) {
      const { nodes: newNodes, edges: newEdges } = getLayoutedElements(dag)
      setNodes(newNodes)
      setEdges(newEdges)
    }
  }, [dag, setNodes, setEdges])

  // Center on focus node when it changes
  useEffect(() => {
    if (focusNode && !hasFocused.current) {
      // Small delay to ensure nodes are rendered
      const timer = setTimeout(() => {
        const node = getNode(focusNode)
        if (node) {
          const x = node.position.x + (nodeWidth / 2)
          const y = node.position.y + (nodeHeight / 2)
          setCenter(x, y, { zoom: 1.2, duration: 500 })
          hasFocused.current = true
          onFocusComplete?.()
        }
      }, 100)
      return () => clearTimeout(timer)
    }
    if (!focusNode) {
      hasFocused.current = false
    }
  }, [focusNode, getNode, setCenter, onFocusComplete])

  // Update edge styles based on source node
  useEffect(() => {
    if (dag) {
      const nodeMap = new Map(dag.nodes.map((n) => [n.name, n]))
      setEdges((eds) =>
        eds.map((edge) => {
          const sourceNode = nodeMap.get(edge.source)
          const style = getEdgeStyle(sourceNode)
          return {
            ...edge,
            style: {
              ...edge.style,
              ...style,
              strokeWidth: 2,
            },
            markerEnd: {
              type: MarkerType.ArrowClosed,
              color: style.stroke,
            },
          }
        })
      )
    }
  }, [dag, setEdges])

  // Highlight selected node and its connections
  useEffect(() => {
    if (!dag) return

    const nodeMap = new Map(dag.nodes.map((n) => [n.name, n]))

    setNodes((nds) =>
      nds.map((node) => {
        const isSelected = node.id === selectedNode
        const isConnected = selectedNode
          ? nodeMap.get(selectedNode)?.upstream.includes(node.id) ||
            nodeMap.get(selectedNode)?.downstream.includes(node.id)
          : false

        return {
          ...node,
          data: {
            ...node.data,
            selected: isSelected,
            dimmed: selectedNode ? !isSelected && !isConnected : false,
          },
        }
      })
    )

    setEdges((eds) =>
      eds.map((edge) => {
        const isConnected = selectedNode
          ? edge.source === selectedNode || edge.target === selectedNode
          : true

        return {
          ...edge,
          style: {
            ...edge.style,
            opacity: selectedNode ? (isConnected ? 1 : 0.2) : 1,
          },
          animated: isConnected,
        }
      })
    )
  }, [selectedNode, dag, setNodes, setEdges])

  // Handle node click
  const handleNodeClick = useCallback(
    (_: React.MouseEvent, node: Node) => {
      selectNode(node.id === selectedNode ? null : node.id)
      onNodeClick?.(node.id)
    },
    [selectNode, selectedNode, onNodeClick]
  )

  // Handle pane click (deselect)
  const handlePaneClick = useCallback(() => {
    selectNode(null)
  }, [selectNode])

  if (!dag || dag.nodes.length === 0) {
    return (
      <div className={`flex items-center justify-center h-full ${className}`}>
        <div className="text-center">
          <div className="w-16 h-16 mx-auto mb-4 rounded-full bg-slate-800 flex items-center justify-center">
            <svg
              className="w-8 h-8 text-slate-600"
              fill="none"
              viewBox="0 0 24 24"
              stroke="currentColor"
            >
              <path
                strokeLinecap="round"
                strokeLinejoin="round"
                strokeWidth={2}
                d="M9 5H7a2 2 0 00-2 2v12a2 2 0 002 2h10a2 2 0 002-2V7a2 2 0 00-2-2h-2M9 5a2 2 0 002 2h2a2 2 0 002-2M9 5a2 2 0 012-2h2a2 2 0 012 2"
              />
            </svg>
          </div>
          <h3 className="text-lg font-medium text-slate-300">No Pipeline Data</h3>
          <p className="text-sm text-slate-500 mt-1">
            Load a project to see the DAG visualization
          </p>
        </div>
      </div>
    )
  }

  return (
    <div className={`h-full ${className}`}>
      <ReactFlow
        nodes={nodes}
        edges={edges}
        onNodesChange={onNodesChange}
        onEdgesChange={onEdgesChange}
        onNodeClick={handleNodeClick}
        onPaneClick={handlePaneClick}
        nodeTypes={nodeTypes}
        connectionLineType={ConnectionLineType.SmoothStep}
        fitView
        fitViewOptions={{ padding: 0.2 }}
        minZoom={0.1}
        maxZoom={2}
        defaultEdgeOptions={{
          type: 'smoothstep',
          animated: true,
        }}
        proOptions={{ hideAttribution: true }}
      >
        <Background
          variant={BackgroundVariant.Dots}
          gap={20}
          size={1}
          color="#1e293b"
        />
        <Controls
          className="bg-slate-800 border-slate-700 rounded-lg"
          showInteractive={false}
        />
        <MiniMap
          className="bg-slate-900 rounded-lg border border-slate-800"
          nodeColor={(node) => {
            const colors: Record<string, string> = {
              source: '#10b981',
              model: '#6366f1',
              exposure: '#ec4899',
              test: '#f59e0b',
            }
            return colors[node.type || 'model'] || '#64748b'
          }}
          maskColor="rgba(0, 0, 0, 0.8)"
        />

        {/* Legend */}
        <Panel position="bottom-left" className="bg-slate-900/90 backdrop-blur-sm border border-slate-800 rounded-lg p-3">
          <div className="flex items-center gap-4 text-xs">
            <div className="flex items-center gap-1.5">
              <div className="w-3 h-3 rounded bg-source" />
              <span className="text-slate-400">Source</span>
            </div>
            <div className="flex items-center gap-1.5">
              <div className="w-3 h-3 rounded bg-model" />
              <span className="text-slate-400">Model</span>
            </div>
            <div className="flex items-center gap-1.5">
              <div className="w-3 h-3 rounded bg-exposure" />
              <span className="text-slate-400">Exposure</span>
            </div>
          </div>
        </Panel>
      </ReactFlow>
    </div>
  )
}

// Wrapper component that provides ReactFlow context
export function DAGVisualization(props: DAGVisualizationProps) {
  return (
    <ReactFlowProvider>
      <DAGVisualizationInner {...props} />
    </ReactFlowProvider>
  )
}
