import { useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { ChevronRight, ChevronDown, Database, Boxes, Radio, TestTube2 } from 'lucide-react'
import { cn } from '@/utils/cn'

type NodeType = 'source' | 'model' | 'exposure' | 'test'

interface TreeNode {
  name: string
  type: NodeType
  children: TreeNode[]
}

interface DependencyTreeProps {
  title: string
  rootName: string
  nodes: Map<string, { type: NodeType; upstream: string[]; downstream: string[] }>
  direction: 'upstream' | 'downstream'
  className?: string
}

const nodeConfig: Record<NodeType, { icon: typeof Database; color: string; route: string }> = {
  source: { icon: Database, color: 'text-source', route: '/sources' },
  model: { icon: Boxes, color: 'text-model', route: '/models' },
  exposure: { icon: Radio, color: 'text-exposure', route: '/exposures' },
  test: { icon: TestTube2, color: 'text-test', route: '/tests' },
}

function buildTree(
  nodeName: string,
  nodes: Map<string, { type: NodeType; upstream: string[]; downstream: string[] }>,
  direction: 'upstream' | 'downstream',
  visited = new Set<string>()
): TreeNode | null {
  if (visited.has(nodeName)) return null
  visited.add(nodeName)

  const node = nodes.get(nodeName)
  if (!node) return null

  const deps = direction === 'upstream' ? node.upstream : node.downstream
  const children: TreeNode[] = []

  for (const dep of deps) {
    const child = buildTree(dep, nodes, direction, new Set(visited))
    if (child) {
      children.push(child)
    }
  }

  return {
    name: nodeName,
    type: node.type,
    children,
  }
}

function TreeNodeItem({
  node,
  level = 0,
  isLast = false,
  parentLines = [],
}: {
  node: TreeNode
  level?: number
  isLast?: boolean
  parentLines?: boolean[]
}) {
  const navigate = useNavigate()
  const [expanded, setExpanded] = useState(level < 2) // Auto-expand first 2 levels
  const hasChildren = node.children.length > 0
  const config = nodeConfig[node.type]
  const Icon = config.icon

  const handleClick = () => {
    navigate(`${config.route}/${node.name}`)
  }

  return (
    <div className="select-none">
      <div className="flex items-center group">
        {/* Tree lines */}
        {level > 0 && (
          <div className="flex">
            {parentLines.map((showLine, i) => (
              <div key={i} className="w-4 flex justify-center">
                {showLine && <div className="w-px h-full bg-slate-700" />}
              </div>
            ))}
            <div className="w-4 flex items-center">
              <div className={cn('w-px h-1/2 bg-slate-700', isLast ? 'self-start' : 'self-stretch')} />
              <div className="w-2 h-px bg-slate-700" />
            </div>
          </div>
        )}

        {/* Expand/collapse button */}
        <button
          onClick={() => hasChildren && setExpanded(!expanded)}
          className={cn(
            'w-5 h-5 flex items-center justify-center rounded',
            hasChildren ? 'hover:bg-slate-800 cursor-pointer' : 'cursor-default'
          )}
        >
          {hasChildren ? (
            expanded ? (
              <ChevronDown className="w-3.5 h-3.5 text-slate-500" />
            ) : (
              <ChevronRight className="w-3.5 h-3.5 text-slate-500" />
            )
          ) : (
            <div className="w-1.5 h-1.5 rounded-full bg-slate-600" />
          )}
        </button>

        {/* Node content */}
        <button
          onClick={handleClick}
          className="flex items-center gap-2 px-2 py-1 rounded hover:bg-slate-800/50 transition-colors"
        >
          <Icon className={cn('w-4 h-4', config.color)} />
          <span className="text-sm text-slate-300 group-hover:text-white transition-colors">
            {node.name}
          </span>
          <span className="text-xs text-slate-600">({node.type})</span>
        </button>
      </div>

      {/* Children */}
      {expanded && hasChildren && (
        <div>
          {node.children.map((child, i) => (
            <TreeNodeItem
              key={child.name}
              node={child}
              level={level + 1}
              isLast={i === node.children.length - 1}
              parentLines={[...parentLines, !isLast]}
            />
          ))}
        </div>
      )}
    </div>
  )
}

export function DependencyTree({
  title,
  rootName,
  nodes,
  direction,
  className = '',
}: DependencyTreeProps) {
  const rootNode = nodes.get(rootName)
  if (!rootNode) return null

  const deps = direction === 'upstream' ? rootNode.upstream : rootNode.downstream
  if (deps.length === 0) return null

  // Build tree starting from each direct dependency
  const trees: TreeNode[] = []
  for (const dep of deps) {
    const tree = buildTree(dep, nodes, direction, new Set([rootName]))
    if (tree) {
      trees.push(tree)
    }
  }

  if (trees.length === 0) return null

  return (
    <div className={className}>
      <h4 className="text-sm font-medium text-slate-400 mb-3">{title}</h4>
      <div className="p-3 rounded-lg bg-slate-800/30 border border-slate-800">
        {trees.map((tree, i) => (
          <TreeNodeItem
            key={tree.name}
            node={tree}
            level={0}
            isLast={i === trees.length - 1}
          />
        ))}
      </div>
    </div>
  )
}
