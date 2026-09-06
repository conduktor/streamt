import { memo } from 'react'
import { Handle, Position } from '@xyflow/react'
import { Database, ExternalLink } from 'lucide-react'
import { motion } from 'framer-motion'
import { cn } from '@/utils/cn'
import type { DAGNode } from '@/types'

export interface SourceNodeData extends DAGNode {
  selected?: boolean
  dimmed?: boolean
}

interface SourceNodeProps {
  data: SourceNodeData
}

export const SourceNode = memo(({ data }: SourceNodeProps) => {
  const { name, description, status, selected, dimmed } = data

  return (
    <motion.div
      initial={{ opacity: 0, scale: 0.8 }}
      animate={{
        opacity: dimmed ? 0.3 : 1,
        scale: 1,
      }}
      whileHover={{ scale: 1.02 }}
      transition={{ duration: 0.2 }}
      className={cn(
        'relative px-4 py-3 rounded-xl border-2 min-w-[240px] transition-all duration-200',
        'bg-gradient-to-br from-slate-900 to-slate-900/80',
        selected
          ? 'border-source shadow-lg shadow-source/20 glow-source'
          : 'border-source/40 hover:border-source/60',
        dimmed && 'pointer-events-none'
      )}
    >
      {/* Status indicator */}
      <div
        className={cn(
          'absolute -top-1 -right-1 w-3 h-3 rounded-full border-2 border-slate-900',
          status === 'running' && 'bg-emerald-500 animate-pulse',
          status === 'failed' && 'bg-red-500',
          status === 'pending' && 'bg-yellow-500',
          status === 'stopped' && 'bg-slate-500',
          !status && 'bg-slate-600'
        )}
      />

      {/* Header */}
      <div className="flex items-center gap-2 mb-2">
        <div className="p-1.5 rounded-lg bg-source/20">
          <Database className="w-4 h-4 text-source" />
        </div>
        <div className="flex-1 min-w-0">
          <h3 className="font-medium text-white truncate">{name}</h3>
        </div>
        <span className="px-1.5 py-0.5 text-[10px] font-medium uppercase tracking-wider bg-source/20 text-source rounded">
          Source
        </span>
      </div>

      {/* Description */}
      {description && (
        <p className="text-xs text-slate-400 line-clamp-2 mb-2">{description}</p>
      )}

      {/* Footer */}
      <div className="flex items-center justify-between text-xs text-slate-500">
        <span className="flex items-center gap-1">
          <ExternalLink className="w-3 h-3" />
          External
        </span>
      </div>

      {/* Connection handles */}
      <Handle
        type="target"
        position={Position.Left}
        className="!w-3 !h-3 !bg-source !border-2 !border-slate-900"
      />
      <Handle
        type="source"
        position={Position.Right}
        className="!w-3 !h-3 !bg-source !border-2 !border-slate-900"
      />
    </motion.div>
  )
})

SourceNode.displayName = 'SourceNode'
