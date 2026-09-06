import { memo } from 'react'
import { Handle, Position } from '@xyflow/react'
import {
  Boxes,
  Zap,
  Cloud,
  Upload,
  TestTube2,
  GitBranch,
} from 'lucide-react'
import { motion } from 'framer-motion'
import { cn } from '@/utils/cn'
import type { DAGNode } from '@/types'

export interface ModelNodeData extends DAGNode {
  selected?: boolean
  dimmed?: boolean
}

interface ModelNodeProps {
  data: ModelNodeData
}

const materializationIcons: Record<string, typeof Boxes> = {
  topic: Boxes,
  flink: Zap,
  virtual_topic: Cloud,
  sink: Upload,
}

const materializationColors: Record<string, { bg: string; text: string; border: string; glow: string }> = {
  topic: {
    bg: 'bg-model/20',
    text: 'text-model',
    border: 'border-model',
    glow: 'shadow-model/20',
  },
  flink: {
    bg: 'bg-orange-500/20',
    text: 'text-orange-400',
    border: 'border-orange-500',
    glow: 'shadow-orange-500/20',
  },
  virtual_topic: {
    bg: 'bg-cyan-500/20',
    text: 'text-cyan-400',
    border: 'border-cyan-500',
    glow: 'shadow-cyan-500/20',
  },
  sink: {
    bg: 'bg-sink/20',
    text: 'text-sink',
    border: 'border-sink',
    glow: 'shadow-sink/20',
  },
}

const materializationLabels: Record<string, string> = {
  topic: 'Topic',
  flink: 'Flink',
  virtual_topic: 'Virtual',
  sink: 'Sink',
}

export const ModelNode = memo(({ data }: ModelNodeProps) => {
  const { name, description, materialized, status, selected, dimmed, type } = data

  // Handle test nodes
  const isTest = type === 'test'
  const mat = materialized || 'topic'
  const colors = isTest
    ? { bg: 'bg-test/20', text: 'text-test', border: 'border-test', glow: 'shadow-test/20' }
    : materializationColors[mat] || materializationColors.topic
  const Icon = isTest ? TestTube2 : (materializationIcons[mat] || Boxes)

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
          ? `${colors.border} shadow-lg ${colors.glow}`
          : `${colors.border}/40 hover:${colors.border}/60`,
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
        <div className={cn('p-1.5 rounded-lg', colors.bg)}>
          <Icon className={cn('w-4 h-4', colors.text)} />
        </div>
        <div className="flex-1 min-w-0">
          <h3 className="font-medium text-white truncate">{name}</h3>
        </div>
        <span className={cn(
          'px-1.5 py-0.5 text-[10px] font-medium uppercase tracking-wider rounded',
          colors.bg,
          colors.text
        )}>
          {isTest ? 'Test' : materializationLabels[mat] || mat}
        </span>
      </div>

      {/* Description */}
      {description && (
        <p className="text-xs text-slate-400 line-clamp-2 mb-2">{description}</p>
      )}

      {/* Footer - Materialization details */}
      {!isTest && (
        <div className="flex items-center justify-between text-xs text-slate-500">
          <span className="flex items-center gap-1">
            <GitBranch className="w-3 h-3" />
            {mat === 'flink' && 'Stateful'}
            {mat === 'topic' && 'Stateless'}
            {mat === 'virtual_topic' && 'Read-time'}
            {mat === 'sink' && 'Export'}
          </span>
        </div>
      )}

      {/* Connection handles */}
      <Handle
        type="target"
        position={Position.Left}
        className={cn(
          '!w-3 !h-3 !border-2 !border-slate-900',
          isTest ? '!bg-test' : mat === 'flink' ? '!bg-orange-400' : mat === 'virtual_topic' ? '!bg-cyan-400' : mat === 'sink' ? '!bg-sink' : '!bg-model'
        )}
      />
      <Handle
        type="source"
        position={Position.Right}
        className={cn(
          '!w-3 !h-3 !border-2 !border-slate-900',
          isTest ? '!bg-test' : mat === 'flink' ? '!bg-orange-400' : mat === 'virtual_topic' ? '!bg-cyan-400' : mat === 'sink' ? '!bg-sink' : '!bg-model'
        )}
      />
    </motion.div>
  )
})

ModelNode.displayName = 'ModelNode'
