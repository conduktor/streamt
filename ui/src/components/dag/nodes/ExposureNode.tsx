import { memo } from 'react'
import { Handle, Position } from '@xyflow/react'
import {
  Radio,
  AppWindow,
  BarChart3,
  Brain,
  FileCode,
  ArrowDownToLine,
  ArrowUpFromLine,
  ArrowLeftRight,
} from 'lucide-react'
import { motion } from 'framer-motion'
import { cn } from '@/utils/cn'
import type { DAGNode, ExposureType, ExposureRole } from '@/types'

export interface ExposureNodeData extends DAGNode {
  selected?: boolean
  dimmed?: boolean
  exposureType?: ExposureType
  role?: ExposureRole
}

interface ExposureNodeProps {
  data: ExposureNodeData
}

const exposureTypeIcons: Record<string, typeof Radio> = {
  application: AppWindow,
  dashboard: BarChart3,
  ml_model: Brain,
  notebook: FileCode,
  other: Radio,
}

const roleIcons: Record<string, typeof Radio> = {
  producer: ArrowUpFromLine,
  consumer: ArrowDownToLine,
  both: ArrowLeftRight,
}

export const ExposureNode = memo(({ data }: ExposureNodeProps) => {
  const { name, description, status, selected, dimmed, exposureType = 'application', role = 'consumer' } = data

  const TypeIcon = exposureTypeIcons[exposureType] || Radio
  const RoleIcon = roleIcons[role] || ArrowDownToLine

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
          ? 'border-exposure shadow-lg shadow-exposure/20 glow-exposure'
          : 'border-exposure/40 hover:border-exposure/60',
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
        <div className="p-1.5 rounded-lg bg-exposure/20">
          <TypeIcon className="w-4 h-4 text-exposure" />
        </div>
        <div className="flex-1 min-w-0">
          <h3 className="font-medium text-white truncate">{name}</h3>
        </div>
        <span className="px-1.5 py-0.5 text-[10px] font-medium uppercase tracking-wider bg-exposure/20 text-exposure rounded">
          {exposureType}
        </span>
      </div>

      {/* Description */}
      {description && (
        <p className="text-xs text-slate-400 line-clamp-2 mb-2">{description}</p>
      )}

      {/* Footer */}
      <div className="flex items-center justify-between text-xs text-slate-500">
        <span className="flex items-center gap-1">
          <RoleIcon className="w-3 h-3" />
          <span className="capitalize">{role}</span>
        </span>
      </div>

      {/* Connection handles */}
      <Handle
        type="target"
        position={Position.Left}
        className="!w-3 !h-3 !bg-exposure !border-2 !border-slate-900"
      />
      <Handle
        type="source"
        position={Position.Right}
        className="!w-3 !h-3 !bg-exposure !border-2 !border-slate-900"
      />
    </motion.div>
  )
})

ExposureNode.displayName = 'ExposureNode'
