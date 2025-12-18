import { cn } from '@/utils/cn'
import { CheckCircle2, XCircle, AlertCircle, Clock, HelpCircle } from 'lucide-react'

type Status = 'running' | 'healthy' | 'success' | 'passed' |
              'failed' | 'error' | 'unhealthy' |
              'warning' | 'degraded' |
              'pending' | 'stopped' | 'unknown'

interface StatusIndicatorProps {
  status: Status
  showLabel?: boolean
  size?: 'sm' | 'md' | 'lg'
  className?: string
}

const statusConfig: Record<Status, { color: string; bg: string; icon: typeof CheckCircle2; label: string }> = {
  running: { color: 'text-emerald-400', bg: 'bg-emerald-500', icon: CheckCircle2, label: 'Running' },
  healthy: { color: 'text-emerald-400', bg: 'bg-emerald-500', icon: CheckCircle2, label: 'Healthy' },
  success: { color: 'text-emerald-400', bg: 'bg-emerald-500', icon: CheckCircle2, label: 'Success' },
  passed: { color: 'text-emerald-400', bg: 'bg-emerald-500', icon: CheckCircle2, label: 'Passed' },
  failed: { color: 'text-red-400', bg: 'bg-red-500', icon: XCircle, label: 'Failed' },
  error: { color: 'text-red-400', bg: 'bg-red-500', icon: XCircle, label: 'Error' },
  unhealthy: { color: 'text-red-400', bg: 'bg-red-500', icon: XCircle, label: 'Unhealthy' },
  warning: { color: 'text-yellow-400', bg: 'bg-yellow-500', icon: AlertCircle, label: 'Warning' },
  degraded: { color: 'text-yellow-400', bg: 'bg-yellow-500', icon: AlertCircle, label: 'Degraded' },
  pending: { color: 'text-slate-400', bg: 'bg-slate-500', icon: Clock, label: 'Pending' },
  stopped: { color: 'text-slate-400', bg: 'bg-slate-500', icon: Clock, label: 'Stopped' },
  unknown: { color: 'text-slate-400', bg: 'bg-slate-500', icon: HelpCircle, label: 'Unknown' },
}

const sizeConfig = {
  sm: { dot: 'w-2 h-2', icon: 'w-3 h-3', text: 'text-xs' },
  md: { dot: 'w-2.5 h-2.5', icon: 'w-4 h-4', text: 'text-sm' },
  lg: { dot: 'w-3 h-3', icon: 'w-5 h-5', text: 'text-base' },
}

export function StatusIndicator({ status, showLabel = false, size = 'md', className }: StatusIndicatorProps) {
  const config = statusConfig[status] || statusConfig.unknown
  const sizes = sizeConfig[size]
  const Icon = config.icon

  if (showLabel) {
    return (
      <span className={cn('inline-flex items-center gap-1.5', className)}>
        <Icon className={cn(sizes.icon, config.color)} />
        <span className={cn(sizes.text, config.color)}>{config.label}</span>
      </span>
    )
  }

  return (
    <span
      className={cn(
        'rounded-full',
        sizes.dot,
        config.bg,
        status === 'running' && 'animate-pulse',
        className
      )}
    />
  )
}

export function StatusDot({ status, className }: { status: Status; className?: string }) {
  const config = statusConfig[status] || statusConfig.unknown

  return (
    <span
      className={cn(
        'w-2 h-2 rounded-full',
        config.bg,
        status === 'running' && 'animate-pulse',
        className
      )}
    />
  )
}
