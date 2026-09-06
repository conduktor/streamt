import { ReactNode } from 'react'
import { motion } from 'framer-motion'
import { TrendingUp, TrendingDown, Minus } from 'lucide-react'
import { cn } from '@/utils/cn'

interface StatCardProps {
  title: string
  value: string | number
  icon: ReactNode
  trend?: {
    value: number
    label: string
  }
  description?: string
  variant?: 'default' | 'success' | 'warning' | 'error'
  className?: string
}

const variantStyles = {
  default: 'from-slate-800/50 to-slate-900/50 border-slate-800',
  success: 'from-emerald-900/20 to-slate-900/50 border-emerald-800/50',
  warning: 'from-yellow-900/20 to-slate-900/50 border-yellow-800/50',
  error: 'from-red-900/20 to-slate-900/50 border-red-800/50',
}

const iconVariantStyles = {
  default: 'bg-slate-800 text-slate-400',
  success: 'bg-emerald-900/50 text-emerald-400',
  warning: 'bg-yellow-900/50 text-yellow-400',
  error: 'bg-red-900/50 text-red-400',
}

export function StatCard({
  title,
  value,
  icon,
  trend,
  description,
  variant = 'default',
  className,
}: StatCardProps) {
  return (
    <motion.div
      initial={{ opacity: 0, y: 20 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.3 }}
      className={cn(
        'relative rounded-xl border bg-gradient-to-br p-6',
        variantStyles[variant],
        className
      )}
    >
      <div className="flex items-start justify-between">
        <div>
          <p className="text-sm font-medium text-slate-400">{title}</p>
          <p className="mt-2 text-3xl font-bold text-white">{value}</p>
          {trend && (
            <div className="mt-2 flex items-center gap-1">
              {trend.value > 0 ? (
                <TrendingUp className="w-4 h-4 text-emerald-400" />
              ) : trend.value < 0 ? (
                <TrendingDown className="w-4 h-4 text-red-400" />
              ) : (
                <Minus className="w-4 h-4 text-slate-400" />
              )}
              <span
                className={cn(
                  'text-sm font-medium',
                  trend.value > 0 ? 'text-emerald-400' : trend.value < 0 ? 'text-red-400' : 'text-slate-400'
                )}
              >
                {trend.value > 0 ? '+' : ''}{trend.value}%
              </span>
              <span className="text-sm text-slate-500">{trend.label}</span>
            </div>
          )}
          {description && (
            <p className="mt-2 text-sm text-slate-500">{description}</p>
          )}
        </div>
        <div className={cn('p-3 rounded-lg', iconVariantStyles[variant])}>
          {icon}
        </div>
      </div>
    </motion.div>
  )
}
