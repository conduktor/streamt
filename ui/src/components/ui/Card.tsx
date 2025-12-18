import { ReactNode } from 'react'
import { motion } from 'framer-motion'
import { cn } from '@/utils/cn'

interface CardProps {
  children: ReactNode
  className?: string
  hover?: boolean
  onClick?: () => void
  gradient?: boolean
}

export function Card({ children, className, hover = false, onClick, gradient = false }: CardProps) {
  const Component = onClick ? motion.button : motion.div

  return (
    <Component
      onClick={onClick}
      whileHover={hover ? { scale: 1.01, y: -2 } : undefined}
      transition={{ duration: 0.2 }}
      className={cn(
        'rounded-xl border bg-slate-900/50 backdrop-blur-sm',
        gradient
          ? 'border-transparent bg-gradient-to-br from-slate-800/50 to-slate-900/50'
          : 'border-slate-800',
        hover && 'cursor-pointer hover:border-slate-700 hover:shadow-lg hover:shadow-slate-900/50',
        onClick && 'text-left w-full',
        className
      )}
    >
      {children}
    </Component>
  )
}

interface CardHeaderProps {
  children: ReactNode
  className?: string
}

export function CardHeader({ children, className }: CardHeaderProps) {
  return (
    <div className={cn('px-6 py-4 border-b border-slate-800', className)}>
      {children}
    </div>
  )
}

interface CardContentProps {
  children: ReactNode
  className?: string
}

export function CardContent({ children, className }: CardContentProps) {
  return <div className={cn('p-6', className)}>{children}</div>
}

interface CardTitleProps {
  children: ReactNode
  className?: string
}

export function CardTitle({ children, className }: CardTitleProps) {
  return (
    <h3 className={cn('text-lg font-semibold text-white', className)}>
      {children}
    </h3>
  )
}

interface CardDescriptionProps {
  children: ReactNode
  className?: string
}

export function CardDescription({ children, className }: CardDescriptionProps) {
  return (
    <p className={cn('text-sm text-slate-400 mt-1', className)}>
      {children}
    </p>
  )
}
