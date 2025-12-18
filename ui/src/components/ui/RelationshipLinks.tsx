/**
 * RelationshipLinks Component
 * Displays clickable links to related sources, models, tests, and exposures
 */

import { useNavigate } from 'react-router-dom'
import { Database, Boxes, FlaskConical, Radio, ArrowRight, ArrowLeft } from 'lucide-react'
import { cn } from '@/utils/cn'

interface RelationshipItem {
  type: 'source' | 'model' | 'test' | 'exposure'
  name: string
}

interface RelationshipLinksProps {
  title: string
  icon?: 'upstream' | 'downstream' | 'related'
  items: RelationshipItem[]
  emptyMessage?: string
  className?: string
}

const typeConfig = {
  source: {
    icon: Database,
    color: 'text-source',
    bgColor: 'bg-source/20',
    hoverBg: 'hover:bg-source/30',
    route: '/sources',
  },
  model: {
    icon: Boxes,
    color: 'text-model',
    bgColor: 'bg-model/20',
    hoverBg: 'hover:bg-model/30',
    route: '/models',
  },
  test: {
    icon: FlaskConical,
    color: 'text-info',
    bgColor: 'bg-info/20',
    hoverBg: 'hover:bg-info/30',
    route: '/tests',
  },
  exposure: {
    icon: Radio,
    color: 'text-warning',
    bgColor: 'bg-warning/20',
    hoverBg: 'hover:bg-warning/30',
    route: '/exposures',
  },
}

const directionIcons = {
  upstream: ArrowLeft,
  downstream: ArrowRight,
  related: null,
}

export function RelationshipLinks({
  title,
  icon = 'related',
  items,
  emptyMessage = 'No relationships',
  className = '',
}: RelationshipLinksProps) {
  const navigate = useNavigate()
  const DirectionIcon = directionIcons[icon]

  if (items.length === 0) {
    return (
      <div className={className}>
        <h4 className="text-sm font-medium text-slate-400 mb-2 flex items-center gap-2">
          {DirectionIcon && <DirectionIcon className="w-4 h-4" />}
          {title}
        </h4>
        <p className="text-sm text-slate-500 italic">{emptyMessage}</p>
      </div>
    )
  }

  return (
    <div className={className}>
      <h4 className="text-sm font-medium text-slate-400 mb-2 flex items-center gap-2">
        {DirectionIcon && <DirectionIcon className="w-4 h-4" />}
        {title}
      </h4>
      <div className="flex flex-wrap gap-2">
        {items.map((item) => {
          const config = typeConfig[item.type]
          const Icon = config.icon
          return (
            <button
              key={`${item.type}-${item.name}`}
              onClick={() => navigate(`${config.route}/${item.name}`)}
              className={cn(
                'inline-flex items-center gap-2 px-3 py-1.5 rounded-lg text-sm transition-colors',
                config.bgColor,
                config.hoverBg,
                'border border-transparent hover:border-slate-600'
              )}
            >
              <Icon className={cn('w-4 h-4', config.color)} />
              <span className="text-slate-200">{item.name}</span>
            </button>
          )
        })}
      </div>
    </div>
  )
}
