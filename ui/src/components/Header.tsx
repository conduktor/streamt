import { useLocation } from 'react-router-dom'
import {
  Search,
  Bell,
  RefreshCw,
  CheckCircle2,
  XCircle,
  Command,
} from 'lucide-react'
import { useProjectStore } from '@/stores/projectStore'
import { cn } from '@/utils/cn'

const pageTitles: Record<string, string> = {
  '/dashboard': 'Dashboard',
  '/dag': 'Pipeline DAG',
  '/sources': 'Sources',
  '/models': 'Models',
  '/tests': 'Tests',
  '/exposures': 'Exposures',
  '/deploy': 'Deploy',
  '/editor': 'YAML Editor',
  '/settings': 'Settings',
}

export function Header() {
  const location = useLocation()
  const { project, status, validation, isLoading } = useProjectStore()

  const pageTitle = Object.entries(pageTitles).find(([path]) =>
    location.pathname.startsWith(path)
  )?.[1] || 'Dashboard'

  return (
    <header className="h-16 flex items-center justify-between px-6 border-b border-slate-800 bg-slate-900/50 backdrop-blur-xl">
      {/* Page title and project info */}
      <div className="flex items-center gap-4">
        <h1 className="text-xl font-semibold text-white">{pageTitle}</h1>
        {project && (
          <div className="flex items-center gap-2">
            <span className="text-slate-500">·</span>
            <span className="text-sm text-slate-400">{project.project.name}</span>
            <span className="px-1.5 py-0.5 text-xs bg-slate-800 text-slate-400 rounded">
              v{project.project.version}
            </span>
          </div>
        )}
      </div>

      {/* Actions */}
      <div className="flex items-center gap-3">
        {/* Search button */}
        <button className="flex items-center gap-2 px-3 py-1.5 text-sm text-slate-400 bg-slate-800 rounded-lg hover:bg-slate-700 transition-colors">
          <Search className="w-4 h-4" />
          <span>Search</span>
          <kbd className="hidden sm:flex items-center gap-0.5 px-1.5 py-0.5 text-xs bg-slate-900 rounded border border-slate-700">
            <Command className="w-3 h-3" />K
          </kbd>
        </button>

        {/* Status indicators */}
        <div className="flex items-center gap-2 px-3 py-1.5 bg-slate-800 rounded-lg">
          {/* Validation status */}
          {validation && (
            <div className="flex items-center gap-1.5">
              {validation.is_valid ? (
                <CheckCircle2 className="w-4 h-4 text-emerald-500" />
              ) : (
                <XCircle className="w-4 h-4 text-red-500" />
              )}
              <span className="text-xs text-slate-400">
                {validation.is_valid ? 'Valid' : `${validation.messages.filter(m => m.level === 'error').length} errors`}
              </span>
            </div>
          )}

          {/* Separator */}
          <div className="w-px h-4 bg-slate-700" />

          {/* Health status */}
          {status && (
            <div className="flex items-center gap-1.5">
              <div
                className={cn(
                  'w-2 h-2 rounded-full',
                  status.health === 'healthy'
                    ? 'bg-emerald-500'
                    : status.health === 'degraded'
                    ? 'bg-yellow-500'
                    : 'bg-red-500'
                )}
              />
              <span className="text-xs text-slate-400 capitalize">{status.health}</span>
            </div>
          )}
        </div>

        {/* Refresh button */}
        <button
          className={cn(
            'p-2 text-slate-400 hover:text-white hover:bg-slate-800 rounded-lg transition-all',
            isLoading && 'animate-spin'
          )}
          disabled={isLoading}
        >
          <RefreshCw className="w-5 h-5" />
        </button>

        {/* Notifications */}
        <button className="relative p-2 text-slate-400 hover:text-white hover:bg-slate-800 rounded-lg transition-all">
          <Bell className="w-5 h-5" />
          {validation && validation.messages.length > 0 && (
            <span className="absolute top-1 right-1 w-2 h-2 bg-yellow-500 rounded-full" />
          )}
        </button>
      </div>
    </header>
  )
}
