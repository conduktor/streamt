import { useState } from 'react'
import {
  Settings,
  Moon,
  Sun,
  Server,
  Database,
  Zap,
  Upload,
  Shield,
  Keyboard,
  ExternalLink,
  CheckCircle2,
  XCircle,
  RefreshCw,
} from 'lucide-react'
import { Card, CardContent, CardHeader, CardTitle, Button } from '@/components/ui'
import { useProjectStore } from '@/stores/projectStore'
import { cn } from '@/utils/cn'

interface ConnectionStatus {
  name: string
  url: string
  status: 'connected' | 'disconnected' | 'checking'
}

export function SettingsPage() {
  const { darkMode, toggleDarkMode, project } = useProjectStore()
  const [connections, setConnections] = useState<ConnectionStatus[]>([
    { name: 'Kafka', url: project?.runtime.kafka.bootstrap_servers || 'localhost:9092', status: 'connected' },
    { name: 'Schema Registry', url: project?.runtime.schema_registry?.url || 'http://localhost:8081', status: 'connected' },
    { name: 'Flink', url: project?.runtime.flink?.clusters.local?.rest_url || 'http://localhost:8082', status: 'connected' },
    { name: 'Connect', url: project?.runtime.connect?.clusters.local?.rest_url || 'http://localhost:8083', status: 'connected' },
  ])

  const handleCheckConnections = async () => {
    setConnections((prev) =>
      prev.map((c) => ({ ...c, status: 'checking' as const }))
    )
    // Simulate connection check
    await new Promise((resolve) => setTimeout(resolve, 1500))
    setConnections((prev) =>
      prev.map((c) => ({ ...c, status: 'connected' as const }))
    )
  }

  const shortcuts = [
    { keys: ['⌘', 'K'], description: 'Open command palette' },
    { keys: ['⌘', 'S'], description: 'Save changes' },
    { keys: ['⌘', 'Enter'], description: 'Apply/Deploy' },
    { keys: ['⌘', '.'], description: 'Toggle sidebar' },
    { keys: ['Esc'], description: 'Close dialogs' },
  ]

  return (
    <div className="max-w-4xl mx-auto space-y-6">
      {/* Header */}
      <div>
        <h2 className="text-2xl font-bold text-white">Settings</h2>
        <p className="text-slate-400 mt-1">
          Configure your streamt environment and preferences
        </p>
      </div>

      {/* Appearance */}
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            {darkMode ? <Moon className="w-5 h-5" /> : <Sun className="w-5 h-5" />}
            Appearance
          </CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="flex items-center justify-between">
            <div>
              <p className="font-medium text-white">Dark Mode</p>
              <p className="text-sm text-slate-400">
                Toggle between light and dark themes
              </p>
            </div>
            <button
              onClick={toggleDarkMode}
              className={cn(
                'relative w-12 h-6 rounded-full transition-colors',
                darkMode ? 'bg-streamt-600' : 'bg-slate-700'
              )}
            >
              <span
                className={cn(
                  'absolute top-1 w-4 h-4 rounded-full bg-white transition-transform',
                  darkMode ? 'translate-x-7' : 'translate-x-1'
                )}
              />
            </button>
          </div>
        </CardContent>
      </Card>

      {/* Connections */}
      <Card>
        <CardHeader className="flex flex-row items-center justify-between">
          <CardTitle className="flex items-center gap-2">
            <Server className="w-5 h-5" />
            Connections
          </CardTitle>
          <Button
            variant="secondary"
            size="sm"
            onClick={handleCheckConnections}
            icon={<RefreshCw className="w-4 h-4" />}
          >
            Check All
          </Button>
        </CardHeader>
        <CardContent>
          <div className="space-y-3">
            {connections.map((conn) => {
              const icons: Record<string, typeof Database> = {
                Kafka: Database,
                'Schema Registry': Shield,
                Flink: Zap,
                Connect: Upload,
              }
              const Icon = icons[conn.name] || Server

              return (
                <div
                  key={conn.name}
                  className="flex items-center justify-between p-3 rounded-lg bg-slate-800/50"
                >
                  <div className="flex items-center gap-3">
                    <div className="p-2 rounded-lg bg-slate-700">
                      <Icon className="w-4 h-4 text-slate-400" />
                    </div>
                    <div>
                      <p className="font-medium text-white">{conn.name}</p>
                      <p className="text-sm text-slate-500 font-mono">{conn.url}</p>
                    </div>
                  </div>
                  <div className="flex items-center gap-2">
                    {conn.status === 'checking' ? (
                      <RefreshCw className="w-4 h-4 text-slate-400 animate-spin" />
                    ) : conn.status === 'connected' ? (
                      <CheckCircle2 className="w-4 h-4 text-emerald-400" />
                    ) : (
                      <XCircle className="w-4 h-4 text-red-400" />
                    )}
                    <span
                      className={cn(
                        'text-sm capitalize',
                        conn.status === 'connected' && 'text-emerald-400',
                        conn.status === 'disconnected' && 'text-red-400',
                        conn.status === 'checking' && 'text-slate-400'
                      )}
                    >
                      {conn.status}
                    </span>
                  </div>
                </div>
              )
            })}
          </div>
        </CardContent>
      </Card>

      {/* Keyboard Shortcuts */}
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Keyboard className="w-5 h-5" />
            Keyboard Shortcuts
          </CardTitle>
        </CardHeader>
        <CardContent>
          <div className="grid grid-cols-2 gap-3">
            {shortcuts.map((shortcut, i) => (
              <div
                key={i}
                className="flex items-center justify-between p-3 rounded-lg bg-slate-800/50"
              >
                <span className="text-sm text-slate-300">{shortcut.description}</span>
                <div className="flex items-center gap-1">
                  {shortcut.keys.map((key, j) => (
                    <kbd
                      key={j}
                      className="px-2 py-1 text-xs font-medium bg-slate-700 text-slate-300 rounded border border-slate-600"
                    >
                      {key}
                    </kbd>
                  ))}
                </div>
              </div>
            ))}
          </div>
        </CardContent>
      </Card>

      {/* Project Info */}
      {project && (
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <Settings className="w-5 h-5" />
              Project Information
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="space-y-4">
              <div className="grid grid-cols-2 gap-4">
                <div className="p-3 rounded-lg bg-slate-800/50">
                  <p className="text-xs text-slate-500">Project Name</p>
                  <p className="font-medium text-white">{project.project.name}</p>
                </div>
                <div className="p-3 rounded-lg bg-slate-800/50">
                  <p className="text-xs text-slate-500">Version</p>
                  <p className="font-medium text-white">{project.project.version}</p>
                </div>
              </div>
              {project.project.description && (
                <div className="p-3 rounded-lg bg-slate-800/50">
                  <p className="text-xs text-slate-500">Description</p>
                  <p className="text-sm text-slate-300">{project.project.description}</p>
                </div>
              )}
              <div className="grid grid-cols-4 gap-4 pt-2">
                <div className="text-center">
                  <p className="text-2xl font-bold text-white">{project.sources.length}</p>
                  <p className="text-xs text-slate-500">Sources</p>
                </div>
                <div className="text-center">
                  <p className="text-2xl font-bold text-white">{project.models.length}</p>
                  <p className="text-xs text-slate-500">Models</p>
                </div>
                <div className="text-center">
                  <p className="text-2xl font-bold text-white">{project.tests.length}</p>
                  <p className="text-xs text-slate-500">Tests</p>
                </div>
                <div className="text-center">
                  <p className="text-2xl font-bold text-white">{project.exposures.length}</p>
                  <p className="text-xs text-slate-500">Exposures</p>
                </div>
              </div>
            </div>
          </CardContent>
        </Card>
      )}

      {/* About */}
      <Card>
        <CardHeader>
          <CardTitle>About streamt</CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          <p className="text-sm text-slate-400">
            streamt is a declarative streaming data pipeline framework that brings dbt-like
            practices to Kafka, Flink, and Kafka Connect. Define your streaming pipelines
            using YAML and SQL, and let streamt handle the deployment.
          </p>
          <div className="flex items-center gap-4">
            <a
              href="https://github.com/conduktor/streamt"
              target="_blank"
              rel="noopener noreferrer"
              className="flex items-center gap-2 text-sm text-streamt-400 hover:text-streamt-300"
            >
              <ExternalLink className="w-4 h-4" />
              GitHub Repository
            </a>
            <a
              href="https://streamt.dev/docs"
              target="_blank"
              rel="noopener noreferrer"
              className="flex items-center gap-2 text-sm text-streamt-400 hover:text-streamt-300"
            >
              <ExternalLink className="w-4 h-4" />
              Documentation
            </a>
          </div>
          <div className="pt-4 border-t border-slate-800">
            <p className="text-xs text-slate-600">
              Version 0.1.0 · Apache 2.0 License · Made with love by the streamt team
            </p>
          </div>
        </CardContent>
      </Card>
    </div>
  )
}
