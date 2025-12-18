import { useState, useEffect, useMemo } from 'react'
import { useNavigate } from 'react-router-dom'
import { motion, AnimatePresence } from 'framer-motion'
import {
  Search,
  Database,
  Boxes,
  TestTube2,
  Radio,
  LayoutDashboard,
  GitBranch,
  Rocket,
  Code2,
  Settings,
  ArrowRight,
} from 'lucide-react'
import { useProjectStore } from '@/stores/projectStore'
import { cn } from '@/utils/cn'

interface CommandItem {
  id: string
  name: string
  description?: string
  icon: typeof Search
  category: 'navigation' | 'source' | 'model' | 'test' | 'exposure' | 'action'
  action: () => void
}

export function CommandPalette() {
  const [isOpen, setIsOpen] = useState(false)
  const [search, setSearch] = useState('')
  const [selectedIndex, setSelectedIndex] = useState(0)
  const navigate = useNavigate()
  const { project } = useProjectStore()

  // Build command list
  const commands = useMemo<CommandItem[]>(() => {
    const items: CommandItem[] = [
      // Navigation
      { id: 'nav-dashboard', name: 'Dashboard', icon: LayoutDashboard, category: 'navigation', action: () => navigate('/dashboard') },
      { id: 'nav-dag', name: 'Pipeline DAG', icon: GitBranch, category: 'navigation', action: () => navigate('/dag') },
      { id: 'nav-sources', name: 'Sources', icon: Database, category: 'navigation', action: () => navigate('/sources') },
      { id: 'nav-models', name: 'Models', icon: Boxes, category: 'navigation', action: () => navigate('/models') },
      { id: 'nav-tests', name: 'Tests', icon: TestTube2, category: 'navigation', action: () => navigate('/tests') },
      { id: 'nav-exposures', name: 'Exposures', icon: Radio, category: 'navigation', action: () => navigate('/exposures') },
      { id: 'nav-deploy', name: 'Deploy', icon: Rocket, category: 'navigation', action: () => navigate('/deploy') },
      { id: 'nav-editor', name: 'YAML Editor', icon: Code2, category: 'navigation', action: () => navigate('/editor') },
      { id: 'nav-settings', name: 'Settings', icon: Settings, category: 'navigation', action: () => navigate('/settings') },
    ]

    // Add sources
    if (project?.sources) {
      project.sources.forEach((source) => {
        items.push({
          id: `source-${source.name}`,
          name: source.name,
          description: source.description || `Source: ${source.topic}`,
          icon: Database,
          category: 'source',
          action: () => navigate(`/sources/${source.name}`),
        })
      })
    }

    // Add models
    if (project?.models) {
      project.models.forEach((model) => {
        items.push({
          id: `model-${model.name}`,
          name: model.name,
          description: model.description || `${model.materialized} model`,
          icon: Boxes,
          category: 'model',
          action: () => navigate(`/models/${model.name}`),
        })
      })
    }

    // Add tests
    if (project?.tests) {
      project.tests.forEach((test) => {
        items.push({
          id: `test-${test.name}`,
          name: test.name,
          description: test.description || `${test.type} test for ${test.model}`,
          icon: TestTube2,
          category: 'test',
          action: () => navigate(`/tests/${test.name}`),
        })
      })
    }

    // Add exposures
    if (project?.exposures) {
      project.exposures.forEach((exposure) => {
        items.push({
          id: `exposure-${exposure.name}`,
          name: exposure.name,
          description: exposure.description || `${exposure.type} exposure`,
          icon: Radio,
          category: 'exposure',
          action: () => navigate(`/exposures/${exposure.name}`),
        })
      })
    }

    return items
  }, [project, navigate])

  // Filter commands based on search
  const filteredCommands = useMemo(() => {
    if (!search) return commands
    const lowerSearch = search.toLowerCase()
    return commands.filter(
      (cmd) =>
        cmd.name.toLowerCase().includes(lowerSearch) ||
        cmd.description?.toLowerCase().includes(lowerSearch)
    )
  }, [commands, search])

  // Group commands by category
  const groupedCommands = useMemo(() => {
    const groups: Record<string, CommandItem[]> = {}
    filteredCommands.forEach((cmd) => {
      if (!groups[cmd.category]) groups[cmd.category] = []
      groups[cmd.category].push(cmd)
    })
    return groups
  }, [filteredCommands])

  // Keyboard shortcuts
  useEffect(() => {
    const handleKeyDown = (e: KeyboardEvent) => {
      if ((e.metaKey || e.ctrlKey) && e.key === 'k') {
        e.preventDefault()
        setIsOpen((prev) => !prev)
      }
      if (e.key === 'Escape') {
        setIsOpen(false)
      }
    }
    window.addEventListener('keydown', handleKeyDown)
    return () => window.removeEventListener('keydown', handleKeyDown)
  }, [])

  // Handle navigation with arrow keys
  useEffect(() => {
    if (!isOpen) return

    const handleKeyDown = (e: KeyboardEvent) => {
      if (e.key === 'ArrowDown') {
        e.preventDefault()
        setSelectedIndex((prev) => Math.min(prev + 1, filteredCommands.length - 1))
      } else if (e.key === 'ArrowUp') {
        e.preventDefault()
        setSelectedIndex((prev) => Math.max(prev - 1, 0))
      } else if (e.key === 'Enter') {
        e.preventDefault()
        filteredCommands[selectedIndex]?.action()
        setIsOpen(false)
        setSearch('')
      }
    }

    window.addEventListener('keydown', handleKeyDown)
    return () => window.removeEventListener('keydown', handleKeyDown)
  }, [isOpen, selectedIndex, filteredCommands])

  // Reset selection when search changes
  useEffect(() => {
    setSelectedIndex(0)
  }, [search])

  const categoryLabels: Record<string, string> = {
    navigation: 'Navigation',
    source: 'Sources',
    model: 'Models',
    test: 'Tests',
    exposure: 'Exposures',
    action: 'Actions',
  }

  return (
    <AnimatePresence>
      {isOpen && (
        <>
          {/* Backdrop */}
          <motion.div
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            exit={{ opacity: 0 }}
            className="fixed inset-0 bg-black/50 backdrop-blur-sm z-50"
            onClick={() => setIsOpen(false)}
          />

          {/* Palette */}
          <motion.div
            initial={{ opacity: 0, scale: 0.95, y: -20 }}
            animate={{ opacity: 1, scale: 1, y: 0 }}
            exit={{ opacity: 0, scale: 0.95, y: -20 }}
            transition={{ duration: 0.15 }}
            className="fixed top-[20%] left-1/2 -translate-x-1/2 w-full max-w-2xl z-50"
          >
            <div className="bg-slate-900 border border-slate-700 rounded-xl shadow-2xl overflow-hidden">
              {/* Search input */}
              <div className="flex items-center gap-3 px-4 py-3 border-b border-slate-800">
                <Search className="w-5 h-5 text-slate-500" />
                <input
                  type="text"
                  placeholder="Search commands, sources, models..."
                  value={search}
                  onChange={(e) => setSearch(e.target.value)}
                  className="flex-1 bg-transparent text-white placeholder-slate-500 outline-none"
                  autoFocus
                />
                <kbd className="px-2 py-0.5 text-xs text-slate-500 bg-slate-800 rounded border border-slate-700">
                  ESC
                </kbd>
              </div>

              {/* Results */}
              <div className="max-h-96 overflow-y-auto">
                {filteredCommands.length === 0 ? (
                  <div className="px-4 py-8 text-center text-slate-500">
                    No results found for "{search}"
                  </div>
                ) : (
                  Object.entries(groupedCommands).map(([category, items]) => (
                    <div key={category}>
                      <div className="px-4 py-2 text-xs font-medium text-slate-500 uppercase tracking-wider bg-slate-900/50">
                        {categoryLabels[category]}
                      </div>
                      {items.map((item) => {
                        const globalIndex = filteredCommands.indexOf(item)
                        const isSelected = globalIndex === selectedIndex

                        return (
                          <button
                            key={item.id}
                            onClick={() => {
                              item.action()
                              setIsOpen(false)
                              setSearch('')
                            }}
                            className={cn(
                              'w-full flex items-center gap-3 px-4 py-3 text-left transition-colors',
                              isSelected
                                ? 'bg-streamt-600/20 text-white'
                                : 'text-slate-300 hover:bg-slate-800'
                            )}
                          >
                            <item.icon
                              className={cn(
                                'w-5 h-5 flex-shrink-0',
                                isSelected ? 'text-streamt-400' : 'text-slate-500'
                              )}
                            />
                            <div className="flex-1 min-w-0">
                              <div className="font-medium truncate">{item.name}</div>
                              {item.description && (
                                <div className="text-sm text-slate-500 truncate">
                                  {item.description}
                                </div>
                              )}
                            </div>
                            {isSelected && (
                              <ArrowRight className="w-4 h-4 text-streamt-400" />
                            )}
                          </button>
                        )
                      })}
                    </div>
                  ))
                )}
              </div>

              {/* Footer */}
              <div className="flex items-center gap-4 px-4 py-2 border-t border-slate-800 text-xs text-slate-500">
                <span className="flex items-center gap-1">
                  <kbd className="px-1.5 py-0.5 bg-slate-800 rounded border border-slate-700">↑↓</kbd>
                  navigate
                </span>
                <span className="flex items-center gap-1">
                  <kbd className="px-1.5 py-0.5 bg-slate-800 rounded border border-slate-700">↵</kbd>
                  select
                </span>
                <span className="flex items-center gap-1">
                  <kbd className="px-1.5 py-0.5 bg-slate-800 rounded border border-slate-700">esc</kbd>
                  close
                </span>
              </div>
            </div>
          </motion.div>
        </>
      )}
    </AnimatePresence>
  )
}
