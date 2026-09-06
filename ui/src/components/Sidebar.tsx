import { NavLink, useLocation } from 'react-router-dom'
import { motion, AnimatePresence } from 'framer-motion'
import {
  LayoutDashboard,
  GitBranch,
  Database,
  Boxes,
  TestTube2,
  Radio,
  Rocket,
  Code2,
  Settings,
  ChevronLeft,
  ChevronRight,
  Waves,
} from 'lucide-react'
import { useProjectStore } from '@/stores/projectStore'
import { cn } from '@/utils/cn'

const navigation = [
  { name: 'Dashboard', href: '/dashboard', icon: LayoutDashboard },
  { name: 'Pipeline DAG', href: '/dag', icon: GitBranch },
  { name: 'Sources', href: '/sources', icon: Database, badge: 'sources' },
  { name: 'Models', href: '/models', icon: Boxes, badge: 'models' },
  { name: 'Tests', href: '/tests', icon: TestTube2, badge: 'tests' },
  { name: 'Exposures', href: '/exposures', icon: Radio, badge: 'exposures' },
  { name: 'Deploy', href: '/deploy', icon: Rocket },
  { name: 'Editor', href: '/editor', icon: Code2 },
]

const secondaryNavigation = [
  { name: 'Settings', href: '/settings', icon: Settings },
]

export function Sidebar() {
  const { sidebarOpen, toggleSidebar, project } = useProjectStore()
  const location = useLocation()

  const getBadgeCount = (type: string) => {
    if (!project) return 0
    switch (type) {
      case 'sources':
        return project.sources.length
      case 'models':
        return project.models.length
      case 'tests':
        return project.tests.length
      case 'exposures':
        return project.exposures.length
      default:
        return 0
    }
  }

  return (
    <motion.aside
      initial={false}
      animate={{ width: sidebarOpen ? 256 : 64 }}
      transition={{ duration: 0.3, ease: 'easeInOut' }}
      className="fixed left-0 top-0 bottom-0 z-40 flex flex-col bg-slate-900 border-r border-slate-800"
    >
      {/* Logo */}
      <div className="flex items-center h-16 px-4 border-b border-slate-800">
        <div className="flex items-center gap-3">
          <div className="w-8 h-8 rounded-lg bg-gradient-to-br from-streamt-500 to-indigo-600 flex items-center justify-center">
            <Waves className="w-5 h-5 text-white" />
          </div>
          <AnimatePresence>
            {sidebarOpen && (
              <motion.span
                initial={{ opacity: 0, width: 0 }}
                animate={{ opacity: 1, width: 'auto' }}
                exit={{ opacity: 0, width: 0 }}
                className="font-semibold text-lg text-white overflow-hidden whitespace-nowrap"
              >
                streamt
              </motion.span>
            )}
          </AnimatePresence>
        </div>
      </div>

      {/* Navigation */}
      <nav className="flex-1 px-2 py-4 space-y-1 overflow-y-auto">
        {navigation.map((item) => {
          const isActive = location.pathname.startsWith(item.href)
          const badgeCount = item.badge ? getBadgeCount(item.badge) : 0

          return (
            <NavLink
              key={item.name}
              to={item.href}
              className={cn(
                'group flex items-center gap-3 px-3 py-2.5 rounded-lg transition-all duration-200',
                isActive
                  ? 'bg-streamt-600/20 text-streamt-400'
                  : 'text-slate-400 hover:bg-slate-800 hover:text-white'
              )}
            >
              <item.icon
                className={cn(
                  'w-5 h-5 flex-shrink-0 transition-colors',
                  isActive ? 'text-streamt-400' : 'text-slate-500 group-hover:text-slate-300'
                )}
              />
              <AnimatePresence>
                {sidebarOpen && (
                  <motion.div
                    initial={{ opacity: 0, width: 0 }}
                    animate={{ opacity: 1, width: 'auto' }}
                    exit={{ opacity: 0, width: 0 }}
                    className="flex items-center justify-between flex-1 overflow-hidden"
                  >
                    <span className="whitespace-nowrap">{item.name}</span>
                    {badgeCount > 0 && (
                      <span
                        className={cn(
                          'ml-auto px-2 py-0.5 text-xs rounded-full',
                          isActive
                            ? 'bg-streamt-600/30 text-streamt-300'
                            : 'bg-slate-800 text-slate-400'
                        )}
                      >
                        {badgeCount}
                      </span>
                    )}
                  </motion.div>
                )}
              </AnimatePresence>
            </NavLink>
          )
        })}
      </nav>

      {/* Secondary navigation */}
      <div className="px-2 py-4 border-t border-slate-800">
        {secondaryNavigation.map((item) => {
          const isActive = location.pathname === item.href

          return (
            <NavLink
              key={item.name}
              to={item.href}
              className={cn(
                'group flex items-center gap-3 px-3 py-2.5 rounded-lg transition-all duration-200',
                isActive
                  ? 'bg-slate-800 text-white'
                  : 'text-slate-400 hover:bg-slate-800 hover:text-white'
              )}
            >
              <item.icon className="w-5 h-5 flex-shrink-0 text-slate-500 group-hover:text-slate-300" />
              <AnimatePresence>
                {sidebarOpen && (
                  <motion.span
                    initial={{ opacity: 0, width: 0 }}
                    animate={{ opacity: 1, width: 'auto' }}
                    exit={{ opacity: 0, width: 0 }}
                    className="whitespace-nowrap overflow-hidden"
                  >
                    {item.name}
                  </motion.span>
                )}
              </AnimatePresence>
            </NavLink>
          )
        })}

        {/* Toggle button */}
        <button
          onClick={toggleSidebar}
          className="w-full flex items-center gap-3 px-3 py-2.5 mt-2 rounded-lg text-slate-400 hover:bg-slate-800 hover:text-white transition-all duration-200"
        >
          {sidebarOpen ? (
            <>
              <ChevronLeft className="w-5 h-5 flex-shrink-0" />
              <span className="whitespace-nowrap">Collapse</span>
            </>
          ) : (
            <ChevronRight className="w-5 h-5 flex-shrink-0" />
          )}
        </button>
      </div>
    </motion.aside>
  )
}
