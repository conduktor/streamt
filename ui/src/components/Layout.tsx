import { ReactNode } from 'react'
import { Sidebar } from './Sidebar'
import { Header } from './Header'
import { CommandPalette } from './CommandPalette'
import { useProjectStore } from '@/stores/projectStore'
import { cn } from '@/utils/cn'

interface LayoutProps {
  children: ReactNode
}

export function Layout({ children }: LayoutProps) {
  const { sidebarOpen, isLoading } = useProjectStore()

  return (
    <div className="flex h-screen overflow-hidden bg-slate-950" data-testid="app-root" data-loading={isLoading}>
      {/* Sidebar */}
      <Sidebar />

      {/* Main content area */}
      <div
        className={cn(
          'flex-1 flex flex-col transition-all duration-300 ease-in-out',
          sidebarOpen ? 'ml-64' : 'ml-16'
        )}
      >
        {/* Header */}
        <Header />

        {/* Main content */}
        <main className="flex-1 overflow-auto p-6" data-testid="main-content">
          {isLoading ? (
            <div className="flex items-center justify-center h-full" data-testid="loading-spinner">
              <LoadingSpinner />
            </div>
          ) : (
            <div className="animate-fade-in" data-testid="content-loaded">{children}</div>
          )}
        </main>
      </div>

      {/* Command palette (Cmd+K) */}
      <CommandPalette />
    </div>
  )
}

function LoadingSpinner() {
  return (
    <div className="flex flex-col items-center gap-4">
      <div className="relative w-16 h-16">
        <div className="absolute inset-0 border-4 border-slate-800 rounded-full"></div>
        <div className="absolute inset-0 border-4 border-t-streamt-500 rounded-full animate-spin"></div>
      </div>
      <p className="text-slate-400 text-sm">Loading pipeline...</p>
    </div>
  )
}
