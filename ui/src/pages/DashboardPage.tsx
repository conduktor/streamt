import { motion } from 'framer-motion'
import { useNavigate } from 'react-router-dom'
import {
  Database,
  Boxes,
  TestTube2,
  Radio,
  Activity,
  CheckCircle2,
  AlertTriangle,
  XCircle,
  GitBranch,
  Zap,
  ArrowRight,
  TrendingUp,
} from 'lucide-react'
import { Card, CardContent, CardHeader, CardTitle, StatCard, StatusIndicator } from '@/components/ui'
import { DAGVisualization } from '@/components/dag'
import { useProjectStore } from '@/stores/projectStore'
import { cn } from '@/utils/cn'

export function DashboardPage() {
  const navigate = useNavigate()
  const { project, dag, status, validation } = useProjectStore()

  // Calculate stats
  const sourceCount = project?.sources.length ?? 0
  const modelCount = project?.models.length ?? 0
  const testCount = project?.tests.length ?? 0
  const exposureCount = project?.exposures.length ?? 0

  const runningJobs = status?.flink_jobs.filter((j) => j.status === 'RUNNING').length ?? 0
  const failedJobs = status?.flink_jobs.filter((j) => j.status === 'FAILED').length ?? 0

  const validationErrors = validation?.messages.filter((m) => m.level === 'error').length ?? 0
  const validationWarnings = validation?.messages.filter((m) => m.level === 'warning').length ?? 0

  return (
    <div className="space-y-6">
      {/* Page header */}
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-2xl font-bold text-white">
            Welcome to {project?.project.name ?? 'streamt'}
          </h2>
          <p className="text-slate-400 mt-1">
            {project?.project.description ?? 'Monitor your streaming pipeline at a glance'}
          </p>
        </div>
        <button
          onClick={() => navigate('/dag')}
          className="btn-primary flex items-center gap-2"
        >
          View Pipeline DAG
          <ArrowRight className="w-4 h-4" />
        </button>
      </div>

      {/* Stats grid */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
        <StatCard
          title="Sources"
          value={sourceCount}
          icon={<Database className="w-6 h-6" />}
          description="External data sources"
          variant="default"
        />
        <StatCard
          title="Models"
          value={modelCount}
          icon={<Boxes className="w-6 h-6" />}
          description="Data transformations"
          variant="default"
        />
        <StatCard
          title="Tests"
          value={testCount}
          icon={<TestTube2 className="w-6 h-6" />}
          description="Quality assertions"
          variant={testCount > 0 ? 'success' : 'warning'}
        />
        <StatCard
          title="Exposures"
          value={exposureCount}
          icon={<Radio className="w-6 h-6" />}
          description="Downstream consumers"
          variant="default"
        />
      </div>

      {/* Main content grid */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        {/* Pipeline health */}
        <Card className="lg:col-span-2">
          <CardHeader className="flex flex-row items-center justify-between">
            <CardTitle className="flex items-center gap-2">
              <Activity className="w-5 h-5 text-streamt-400" />
              Pipeline Health
            </CardTitle>
            {status && (
              <StatusIndicator status={status.health as 'healthy' | 'degraded' | 'unhealthy'} showLabel />
            )}
          </CardHeader>
          <CardContent>
            <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
              {/* Topics */}
              <div className="p-4 rounded-lg bg-slate-800/50">
                <div className="flex items-center justify-between mb-2">
                  <span className="text-sm text-slate-400">Topics</span>
                  <CheckCircle2 className="w-4 h-4 text-emerald-400" />
                </div>
                <p className="text-2xl font-bold text-white">
                  {status?.topics.filter((t) => t.exists).length ?? 0}
                </p>
                <p className="text-xs text-slate-500 mt-1">Active topics</p>
              </div>

              {/* Flink Jobs */}
              <div className="p-4 rounded-lg bg-slate-800/50">
                <div className="flex items-center justify-between mb-2">
                  <span className="text-sm text-slate-400">Flink Jobs</span>
                  {failedJobs > 0 ? (
                    <XCircle className="w-4 h-4 text-red-400" />
                  ) : (
                    <Zap className="w-4 h-4 text-orange-400" />
                  )}
                </div>
                <p className="text-2xl font-bold text-white">{runningJobs}</p>
                <p className="text-xs text-slate-500 mt-1">
                  {failedJobs > 0 ? `${failedJobs} failed` : 'Running'}
                </p>
              </div>

              {/* Connectors */}
              <div className="p-4 rounded-lg bg-slate-800/50">
                <div className="flex items-center justify-between mb-2">
                  <span className="text-sm text-slate-400">Connectors</span>
                  <CheckCircle2 className="w-4 h-4 text-emerald-400" />
                </div>
                <p className="text-2xl font-bold text-white">
                  {status?.connectors.filter((c) => c.state === 'RUNNING').length ?? 0}
                </p>
                <p className="text-xs text-slate-500 mt-1">Active sinks</p>
              </div>

              {/* Health Score */}
              <div className="p-4 rounded-lg bg-slate-800/50">
                <div className="flex items-center justify-between mb-2">
                  <span className="text-sm text-slate-400">Health Score</span>
                  <TrendingUp className="w-4 h-4 text-emerald-400" />
                </div>
                <p className="text-2xl font-bold text-white">
                  {status?.health_score ?? 0}%
                </p>
                <p className="text-xs text-slate-500 mt-1">Overall health</p>
              </div>
            </div>
          </CardContent>
        </Card>

        {/* Validation status */}
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <CheckCircle2 className="w-5 h-5 text-streamt-400" />
              Validation
            </CardTitle>
          </CardHeader>
          <CardContent className="space-y-4">
            {validation?.is_valid ? (
              <div className="flex items-center gap-3 p-4 rounded-lg bg-emerald-900/20 border border-emerald-800/50">
                <CheckCircle2 className="w-8 h-8 text-emerald-400" />
                <div>
                  <p className="font-medium text-emerald-400">Pipeline Valid</p>
                  <p className="text-sm text-slate-400">
                    {validationWarnings > 0
                      ? `${validationWarnings} warning${validationWarnings > 1 ? 's' : ''}`
                      : 'No issues found'}
                  </p>
                </div>
              </div>
            ) : (
              <div className="flex items-center gap-3 p-4 rounded-lg bg-red-900/20 border border-red-800/50">
                <XCircle className="w-8 h-8 text-red-400" />
                <div>
                  <p className="font-medium text-red-400">Validation Failed</p>
                  <p className="text-sm text-slate-400">
                    {validationErrors} error{validationErrors > 1 ? 's' : ''}
                  </p>
                </div>
              </div>
            )}

            {/* Recent issues */}
            {validation?.messages.slice(0, 3).map((msg, i) => (
              <div
                key={i}
                className={cn(
                  'p-3 rounded-lg border',
                  msg.level === 'error'
                    ? 'bg-red-900/10 border-red-900/30'
                    : 'bg-yellow-900/10 border-yellow-900/30'
                )}
              >
                <div className="flex items-start gap-2">
                  {msg.level === 'error' ? (
                    <XCircle className="w-4 h-4 text-red-400 mt-0.5" />
                  ) : (
                    <AlertTriangle className="w-4 h-4 text-yellow-400 mt-0.5" />
                  )}
                  <div className="flex-1 min-w-0">
                    <p className="text-sm text-slate-300 line-clamp-2">{msg.message}</p>
                    {msg.location && (
                      <p className="text-xs text-slate-500 mt-1">{msg.location}</p>
                    )}
                  </div>
                </div>
              </div>
            ))}
          </CardContent>
        </Card>
      </div>

      {/* Mini DAG preview */}
      <Card>
        <CardHeader className="flex flex-row items-center justify-between">
          <CardTitle className="flex items-center gap-2">
            <GitBranch className="w-5 h-5 text-streamt-400" />
            Pipeline Overview
          </CardTitle>
          <button
            onClick={() => navigate('/dag')}
            className="text-sm text-streamt-400 hover:text-streamt-300 flex items-center gap-1"
          >
            Full View
            <ArrowRight className="w-4 h-4" />
          </button>
        </CardHeader>
        <CardContent className="p-0">
          <div className="h-[400px]">
            <DAGVisualization dag={dag} />
          </div>
        </CardContent>
      </Card>

      {/* Quick actions */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
        <motion.button
          whileHover={{ scale: 1.02 }}
          whileTap={{ scale: 0.98 }}
          onClick={() => navigate('/deploy')}
          className="p-6 rounded-xl border border-slate-800 bg-gradient-to-br from-streamt-900/20 to-slate-900 hover:border-streamt-700/50 transition-all text-left group"
        >
          <Zap className="w-8 h-8 text-streamt-400 mb-3 group-hover:scale-110 transition-transform" />
          <h3 className="font-semibold text-white mb-1">Deploy Pipeline</h3>
          <p className="text-sm text-slate-400">
            Plan and apply changes to your streaming infrastructure
          </p>
        </motion.button>

        <motion.button
          whileHover={{ scale: 1.02 }}
          whileTap={{ scale: 0.98 }}
          onClick={() => navigate('/tests')}
          className="p-6 rounded-xl border border-slate-800 bg-gradient-to-br from-test/10 to-slate-900 hover:border-test/30 transition-all text-left group"
        >
          <TestTube2 className="w-8 h-8 text-test mb-3 group-hover:scale-110 transition-transform" />
          <h3 className="font-semibold text-white mb-1">Run Tests</h3>
          <p className="text-sm text-slate-400">
            Execute data quality tests and validate assertions
          </p>
        </motion.button>

        <motion.button
          whileHover={{ scale: 1.02 }}
          whileTap={{ scale: 0.98 }}
          onClick={() => navigate('/editor')}
          className="p-6 rounded-xl border border-slate-800 bg-gradient-to-br from-model/10 to-slate-900 hover:border-model/30 transition-all text-left group"
        >
          <Boxes className="w-8 h-8 text-model mb-3 group-hover:scale-110 transition-transform" />
          <h3 className="font-semibold text-white mb-1">Edit Pipeline</h3>
          <p className="text-sm text-slate-400">
            Modify your pipeline configuration in the YAML editor
          </p>
        </motion.button>
      </div>
    </div>
  )
}
