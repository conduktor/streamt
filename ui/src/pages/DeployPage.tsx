import { useState } from 'react'
import { motion } from 'framer-motion'
import {
  Rocket,
  CheckCircle2,
  XCircle,
  AlertTriangle,
  Clock,
  Plus,
  Minus,
  RefreshCw,
  Database,
  Zap,
  Upload,
} from 'lucide-react'
import { Card, CardContent, CardHeader, CardTitle, Badge, Button } from '@/components/ui'
import { useProjectStore } from '@/stores/projectStore'
import { cn } from '@/utils/cn'

interface Change {
  type: 'schema' | 'topic' | 'flink_job' | 'connector'
  name: string
  action: 'create' | 'update' | 'delete'
  details?: Record<string, { from?: string; to?: string }>
}

const mockPlan: Change[] = [
  { type: 'topic', name: 'payments_validated', action: 'create', details: { partitions: { to: '6' }, replication_factor: { to: '3' } } },
  { type: 'topic', name: 'payments_enriched', action: 'create', details: { partitions: { to: '12' } } },
  { type: 'topic', name: 'fraud_scores', action: 'create', details: { partitions: { to: '6' } } },
  { type: 'flink_job', name: 'payments_enriched_processor', action: 'create' },
  { type: 'flink_job', name: 'fraud_scores_processor', action: 'create' },
  { type: 'flink_job', name: 'customer_daily_totals_processor', action: 'create' },
  { type: 'connector', name: 'to_snowflake', action: 'create' },
]

const typeIcons: Record<string, typeof Database> = {
  schema: Database,
  topic: Database,
  flink_job: Zap,
  connector: Upload,
}

const typeColors: Record<string, string> = {
  schema: 'text-blue-400',
  topic: 'text-emerald-400',
  flink_job: 'text-orange-400',
  connector: 'text-purple-400',
}

export function DeployPage() {
  const { validation } = useProjectStore()
  const [step, setStep] = useState<'validate' | 'plan' | 'apply' | 'done'>('validate')
  const [isRunning, setIsRunning] = useState(false)
  const [plan, setPlan] = useState<Change[] | null>(null)
  const [applyResult, setApplyResult] = useState<{ created: string[]; errors: string[] } | null>(null)

  const handleValidate = async () => {
    setIsRunning(true)
    await new Promise((resolve) => setTimeout(resolve, 1500))
    setIsRunning(false)
    setStep('plan')
  }

  const handlePlan = async () => {
    setIsRunning(true)
    await new Promise((resolve) => setTimeout(resolve, 2000))
    setPlan(mockPlan)
    setIsRunning(false)
  }

  const handleApply = async () => {
    setIsRunning(true)
    await new Promise((resolve) => setTimeout(resolve, 3000))
    setApplyResult({
      created: mockPlan.map((c) => `${c.type}:${c.name}`),
      errors: [],
    })
    setIsRunning(false)
    setStep('done')
  }

  const handleReset = () => {
    setStep('validate')
    setPlan(null)
    setApplyResult(null)
  }

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-2xl font-bold text-white">Deploy Pipeline</h2>
          <p className="text-slate-400 mt-1">
            Plan and apply changes to your streaming infrastructure
          </p>
        </div>
        {step !== 'validate' && (
          <Button variant="ghost" onClick={handleReset} icon={<RefreshCw className="w-4 h-4" />}>
            Start Over
          </Button>
        )}
      </div>

      {/* Progress steps */}
      <div className="flex items-center justify-center gap-4 py-4">
        {['validate', 'plan', 'apply', 'done'].map((s, i) => {
          const isActive = step === s
          const isPast = ['validate', 'plan', 'apply', 'done'].indexOf(step) > i
          const Icon = i === 0 ? CheckCircle2 : i === 1 ? Clock : i === 2 ? Rocket : CheckCircle2

          return (
            <div key={s} className="flex items-center">
              {i > 0 && (
                <div className={cn('w-12 h-0.5 mr-4', isPast ? 'bg-streamt-500' : 'bg-slate-700')} />
              )}
              <div
                className={cn(
                  'flex items-center gap-2 px-4 py-2 rounded-lg transition-all',
                  isActive
                    ? 'bg-streamt-600/20 text-streamt-400 ring-2 ring-streamt-500'
                    : isPast
                    ? 'bg-emerald-600/20 text-emerald-400'
                    : 'bg-slate-800 text-slate-500'
                )}
              >
                <Icon className="w-5 h-5" />
                <span className="font-medium capitalize">{s}</span>
              </div>
            </div>
          )
        })}
      </div>

      {/* Content based on step */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Validation */}
        <Card className={cn(step !== 'validate' && 'opacity-50')}>
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <CheckCircle2 className="w-5 h-5 text-streamt-400" />
              1. Validate Configuration
            </CardTitle>
          </CardHeader>
          <CardContent className="space-y-4">
            <p className="text-sm text-slate-400">
              Check your pipeline configuration for errors and warnings before deploying.
            </p>

            {validation && (
              <div className="space-y-3">
                {validation.is_valid ? (
                  <div className="flex items-center gap-3 p-4 rounded-lg bg-emerald-900/20 border border-emerald-800/50">
                    <CheckCircle2 className="w-6 h-6 text-emerald-400" />
                    <div>
                      <p className="font-medium text-emerald-400">Configuration Valid</p>
                      <p className="text-sm text-slate-400">
                        {validation.messages.length} warning(s)
                      </p>
                    </div>
                  </div>
                ) : (
                  <div className="flex items-center gap-3 p-4 rounded-lg bg-red-900/20 border border-red-800/50">
                    <XCircle className="w-6 h-6 text-red-400" />
                    <div>
                      <p className="font-medium text-red-400">Validation Failed</p>
                      <p className="text-sm text-slate-400">
                        {validation.messages.filter((m) => m.level === 'error').length} error(s)
                      </p>
                    </div>
                  </div>
                )}

                {validation.messages.slice(0, 3).map((msg, i) => (
                  <div
                    key={i}
                    className={cn(
                      'p-3 rounded-lg border text-sm',
                      msg.level === 'error'
                        ? 'bg-red-900/10 border-red-900/30 text-red-300'
                        : 'bg-yellow-900/10 border-yellow-900/30 text-yellow-300'
                    )}
                  >
                    {msg.message}
                  </div>
                ))}
              </div>
            )}

            <Button
              onClick={handleValidate}
              loading={isRunning && step === 'validate'}
              disabled={step !== 'validate'}
              className="w-full"
            >
              {step === 'validate' ? 'Validate' : 'Validated'}
            </Button>
          </CardContent>
        </Card>

        {/* Plan */}
        <Card className={cn(step === 'validate' && 'opacity-50')}>
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <Clock className="w-5 h-5 text-streamt-400" />
              2. Plan Changes
            </CardTitle>
          </CardHeader>
          <CardContent className="space-y-4">
            <p className="text-sm text-slate-400">
              Generate a deployment plan showing what resources will be created, updated, or deleted.
            </p>

            {plan && (
              <div className="space-y-3 max-h-64 overflow-y-auto">
                {/* Summary */}
                <div className="flex items-center gap-4 p-3 rounded-lg bg-slate-800/50">
                  <span className="flex items-center gap-1 text-emerald-400">
                    <Plus className="w-4 h-4" />
                    {plan.filter((c) => c.action === 'create').length} create
                  </span>
                  <span className="flex items-center gap-1 text-yellow-400">
                    <RefreshCw className="w-4 h-4" />
                    {plan.filter((c) => c.action === 'update').length} update
                  </span>
                  <span className="flex items-center gap-1 text-red-400">
                    <Minus className="w-4 h-4" />
                    {plan.filter((c) => c.action === 'delete').length} delete
                  </span>
                </div>

                {/* Changes list */}
                {plan.map((change, i) => {
                  const Icon = typeIcons[change.type]
                  return (
                    <motion.div
                      key={i}
                      initial={{ opacity: 0, x: -10 }}
                      animate={{ opacity: 1, x: 0 }}
                      transition={{ delay: i * 0.05 }}
                      className="flex items-center gap-3 p-3 rounded-lg bg-slate-800/50"
                    >
                      <span
                        className={cn(
                          'w-6 h-6 flex items-center justify-center rounded',
                          change.action === 'create' && 'bg-emerald-500/20 text-emerald-400',
                          change.action === 'update' && 'bg-yellow-500/20 text-yellow-400',
                          change.action === 'delete' && 'bg-red-500/20 text-red-400'
                        )}
                      >
                        {change.action === 'create' && <Plus className="w-4 h-4" />}
                        {change.action === 'update' && <RefreshCw className="w-4 h-4" />}
                        {change.action === 'delete' && <Minus className="w-4 h-4" />}
                      </span>
                      <Icon className={cn('w-4 h-4', typeColors[change.type])} />
                      <span className="text-sm text-white font-medium">{change.name}</span>
                      <Badge variant="default" size="sm" className="ml-auto">
                        {change.type.replace('_', ' ')}
                      </Badge>
                    </motion.div>
                  )
                })}
              </div>
            )}

            <Button
              onClick={handlePlan}
              loading={isRunning && step === 'plan' && !plan}
              disabled={step === 'validate' || (step === 'plan' && plan !== null)}
              className="w-full"
            >
              {plan ? 'Plan Generated' : 'Generate Plan'}
            </Button>
          </CardContent>
        </Card>

        {/* Apply */}
        <Card className={cn((step === 'validate' || (step === 'plan' && !plan)) && 'opacity-50')}>
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <Rocket className="w-5 h-5 text-streamt-400" />
              3. Apply Changes
            </CardTitle>
          </CardHeader>
          <CardContent className="space-y-4">
            <p className="text-sm text-slate-400">
              Deploy the planned changes to your Kafka, Flink, and Connect clusters.
            </p>

            {plan && !applyResult && (
              <div className="p-4 rounded-lg bg-yellow-900/20 border border-yellow-800/50">
                <div className="flex items-center gap-2">
                  <AlertTriangle className="w-5 h-5 text-yellow-400" />
                  <p className="font-medium text-yellow-400">Review before applying</p>
                </div>
                <p className="text-sm text-slate-400 mt-2">
                  This will create {plan.filter((c) => c.action === 'create').length} resources
                  in your streaming infrastructure.
                </p>
              </div>
            )}

            <Button
              onClick={handleApply}
              loading={isRunning && step === 'apply'}
              disabled={!plan || step === 'done'}
              variant={plan ? 'primary' : 'secondary'}
              className="w-full"
            >
              {step === 'done' ? 'Applied Successfully' : 'Apply Changes'}
            </Button>
          </CardContent>
        </Card>

        {/* Result */}
        <Card className={cn(step !== 'done' && 'opacity-50')}>
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              {applyResult?.errors.length === 0 ? (
                <CheckCircle2 className="w-5 h-5 text-emerald-400" />
              ) : (
                <XCircle className="w-5 h-5 text-red-400" />
              )}
              4. Deployment Result
            </CardTitle>
          </CardHeader>
          <CardContent className="space-y-4">
            {applyResult ? (
              <>
                {applyResult.errors.length === 0 ? (
                  <div className="flex items-center gap-3 p-4 rounded-lg bg-emerald-900/20 border border-emerald-800/50">
                    <CheckCircle2 className="w-8 h-8 text-emerald-400" />
                    <div>
                      <p className="font-medium text-emerald-400">Deployment Successful!</p>
                      <p className="text-sm text-slate-400">
                        {applyResult.created.length} resources deployed
                      </p>
                    </div>
                  </div>
                ) : (
                  <div className="flex items-center gap-3 p-4 rounded-lg bg-red-900/20 border border-red-800/50">
                    <XCircle className="w-8 h-8 text-red-400" />
                    <div>
                      <p className="font-medium text-red-400">Deployment Failed</p>
                      <p className="text-sm text-slate-400">
                        {applyResult.errors.length} error(s)
                      </p>
                    </div>
                  </div>
                )}

                <div className="space-y-2 max-h-48 overflow-y-auto">
                  {applyResult.created.map((resource, i) => (
                    <div
                      key={i}
                      className="flex items-center gap-2 p-2 rounded-lg bg-slate-800/50 text-sm"
                    >
                      <CheckCircle2 className="w-4 h-4 text-emerald-400" />
                      <span className="text-slate-300">{resource}</span>
                    </div>
                  ))}
                </div>
              </>
            ) : (
              <p className="text-sm text-slate-500 text-center py-8">
                Apply changes to see the result
              </p>
            )}
          </CardContent>
        </Card>
      </div>
    </div>
  )
}
