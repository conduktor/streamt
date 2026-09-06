import { useState, useEffect } from 'react'
import { useParams, useNavigate } from 'react-router-dom'
import { motion } from 'framer-motion'
import {
  TestTube2,
  Search,
  Play,
  FileCheck,
  BarChart3,
  Zap,
  AlertTriangle,
  Plus,
  Pencil,
  Trash2,
  ChevronRight,
  Code,
} from 'lucide-react'
import { Card, CardContent, CardHeader, CardTitle, Badge, Button, RelationshipLinks, CascadeDeleteDialog } from '@/components/ui'
import { useProjectStore } from '@/stores/projectStore'
import { cn } from '@/utils/cn'
import type { TestType } from '@/types'

const testTypeConfig: Record<TestType, { icon: typeof TestTube2; color: string; label: string; description: string }> = {
  schema: { icon: FileCheck, color: 'info', label: 'Schema', description: 'Validates structure against Schema Registry' },
  sample: { icon: BarChart3, color: 'warning', label: 'Sample', description: 'Consumes N messages and validates assertions' },
  continuous: { icon: Zap, color: 'success', label: 'Continuous', description: 'Long-running Flink job monitoring data quality' },
}

export function TestsPage() {
  const { name } = useParams()
  const navigate = useNavigate()
  const { project, deleteTest } = useProjectStore()
  const [searchQuery, setSearchQuery] = useState('')
  const [isRunning, setIsRunning] = useState(false)

  // Delete dialog state
  const [deleteDialogOpen, setDeleteDialogOpen] = useState(false)
  const [testToDelete, setTestToDelete] = useState<string | null>(null)
  const [saving, setSaving] = useState(false)

  const tests = project?.tests ?? []
  const filteredTests = tests.filter(
    (t) =>
      t.name.toLowerCase().includes(searchQuery.toLowerCase()) ||
      t.model.toLowerCase().includes(searchQuery.toLowerCase()) ||
      t.description?.toLowerCase().includes(searchQuery.toLowerCase())
  )

  const selectedTest = name ? tests.find((t) => t.name === name) : null

  // Auto-select last viewed item or first item if none selected
  useEffect(() => {
    if (!name && tests.length > 0) {
      const lastViewed = localStorage.getItem('lastTest')
      const targetTest = lastViewed && tests.find(t => t.name === lastViewed)
        ? lastViewed
        : tests[0].name
      navigate(`/tests/${targetTest}`, { replace: true })
    }
  }, [name, tests, navigate])

  // Remember last viewed item
  useEffect(() => {
    if (name && tests.find(t => t.name === name)) {
      localStorage.setItem('lastTest', name)
    }
  }, [name, tests])

  const handleRunTests = async () => {
    setIsRunning(true)
    // Simulate test run
    await new Promise((resolve) => setTimeout(resolve, 2000))
    setIsRunning(false)
  }

  const handleDeleteClick = (testName: string) => {
    setTestToDelete(testName)
    setDeleteDialogOpen(true)
  }

  const handleDeleteConfirm = async () => {
    if (!testToDelete) return
    setSaving(true)
    try {
      deleteTest(testToDelete)
      setDeleteDialogOpen(false)
      setTestToDelete(null)
      navigate('/tests')
    } finally {
      setSaving(false)
    }
  }

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-2xl font-bold text-white">Tests</h2>
          <p className="text-slate-400 mt-1">
            Data quality assertions and continuous monitoring
          </p>
        </div>
        <div className="flex items-center gap-3">
          <Badge variant="test">{tests.length} tests</Badge>
          <Button onClick={handleRunTests} loading={isRunning} variant="secondary" icon={<Play className="w-4 h-4" />}>
            Run All Tests
          </Button>
          <Button onClick={() => navigate('/tests/new/edit')} icon={<Plus className="w-4 h-4" />}>
            Add Test
          </Button>
        </div>
      </div>

      {/* Search */}
      <div className="relative">
        <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-5 h-5 text-slate-500" />
        <input
          type="text"
          placeholder="Search tests..."
          value={searchQuery}
          onChange={(e) => setSearchQuery(e.target.value)}
          className="input w-full pl-10"
        />
      </div>

      {/* Content */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        {/* Test list */}
        <div className="lg:col-span-1 space-y-3">
          {filteredTests.length === 0 ? (
            <Card>
              <CardContent className="py-12 text-center">
                <TestTube2 className="w-12 h-12 text-slate-600 mx-auto mb-4" />
                <p className="text-slate-400">No tests found</p>
              </CardContent>
            </Card>
          ) : (
            filteredTests.map((test) => {
              const config = testTypeConfig[test.type]
              const Icon = config.icon

              return (
                <motion.div
                  key={test.name}
                  initial={{ opacity: 0, y: 10 }}
                  animate={{ opacity: 1, y: 0 }}
                  transition={{ duration: 0.2 }}
                >
                  <Card
                    hover
                    onClick={() => navigate(`/tests/${test.name}`)}
                    className={cn(
                      'cursor-pointer',
                      selectedTest?.name === test.name && 'ring-2 ring-test'
                    )}
                  >
                    <CardContent className="p-4">
                      <div className="flex items-start justify-between">
                        <div className="flex items-center gap-3">
                          <div className="p-2 rounded-lg bg-test/20">
                            <Icon className="w-5 h-5 text-test" />
                          </div>
                          <div>
                            <h3 className="font-medium text-white">{test.name}</h3>
                            <p className="text-xs text-slate-500 mt-0.5">
                              Testing: {test.model}
                            </p>
                          </div>
                        </div>
                        <ChevronRight className="w-5 h-5 text-slate-500" />
                      </div>
                      <div className="flex items-center gap-2 mt-3">
                        <Badge variant={config.color as any} size="sm">
                          {config.label}
                        </Badge>
                        <Badge variant="default" size="sm">
                          {test.assertions.length} assertion{test.assertions.length !== 1 ? 's' : ''}
                        </Badge>
                      </div>
                    </CardContent>
                  </Card>
                </motion.div>
              )
            })
          )}
        </div>

        {/* Test details */}
        <div className="lg:col-span-2">
          {selectedTest ? (
            <Card>
              <CardHeader className="flex flex-row items-center justify-between">
                <div className="flex items-center gap-3">
                  <div className="p-2 rounded-lg bg-test/20">
                    {(() => {
                      const Icon = testTypeConfig[selectedTest.type].icon
                      return <Icon className="w-6 h-6 text-test" />
                    })()}
                  </div>
                  <div>
                    <CardTitle>{selectedTest.name}</CardTitle>
                    <div className="flex items-center gap-2 mt-1">
                      <Badge variant={testTypeConfig[selectedTest.type].color as any}>
                        {testTypeConfig[selectedTest.type].label}
                      </Badge>
                      <span className="text-sm text-slate-500">
                        Testing: <code className="text-streamt-400">{selectedTest.model}</code>
                      </span>
                    </div>
                  </div>
                </div>
                <div className="flex items-center gap-2">
                  <Button
                    variant="ghost"
                    size="icon"
                    onClick={() => navigate(`/tests/${selectedTest.name}/edit`)}
                    title="Edit test"
                  >
                    <Pencil className="w-4 h-4" />
                  </Button>
                  <Button
                    variant="ghost"
                    size="icon"
                    onClick={() => navigate(`/tests/${selectedTest.name}/edit?view=yaml`)}
                    title="Edit as YAML"
                  >
                    <Code className="w-4 h-4" />
                  </Button>
                  <Button
                    variant="ghost"
                    size="icon"
                    onClick={() => handleDeleteClick(selectedTest.name)}
                    title="Delete test"
                    className="text-slate-400 hover:text-red-400"
                  >
                    <Trash2 className="w-4 h-4" />
                  </Button>
                  <Button
                    variant="ghost"
                    size="icon"
                    title="Run test"
                  >
                    <Play className="w-4 h-4" />
                  </Button>
                </div>
              </CardHeader>
              <CardContent className="space-y-6">
                {/* Description */}
                {selectedTest.description && (
                  <div>
                    <h4 className="text-sm font-medium text-slate-400 mb-2">Description</h4>
                    <p className="text-slate-300">{selectedTest.description}</p>
                  </div>
                )}

                {/* Test Type Info */}
                <div className="p-4 rounded-lg bg-slate-800/50 flex items-start gap-3">
                  <div className="p-2 rounded-lg bg-test/20">
                    {(() => {
                      const Icon = testTypeConfig[selectedTest.type].icon
                      return <Icon className="w-5 h-5 text-test" />
                    })()}
                  </div>
                  <div>
                    <p className="font-medium text-white">
                      {testTypeConfig[selectedTest.type].label} Test
                    </p>
                    <p className="text-sm text-slate-400 mt-1">
                      {testTypeConfig[selectedTest.type].description}
                    </p>
                  </div>
                </div>

                {/* Sample size for sample tests */}
                {selectedTest.type === 'sample' && selectedTest.sample_size && (
                  <div className="flex items-center gap-4">
                    <div className="p-3 rounded-lg bg-slate-800/50">
                      <p className="text-xs text-slate-500">Sample Size</p>
                      <p className="text-lg font-semibold text-white">
                        {selectedTest.sample_size}
                      </p>
                    </div>
                    {selectedTest.timeout_seconds && (
                      <div className="p-3 rounded-lg bg-slate-800/50">
                        <p className="text-xs text-slate-500">Timeout</p>
                        <p className="text-lg font-semibold text-white">
                          {selectedTest.timeout_seconds}s
                        </p>
                      </div>
                    )}
                  </div>
                )}

                {/* Assertions */}
                <div>
                  <h4 className="text-sm font-medium text-slate-400 mb-3">
                    Assertions ({selectedTest.assertions.length})
                  </h4>
                  <div className="space-y-3">
                    {selectedTest.assertions.map((assertion, i) => {
                      const assertionType = Object.keys(assertion)[0]
                      const config = assertion[assertionType]

                      return (
                        <div
                          key={i}
                          className="p-4 rounded-lg border border-slate-800 bg-slate-900/50"
                        >
                          <div className="flex items-center gap-2 mb-2">
                            <Badge variant="test" size="sm">
                              {assertionType}
                            </Badge>
                          </div>
                          <div className="text-sm text-slate-300 space-y-1">
                            {config.columns && (
                              <p>
                                <span className="text-slate-500">Columns:</span>{' '}
                                <code className="text-streamt-400">
                                  {config.columns.join(', ')}
                                </code>
                              </p>
                            )}
                            {config.column && (
                              <p>
                                <span className="text-slate-500">Column:</span>{' '}
                                <code className="text-streamt-400">{config.column}</code>
                              </p>
                            )}
                            {config.values && (
                              <p>
                                <span className="text-slate-500">Values:</span>{' '}
                                <code className="text-streamt-400">
                                  [{config.values.join(', ')}]
                                </code>
                              </p>
                            )}
                            {config.min !== undefined && (
                              <p>
                                <span className="text-slate-500">Min:</span>{' '}
                                <code className="text-emerald-400">{config.min}</code>
                              </p>
                            )}
                            {config.max !== undefined && (
                              <p>
                                <span className="text-slate-500">Max:</span>{' '}
                                <code className="text-red-400">{config.max}</code>
                              </p>
                            )}
                          </div>
                        </div>
                      )
                    })}
                  </div>
                </div>

                {/* On Failure Actions */}
                {selectedTest.on_failure && (
                  <div>
                    <h4 className="text-sm font-medium text-slate-400 mb-3 flex items-center gap-2">
                      <AlertTriangle className="w-4 h-4" />
                      On Failure Actions
                    </h4>
                    <div className="p-4 rounded-lg bg-red-900/20 border border-red-800/50">
                      <p className="text-sm text-slate-300">
                        {selectedTest.on_failure.actions.length} action(s) configured
                      </p>
                    </div>
                  </div>
                )}

                {/* Relationships */}
                <RelationshipLinks
                  title="Tests Model"
                  icon="upstream"
                  items={[{ type: 'model', name: selectedTest.model }]}
                />
              </CardContent>
            </Card>
          ) : (
            <Card>
              <CardContent className="py-24 text-center">
                <TestTube2 className="w-16 h-16 text-slate-700 mx-auto mb-4" />
                <h3 className="text-lg font-medium text-slate-400 mb-2">
                  Select a test
                </h3>
                <p className="text-sm text-slate-500">
                  Choose a test from the list to view its details
                </p>
              </CardContent>
            </Card>
          )}
        </div>
      </div>

      {/* Cascade Delete Dialog */}
      <CascadeDeleteDialog
        open={deleteDialogOpen}
        onClose={() => {
          setDeleteDialogOpen(false)
          setTestToDelete(null)
        }}
        onConfirm={handleDeleteConfirm}
        entityName={testToDelete || ''}
        entityType="test"
        dependents={[]}
        loading={saving}
      />
    </div>
  )
}
