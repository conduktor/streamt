import { useEffect } from 'react'
import { Routes, Route, Navigate } from 'react-router-dom'
import { useQuery } from '@tanstack/react-query'
import { Layout } from '@/components/Layout'
import { DashboardPage } from '@/pages/DashboardPage'
import { DAGPage } from '@/pages/DAGPage'
import { SourcesPage } from '@/pages/SourcesPage'
import { SourceEditPage } from '@/pages/SourceEditPage'
import { ModelsPage } from '@/pages/ModelsPage'
import { ModelEditPage } from '@/pages/ModelEditPage'
import { TestsPage } from '@/pages/TestsPage'
import { TestEditPage } from '@/pages/TestEditPage'
import { ExposuresPage } from '@/pages/ExposuresPage'
import { ExposureEditPage } from '@/pages/ExposureEditPage'
import { DeployPage } from '@/pages/DeployPage'
import { EditorPage } from '@/pages/EditorPage'
import { SettingsPage } from '@/pages/SettingsPage'
import { useProjectStore } from '@/stores/projectStore'
import { loadProject, loadDag, loadStatus, loadValidation } from '@/utils/api'

function App() {
  const { setProject, setDag, setStatus, setValidation, setLoading, setError, darkMode } = useProjectStore()

  // Load initial data
  const projectQuery = useQuery({
    queryKey: ['project'],
    queryFn: loadProject,
  })

  const dagQuery = useQuery({
    queryKey: ['dag'],
    queryFn: loadDag,
  })

  const statusQuery = useQuery({
    queryKey: ['status'],
    queryFn: loadStatus,
    refetchInterval: 30000, // Refresh every 30 seconds
  })

  const validationQuery = useQuery({
    queryKey: ['validation'],
    queryFn: loadValidation,
  })

  // Update store when data loads
  useEffect(() => {
    if (projectQuery.data) setProject(projectQuery.data)
  }, [projectQuery.data, setProject])

  useEffect(() => {
    if (dagQuery.data) setDag(dagQuery.data)
  }, [dagQuery.data, setDag])

  useEffect(() => {
    if (statusQuery.data) setStatus(statusQuery.data)
  }, [statusQuery.data, setStatus])

  useEffect(() => {
    if (validationQuery.data) setValidation(validationQuery.data)
  }, [validationQuery.data, setValidation])

  // Handle loading state
  useEffect(() => {
    const isLoading = projectQuery.isLoading || dagQuery.isLoading
    setLoading(isLoading)
  }, [projectQuery.isLoading, dagQuery.isLoading, setLoading])

  // Handle errors
  useEffect(() => {
    const error = projectQuery.error || dagQuery.error
    if (error) setError((error as Error).message)
  }, [projectQuery.error, dagQuery.error, setError])

  // Apply dark mode class to html element
  useEffect(() => {
    if (darkMode) {
      document.documentElement.classList.add('dark')
    } else {
      document.documentElement.classList.remove('dark')
    }
  }, [darkMode])

  return (
    <Layout>
      <Routes>
        <Route path="/" element={<Navigate to="/dashboard" replace />} />
        <Route path="/dashboard" element={<DashboardPage />} />
        <Route path="/dag" element={<DAGPage />} />
        <Route path="/sources" element={<SourcesPage />} />
        <Route path="/sources/:name" element={<SourcesPage />} />
        <Route path="/sources/:name/edit" element={<SourceEditPage />} />
        <Route path="/models" element={<ModelsPage />} />
        <Route path="/models/:name" element={<ModelsPage />} />
        <Route path="/models/:name/edit" element={<ModelEditPage />} />
        <Route path="/tests" element={<TestsPage />} />
        <Route path="/tests/:name" element={<TestsPage />} />
        <Route path="/tests/:name/edit" element={<TestEditPage />} />
        <Route path="/exposures" element={<ExposuresPage />} />
        <Route path="/exposures/:name" element={<ExposuresPage />} />
        <Route path="/exposures/:name/edit" element={<ExposureEditPage />} />
        <Route path="/deploy" element={<DeployPage />} />
        <Route path="/editor" element={<EditorPage />} />
        <Route path="/settings" element={<SettingsPage />} />
      </Routes>
    </Layout>
  )
}

export default App
