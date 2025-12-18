import { create } from 'zustand'
import { persist } from 'zustand/middleware'
import type {
  StreamtProject,
  DAG,
  ProjectStatus,
  DeploymentPlan,
  ValidationResult,
  Source,
  Model,
  DataTest,
  Exposure,
} from '@/types'

interface ProjectState {
  // Current project data
  project: StreamtProject | null
  dag: DAG | null
  status: ProjectStatus | null
  validation: ValidationResult | null
  deploymentPlan: DeploymentPlan | null

  // UI state
  selectedNode: string | null
  sidebarOpen: boolean
  darkMode: boolean
  viewMode: 'dag' | 'list' | 'yaml'

  // Loading states
  isLoading: boolean
  isDeploying: boolean
  isValidating: boolean
  isSaving: boolean

  // Error state
  error: string | null

  // Actions
  setProject: (project: StreamtProject) => void
  setDag: (dag: DAG) => void
  setStatus: (status: ProjectStatus) => void
  setValidation: (validation: ValidationResult) => void
  setDeploymentPlan: (plan: DeploymentPlan | null) => void

  selectNode: (nodeId: string | null) => void
  toggleSidebar: () => void
  toggleDarkMode: () => void
  setViewMode: (mode: 'dag' | 'list' | 'yaml') => void

  setLoading: (loading: boolean) => void
  setDeploying: (deploying: boolean) => void
  setValidating: (validating: boolean) => void
  setSaving: (saving: boolean) => void
  setError: (error: string | null) => void

  // Entity getters
  getSource: (name: string) => Source | undefined
  getModel: (name: string) => Model | undefined
  getTest: (name: string) => DataTest | undefined
  getExposure: (name: string) => Exposure | undefined

  // CRUD operations (local state updates)
  addSource: (source: Source) => void
  updateSource: (name: string, source: Partial<Source>) => void
  deleteSource: (name: string) => void

  addModel: (model: Model) => void
  updateModel: (name: string, model: Partial<Model>) => void
  deleteModel: (name: string) => void

  addTest: (test: DataTest) => void
  updateTest: (name: string, test: Partial<DataTest>) => void
  deleteTest: (name: string) => void

  addExposure: (exposure: Exposure) => void
  updateExposure: (name: string, exposure: Partial<Exposure>) => void
  deleteExposure: (name: string) => void

  // Derived data
  getNodeDetails: (nodeId: string) => Source | Model | DataTest | Exposure | undefined
  getUpstreamNodes: (nodeId: string) => string[]
  getDownstreamNodes: (nodeId: string) => string[]
}

export const useProjectStore = create<ProjectState>()(
  persist(
    (set, get) => ({
      // Initial state
      project: null,
      dag: null,
      status: null,
      validation: null,
      deploymentPlan: null,

      selectedNode: null,
      sidebarOpen: true,
      darkMode: true,
      viewMode: 'dag',

      isLoading: false,
      isDeploying: false,
      isValidating: false,
      isSaving: false,

      error: null,

      // Actions
      setProject: (project) => set({ project, error: null }),
      setDag: (dag) => set({ dag }),
      setStatus: (status) => set({ status }),
      setValidation: (validation) => set({ validation }),
      setDeploymentPlan: (plan) => set({ deploymentPlan: plan }),

      selectNode: (nodeId) => set({ selectedNode: nodeId }),
      toggleSidebar: () => set((state) => ({ sidebarOpen: !state.sidebarOpen })),
      toggleDarkMode: () => set((state) => ({ darkMode: !state.darkMode })),
      setViewMode: (mode) => set({ viewMode: mode }),

      setLoading: (loading) => set({ isLoading: loading }),
      setDeploying: (deploying) => set({ isDeploying: deploying }),
      setValidating: (validating) => set({ isValidating: validating }),
      setSaving: (saving) => set({ isSaving: saving }),
      setError: (error) => set({ error }),

      // Entity getters
      getSource: (name) => get().project?.sources.find((s) => s.name === name),
      getModel: (name) => get().project?.models.find((m) => m.name === name),
      getTest: (name) => get().project?.tests.find((t) => t.name === name),
      getExposure: (name) => get().project?.exposures.find((e) => e.name === name),

      // CRUD operations for Sources
      addSource: (source) =>
        set((state) => {
          if (!state.project) return state
          return {
            project: {
              ...state.project,
              sources: [...state.project.sources, source],
            },
          }
        }),

      updateSource: (name, updates) =>
        set((state) => {
          if (!state.project) return state
          return {
            project: {
              ...state.project,
              sources: state.project.sources.map((s) =>
                s.name === name ? { ...s, ...updates } : s
              ),
            },
          }
        }),

      deleteSource: (name) =>
        set((state) => {
          if (!state.project) return state
          return {
            project: {
              ...state.project,
              sources: state.project.sources.filter((s) => s.name !== name),
            },
            selectedNode: state.selectedNode === name ? null : state.selectedNode,
          }
        }),

      // CRUD operations for Models
      addModel: (model) =>
        set((state) => {
          if (!state.project) return state
          return {
            project: {
              ...state.project,
              models: [...state.project.models, model],
            },
          }
        }),

      updateModel: (name, updates) =>
        set((state) => {
          if (!state.project) return state
          return {
            project: {
              ...state.project,
              models: state.project.models.map((m) =>
                m.name === name ? { ...m, ...updates } : m
              ),
            },
          }
        }),

      deleteModel: (name) =>
        set((state) => {
          if (!state.project) return state
          return {
            project: {
              ...state.project,
              models: state.project.models.filter((m) => m.name !== name),
            },
            selectedNode: state.selectedNode === name ? null : state.selectedNode,
          }
        }),

      // CRUD operations for Tests
      addTest: (test) =>
        set((state) => {
          if (!state.project) return state
          return {
            project: {
              ...state.project,
              tests: [...state.project.tests, test],
            },
          }
        }),

      updateTest: (name, updates) =>
        set((state) => {
          if (!state.project) return state
          return {
            project: {
              ...state.project,
              tests: state.project.tests.map((t) =>
                t.name === name ? { ...t, ...updates } : t
              ),
            },
          }
        }),

      deleteTest: (name) =>
        set((state) => {
          if (!state.project) return state
          return {
            project: {
              ...state.project,
              tests: state.project.tests.filter((t) => t.name !== name),
            },
            selectedNode: state.selectedNode === name ? null : state.selectedNode,
          }
        }),

      // CRUD operations for Exposures
      addExposure: (exposure) =>
        set((state) => {
          if (!state.project) return state
          return {
            project: {
              ...state.project,
              exposures: [...state.project.exposures, exposure],
            },
          }
        }),

      updateExposure: (name, updates) =>
        set((state) => {
          if (!state.project) return state
          return {
            project: {
              ...state.project,
              exposures: state.project.exposures.map((e) =>
                e.name === name ? { ...e, ...updates } : e
              ),
            },
          }
        }),

      deleteExposure: (name) =>
        set((state) => {
          if (!state.project) return state
          return {
            project: {
              ...state.project,
              exposures: state.project.exposures.filter((e) => e.name !== name),
            },
            selectedNode: state.selectedNode === name ? null : state.selectedNode,
          }
        }),

      getNodeDetails: (nodeId) => {
        const state = get()
        const dagNode = state.dag?.nodes.find((n) => n.name === nodeId)
        if (!dagNode) return undefined

        switch (dagNode.type) {
          case 'source':
            return state.getSource(nodeId)
          case 'model':
            return state.getModel(nodeId)
          case 'test':
            return state.getTest(nodeId)
          case 'exposure':
            return state.getExposure(nodeId)
          default:
            return undefined
        }
      },

      getUpstreamNodes: (nodeId) => {
        const dagNode = get().dag?.nodes.find((n) => n.name === nodeId)
        return dagNode?.upstream ?? []
      },

      getDownstreamNodes: (nodeId) => {
        const dagNode = get().dag?.nodes.find((n) => n.name === nodeId)
        return dagNode?.downstream ?? []
      },
    }),
    {
      name: 'streamt-project-storage',
      partialize: (state) => ({
        darkMode: state.darkMode,
        sidebarOpen: state.sidebarOpen,
        viewMode: state.viewMode,
      }),
    }
  )
)

// Selectors for performance optimization
export const useSelectedNode = () => useProjectStore((state) => state.selectedNode)
export const useDag = () => useProjectStore((state) => state.dag)
export const useProject = () => useProjectStore((state) => state.project)
export const useStatus = () => useProjectStore((state) => state.status)
export const useValidation = () => useProjectStore((state) => state.validation)
export const useViewMode = () => useProjectStore((state) => state.viewMode)
export const useIsLoading = () => useProjectStore((state) => state.isLoading)
export const useIsSaving = () => useProjectStore((state) => state.isSaving)
