import { useState, useEffect } from 'react'
import { AlertTriangle, Trash2, AlertOctagon } from 'lucide-react'
import { Dialog, DialogFooter } from './Dialog'
import { Button } from './Button'
import { Input } from './FormField'

interface ConfirmDialogProps {
  open: boolean
  onClose: () => void
  onConfirm: () => void
  title: string
  message: string
  confirmLabel?: string
  cancelLabel?: string
  variant?: 'danger' | 'warning'
  loading?: boolean
}

export function ConfirmDialog({
  open,
  onClose,
  onConfirm,
  title,
  message,
  confirmLabel = 'Delete',
  cancelLabel = 'Cancel',
  variant = 'danger',
  loading = false,
}: ConfirmDialogProps) {
  return (
    <Dialog open={open} onClose={onClose} title={title} size="sm">
      <div className="flex flex-col items-center text-center">
        <div className={`p-4 rounded-full ${variant === 'danger' ? 'bg-red-900/30' : 'bg-yellow-900/30'} mb-4`}>
          {variant === 'danger' ? (
            <Trash2 className="w-8 h-8 text-red-400" />
          ) : (
            <AlertTriangle className="w-8 h-8 text-yellow-400" />
          )}
        </div>
        <p className="text-slate-300">{message}</p>
      </div>

      <DialogFooter>
        <Button variant="ghost" onClick={onClose} disabled={loading}>
          {cancelLabel}
        </Button>
        <Button
          variant={variant === 'danger' ? 'danger' : 'secondary'}
          onClick={onConfirm}
          loading={loading}
        >
          {confirmLabel}
        </Button>
      </DialogFooter>
    </Dialog>
  )
}

interface CascadeDeleteDialogProps {
  open: boolean
  onClose: () => void
  onConfirm: () => void
  entityName: string
  entityType: 'source' | 'model' | 'test' | 'exposure'
  dependents: Array<{ type: string; name: string }>
  loading?: boolean
}

export function CascadeDeleteDialog({
  open,
  onClose,
  onConfirm,
  entityName,
  entityType,
  dependents,
  loading = false,
}: CascadeDeleteDialogProps) {
  const [confirmText, setConfirmText] = useState('')
  const confirmPhrase = `delete ${entityName}`

  // Reset confirmation text when dialog opens/closes
  useEffect(() => {
    if (open) {
      setConfirmText('')
    }
  }, [open])

  const isConfirmValid = confirmText.toLowerCase() === confirmPhrase.toLowerCase()

  const handleConfirm = () => {
    if (isConfirmValid) {
      onConfirm()
    }
  }

  return (
    <Dialog open={open} onClose={onClose} title="Cascade Delete" size="md">
      <div className="space-y-6">
        {/* Warning Header */}
        <div className="flex items-start gap-4 p-4 bg-red-900/20 border border-red-800/50 rounded-lg">
          <div className="p-3 rounded-full bg-red-900/50">
            <AlertOctagon className="w-8 h-8 text-red-400" />
          </div>
          <div>
            <h3 className="text-lg font-bold text-red-400">DESTRUCTIVE OPERATION</h3>
            <p className="text-sm text-slate-300 mt-1">
              This action <span className="font-bold text-white">CANNOT be undone</span>.
              This will permanently delete the {entityType}{' '}
              <code className="px-1.5 py-0.5 bg-slate-800 rounded text-red-300">{entityName}</code>
              {dependents.length > 0 && ' and ALL its dependents'}.
            </p>
          </div>
        </div>

        {/* Dependents List */}
        {dependents.length > 0 && (
          <div>
            <h4 className="text-sm font-medium text-slate-400 mb-3">
              The following {dependents.length} item(s) will also be deleted:
            </h4>
            <div className="max-h-48 overflow-y-auto space-y-1.5 p-3 bg-slate-800/50 rounded-lg border border-slate-700">
              {dependents.map((dep, i) => (
                <div key={i} className="flex items-center gap-2 text-sm">
                  <span className={`px-1.5 py-0.5 rounded text-xs font-medium ${
                    dep.type === 'source' ? 'bg-source/20 text-source' :
                    dep.type === 'model' ? 'bg-model/20 text-model' :
                    dep.type === 'test' ? 'bg-test/20 text-test' :
                    'bg-exposure/20 text-exposure'
                  }`}>
                    {dep.type}
                  </span>
                  <span className="text-slate-300">{dep.name}</span>
                </div>
              ))}
            </div>
          </div>
        )}

        {/* Confirmation Input */}
        <div className="p-4 bg-slate-800/50 border border-slate-700 rounded-lg">
          <label className="block text-sm text-slate-300 mb-2">
            To confirm, type <code className="px-1.5 py-0.5 bg-slate-700 rounded text-amber-300 font-bold">{confirmPhrase}</code> below:
          </label>
          <Input
            value={confirmText}
            onChange={(e) => setConfirmText(e.target.value)}
            placeholder={confirmPhrase}
            className="font-mono"
            error={confirmText.length > 0 && !isConfirmValid}
          />
          {confirmText.length > 0 && !isConfirmValid && (
            <p className="text-xs text-red-400 mt-1">Text doesn't match. Type exactly: {confirmPhrase}</p>
          )}
        </div>
      </div>

      <DialogFooter>
        <Button variant="ghost" onClick={onClose} disabled={loading}>
          Cancel
        </Button>
        <Button
          variant="danger"
          onClick={handleConfirm}
          loading={loading}
          disabled={!isConfirmValid}
        >
          I understand, delete permanently
        </Button>
      </DialogFooter>
    </Dialog>
  )
}
