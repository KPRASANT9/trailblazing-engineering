import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from "@/components/ui/alert-dialog";

export interface DeleteServerDialogProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  serverName: string;
  onConfirmDelete: () => void;
  isDeleting?: boolean;
}

export default function DeleteServerDialog({
  open,
  onOpenChange,
  serverName,
  onConfirmDelete,
  isDeleting = false
}: DeleteServerDialogProps) {
  return (
    <AlertDialog open={open} onOpenChange={onOpenChange}>
      <AlertDialogContent data-testid="dialog-delete-server">
        <AlertDialogHeader>
          <AlertDialogTitle>Delete Server</AlertDialogTitle>
          <AlertDialogDescription>
            Are you sure you want to delete "{serverName}"? This action cannot be undone.
            Any active connections to this server will be terminated.
          </AlertDialogDescription>
        </AlertDialogHeader>
        <AlertDialogFooter>
          <AlertDialogCancel 
            disabled={isDeleting}
            data-testid="button-cancel-delete"
          >
            Cancel
          </AlertDialogCancel>
          <AlertDialogAction
            onClick={onConfirmDelete}
            disabled={isDeleting}
            className="bg-destructive text-destructive-foreground hover:bg-destructive/90"
            data-testid="button-confirm-delete"
          >
            {isDeleting ? 'Deleting...' : 'Delete Server'}
          </AlertDialogAction>
        </AlertDialogFooter>
      </AlertDialogContent>
    </AlertDialog>
  );
}