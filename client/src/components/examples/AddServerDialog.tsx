import AddServerDialog from '../AddServerDialog';

export default function AddServerDialogExample() {
  return (
    <div className="p-4">
      <AddServerDialog
        onAddServer={(server) => {
          console.log('New server added:', server);
          // In real app, this would add to state/database
        }}
      />
    </div>
  );
}