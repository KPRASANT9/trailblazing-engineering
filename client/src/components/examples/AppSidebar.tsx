import { SidebarProvider } from "@/components/ui/sidebar";
import { AppSidebar, ServerConnection } from '../AppSidebar';

export default function AppSidebarExample() {
  //todo: remove mock functionality  
  const mockServers: ServerConnection[] = [
    {
      id: 'srv-1',
      name: 'Production Web',
      host: '192.168.1.100',
      port: 22,
      username: 'admin',
      status: 'connected',
      lastConnected: new Date('2024-01-15T10:30:00')
    },
    {
      id: 'srv-2', 
      name: 'Dev Environment',
      host: 'dev.company.com',
      port: 2222,
      username: 'developer',
      status: 'disconnected',
      lastConnected: new Date('2024-01-14T16:45:00')
    },
    {
      id: 'srv-3',
      name: 'Database Server',
      host: '10.0.1.50',
      port: 22,
      username: 'dbadmin',
      status: 'connecting'
    }
  ];

  const style = {
    "--sidebar-width": "20rem",
    "--sidebar-width-icon": "4rem",
  };

  return (
    <SidebarProvider style={style as React.CSSProperties}>
      <div className="flex h-screen w-full">
        <AppSidebar
          servers={mockServers}
          onAddServer={(server) => console.log('Adding server:', server)}
          onConnectServer={(id) => console.log('Connecting to server:', id)}
          onEditServer={(id) => console.log('Editing server:', id)}
          onOpenTerminal={(id) => console.log('Opening terminal for:', id)}
        />
        <div className="flex-1 p-8">
          <h2 className="text-2xl font-bold mb-4">Main Content Area</h2>
          <p className="text-muted-foreground">
            This is where the terminal windows and other content would appear.
          </p>
        </div>
      </div>
    </SidebarProvider>
  );
}