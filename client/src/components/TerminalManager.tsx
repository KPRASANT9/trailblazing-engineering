import { useState, useEffect } from "react";
import { SidebarProvider, SidebarTrigger } from "@/components/ui/sidebar";
import { AppSidebar, ServerConnection } from "./AppSidebar";
import TerminalWindow, { TerminalLine } from "./TerminalWindow";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Button } from "@/components/ui/button";
import { useToast } from "@/hooks/use-toast";
import { Moon, Sun } from "lucide-react";
import { useTheme } from "./ThemeProvider";
import { terminalWS } from "@/lib/websocket";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { apiRequest } from "@/lib/queryClient";
import EditServerDialog from "./EditServerDialog";
import DeleteServerDialog from "./DeleteServerDialog";

interface TerminalSession {
  id: string;
  serverId: string;
  serverName: string;
  isConnected: boolean;
  lines: TerminalLine[];
  currentDirectory: string;
}

export default function TerminalManager() {
  const { theme, setTheme } = useTheme();
  const { toast } = useToast();
  const queryClient = useQueryClient();
  
  // Edit/Delete dialog state
  const [editDialogOpen, setEditDialogOpen] = useState(false);
  const [deleteDialogOpen, setDeleteDialogOpen] = useState(false);
  const [selectedServer, setSelectedServer] = useState<ServerConnection | null>(null);
  
  // Fetch servers from API
  const { data: servers = [], isLoading: loadingServers } = useQuery({
    queryKey: ['/api/servers'],
  });
  
  // Add server mutation
  const addServerMutation = useMutation({
    mutationFn: async (serverData: any) => {
      const res = await apiRequest('POST', '/api/servers', serverData);
      return res.json();
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['/api/servers'] });
      toast({ title: 'Server added successfully' });
    },
    onError: (error: any) => {
      toast({ 
        title: 'Failed to add server', 
        description: error.message,
        variant: 'destructive'
      });
    },
  });
  
  // Update server mutation
  const updateServerMutation = useMutation({
    mutationFn: async ({ id, ...updates }: { id: string } & Partial<ServerConnection>) => {
      const res = await apiRequest('PUT', `/api/servers/${id}`, updates);
      return res.json();
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['/api/servers'] });
      toast({ title: 'Server updated successfully' });
      setEditDialogOpen(false);
      setSelectedServer(null);
    },
    onError: (error: any) => {
      toast({ 
        title: 'Failed to update server', 
        description: error.message,
        variant: 'destructive'
      });
    },
  });

  // Delete server mutation
  const deleteServerMutation = useMutation({
    mutationFn: async (id: string) => {
      const res = await apiRequest('DELETE', `/api/servers/${id}`);
      return res.json();
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['/api/servers'] });
      toast({ title: 'Server deleted successfully' });
      setDeleteDialogOpen(false);
      setSelectedServer(null);
      
      // Disconnect any active sessions for this server
      if (selectedServer) {
        setConnectionStates(prev => {
          const { [selectedServer.id]: removed, ...rest } = prev;
          return rest;
        });
        
        // Remove terminal sessions for deleted server
        setTerminalSessions(prev => prev.filter(session => session.serverId !== selectedServer.id));
      }
    },
    onError: (error: any) => {
      toast({ 
        title: 'Failed to delete server', 
        description: error.message,
        variant: 'destructive'
      });
    },
  });

  const [terminalSessions, setTerminalSessions] = useState<TerminalSession[]>([]);
  const [activeTab, setActiveTab] = useState<string>('');
  const [connectionStates, setConnectionStates] = useState<Record<string, 'connected' | 'disconnected' | 'connecting'>>({});
  
  // Initialize WebSocket connection
  useEffect(() => {
    terminalWS.connect();
    
    // Set up WebSocket event handlers
    terminalWS.onConnected(({ sessionId, serverId }) => {
      const server = Array.isArray(servers) ? servers.find((s: any) => s.id === serverId) : null;
      if (!server) return;
      
      const newSession: TerminalSession = {
        id: sessionId,
        serverId,
        serverName: server.name,
        isConnected: true,
        currentDirectory: `/home/${server.username}`,
        lines: [{
          id: `${sessionId}-welcome`,
          content: `Connected to ${server.name} (${server.host})`,
          type: 'output',
          timestamp: new Date()
        }]
      };
      
      setTerminalSessions(prev => [...prev, newSession]);
      setActiveTab(sessionId);
      setConnectionStates(prev => ({ ...prev, [serverId]: 'connected' }));
      
      toast({ title: 'Connected', description: `Connected to ${server.name}` });
    });
    
    terminalWS.onOutput(({ sessionId, data }) => {
      setTerminalSessions(prev => prev.map(session => 
        session.id === sessionId 
          ? {
              ...session, 
              lines: [...session.lines, {
                id: `${sessionId}-${Date.now()}`,
                content: data,
                type: 'output' as const,
                timestamp: new Date()
              }]
            }
          : session
      ));
    });
    
    terminalWS.onDisconnected(({ sessionId }) => {
      setTerminalSessions(prev => {
        const session = prev.find(s => s.id === sessionId);
        if (session) {
          setConnectionStates(prevStates => ({ ...prevStates, [session.serverId]: 'disconnected' }));
        }
        return prev.map(s => s.id === sessionId ? { ...s, isConnected: false } : s);
      });
      
      toast({ title: 'Disconnected', description: 'Terminal session disconnected' });
    });
    
    terminalWS.onError(({ message }) => {
      toast({ 
        title: 'Connection Error', 
        description: message,
        variant: 'destructive'
      });
    });
    
    return () => {
      terminalWS.disconnect();
    };
  }, [servers, toast]);

  const handleAddServer = (serverData: any) => {
    addServerMutation.mutate(serverData);
  };

  const handleEditServer = (serverId: string) => {
    const server = Array.isArray(servers) ? servers.find((s: any) => s.id === serverId) : null;
    if (server) {
      setSelectedServer({
        ...server,
        status: connectionStates[server.id] || 'disconnected'
      });
      setEditDialogOpen(true);
    }
  };

  const handleUpdateServer = (id: string, updates: any) => {
    updateServerMutation.mutate({ id, ...updates });
  };

  const handleDeleteServer = (serverId: string) => {
    const server = Array.isArray(servers) ? servers.find((s: any) => s.id === serverId) : null;
    if (server) {
      setSelectedServer({
        ...server,
        status: connectionStates[server.id] || 'disconnected'
      });
      setDeleteDialogOpen(true);
    }
  };

  const handleConfirmDelete = () => {
    if (selectedServer) {
      deleteServerMutation.mutate(selectedServer.id);
    }
  };

  const handleConnectServer = (serverId: string) => {
    const currentState = connectionStates[serverId] || 'disconnected';
    
    if (currentState === 'connected') {
      // Disconnect all sessions for this server
      const serverSessions = terminalSessions.filter(s => s.serverId === serverId);
      serverSessions.forEach(session => {
        terminalWS.disconnectSession(session.id);
      });
      setConnectionStates(prev => ({ ...prev, [serverId]: 'disconnected' }));
    } else {
      // Connect to server
      setConnectionStates(prev => ({ ...prev, [serverId]: 'connecting' }));
      try {
        terminalWS.connectToServer(serverId);
      } catch (error) {
        setConnectionStates(prev => ({ ...prev, [serverId]: 'disconnected' }));
        toast({ 
          title: 'Connection Failed', 
          description: 'Please check if WebSocket is connected',
          variant: 'destructive'
        });
      }
    }
  };

  const handleOpenTerminal = (serverId: string) => {
    const currentState = connectionStates[serverId] || 'disconnected';
    
    if (currentState !== 'connected') {
      // Auto-connect if not connected
      handleConnectServer(serverId);
    } else {
      // If already connected, just focus existing session
      const existingSession = terminalSessions.find(s => s.serverId === serverId && s.isConnected);
      if (existingSession) {
        setActiveTab(existingSession.id);
      }
    }
  };

  const handleCommand = (sessionId: string, command: string) => {
    // Add command to terminal display
    const commandLine: TerminalLine = {
      id: `${sessionId}-${Date.now()}-cmd`,
      content: command,
      type: 'command',
      timestamp: new Date()
    };
    
    setTerminalSessions(prev => prev.map(session => 
      session.id === sessionId 
        ? { ...session, lines: [...session.lines, commandLine] }
        : session
    ));
    
    // Send command via WebSocket
    try {
      terminalWS.sendCommand(sessionId, command);
    } catch (error) {
      toast({ 
        title: 'Command Failed', 
        description: 'WebSocket connection lost',
        variant: 'destructive'
      });
    }
  };

  const handleCloseTerminal = (sessionId: string) => {
    // Send disconnect message to backend
    try {
      terminalWS.forceDisconnectSession(sessionId);
    } catch (error) {
      console.warn('Failed to send disconnect message:', error);
    }
    
    setTerminalSessions(prev => prev.filter(session => session.id !== sessionId));
    if (activeTab === sessionId && terminalSessions.length > 1) {
      const remainingSessions = terminalSessions.filter(s => s.id !== sessionId);
      setActiveTab(remainingSessions[0]?.id || '');
    }
  };

  const style = {
    "--sidebar-width": "20rem",
    "--sidebar-width-icon": "4rem",
  };

  return (
    <SidebarProvider style={style as React.CSSProperties}>
      <div className="flex h-screen w-full">
        <AppSidebar
          servers={Array.isArray(servers) ? servers.map((server: any) => ({
            ...server,
            status: connectionStates[server.id] || 'disconnected'
          })) : []}
          onAddServer={handleAddServer}
          onConnectServer={handleConnectServer}
          onEditServer={handleEditServer}
          onOpenTerminal={handleOpenTerminal}
        />
        <div className="flex flex-col flex-1">
          <header className="flex items-center justify-between p-4 border-b">
            <div className="flex items-center gap-2">
              <SidebarTrigger data-testid="button-sidebar-toggle" />
              <h1 className="text-xl font-semibold">SSH Terminal Manager</h1>
            </div>
            <Button
              variant="ghost"
              size="icon"
              onClick={() => setTheme(theme === 'dark' ? 'light' : 'dark')}
              data-testid="button-theme-toggle"
            >
              {theme === 'dark' ? <Sun className="h-4 w-4" /> : <Moon className="h-4 w-4" />}
            </Button>
          </header>
          
          <main className="flex-1 overflow-hidden">
            {terminalSessions.length === 0 ? (
              <div className="flex items-center justify-center h-full">
                <div className="text-center" data-testid="empty-state">
                  <h2 className="text-2xl font-semibold mb-2">No Active Terminals</h2>
                  <p className="text-muted-foreground mb-4">
                    Connect to a server and open a terminal to get started.
                  </p>
                </div>
              </div>
            ) : (
              <Tabs value={activeTab} onValueChange={setActiveTab} className="h-full flex flex-col">
                <TabsList className="grid w-full" style={{ gridTemplateColumns: `repeat(${terminalSessions.length}, 1fr)` }}>
                  {terminalSessions.map((session) => (
                    <TabsTrigger 
                      key={session.id} 
                      value={session.id}
                      data-testid={`tab-${session.id}`}
                    >
                      {session.serverName}
                    </TabsTrigger>
                  ))}
                </TabsList>
                {terminalSessions.map((session) => (
                  <TabsContent key={session.id} value={session.id} className="flex-1 mt-0">
                    <TerminalWindow
                      serverId={session.serverId}
                      serverName={session.serverName}
                      isConnected={session.isConnected}
                      lines={session.lines}
                      currentDirectory={session.currentDirectory}
                      onCommand={(command) => handleCommand(session.id, command)}
                      onClose={() => handleCloseTerminal(session.id)}
                      onMinimize={() => console.log('Minimize:', session.id)}
                      onMaximize={() => console.log('Maximize:', session.id)}
                    />
                  </TabsContent>
                ))}
              </Tabs>
            )}
          </main>
        </div>
      </div>

      {/* Edit Server Dialog */}
      <EditServerDialog
        open={editDialogOpen}
        onOpenChange={setEditDialogOpen}
        server={selectedServer}
        onUpdateServer={handleUpdateServer}
        onDeleteServer={handleDeleteServer}
        isUpdating={updateServerMutation.isPending}
        isDeleting={deleteServerMutation.isPending}
      />

      {/* Delete Confirmation Dialog */}
      <DeleteServerDialog
        open={deleteDialogOpen}
        onOpenChange={setDeleteDialogOpen}
        serverName={selectedServer?.name || ''}
        onConfirmDelete={handleConfirmDelete}
        isDeleting={deleteServerMutation.isPending}
      />
    </SidebarProvider>
  );
}