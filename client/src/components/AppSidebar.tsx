import {
  Sidebar,
  SidebarContent,
  SidebarGroup,
  SidebarGroupContent,
  SidebarGroupLabel,
  SidebarHeader,
  SidebarMenu,
  SidebarMenuButton,
  SidebarMenuItem,
} from "@/components/ui/sidebar";
import { Terminal, Server, Settings, FolderOpen } from "lucide-react";
import AddServerDialog from "./AddServerDialog";
import ServerCard from "./ServerCard";
import { ScrollArea } from "@/components/ui/scroll-area";

export interface ServerConnection {
  id: string;
  name: string;
  host: string;
  port: number;
  username: string;
  status: 'connected' | 'disconnected' | 'connecting';
  lastConnected?: Date;
}

export interface AppSidebarProps {
  servers: ServerConnection[];
  onAddServer: (server: any) => void;
  onConnectServer: (id: string) => void;
  onEditServer: (id: string) => void;
  onOpenTerminal: (id: string) => void;
}

export function AppSidebar({
  servers,
  onAddServer,
  onConnectServer,
  onEditServer,
  onOpenTerminal
}: AppSidebarProps) {
  const menuItems = [
    {
      title: "Terminals",
      icon: Terminal,
      url: "/terminals",
    },
    {
      title: "File Browser",
      icon: FolderOpen,
      url: "/files",
    },
    {
      title: "Settings",
      icon: Settings,
      url: "/settings",
    },
  ];

  return (
    <Sidebar data-testid="app-sidebar">
      <SidebarHeader className="p-4">
        <div className="flex items-center gap-2">
          <Terminal className="h-6 w-6" />
          <h1 className="font-semibold text-lg">SSH Terminal</h1>
        </div>
      </SidebarHeader>
      <SidebarContent>
        <SidebarGroup>
          <SidebarGroupLabel>Navigation</SidebarGroupLabel>
          <SidebarGroupContent>
            <SidebarMenu>
              {menuItems.map((item) => (
                <SidebarMenuItem key={item.title}>
                  <SidebarMenuButton asChild data-testid={`nav-${item.title.toLowerCase()}`}>
                    <a href={item.url}>
                      <item.icon />
                      <span>{item.title}</span>
                    </a>
                  </SidebarMenuButton>
                </SidebarMenuItem>
              ))}
            </SidebarMenu>
          </SidebarGroupContent>
        </SidebarGroup>

        <SidebarGroup>
          <SidebarGroupLabel className="flex items-center justify-between">
            <span>Servers ({servers.length})</span>
            <AddServerDialog onAddServer={onAddServer} />
          </SidebarGroupLabel>
          <SidebarGroupContent>
            <ScrollArea className="h-[400px]">
              <div className="space-y-2 p-1" data-testid="servers-list">
                {servers.length === 0 ? (
                  <div className="text-sm text-muted-foreground text-center py-8">
                    No servers configured
                  </div>
                ) : (
                  servers.map((server) => (
                    <ServerCard
                      key={server.id}
                      {...server}
                      onConnect={onConnectServer}
                      onEdit={onEditServer}
                      onTerminal={onOpenTerminal}
                    />
                  ))
                )}
              </div>
            </ScrollArea>
          </SidebarGroupContent>
        </SidebarGroup>
      </SidebarContent>
    </Sidebar>
  );
}