import { Card, CardContent, CardHeader } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Server, Wifi, WifiOff, Terminal, Settings } from "lucide-react";

export interface ServerCardProps {
  id: string;
  name: string;
  host: string;
  port: number;
  username: string;
  status: 'connected' | 'disconnected' | 'connecting';
  lastConnected?: Date;
  onConnect: (id: string) => void;
  onEdit: (id: string) => void;
  onTerminal: (id: string) => void;
}

export default function ServerCard({
  id,
  name,
  host,
  port,
  username,
  status,
  lastConnected,
  onConnect,
  onEdit,
  onTerminal
}: ServerCardProps) {
  const statusColors = {
    connected: 'bg-green-500',
    connecting: 'bg-yellow-500',
    disconnected: 'bg-gray-500'
  };

  const statusIcons = {
    connected: Wifi,
    connecting: Wifi,
    disconnected: WifiOff
  };

  const StatusIcon = statusIcons[status];

  return (
    <Card className="hover-elevate" data-testid={`card-server-${id}`}>
      <CardHeader className="flex flex-row items-center justify-between gap-2 space-y-0 pb-2">
        <div className="flex items-center gap-2">
          <Server className="h-4 w-4 text-muted-foreground" />
          <h3 className="font-medium text-sm" data-testid={`text-server-name-${id}`}>
            {name}
          </h3>
        </div>
        <div className="flex items-center gap-2">
          <div className={`w-2 h-2 rounded-full ${statusColors[status]}`} data-testid={`status-indicator-${id}`} />
          <Badge variant="secondary" className="text-xs" data-testid={`badge-status-${id}`}>
            {status}
          </Badge>
        </div>
      </CardHeader>
      <CardContent className="space-y-3">
        <div className="space-y-1">
          <div className="flex items-center gap-1 text-xs text-muted-foreground">
            <span data-testid={`text-connection-${id}`}>{username}@{host}:{port}</span>
          </div>
          {lastConnected && (
            <div className="text-xs text-muted-foreground" data-testid={`text-last-connected-${id}`}>
              Last: {lastConnected.toLocaleDateString()}
            </div>
          )}
        </div>
        <div className="flex gap-2">
          <Button
            size="sm"
            variant={status === 'connected' ? 'secondary' : 'default'}
            onClick={() => onConnect(id)}
            className="flex-1"
            data-testid={`button-connect-${id}`}
          >
            <StatusIcon className="h-3 w-3 mr-1" />
            {status === 'connected' ? 'Disconnect' : 'Connect'}
          </Button>
          <Button
            size="sm"
            variant="outline"
            onClick={() => onTerminal(id)}
            disabled={status !== 'connected'}
            data-testid={`button-terminal-${id}`}
          >
            <Terminal className="h-3 w-3" />
          </Button>
          <Button
            size="sm"
            variant="ghost"
            onClick={() => onEdit(id)}
            data-testid={`button-edit-${id}`}
          >
            <Settings className="h-3 w-3" />
          </Button>
        </div>
      </CardContent>
    </Card>
  );
}