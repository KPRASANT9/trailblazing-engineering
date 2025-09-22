import { Card, CardContent, CardHeader } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Input } from "@/components/ui/input";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Terminal, X, Minimize2, Maximize2 } from "lucide-react";
import { useState, useRef, useEffect } from "react";

export interface TerminalLine {
  id: string;
  content: string;
  type: 'command' | 'output' | 'error';
  timestamp: Date;
}

export interface TerminalWindowProps {
  serverId: string;
  serverName: string;
  isConnected: boolean;
  lines: TerminalLine[];
  currentDirectory: string;
  onCommand: (command: string) => void;
  onClose: () => void;
  onMinimize: () => void;
  onMaximize: () => void;
}

export default function TerminalWindow({
  serverId,
  serverName,
  isConnected,
  lines,
  currentDirectory,
  onCommand,
  onClose,
  onMinimize,
  onMaximize
}: TerminalWindowProps) {
  const [currentCommand, setCurrentCommand] = useState('');
  const [commandHistory, setCommandHistory] = useState<string[]>([]);
  const [historyIndex, setHistoryIndex] = useState(-1);
  const inputRef = useRef<HTMLInputElement>(null);
  const scrollRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (scrollRef.current) {
      scrollRef.current.scrollTop = scrollRef.current.scrollHeight;
    }
  }, [lines]);

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    if (!currentCommand.trim() || !isConnected) return;

    // Add to history
    setCommandHistory(prev => [...prev, currentCommand]);
    setHistoryIndex(-1);
    
    onCommand(currentCommand);
    setCurrentCommand('');
  };

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === 'ArrowUp') {
      e.preventDefault();
      if (commandHistory.length > 0) {
        const newIndex = historyIndex === -1 ? commandHistory.length - 1 : Math.max(0, historyIndex - 1);
        setHistoryIndex(newIndex);
        setCurrentCommand(commandHistory[newIndex]);
      }
    } else if (e.key === 'ArrowDown') {
      e.preventDefault();
      if (historyIndex >= 0) {
        const newIndex = historyIndex + 1;
        if (newIndex >= commandHistory.length) {
          setHistoryIndex(-1);
          setCurrentCommand('');
        } else {
          setHistoryIndex(newIndex);
          setCurrentCommand(commandHistory[newIndex]);
        }
      }
    }
  };

  const getLineColor = (type: TerminalLine['type']) => {
    switch (type) {
      case 'command': return 'text-blue-400';
      case 'error': return 'text-red-400';
      default: return 'text-green-400';
    }
  };

  return (
    <Card className="h-full flex flex-col" data-testid={`terminal-window-${serverId}`}>
      <CardHeader className="flex flex-row items-center justify-between gap-2 space-y-0 pb-2">
        <div className="flex items-center gap-2">
          <Terminal className="h-4 w-4" />
          <span className="font-mono text-sm" data-testid={`text-terminal-title-${serverId}`}>
            {serverName}
          </span>
          <Badge 
            variant={isConnected ? 'default' : 'secondary'} 
            className="text-xs"
            data-testid={`badge-connection-status-${serverId}`}
          >
            {isConnected ? 'Connected' : 'Disconnected'}
          </Badge>
        </div>
        <div className="flex items-center gap-1">
          <Button 
            size="icon" 
            variant="ghost" 
            onClick={onMinimize}
            className="h-6 w-6"
            data-testid={`button-minimize-${serverId}`}
          >
            <Minimize2 className="h-3 w-3" />
          </Button>
          <Button 
            size="icon" 
            variant="ghost" 
            onClick={onMaximize}
            className="h-6 w-6"
            data-testid={`button-maximize-${serverId}`}
          >
            <Maximize2 className="h-3 w-3" />
          </Button>
          <Button 
            size="icon" 
            variant="ghost" 
            onClick={onClose}
            className="h-6 w-6"
            data-testid={`button-close-${serverId}`}
          >
            <X className="h-3 w-3" />
          </Button>
        </div>
      </CardHeader>
      <CardContent className="flex-1 flex flex-col gap-2 p-0">
        <ScrollArea className="flex-1 p-4 bg-black/90 text-green-400 font-mono text-sm" ref={scrollRef}>
          <div className="space-y-1" data-testid={`terminal-output-${serverId}`}>
            {lines.map((line) => (
              <div key={line.id} className={`whitespace-pre-wrap ${getLineColor(line.type)}`}>
                {line.type === 'command' && (
                  <span className="text-blue-300">$ </span>
                )}
                {line.content}
              </div>
            ))}
          </div>
        </ScrollArea>
        
        <form onSubmit={handleSubmit} className="p-4 border-t">
          <div className="flex items-center gap-2">
            <span className="font-mono text-sm text-muted-foreground" data-testid={`text-current-dir-${serverId}`}>
              {currentDirectory}$
            </span>
            <Input
              ref={inputRef}
              value={currentCommand}
              onChange={(e) => setCurrentCommand(e.target.value)}
              onKeyDown={handleKeyDown}
              placeholder={isConnected ? "Enter command..." : "Not connected"}
              disabled={!isConnected}
              className="font-mono bg-black/50 border-gray-600 text-green-400"
              data-testid={`input-command-${serverId}`}
            />
          </div>
        </form>
      </CardContent>
    </Card>
  );
}