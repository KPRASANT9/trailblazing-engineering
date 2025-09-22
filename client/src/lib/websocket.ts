import { WSMessage } from "@shared/schema";

type WSEventHandlers = {
  connected: (data: { sessionId: string; serverId: string }) => void;
  disconnected: (data: { sessionId: string }) => void;
  output: (data: { sessionId: string; data: string }) => void;
  error: (data: { message: string }) => void;
};

export class TerminalWebSocket {
  private ws: WebSocket | null = null;
  private handlers: Partial<WSEventHandlers> = {};
  private reconnectAttempts = 0;
  private maxReconnectAttempts = 5;
  private reconnectDelay = 1000;
  private messageQueue: WSMessage[] = [];

  connect() {
    if (this.ws?.readyState === WebSocket.OPEN) {
      return;
    }

    const protocol = window.location.protocol === "https:" ? "wss:" : "ws:";
    const wsUrl = `${protocol}//${window.location.host}/ws`;
    
    console.log('Connecting to WebSocket:', wsUrl);
    this.ws = new WebSocket(wsUrl);

    this.ws.onopen = () => {
      console.log('WebSocket connected');
      this.reconnectAttempts = 0;
      
      // Send any queued messages
      while (this.messageQueue.length > 0) {
        const message = this.messageQueue.shift()!
        this.ws!.send(JSON.stringify(message));
      }
    };

    this.ws.onmessage = (event) => {
      try {
        const message = JSON.parse(event.data);
        this.handleMessage(message);
      } catch (error) {
        console.error('Failed to parse WebSocket message:', error);
      }
    };

    this.ws.onclose = () => {
      console.log('WebSocket disconnected');
      this.attemptReconnect();
    };

    this.ws.onerror = (error) => {
      console.error('WebSocket error:', error);
    };
  }

  disconnect() {
    if (this.ws) {
      this.ws.close();
      this.ws = null;
    }
  }

  private attemptReconnect() {
    if (this.reconnectAttempts < this.maxReconnectAttempts) {
      this.reconnectAttempts++;
      console.log(`Attempting to reconnect (${this.reconnectAttempts}/${this.maxReconnectAttempts})...`);
      
      setTimeout(() => {
        this.connect();
      }, this.reconnectDelay * this.reconnectAttempts);
    } else {
      console.error('Max reconnection attempts reached');
      this.handlers.error?.({ message: 'Connection lost. Please refresh the page.' });
    }
  }

  private handleMessage(message: any) {
    switch (message.type) {
      case 'connected':
        this.handlers.connected?.(message);
        break;
      case 'disconnected':
        this.handlers.disconnected?.(message);
        break;
      case 'output':
        this.handlers.output?.(message);
        break;
      case 'error':
        this.handlers.error?.(message);
        break;
      default:
        console.warn('Unknown message type:', message.type);
    }
  }

  send(message: WSMessage) {
    if (this.ws?.readyState === WebSocket.OPEN) {
      this.ws.send(JSON.stringify(message));
    } else if (this.ws?.readyState === WebSocket.CONNECTING) {
      // Queue message until connected
      this.messageQueue.push(message);
    } else {
      console.error('WebSocket not connected');
      throw new Error('WebSocket not connected');
    }
  }

  // Event handler setters
  onConnected(handler: WSEventHandlers['connected']) {
    this.handlers.connected = handler;
  }

  onDisconnected(handler: WSEventHandlers['disconnected']) {
    this.handlers.disconnected = handler;
  }

  onOutput(handler: WSEventHandlers['output']) {
    this.handlers.output = handler;
  }

  onError(handler: WSEventHandlers['error']) {
    this.handlers.error = handler;
  }

  // Terminal actions
  connectToServer(serverId: string) {
    this.send({ type: 'connect', serverId });
  }
  
  forceDisconnectSession(sessionId: string) {
    this.send({ type: 'disconnect', sessionId });
  }

  sendCommand(sessionId: string, command: string) {
    this.send({ type: 'command', sessionId, command });
  }

  resizeTerminal(sessionId: string, cols: number, rows: number) {
    this.send({ type: 'resize', sessionId, cols, rows });
  }

  disconnectSession(sessionId: string) {
    this.send({ type: 'disconnect', sessionId });
  }

  getConnectionState(): string {
    if (!this.ws) return 'disconnected';
    
    switch (this.ws.readyState) {
      case WebSocket.CONNECTING: return 'connecting';
      case WebSocket.OPEN: return 'connected';
      case WebSocket.CLOSING: return 'disconnecting';
      case WebSocket.CLOSED: return 'disconnected';
      default: return 'unknown';
    }
  }
}

// Singleton instance
export const terminalWS = new TerminalWebSocket();