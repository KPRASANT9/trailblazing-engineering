import type { Express } from "express";
import { createServer, type Server } from "http";
import { WebSocketServer, WebSocket } from "ws";
import { Client } from "ssh2";
import { storage } from "./storage";
import { insertServerSchema, updateServerSchema, wsMessageSchema, type WSMessage } from "@shared/schema";
import { z } from "zod";

// SSH connection manager
class SSHConnectionManager {
  private connections = new Map<string, Client>();
  private sessions = new Map<string, { conn: Client; stream: any; sessionId: string; serverId: string }>();
  private serverSessions = new Map<string, Set<string>>(); // serverId -> Set<sessionId>
  private wsSessions = new Map<WebSocket, Set<string>>(); // ws -> Set<sessionId>

  async connect(serverId: string, serverConfig: any): Promise<{ sessionId: string; conn: Client }> {
    const conn = new Client();
    
    return new Promise((resolve, reject) => {
      conn.on('ready', async () => {
        console.log(`SSH connection ready for server ${serverId}`);
        
        // Create terminal session in storage
        const session = await storage.createSession({
          serverId,
          currentDirectory: `/home/${serverConfig.username}`,
        });
        
        this.connections.set(serverId, conn);
        
        // Track server sessions
        if (!this.serverSessions.has(serverId)) {
          this.serverSessions.set(serverId, new Set());
        }
        this.serverSessions.get(serverId)!.add(session.id);
        
        resolve({ sessionId: session.id, conn });
      });
      
      conn.on('error', (err) => {
        console.error(`SSH connection error for server ${serverId}:`, err);
        reject(err);
      });
      
      conn.connect({
        host: serverConfig.host,
        port: serverConfig.port,
        username: serverConfig.username,
        password: serverConfig.password,
        // Add SSH key support later
      });
    });
  }

  async createShell(sessionId: string, conn: Client, serverId: string): Promise<any> {
    return new Promise((resolve, reject) => {
      conn.shell((err, stream) => {
        if (err) {
          reject(err);
          return;
        }
        
        // Set initial terminal size
        stream.setWindow(24, 80, 24, 80);
        
        this.sessions.set(sessionId, { conn, stream, sessionId, serverId });
        resolve(stream);
      });
    });
  }

  getSession(sessionId: string) {
    return this.sessions.get(sessionId);
  }
  
  private getServerIdForSession(sessionId: string): string {
    // Find serverId from session mapping
    const entries = Array.from(this.serverSessions.entries());
    for (const [serverId, sessionIds] of entries) {
      if (sessionIds.has(sessionId)) {
        return serverId;
      }
    }
    return '';
  }
  
  associateWebSocket(ws: WebSocket, sessionId: string) {
    if (!this.wsSessions.has(ws)) {
      this.wsSessions.set(ws, new Set());
    }
    this.wsSessions.get(ws)!.add(sessionId);
  }
  
  disconnectWebSocket(ws: WebSocket) {
    const sessionIds = this.wsSessions.get(ws);
    if (sessionIds) {
      Array.from(sessionIds).forEach(sessionId => {
        this.disconnect(sessionId);
      });
      this.wsSessions.delete(ws);
    }
  }

  disconnect(sessionId: string) {
    const session = this.sessions.get(sessionId);
    if (session) {
      session.stream?.end();
      session.conn?.end();
      this.sessions.delete(sessionId);
      
      // Remove from server sessions tracking
      if (session.serverId && this.serverSessions.has(session.serverId)) {
        this.serverSessions.get(session.serverId)!.delete(sessionId);
        if (this.serverSessions.get(session.serverId)!.size === 0) {
          this.serverSessions.delete(session.serverId);
        }
      }
      
      // Update session in storage
      storage.updateSession(sessionId, { isActive: false });
    }
  }

  disconnectServer(serverId: string) {
    const conn = this.connections.get(serverId);
    if (conn) {
      conn.end();
      this.connections.delete(serverId);
    }
    
    // Close all sessions for this server
    const sessionIds = this.serverSessions.get(serverId);
    if (sessionIds) {
      Array.from(sessionIds).forEach(sessionId => {
        this.disconnect(sessionId);
      });
    }
  }
}

const sshManager = new SSHConnectionManager();

export async function registerRoutes(app: Express): Promise<Server> {
  // Server management routes
  app.get('/api/servers', async (req, res) => {
    try {
      const servers = await storage.getAllServers();
      // Don't send encrypted credentials to client
      const safeServers = servers.map(({ passwordEncrypted, privateKeyEncrypted, ...server }) => server);
      res.json(safeServers);
    } catch (error) {
      res.status(500).json({ error: 'Failed to fetch servers' });
    }
  });

  app.post('/api/servers', async (req, res) => {
    try {
      console.log('Creating server with data:', req.body);
      const serverData = insertServerSchema.parse(req.body);
      console.log('Parsed server data:', serverData);
      const server = await storage.createServer(serverData);
      console.log('Server created successfully:', server.id);
      // Don't send encrypted credentials back
      const { passwordEncrypted, privateKeyEncrypted, ...safeServer } = server;
      res.json(safeServer);
    } catch (error: any) {
      console.error('Error creating server:', error);
      if (error instanceof z.ZodError) {
        res.status(400).json({ error: 'Invalid server data', details: error.errors });
      } else {
        res.status(500).json({ error: 'Failed to create server', details: error?.message || 'Unknown error' });
      }
    }
  });

  app.put('/api/servers/:id', async (req, res) => {
    try {
      const { id } = req.params;
      const updates = updateServerSchema.parse(req.body);
      const server = await storage.updateServer(id, updates);
      
      if (!server) {
        res.status(404).json({ error: 'Server not found' });
        return;
      }
      
      const { passwordEncrypted, privateKeyEncrypted, ...safeServer } = server;
      res.json(safeServer);
    } catch (error) {
      res.status(500).json({ error: 'Failed to update server' });
    }
  });

  app.delete('/api/servers/:id', async (req, res) => {
    try {
      const { id } = req.params;
      
      // Disconnect all sessions for this server
      sshManager.disconnectServer(id);
      
      const deleted = await storage.deleteServer(id);
      if (!deleted) {
        res.status(404).json({ error: 'Server not found' });
        return;
      }
      
      res.json({ success: true });
    } catch (error) {
      res.status(500).json({ error: 'Failed to delete server' });
    }
  });

  // Terminal session routes
  app.get('/api/sessions', async (req, res) => {
    try {
      const { serverId } = req.query;
      if (serverId) {
        const sessions = await storage.getActiveSessionsForServer(serverId as string);
        res.json(sessions);
      } else {
        res.status(400).json({ error: 'serverId is required' });
      }
    } catch (error) {
      res.status(500).json({ error: 'Failed to fetch sessions' });
    }
  });

  const httpServer = createServer(app);

  // WebSocket server for real-time terminal communication
  const wss = new WebSocketServer({ server: httpServer, path: '/ws' });
  
  wss.on('connection', (ws) => {
    console.log('WebSocket client connected');
    
    ws.on('message', async (data) => {
      try {
        const message: WSMessage = wsMessageSchema.parse(JSON.parse(data.toString()));
        
        switch (message.type) {
          case 'connect': {
            const server = await storage.getServer(message.serverId);
            if (!server) {
              ws.send(JSON.stringify({ type: 'error', message: 'Server not found' }));
              return;
            }
            
            // Get decrypted credentials
            const credentials = await storage.getServerCredentials(message.serverId);
            if (!credentials) {
              ws.send(JSON.stringify({ type: 'error', message: 'Failed to decrypt server credentials' }));
              return;
            }
            
            try {
              // Update server last connected time (note: updateServerSchema doesn't include lastConnected)
              // await storage.updateServer(message.serverId, { lastConnected: new Date() });
              
              const { sessionId, conn } = await sshManager.connect(message.serverId, { ...server, ...credentials });
              const stream = await sshManager.createShell(sessionId, conn, message.serverId);
              
              // Associate WebSocket with this session
              sshManager.associateWebSocket(ws, sessionId);
              
              // Send welcome message
              ws.send(JSON.stringify({
                type: 'connected',
                sessionId,
                serverId: message.serverId,
              }));
              
              // Stream SSH output to WebSocket
              stream.on('data', (data: Buffer) => {
                if (ws.readyState === WebSocket.OPEN) {
                  ws.send(JSON.stringify({
                    type: 'output',
                    sessionId,
                    data: data.toString(),
                  }));
                }
              });
              
              stream.on('close', () => {
                if (ws.readyState === WebSocket.OPEN) {
                  ws.send(JSON.stringify({
                    type: 'disconnected',
                    sessionId,
                  }));
                }
                sshManager.disconnect(sessionId);
              });
              
            } catch (error) {
              console.error('SSH connection failed:', error);
              ws.send(JSON.stringify({
                type: 'error',
                message: `Failed to connect: ${error instanceof Error ? error.message : 'Unknown error'}`,
              }));
            }
            break;
          }
          
          case 'command': {
            const session = sshManager.getSession(message.sessionId);
            if (session && session.stream) {
              session.stream.write(message.command + '\n');
              
              // Update session activity
              await storage.updateSession(message.sessionId, { lastActivity: new Date() });
            } else {
              ws.send(JSON.stringify({
                type: 'error',
                message: 'Session not found or not connected',
              }));
            }
            break;
          }
          
          case 'resize': {
            const session = sshManager.getSession(message.sessionId);
            if (session && session.stream) {
              session.stream.setWindow(message.rows, message.cols);
            }
            break;
          }
          
          case 'disconnect': {
            sshManager.disconnect(message.sessionId);
            ws.send(JSON.stringify({
              type: 'disconnected',
              sessionId: message.sessionId,
            }));
            break;
          }
        }
      } catch (error) {
        console.error('WebSocket message error:', error);
        ws.send(JSON.stringify({
          type: 'error',
          message: 'Invalid message format',
        }));
      }
    });
    
    ws.on('close', () => {
      console.log('WebSocket client disconnected');
      // Clean up all sessions associated with this WebSocket
      sshManager.disconnectWebSocket(ws);
    });
    
    ws.on('error', (error) => {
      console.error('WebSocket error:', error);
    });
  });

  return httpServer;
}
