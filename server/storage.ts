import { type User, type InsertUser, type Server, type InsertServer, type UpdateServer, type TerminalSession, type InsertTerminalSession, users, servers, terminalSessions } from "@shared/schema";
import { randomUUID } from "crypto";
import { db } from "./db";
import { eq, and } from "drizzle-orm";
import { CredentialCrypto } from "./crypto";

// Storage interface for SSH terminal manager
export interface IStorage {
  // User methods
  getUser(id: string): Promise<User | undefined>;
  getUserByUsername(username: string): Promise<User | undefined>;
  createUser(user: InsertUser): Promise<User>;
  
  // Server methods
  getServer(id: string): Promise<Server | undefined>;
  getAllServers(): Promise<Server[]>;
  createServer(server: InsertServer): Promise<Server>;
  updateServer(id: string, updates: UpdateServer): Promise<Server | undefined>;
  getServerCredentials(id: string): Promise<{ password: string; privateKey?: string } | undefined>;
  deleteServer(id: string): Promise<boolean>;
  
  // Terminal session methods
  getSession(id: string): Promise<TerminalSession | undefined>;
  getActiveSessionsForServer(serverId: string): Promise<TerminalSession[]>;
  createSession(session: InsertTerminalSession): Promise<TerminalSession>;
  updateSession(id: string, updates: Partial<TerminalSession>): Promise<TerminalSession | undefined>;
  deleteSession(id: string): Promise<boolean>;
}

export class MemStorage implements IStorage {
  private users: Map<string, User>;
  private servers: Map<string, Server>;
  private sessions: Map<string, TerminalSession>;

  constructor() {
    this.users = new Map();
    this.servers = new Map();
    this.sessions = new Map();
  }

  // User methods
  async getUser(id: string): Promise<User | undefined> {
    return this.users.get(id);
  }

  async getUserByUsername(username: string): Promise<User | undefined> {
    return Array.from(this.users.values()).find(
      (user) => user.username === username,
    );
  }

  async createUser(insertUser: InsertUser): Promise<User> {
    const id = randomUUID();
    const user: User = { ...insertUser, id };
    this.users.set(id, user);
    return user;
  }

  // Server methods
  async getServer(id: string): Promise<Server | undefined> {
    return this.servers.get(id);
  }

  async getAllServers(): Promise<Server[]> {
    return Array.from(this.servers.values());
  }

  async createServer(insertServer: InsertServer): Promise<Server> {
    const id = randomUUID();
    const server: Server = {
      id,
      name: insertServer.name,
      username: insertServer.username,
      host: insertServer.host,
      port: insertServer.port ?? 22,
      passwordEncrypted: insertServer.password, // MemStorage uses plaintext for dev
      privateKeyEncrypted: insertServer.privateKey || null,
      createdAt: new Date(),
      lastConnected: null,
      isActive: true,
    };
    this.servers.set(id, server);
    return server;
  }

  async updateServer(id: string, updates: UpdateServer): Promise<Server | undefined> {
    const server = this.servers.get(id);
    if (!server) return undefined;
    
    const updatedServer = { ...server, ...updates };
    this.servers.set(id, updatedServer);
    return updatedServer;
  }

  async getServerCredentials(id: string): Promise<{ password: string; privateKey?: string } | undefined> {
    const server = this.servers.get(id);
    if (!server) return undefined;
    // For MemStorage, credentials are stored in plaintext in encrypted fields (for development only)
    return {
      password: server.passwordEncrypted || '',
      privateKey: server.privateKeyEncrypted || undefined
    };
  }

  async deleteServer(id: string): Promise<boolean> {
    return this.servers.delete(id);
  }

  // Terminal session methods
  async getSession(id: string): Promise<TerminalSession | undefined> {
    return this.sessions.get(id);
  }

  async getActiveSessionsForServer(serverId: string): Promise<TerminalSession[]> {
    return Array.from(this.sessions.values()).filter(
      session => session.serverId === serverId && session.isActive
    );
  }

  async createSession(insertSession: InsertTerminalSession): Promise<TerminalSession> {
    const id = randomUUID();
    const session: TerminalSession = {
      ...insertSession,
      id,
      isActive: true,
      createdAt: new Date(),
      lastActivity: new Date(),
      currentDirectory: insertSession.currentDirectory || "/",
    };
    this.sessions.set(id, session);
    return session;
  }

  async updateSession(id: string, updates: Partial<TerminalSession>): Promise<TerminalSession | undefined> {
    const session = this.sessions.get(id);
    if (!session) return undefined;
    
    const updatedSession = { ...session, ...updates, lastActivity: new Date() };
    this.sessions.set(id, updatedSession);
    return updatedSession;
  }

  async deleteSession(id: string): Promise<boolean> {
    return this.sessions.delete(id);
  }
}

export class DatabaseStorage implements IStorage {
  // User methods
  async getUser(id: string): Promise<User | undefined> {
    const [user] = await db.select().from(users).where(eq(users.id, id));
    return user || undefined;
  }

  async getUserByUsername(username: string): Promise<User | undefined> {
    const [user] = await db.select().from(users).where(eq(users.username, username));
    return user || undefined;
  }

  async createUser(insertUser: InsertUser): Promise<User> {
    const [user] = await db
      .insert(users)
      .values(insertUser)
      .returning();
    return user;
  }

  // Server methods
  async getServer(id: string): Promise<Server | undefined> {
    const [server] = await db.select().from(servers).where(and(eq(servers.id, id), eq(servers.isActive, true)));
    return server || undefined;
  }

  async getAllServers(): Promise<Server[]> {
    return await db.select().from(servers).where(eq(servers.isActive, true));
  }

  async createServer(insertServer: InsertServer): Promise<Server> {
    // Encrypt credentials before storing
    const passwordEncrypted = await CredentialCrypto.encrypt(insertServer.password);
    const privateKeyEncrypted = insertServer.privateKey ? 
      await CredentialCrypto.encrypt(insertServer.privateKey) : null;

    const { password, privateKey, ...serverData } = insertServer;
    
    const [server] = await db
      .insert(servers)
      .values({
        ...serverData,
        passwordEncrypted,
        privateKeyEncrypted
      })
      .returning();
    return server;
  }

  async updateServer(id: string, updates: UpdateServer): Promise<Server | undefined> {
    const [server] = await db
      .update(servers)
      .set(updates)
      .where(and(eq(servers.id, id), eq(servers.isActive, true)))
      .returning();
    return server || undefined;
  }

  async getServerCredentials(id: string): Promise<{ password: string; privateKey?: string } | undefined> {
    const [server] = await db
      .select({ passwordEncrypted: servers.passwordEncrypted, privateKeyEncrypted: servers.privateKeyEncrypted })
      .from(servers)
      .where(and(eq(servers.id, id), eq(servers.isActive, true)));
      
    if (!server) return undefined;

    try {
      const password = await CredentialCrypto.decrypt(server.passwordEncrypted);
      const privateKey = server.privateKeyEncrypted ? 
        await CredentialCrypto.decrypt(server.privateKeyEncrypted) : undefined;
      
      return { password, privateKey };
    } catch (error) {
      console.error('Failed to decrypt server credentials:', error);
      return undefined;
    }
  }

  async deleteServer(id: string): Promise<boolean> {
    const result = await db
      .delete(servers)
      .where(eq(servers.id, id));
    return result.rowCount ? result.rowCount > 0 : false;
  }

  // Terminal session methods
  async getSession(id: string): Promise<TerminalSession | undefined> {
    const [session] = await db.select().from(terminalSessions).where(eq(terminalSessions.id, id));
    return session || undefined;
  }

  async getActiveSessionsForServer(serverId: string): Promise<TerminalSession[]> {
    return await db
      .select()
      .from(terminalSessions)
      .where(and(
        eq(terminalSessions.serverId, serverId),
        eq(terminalSessions.isActive, true)
      ));
  }

  async createSession(insertSession: InsertTerminalSession): Promise<TerminalSession> {
    const [session] = await db
      .insert(terminalSessions)
      .values(insertSession)
      .returning();
    return session;
  }

  async updateSession(id: string, updates: Partial<TerminalSession>): Promise<TerminalSession | undefined> {
    const [session] = await db
      .update(terminalSessions)
      .set({ ...updates, lastActivity: new Date() })
      .where(eq(terminalSessions.id, id))
      .returning();
    return session || undefined;
  }

  async deleteSession(id: string): Promise<boolean> {
    const result = await db
      .update(terminalSessions)
      .set({ isActive: false })
      .where(eq(terminalSessions.id, id));
    return result.rowCount ? result.rowCount > 0 : false;
  }
}

export const storage = new DatabaseStorage();
