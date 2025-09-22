import { sql } from "drizzle-orm";
import { pgTable, text, varchar, integer, timestamp, boolean } from "drizzle-orm/pg-core";
import { createInsertSchema } from "drizzle-zod";
import { z } from "zod";

export const users = pgTable("users", {
  id: varchar("id").primaryKey().default(sql`gen_random_uuid()`),
  username: text("username").notNull().unique(),
  password: text("password").notNull(),
});

export const servers = pgTable("servers", {
  id: varchar("id").primaryKey().default(sql`gen_random_uuid()`),
  name: text("name").notNull(),
  host: text("host").notNull(),
  port: integer("port").notNull().default(22),
  username: text("username").notNull(),
  passwordEncrypted: text("password_encrypted").notNull(), // Encrypted password
  privateKeyEncrypted: text("private_key_encrypted"), // Encrypted SSH key
  createdAt: timestamp("created_at").defaultNow(),
  lastConnected: timestamp("last_connected"),
  isActive: boolean("is_active").default(true),
});

export const terminalSessions = pgTable("terminal_sessions", {
  id: varchar("id").primaryKey().default(sql`gen_random_uuid()`),
  serverId: varchar("server_id").notNull().references(() => servers.id),
  isActive: boolean("is_active").default(true),
  createdAt: timestamp("created_at").defaultNow(),
  lastActivity: timestamp("last_activity").defaultNow(),
  currentDirectory: text("current_directory").default("/"),
});

export const insertUserSchema = createInsertSchema(users).pick({
  username: true,
  password: true,
});

export const insertServerSchema = createInsertSchema(servers).omit({
  id: true,
  createdAt: true,
  lastConnected: true,
  isActive: true,
  passwordEncrypted: true,
  privateKeyEncrypted: true,
}).extend({
  password: z.string().min(1, "Password is required"),
  privateKey: z.string().optional(),
});

export const updateServerSchema = z.object({
  name: z.string().min(1).optional(),
  host: z.string().min(1).optional(),
  port: z.number().int().min(1).max(65535).optional(),
  username: z.string().min(1).optional(),
});

export const insertTerminalSessionSchema = createInsertSchema(terminalSessions).omit({
  id: true,
  createdAt: true,
  lastActivity: true,
  isActive: true,
});

export type InsertUser = z.infer<typeof insertUserSchema>;
export type User = typeof users.$inferSelect;
export type Server = typeof servers.$inferSelect;
export type InsertServer = z.infer<typeof insertServerSchema>;
export type UpdateServer = z.infer<typeof updateServerSchema>;
export type TerminalSession = typeof terminalSessions.$inferSelect;
export type InsertTerminalSession = z.infer<typeof insertTerminalSessionSchema>;

// WebSocket message types
export const wsMessageSchema = z.discriminatedUnion("type", [
  z.object({
    type: z.literal("connect"),
    serverId: z.string(),
  }),
  z.object({
    type: z.literal("disconnect"),
    sessionId: z.string(),
  }),
  z.object({
    type: z.literal("command"),
    sessionId: z.string(),
    command: z.string(),
  }),
  z.object({
    type: z.literal("resize"),
    sessionId: z.string(),
    cols: z.number(),
    rows: z.number(),
  }),
]);

export type WSMessage = z.infer<typeof wsMessageSchema>;
