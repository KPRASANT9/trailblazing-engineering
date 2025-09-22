# SSH Terminal Manager - Remote Server Management Tool

## Overview

A professional web-based SSH terminal manager designed for remote server administration with a terminal-focused interface. Built with React, Express, and WebSocket technology, it provides secure SSH connections with an interface optimized for iPad and touch devices while maintaining the classic terminal aesthetic with Material Design principles.

## User Preferences

Preferred communication style: Simple, everyday language.

## System Architecture

### Frontend Architecture
- **Framework**: React with TypeScript using Vite for development and build tooling
- **UI Library**: Shadcn/ui components built on Radix UI primitives for accessibility
- **Styling**: Tailwind CSS with custom design system following Material Design principles
- **State Management**: TanStack Query for server state management and caching
- **Routing**: Wouter for lightweight client-side routing
- **Theme System**: Custom dark-mode focused theme provider with CSS variables

### Backend Architecture
- **Framework**: Express.js with TypeScript
- **Real-time Communication**: WebSocket Server for terminal session management
- **SSH Integration**: ssh2 library for secure server connections
- **Session Management**: In-memory connection manager with persistent session tracking
- **API Design**: RESTful endpoints for server CRUD operations with WebSocket for terminal I/O

### Data Storage Solutions
- **Database**: PostgreSQL with Drizzle ORM for type-safe database operations
- **Connection Pooling**: Neon serverless PostgreSQL with connection pooling
- **Schema Management**: Drizzle migrations for database versioning
- **Session Storage**: Database-backed terminal session persistence

### Security & Encryption
- **Credential Security**: AES-256-GCM encryption for storing SSH passwords and private keys
- **Key Derivation**: PBKDF2 with scrypt for secure password-based encryption
- **Environment Variables**: Master key management for credential encryption/decryption
- **SSH Authentication**: Support for both password and key-based authentication

### Terminal Session Management
- **Connection Pooling**: Reusable SSH connections per server
- **Session Isolation**: Individual terminal sessions with state tracking
- **Real-time Sync**: WebSocket-based bidirectional terminal I/O
- **Command History**: Persistent command history per session
- **Directory Tracking**: Current working directory persistence

## External Dependencies

### Core Dependencies
- **@neondatabase/serverless**: PostgreSQL serverless database client
- **drizzle-orm**: Type-safe ORM with PostgreSQL dialect
- **ssh2**: SSH2 client for secure server connections
- **ws**: WebSocket library for real-time terminal communication

### UI Framework
- **@radix-ui/***: Headless UI components for accessibility
- **@tanstack/react-query**: Server state management and caching
- **react-hook-form**: Form validation and management
- **@hookform/resolvers**: Zod integration for form validation

### Development Tools
- **tsx**: TypeScript execution for development
- **esbuild**: Fast JavaScript bundler for production builds
- **tailwindcss**: Utility-first CSS framework
- **vite**: Development server and build tool

### Security
- **crypto**: Node.js built-in encryption module
- **zod**: Runtime type validation and schema definition
- **connect-pg-simple**: PostgreSQL session store for Express