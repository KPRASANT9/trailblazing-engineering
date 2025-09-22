# Remote Server Management Tool - Design Guidelines

## Design Approach
**System-Based Approach**: Following **Material Design** principles for a professional, terminal-focused interface that prioritizes functionality and information clarity over decorative elements.

## Core Design Elements

### Color Palette
- **Primary**: 240 15% 15% (Dark slate for backgrounds)
- **Secondary**: 200 20% 25% (Steel blue for panels)  
- **Accent**: 120 40% 45% (Muted green for success states)
- **Terminal Green**: 120 60% 70% (Classic terminal text)
- **Warning**: 30 80% 60% (Orange for alerts)
- **Error**: 0 70% 55% (Red for errors)

**Dark Mode Focus**: This is primarily a dark-themed application reflecting traditional terminal aesthetics.

### Typography
- **Primary**: JetBrains Mono (monospace for terminal content)
- **Secondary**: Inter (sans-serif for UI elements)
- **Sizes**: Use consistent scale - text-sm, text-base, text-lg, text-xl

### Layout System
**Tailwind Spacing**: Standardize on units of **2, 4, 6, and 8** for consistent spacing (p-2, m-4, gap-6, h-8).

### Component Library

**Core UI Elements:**
- **Terminal Window**: Dark background with green text, scrollable output area
- **Connection Cards**: Server status indicators with connection details
- **Command Input**: Bottom-anchored input with autocomplete
- **Sidebar Navigation**: Collapsible server list with status dots

**Navigation:**
- **Primary Nav**: Left sidebar with server connections
- **Tab System**: Multiple terminal sessions
- **Breadcrumbs**: Current directory path display

**Forms:**
- **Connection Setup**: Server credentials and SSH key management
- **Settings Panel**: Terminal preferences and appearance options

**Data Displays:**
- **Server Metrics**: CPU, memory, disk usage widgets
- **File Browser**: Directory tree with terminal integration
- **Command History**: Searchable command log

**Overlays:**
- **Connection Modal**: New server setup dialog
- **Settings Drawer**: Sliding panel for preferences
- **Error Notifications**: Toast messages for connection issues

### Key Design Principles

1. **Terminal-First**: Interface should feel familiar to terminal users
2. **Multi-Session**: Support multiple concurrent SSH connections
3. **Responsive**: Optimized for iPad landscape and portrait modes
4. **Status-Aware**: Clear visual feedback for connection states
5. **Keyboard-Friendly**: Extensive keyboard shortcuts and navigation

### iPad-Specific Considerations

- **Touch Targets**: Minimum 44px for iPad finger navigation
- **Split View**: Support for iPad multitasking
- **Keyboard**: External keyboard shortcuts for power users
- **Gesture Support**: Swipe gestures for tab switching and navigation

### Performance Features

- **Connection Pooling**: Visual indicators for active/inactive connections
- **Command Caching**: Recent commands easily accessible
- **Session Persistence**: Reconnect to previous sessions seamlessly

This design creates a professional, efficient terminal management experience that bridges the gap between traditional command-line interfaces and modern touch-based interaction patterns.