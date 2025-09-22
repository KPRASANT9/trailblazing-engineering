import TerminalWindow, { TerminalLine } from '../TerminalWindow';

export default function TerminalWindowExample() {
  //todo: remove mock functionality
  const mockLines: TerminalLine[] = [
    {
      id: '1',
      content: 'ls -la',
      type: 'command',
      timestamp: new Date()
    },
    {
      id: '2',
      content: 'total 48\ndrwxr-xr-x  12 user  staff   384 Jan 15 10:30 .\ndrwxr-xr-x   6 user  staff   192 Jan 14 15:20 ..\n-rw-r--r--   1 user  staff  1024 Jan 15 09:45 config.json\ndrwxr-xr-x   8 user  staff   256 Jan 15 10:15 logs\n-rwxr-xr-x   1 user  staff  2048 Jan 15 08:30 startup.sh',
      type: 'output',
      timestamp: new Date()
    },
    {
      id: '3',
      content: 'cat config.json',
      type: 'command',
      timestamp: new Date()
    },
    {
      id: '4',
      content: '{\n  "server": {\n    "port": 3000,\n    "host": "0.0.0.0"\n  },\n  "database": {\n    "url": "postgresql://localhost:5432/myapp"\n  }\n}',
      type: 'output',
      timestamp: new Date()
    },
    {
      id: '5',
      content: 'sudo systemctl restart nginx',
      type: 'command',
      timestamp: new Date()
    },
    {
      id: '6',
      content: 'Permission denied. Please run as root.',
      type: 'error',
      timestamp: new Date()
    }
  ];

  return (
    <div className="h-[600px] p-4">
      <TerminalWindow
        serverId="demo-server"
        serverName="Production Server"
        isConnected={true}
        lines={mockLines}
        currentDirectory="/home/user/app"
        onCommand={(command) => console.log('Command executed:', command)}
        onClose={() => console.log('Terminal closed')}
        onMinimize={() => console.log('Terminal minimized')}
        onMaximize={() => console.log('Terminal maximized')}
      />
    </div>
  );
}