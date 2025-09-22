import ServerCard from '../ServerCard';

export default function ServerCardExample() {
  return (
    <div className="p-4 space-y-4">
      <ServerCard
        id="server-1"
        name="Production Server"
        host="192.168.1.100"
        port={22}
        username="admin"
        status="connected"
        lastConnected={new Date('2024-01-15')}
        onConnect={(id) => console.log('Connect clicked for:', id)}
        onEdit={(id) => console.log('Edit clicked for:', id)}
        onTerminal={(id) => console.log('Terminal clicked for:', id)}
      />
      <ServerCard
        id="server-2"
        name="Development Server"
        host="dev.example.com"
        port={2222}
        username="developer"
        status="disconnected"
        lastConnected={new Date('2024-01-14')}
        onConnect={(id) => console.log('Connect clicked for:', id)}
        onEdit={(id) => console.log('Edit clicked for:', id)}
        onTerminal={(id) => console.log('Terminal clicked for:', id)}
      />
      <ServerCard
        id="server-3"
        name="Staging Environment"
        host="staging.example.com"
        port={22}
        username="deploy"
        status="connecting"
        onConnect={(id) => console.log('Connect clicked for:', id)}
        onEdit={(id) => console.log('Edit clicked for:', id)}
        onTerminal={(id) => console.log('Terminal clicked for:', id)}
      />
    </div>
  );
}