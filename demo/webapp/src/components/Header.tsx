export function Header() {
  return (
    <header className="border-b border-border bg-panel/40 backdrop-blur">
      <div className="max-w-[1400px] mx-auto px-6 py-4 flex items-center justify-between">
        <div className="flex items-center gap-3">
          <div className="w-9 h-9 rounded-lg bg-gradient-to-br from-accent to-accent2 flex items-center justify-center font-bold text-bg">
            V
          </div>
          <div>
            <div className="font-semibold tracking-tight text-lg">VentStream</div>
            <div className="text-xs text-muted -mt-0.5">Live Event Dashboard</div>
          </div>
        </div>
        <div className="text-xs text-muted">
          <span className="hidden sm:inline">Apollo Client → graphql-transport-ws → JetStream</span>
        </div>
      </div>
    </header>
  );
}
