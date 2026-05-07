import { useEffect, useState } from "react";
import { Bot, Database, Globe } from "lucide-react";
import EmptyState from "@/components/EmptyState";
import type { DataSource, FinancialInstrument } from "@/types/warehouse";
import { loadDataSources, loadInstruments } from "@/lib/warehouseApi";

export default function Assistant() {
  const [instruments, setInstruments] = useState<FinancialInstrument[]>([]);
  const [sources, setSources] = useState<DataSource[]>([]);

  useEffect(() => {
    let cancelled = false;

    async function load() {
      const [nextInstruments, nextSources] = await Promise.all([
        loadInstruments(),
        loadDataSources(),
      ]);

      if (!cancelled) {
        setInstruments(nextInstruments);
        setSources(nextSources);
      }
    }

    load();

    return () => {
      cancelled = true;
    };
  }, []);

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-bold text-foreground">LLM / Agentic AI Consumer</h1>
        <p className="text-sm text-muted-foreground mt-1">
          Agent UI reserved for real MCP tools and backend integrations
        </p>
      </div>

      <div className="grid grid-cols-1 gap-4 md:grid-cols-2">
        <div className="rounded-xl border border-border bg-card p-5">
          <div className="mb-4 flex items-center gap-2">
            <Database className="h-4 w-4 text-primary" />
            <h2 className="text-sm font-semibold text-card-foreground">Warehouse Instruments</h2>
          </div>
          {instruments.length ? (
            <div className="space-y-2">
              {instruments.slice(0, 8).map((instrument) => (
                <div key={instrument.id} className="flex items-center justify-between gap-3 rounded-lg bg-muted/30 px-3 py-2">
                  <div className="min-w-0">
                    <p className="truncate text-sm font-medium text-card-foreground">{instrument.symbol}</p>
                    <p className="truncate text-xs text-muted-foreground">{instrument.id}</p>
                  </div>
                  <span className="rounded-md bg-secondary px-2 py-0.5 text-xs font-medium text-secondary-foreground">
                    {instrument.class}
                  </span>
                </div>
              ))}
            </div>
          ) : (
            <EmptyState title="No instruments available to agents" />
          )}
        </div>

        <div className="rounded-xl border border-border bg-card p-5">
          <div className="mb-4 flex items-center gap-2">
            <Globe className="h-4 w-4 text-primary" />
            <h2 className="text-sm font-semibold text-card-foreground">Warehouse Sources</h2>
          </div>
          {sources.length ? (
            <div className="space-y-2">
              {sources.map((source) => (
                <div key={source.id} className="rounded-lg bg-muted/30 px-3 py-2">
                  <p className="truncate text-sm font-medium text-card-foreground">{source.name}</p>
                  <p className="truncate text-xs text-muted-foreground">{source.id}</p>
                </div>
              ))}
            </div>
          ) : (
            <EmptyState title="No data sources available to agents" />
          )}
        </div>
      </div>

      <div className="rounded-xl border border-border bg-card p-5">
        <div className="mb-4 flex items-center gap-2">
          <Bot className="h-4 w-4 text-primary" />
          <h2 className="text-sm font-semibold text-card-foreground">Agent Runtime</h2>
        </div>
        <EmptyState
          title="No real agent backend connected"
          description="Market insights, risk analysis, and financial recommendation tools should be attached after their backend/MCP endpoints exist."
        />
      </div>
    </div>
  );
}
