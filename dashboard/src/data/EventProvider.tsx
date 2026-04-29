import {
  createContext,
  useContext,
  useEffect,
  useRef,
  useMemo,
  useState,
} from "react";
import type { AnyEvent, JobDetail } from "../types";

const POLL_INTERVAL_MS = 5_000;

export function mergeEvents(
  prev: AnyEvent[],
  incoming: AnyEvent[]
): AnyEvent[] {
  const knownIds = new Set(prev.map((e) => e.id));
  const novel = incoming.filter((e) => !knownIds.has(e.id));
  return novel.length > 0 ? [...prev, ...novel] : prev;
}
const PAGE_LIMIT = 1000;

export type EventListener = (events: AnyEvent[]) => void;

export interface EventContextValue {
  addEventListener: (callback: EventListener) => () => void;
  jobCache: Record<string, JobDetail>;
}

const EventContext = createContext<EventContextValue>({
  addEventListener: () => () => {},
  jobCache: {},
});

export function EventProvider({ children }: { children: React.ReactNode }) {
  const eventCacheRef = useRef<AnyEvent[]>([]);
  const listenersRef = useRef<Set<EventListener>>(new Set());
  const nextAfterRef = useRef<string | null>(null);
  const pendingJobFetchesRef = useRef<Set<string>>(new Set());
  const [jobCache, setJobCache] = useState<Record<string, JobDetail>>({});

  useEffect(() => {
    async function gc() {
      try {
        const res = await fetch("/gc", { method: "POST" });
        const data = await res.json();
        console.log("[EventProvider] GC completed:", data);
      } catch (err) {
        console.warn("[EventProvider] GC failed:", err);
      }
    }

    gc();
    const gcInterval = setInterval(gc, 60 * 60 * 1000);

    return () => clearInterval(gcInterval);
  }, []);

  useEffect(() => {
    let cancelled = false;

    async function poll() {
      while (!cancelled) {
        try {
          const params = new URLSearchParams({ limit: String(PAGE_LIMIT) });
          if (nextAfterRef.current) params.set("after", nextAfterRef.current);

          const res = await fetch(`/api/v1/events?${params}`);
          if (!res.ok) throw new Error(`HTTP ${res.status}`);

          const data: {
            events: AnyEvent[];
            next_after?: string;
          } = await res.json();

          if (data.events.length > 0) {
            const knownIds = new Set(eventCacheRef.current.map((e) => e.id));
            const newEvents = data.events.filter((e) => !knownIds.has(e.id));

            if (newEvents.length > 0) {
              eventCacheRef.current = [...eventCacheRef.current, ...newEvents];
              for (const cb of listenersRef.current) cb(newEvents);

              for (const e of newEvents) {
                if (e.type === "job_started") {
                  const jobId = (e as any).job_id as string;
                  if (!pendingJobFetchesRef.current.has(jobId)) {
                    pendingJobFetchesRef.current.add(jobId);
                    fetch(`/api/v1/job/${jobId}`)
                      .then((r) => (r.ok ? r.json() : null))
                      .then((detail: JobDetail | null) => {
                        if (detail)
                          setJobCache((prev) => ({
                            ...prev,
                            [jobId]: detail,
                          }));
                      })
                      .catch(() => {});
                  }
                }
              }
            }

            if (data.next_after) nextAfterRef.current = data.next_after;

            if (data.events.length >= PAGE_LIMIT) continue;
          }
        } catch (err) {
          console.error("[EventProvider] poll error:", err);
        }

        await new Promise<void>((r) => setTimeout(r, POLL_INTERVAL_MS));
      }
    }

    poll();
    return () => {
      cancelled = true;
    };
  }, []);

  const value = useMemo<EventContextValue>(
    () => ({
      addEventListener(callback: EventListener) {
        if (eventCacheRef.current.length > 0) callback(eventCacheRef.current);
        listenersRef.current.add(callback);
        return () => {
          listenersRef.current.delete(callback);
        };
      },
      jobCache,
    }),
    [jobCache]
  );

  return (
    <EventContext.Provider value={value}>{children}</EventContext.Provider>
  );
}

export function useEvents(): EventContextValue {
  return useContext(EventContext);
}
