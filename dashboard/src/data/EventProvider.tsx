import { createContext, useContext, useEffect, useRef, useMemo } from "react";
import type { AnyEvent } from "../types";

const POLL_INTERVAL_MS = 5_000;
const PAGE_LIMIT = 1000;

export type EventListener = (events: AnyEvent[]) => void;

export interface EventContextValue {
  addEventListener: (callback: EventListener) => () => void;
}

const EventContext = createContext<EventContextValue>({
  addEventListener: () => () => {},
});

export function EventProvider({ children }: { children: React.ReactNode }) {
  const eventCacheRef = useRef<AnyEvent[]>([]);
  const listenersRef = useRef<Set<EventListener>>(new Set());
  const nextAfterRef = useRef<string | null>(null);

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
            }

            if (data.next_after) nextAfterRef.current = data.next_after;

            // If we got a full page, immediately fetch the next one
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
    }),
    []
  );

  return (
    <EventContext.Provider value={value}>{children}</EventContext.Provider>
  );
}

export function useEvents(): EventContextValue {
  return useContext(EventContext);
}
