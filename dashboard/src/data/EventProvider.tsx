import {
  createContext,
  useContext,
  useEffect,
  useRef,
  useMemo,
  useState,
} from "react";
import type { AnyEvent, BackendJobSummary, JobDetail } from "../types";

const POLL_INTERVAL_MS = 5_000;
const PAGE_LIMIT = 1000;

export function mergeEvents(
  prev: AnyEvent[],
  incoming: AnyEvent[]
): AnyEvent[] {
  const knownIds = new Set(prev.map((e) => e.id));
  const novel = incoming.filter((e) => !knownIds.has(e.id));
  return novel.length > 0 ? [...prev, ...novel] : prev;
}

export type EventListener = (events: AnyEvent[]) => void;

export interface EventContextValue {
  jobs: BackendJobSummary[];
  addJobEventListener: (jobId: string, cb: EventListener) => () => void;
  jobCache: Record<string, JobDetail>;
}

const EventContext = createContext<EventContextValue>({
  jobs: [],
  addJobEventListener: () => () => {},
  jobCache: {},
});

export function EventProvider({ children }: { children: React.ReactNode }) {
  const [jobs, setJobs] = useState<BackendJobSummary[]>([]);
  const [jobCache, setJobCache] = useState<Record<string, JobDetail>>({});

  // Per-job event caches, cursors, listener sets, and polling flags.
  const jobEventCacheRef = useRef<Record<string, AnyEvent[]>>({});
  const jobCursorRef = useRef<Record<string, string | null>>({});
  const jobListenersRef = useRef<Record<string, Set<EventListener>>>({});
  const jobPollingRef = useRef<Record<string, boolean>>({});

  const pendingJobFetchesRef = useRef<Set<string>>(new Set());

  // GC every hour.
  useEffect(() => {
    async function gc() {
      try {
        const data = await fetch("/gc", { method: "POST" }).then((r) =>
          r.json()
        );
        console.log("[EventProvider] GC completed:", data);
      } catch (err) {
        console.warn("[EventProvider] GC failed:", err);
      }
    }
    gc();
    const id = setInterval(gc, 60 * 60 * 1000);
    return () => clearInterval(id);
  }, []);

  // Jobs polling — replaces the old global event stream.
  useEffect(() => {
    let cancelled = false;

    async function pollJobs() {
      while (!cancelled) {
        try {
          const res = await fetch("/jobs/summary");
          if (res.ok) {
            const data: BackendJobSummary[] = await res.json();
            setJobs(data);

            // Eagerly fetch full job detail for any new job IDs.
            for (const j of data) {
              if (!pendingJobFetchesRef.current.has(j.jobID)) {
                pendingJobFetchesRef.current.add(j.jobID);
                fetch(`/api/v1/job/${j.jobID}`)
                  .then((r) => (r.ok ? r.json() : null))
                  .then((detail: JobDetail | null) => {
                    if (detail)
                      setJobCache((prev) => ({ ...prev, [j.jobID]: detail }));
                  })
                  .catch(() => {});
              }
            }
          }
        } catch (err) {
          console.error("[EventProvider] jobs poll error:", err);
        }
        await new Promise<void>((r) => setTimeout(r, POLL_INTERVAL_MS));
      }
    }

    pollJobs();
    return () => {
      cancelled = true;
    };
  }, []);

  const addJobEventListener = useMemo(
    () => (jobId: string, cb: EventListener): (() => void) => {
      // Initialise per-job structures lazily.
      if (!jobListenersRef.current[jobId])
        jobListenersRef.current[jobId] = new Set();
      if (!jobEventCacheRef.current[jobId])
        jobEventCacheRef.current[jobId] = [];
      if (!(jobId in jobCursorRef.current)) jobCursorRef.current[jobId] = null;

      // Replay existing cache immediately.
      if (jobEventCacheRef.current[jobId].length > 0)
        cb(jobEventCacheRef.current[jobId]);

      jobListenersRef.current[jobId].add(cb);

      // Start per-job polling loop if not already running.
      if (!jobPollingRef.current[jobId]) {
        jobPollingRef.current[jobId] = true;

        (async () => {
          while (jobPollingRef.current[jobId]) {
            try {
              const params = new URLSearchParams({
                job_id: jobId,
                limit: String(PAGE_LIMIT),
              });
              const cursor = jobCursorRef.current[jobId];
              if (cursor) params.set("after", cursor);

              const res = await fetch(`/api/v1/events?${params}`);
              if (!res.ok) throw new Error(`HTTP ${res.status}`);

              const data: {
                events: AnyEvent[];
                next_after?: string;
              } = await res.json();

              if (data.events.length > 0) {
                const knownIds = new Set(
                  jobEventCacheRef.current[jobId].map((e) => e.id)
                );
                const newEvents = data.events.filter(
                  (e) => !knownIds.has(e.id)
                );

                if (newEvents.length > 0) {
                  jobEventCacheRef.current[jobId] = [
                    ...jobEventCacheRef.current[jobId],
                    ...newEvents,
                  ];
                  for (const listener of jobListenersRef.current[jobId] ?? [])
                    listener(newEvents);
                }

                if (data.next_after)
                  jobCursorRef.current[jobId] = data.next_after;

                // Drain remaining pages without sleeping.
                if (data.events.length >= PAGE_LIMIT) continue;
              }
            } catch (err) {
              console.error(
                `[EventProvider] job event poll error (${jobId}):`,
                err
              );
            }

            // Stop if no listeners remain.
            if (
              !jobListenersRef.current[jobId] ||
              jobListenersRef.current[jobId].size === 0
            )
              break;

            await new Promise<void>((r) => setTimeout(r, POLL_INTERVAL_MS));
          }
          jobPollingRef.current[jobId] = false;
        })();
      }

      return () => {
        jobListenersRef.current[jobId]?.delete(cb);
        if (jobListenersRef.current[jobId]?.size === 0)
          jobPollingRef.current[jobId] = false;
      };
    },
    []
  );

  const value = useMemo<EventContextValue>(
    () => ({ jobs, addJobEventListener, jobCache }),
    [jobs, addJobEventListener, jobCache]
  );

  return (
    <EventContext.Provider value={value}>{children}</EventContext.Provider>
  );
}

export function useEvents(): EventContextValue {
  return useContext(EventContext);
}
