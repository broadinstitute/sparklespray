import {
  createContext,
  useContext,
  useEffect,
  useRef,
  useMemo,
  useState,
} from "react";
import type { AnyEvent, BackendJobSummary, JobDetail } from "../types";
import { apiFetch } from "../api/client";

const POLL_INTERVAL_MS = 5_000;
const PAGE_LIMIT = 1000;

export function mergeEvents(
  prev: AnyEvent[],
  incoming: AnyEvent[]
): AnyEvent[] {
  const knownIds = new Set(prev.map((e) => e.event_id));
  const novel = incoming.filter((e) => !knownIds.has(e.event_id));
  return novel.length > 0 ? [...prev, ...novel] : prev;
}

export type EventListener = (events: AnyEvent[]) => void;

export interface EventContextValue {
  jobs: BackendJobSummary[];
  addJobEventListener: (jobId: string, cb: EventListener) => () => void;
  jobCache: Record<string, JobDetail>;
  paused: boolean;
  setPaused: (paused: boolean) => void;
  lastUpdatedAt: number | null;
  /** Writes a job's label list straight into jobCache (deriving `metadata`
   * from it) without a round-trip -- the poll loop below only ever fetches
   * each job's details once (pendingJobFetchesRef dedup), so callers that
   * mutate a job's labels out-of-band (e.g. toggling "hidden") already know
   * the resulting label list (either optimistically, or from the mutation
   * endpoint's response) and can push it straight in for an immediate,
   * flicker-free update instead of waiting on a fresh GET. Creates a stub
   * cache entry (blank name/created_at/etc., filled in later by a real
   * fetch) if the job's details haven't been fetched yet, so this always
   * takes effect even if e.g. polling is paused. */
  updateJobLabels: (jobId: string, labels: JobDetail["labels"]) => void;
}

const EventContext = createContext<EventContextValue>({
  jobs: [],
  addJobEventListener: () => () => {},
  jobCache: {},
  paused: false,
  setPaused: () => {},
  lastUpdatedAt: null,
  updateJobLabels: () => {},
});

interface RawJobDetail {
  job_id: string;
  name: string;
  workpool_id: string;
  created_at: string;
  task_count: number;
  labels: { name: string; value: string }[];
}

function computeJobSummary(
  raw: Omit<BackendJobSummary, "taskCount" | "successCount" | "failureCount">
): BackendJobSummary {
  let taskCount = 0,
    successCount = 0,
    failureCount = 0;
  for (const t of raw.tasks) {
    taskCount += t.count;
    if (t.state === "success") successCount += t.count;
    else if (
      t.state === "error" ||
      t.state === "failed" ||
      t.state === "killed"
    )
      failureCount += t.count;
  }
  return { ...raw, taskCount, successCount, failureCount };
}

export function EventProvider({ children }: { children: React.ReactNode }) {
  const [jobs, setJobs] = useState<BackendJobSummary[]>([]);
  const [jobCache, setJobCache] = useState<Record<string, JobDetail>>({});
  const [paused, setPaused] = useState(false);
  const [lastUpdatedAt, setLastUpdatedAt] = useState<number | null>(null);

  const jobEventCacheRef = useRef<Record<string, AnyEvent[]>>({});
  const jobCursorRef = useRef<Record<string, string | null>>({});
  const jobListenersRef = useRef<Record<string, Set<EventListener>>>({});
  const jobPollingRef = useRef<Record<string, boolean>>({});
  const jobLoopGenRef = useRef<Record<string, number>>({});

  const pendingJobFetchesRef = useRef<Set<string>>(new Set());

  const pausedRef = useRef(paused);
  pausedRef.current = paused;

  const fetchJobDetail = useMemo(
    () => async (jobId: string) => {
      try {
        const r = await apiFetch(`/api/v1/job/${jobId}`);
        if (!r.ok) return;
        const raw: RawJobDetail = await r.json();
        const metadata = Object.fromEntries(
          raw.labels.map((l) => [l.name, l.value])
        );
        const detail: JobDetail = { ...raw, metadata };
        setJobCache((prev) => ({ ...prev, [jobId]: detail }));
      } catch {
        /* transient; caller can retry */
      }
    },
    []
  );

  const updateJobLabels = useMemo(
    () => (jobId: string, labels: JobDetail["labels"]) => {
      setJobCache((prev) => {
        const existing = prev[jobId];
        const metadata = Object.fromEntries(
          labels.map((l) => [l.name, l.value])
        );
        // If this job's details haven't been fetched yet (e.g. polling is
        // paused, so the poll loop's one-time per-job fetch never ran),
        // stub in the fields we don't know rather than silently dropping
        // the update -- name/created_at/etc. get filled in for real the
        // next time fetchJobDetail runs; callers (JobsTable) only read
        // `metadata` and tolerate a blank `name` by falling back to the ID.
        const detail: JobDetail = existing
          ? { ...existing, labels, metadata }
          : {
              job_id: jobId,
              name: "",
              workpool_id: "",
              created_at: "",
              task_count: 0,
              labels,
              metadata,
            };
        return { ...prev, [jobId]: detail };
      });
    },
    []
  );

  useEffect(() => {
    let cancelled = false;

    async function pollJobs() {
      while (!cancelled) {
        if (pausedRef.current) {
          await new Promise<void>((r) => setTimeout(r, POLL_INTERVAL_MS));
          continue;
        }
        try {
          const res = await apiFetch("/api/v1/jobs");
          if (res.ok) {
            const rawData: Omit<
              BackendJobSummary,
              "taskCount" | "successCount" | "failureCount"
            >[] = await res.json();
            const data = rawData.map(computeJobSummary);
            setJobs(data);
            setLastUpdatedAt(Date.now());

            for (const j of data) {
              if (!pendingJobFetchesRef.current.has(j.job_id)) {
                pendingJobFetchesRef.current.add(j.job_id);
                fetchJobDetail(j.job_id);
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
      if (!jobListenersRef.current[jobId])
        jobListenersRef.current[jobId] = new Set();
      if (!jobEventCacheRef.current[jobId])
        jobEventCacheRef.current[jobId] = [];
      if (!(jobId in jobCursorRef.current)) jobCursorRef.current[jobId] = null;

      if (jobEventCacheRef.current[jobId].length > 0)
        cb(jobEventCacheRef.current[jobId]);

      jobListenersRef.current[jobId].add(cb);

      if (!jobPollingRef.current[jobId]) {
        jobPollingRef.current[jobId] = true;
        const myGen = (jobLoopGenRef.current[jobId] =
          (jobLoopGenRef.current[jobId] ?? 0) + 1);

        (async () => {
          while (
            jobPollingRef.current[jobId] &&
            jobLoopGenRef.current[jobId] === myGen
          ) {
            try {
              const params = new URLSearchParams({
                job_id: jobId,
                limit: String(PAGE_LIMIT),
              });
              const cursor = jobCursorRef.current[jobId];
              if (cursor) params.set("after", cursor);

              const res = await apiFetch(`/api/v1/events?${params}`);
              if (!res.ok) throw new Error(`HTTP ${res.status}`);

              const data: {
                events: AnyEvent[];
                next_after?: string;
              } = await res.json();

              if (data.events.length > 0) {
                const knownIds = new Set(
                  jobEventCacheRef.current[jobId].map((e) => e.event_id)
                );
                const newEvents = data.events.filter(
                  (e) => !knownIds.has(e.event_id)
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

                if (data.events.length >= PAGE_LIMIT) continue;
              }
            } catch (err) {
              console.error(
                `[EventProvider] job event poll error (${jobId}):`,
                err
              );
            }

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
        if (jobListenersRef.current[jobId]?.size === 0) {
          jobPollingRef.current[jobId] = false;
          jobLoopGenRef.current[jobId] =
            (jobLoopGenRef.current[jobId] ?? 0) + 1;
        }
      };
    },
    []
  );

  const value = useMemo<EventContextValue>(
    () => ({
      jobs,
      addJobEventListener,
      jobCache,
      paused,
      setPaused,
      lastUpdatedAt,
      updateJobLabels,
    }),
    [
      jobs,
      addJobEventListener,
      jobCache,
      paused,
      lastUpdatedAt,
      updateJobLabels,
    ]
  );

  return (
    <EventContext.Provider value={value}>{children}</EventContext.Provider>
  );
}

export function useEvents(): EventContextValue {
  return useContext(EventContext);
}
