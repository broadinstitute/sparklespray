import type { AnyEvent } from "../types";

function formatTime(ms: number): string {
  return new Date(ms).toLocaleTimeString("en-US", {
    hour: "2-digit",
    minute: "2-digit",
  });
}

export interface WorkerCountPoint {
  time: number;
  label: string;
  running: number;
}

export interface WorkerRatePoint {
  time: number;
  label: string;
  started: number; // workers/min
  stopped: number; // workers/min
}

const NUM_BUCKETS = 60;

export function computeClusterTimeSeries(
  events: AnyEvent[],
  clusterId: string
): { counts: WorkerCountPoint[]; rates: WorkerRatePoint[] } {
  const clusterEvents = events
    .filter(
      (e) =>
        (e as any).cluster_id === clusterId &&
        (e.type === "worker_started" || e.type === "worker_stopped")
    )
    .sort((a, b) => a.timestamp.localeCompare(b.timestamp));

  if (clusterEvents.length === 0) return { counts: [], rates: [] };

  const minTime = new Date(clusterEvents[0].timestamp).getTime();
  const maxTime = new Date(
    clusterEvents[clusterEvents.length - 1].timestamp
  ).getTime();

  if (maxTime <= minTime) return { counts: [], rates: [] };

  const bucketSize = (maxTime - minTime) / NUM_BUCKETS;
  const bucketSizeMin = bucketSize / 60_000;

  const transitions = clusterEvents.map((e) => ({
    time: new Date(e.timestamp).getTime(),
    workerId: (e as any).worker_id as string,
    started: e.type === "worker_started",
  }));

  const workerRunning = new Map<string, boolean>();
  let transIdx = 0;
  const counts: WorkerCountPoint[] = [];

  for (let i = 0; i <= NUM_BUCKETS; i++) {
    const t = minTime + i * bucketSize;
    while (transIdx < transitions.length && transitions[transIdx].time <= t) {
      const { workerId, started } = transitions[transIdx];
      workerRunning.set(workerId, started);
      transIdx++;
    }
    let running = 0;
    for (const v of workerRunning.values()) if (v) running++;
    counts.push({ time: t, label: formatTime(t), running });
  }

  const rates: WorkerRatePoint[] = Array.from(
    { length: NUM_BUCKETS },
    (_, i) => ({
      time: minTime + (i + 0.5) * bucketSize,
      label: formatTime(minTime + (i + 0.5) * bucketSize),
      started: 0,
      stopped: 0,
    })
  );

  for (const e of clusterEvents) {
    const t = new Date(e.timestamp).getTime();
    const bi = Math.min(
      Math.floor((t - minTime) / bucketSize),
      NUM_BUCKETS - 1
    );
    if (e.type === "worker_started") rates[bi].started += 1 / bucketSizeMin;
    else rates[bi].stopped += 1 / bucketSizeMin;
  }

  for (const r of rates) {
    r.started = Math.round(r.started * 100) / 100;
    r.stopped = Math.round(r.stopped * 100) / 100;
  }

  return { counts, rates };
}
