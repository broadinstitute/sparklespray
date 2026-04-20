/**
 * Simulated data for features not yet backed by real APIs.
 *
 * Still simulated (tracked in next-steps.md):
 * - Per-task CPU/memory time-series (simulateResourceUsage) — metrics endpoint returns []
 *
 * simulateStdoutLines is no longer used; task output now comes from GET /api/v1/task/{id}/log.
 */
import type { TimingWindows } from "./events";

export interface TimeSeriesPoint {
  time: number; // unix ms
  label: string; // formatted time
  value: number;
}

// Deterministic pseudo-random from a string seed
function seededRng(seed: string) {
  let h = 0;
  for (let i = 0; i < seed.length; i++) {
    h = (Math.imul(31, h) + seed.charCodeAt(i)) | 0;
  }
  return () => {
    h ^= h << 13;
    h ^= h >> 7;
    h ^= h << 17;
    return (h >>> 0) / 0xffffffff;
  };
}

function formatTime(ms: number): string {
  return new Date(ms).toLocaleTimeString("en-US", {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });
}

function linspace(start: number, end: number, n: number): number[] {
  return Array.from(
    { length: n },
    (_, i) => start + ((end - start) * i) / (n - 1)
  );
}

export function simulateMemory(
  taskId: string,
  timings: TimingWindows,
  simTime?: Date
): TimeSeriesPoint[] {
  const { exec_started, exec_complete } = timings;
  if (!exec_started) return [];

  const rng = seededRng(taskId + ":mem");
  const fallbackEnd =
    exec_complete ??
    (simTime && simTime > exec_started
      ? simTime
      : new Date(exec_started.getTime() + 5 * 60_000));
  const execEnd = fallbackEnd;
  const maxMem = timings.maxMemInGb ?? 2.0;

  const stagedMs = exec_started.getTime();
  const execMs = execEnd.getTime();
  const duration = execMs - stagedMs;
  const times = linspace(stagedMs, execMs, 20);
  return times.map((t) => {
    // ramp up to peak at midpoint, then stay near peak
    const progress = (t - stagedMs) / duration;
    const rampShape =
      progress < 0.5
        ? Math.sin(((progress / 0.5) * Math.PI) / 2)
        : 0.9 + 0.1 * Math.sin(((progress - 0.5) / 0.5) * Math.PI);
    const noise = (rng() - 0.5) * 0.08 * maxMem;
    const value = Math.max(0, rampShape * maxMem + noise);
    return {
      time: t,
      label: formatTime(t),
      value: Math.round(value * 100) / 100,
    };
  });
}

export function simulateCpu(
  taskId: string,
  timings: TimingWindows,
  simTime?: Date
): TimeSeriesPoint[] {
  const { claimed, exec_started, exec_complete, complete } = timings;
  if (!claimed) return [];

  const rng = seededRng(taskId + ":cpu");
  const start = claimed.getTime();
  const simMs = simTime?.getTime();
  const end =
    complete?.getTime() ??
    exec_complete?.getTime() ??
    (simMs && simMs > start
      ? simMs
      : exec_started
      ? exec_started.getTime() + 5 * 60_000
      : start + 10 * 60_000);
  const stagedMs = exec_started?.getTime();
  const executedMs = exec_complete?.getTime();

  const times = linspace(start, end, 30);
  return times.map((t) => {
    let base = 0;
    if (stagedMs && executedMs && t >= stagedMs && t <= executedMs) {
      base = 95;
    } else if (stagedMs && !executedMs && t >= stagedMs) {
      base = 95; // partial run
    }
    const noise = base > 0 ? (rng() - 0.5) * 10 : rng() * 2;
    const value = Math.max(0, Math.min(100, base + noise));
    return {
      time: t,
      label: formatTime(t),
      value: Math.round(value * 10) / 10,
    };
  });
}

export interface VolumeDataPoint {
  location: string;
  totalGb: number;
  usedGb: number;
}

export interface ResourceDataPoint {
  time: number;
  label: string;
  processCount: number;
  totalMemoryGb: number;
  totalDataGb: number;
  totalSharedGb: number;
  totalResidentGb: number;
  cpuUser: number;
  cpuSystem: number;
  cpuIdle: number;
  cpuIowait: number;
  memTotalGb: number;
  memAvailableGb: number;
  memFreeGb: number;
  memPressureSomeAvg10: number;
  memPressureFullAvg10: number;
  volumes: VolumeDataPoint[];
}

export function simulateResourceUsage(
  taskId: string,
  timings: TimingWindows,
  simTime?: Date
): ResourceDataPoint[] {
  const { exec_started, exec_complete } = timings;
  if (!exec_started) return [];

  const rng = seededRng(taskId + ":resource");
  const startMs = exec_started.getTime();
  const fallbackEnd =
    exec_complete ??
    (simTime && simTime > exec_started
      ? simTime
      : new Date(exec_started.getTime() + 5 * 60_000));
  const endMs = Math.min(fallbackEnd.getTime(), simTime?.getTime() ?? Infinity);
  if (endMs <= startMs) return [];

  const maxMem = timings.maxMemInGb ?? 2.0;
  const systemMemGb = Math.max(8, Math.ceil(maxMem / 8) * 8) * 2;
  const maxProcesses = 1 + Math.floor(rng() * 7);

  const INTERVAL = 10_000;
  const points: ResourceDataPoint[] = [];

  for (let t = startMs; t <= endMs; t += INTERVAL) {
    const progress = Math.min(1, (t - startMs) / (endMs - startMs));
    const rampShape =
      progress < 0.5
        ? Math.sin(((progress / 0.5) * Math.PI) / 2)
        : 0.9 + 0.1 * Math.sin(((progress - 0.5) / 0.5) * Math.PI);

    const totalMem = Math.max(0, (rampShape + (rng() - 0.5) * 0.06) * maxMem);
    const totalData = totalMem * (0.55 + rng() * 0.1);
    const totalShared = totalMem * (0.04 + rng() * 0.04);
    const totalResident = totalMem * (0.8 + rng() * 0.12);

    const cpuUser = Math.max(0, Math.min(95, 65 + (rng() - 0.5) * 20));
    const cpuSystem = Math.max(0, Math.min(25, 15 + (rng() - 0.5) * 8));
    const cpuIowait = Math.max(0, Math.min(15, 3 + (rng() - 0.5) * 4));
    const cpuIdle = Math.max(0, 100 - cpuUser - cpuSystem - cpuIowait);

    const memFree = Math.max(
      0.1,
      systemMemGb * 0.08 - totalMem * 0.05 + (rng() - 0.5) * 0.2
    );
    const memAvailable = Math.max(
      memFree,
      memFree + systemMemGb * 0.12 + (rng() - 0.5) * 0.3
    );

    const pressureBase = rampShape * 45;
    const memPressureSome = Math.max(
      0,
      Math.min(100, pressureBase + (rng() - 0.5) * 10)
    );
    const memPressureFull = Math.max(
      0,
      Math.min(100, pressureBase * 0.3 + (rng() - 0.5) * 5)
    );

    const processCount = Math.max(
      1,
      Math.min(
        maxProcesses,
        Math.round(maxProcesses * Math.min(1, rampShape * 1.2)) +
          Math.floor((rng() - 0.5) * 2)
      )
    );

    points.push({
      time: t,
      label: formatTime(t),
      processCount,
      totalMemoryGb: Math.round(totalMem * 100) / 100,
      totalDataGb: Math.round(totalData * 100) / 100,
      totalSharedGb: Math.round(totalShared * 100) / 100,
      totalResidentGb: Math.round(totalResident * 100) / 100,
      cpuUser: Math.round(cpuUser * 10) / 10,
      cpuSystem: Math.round(cpuSystem * 10) / 10,
      cpuIdle: Math.round(cpuIdle * 10) / 10,
      cpuIowait: Math.round(cpuIowait * 10) / 10,
      memTotalGb: systemMemGb,
      memAvailableGb: Math.round(memAvailable * 100) / 100,
      memFreeGb: Math.round(memFree * 100) / 100,
      memPressureSomeAvg10: Math.round(memPressureSome),
      memPressureFullAvg10: Math.round(memPressureFull),
      volumes: [],
    });
  }

  return points;
}

export interface StdoutLine {
  time: number; // unix ms
  text: string;
}

const LOG_TEMPLATES: ((id: string, pct?: number) => string[])[] = [
  // startup
  (id) => [
    `[INFO]  Initializing worker for task ${id}`,
    `[INFO]  Loading configuration from /etc/worker/config.yaml`,
    `[INFO]  Docker image ready, starting entrypoint`,
  ],
  // data loading
  () => [
    `[INFO]  Scanning input directory...`,
    `[INFO]  Found {n} input files ({sz} MB total)`,
    `[INFO]  Validated checksums OK`,
  ],
  // processing start
  () => [`[INFO]  Spawning {p} worker processes`, `[INFO]  Processing started`],
  // progress rows — repeated
  (_id, pct = 0) => {
    const filled = Math.round(pct / 5);
    return [
      `[PROG]  [${"=".repeat(filled)}${" ".repeat(
        20 - filled
      )}] ${pct}% — {items} items/s`,
    ];
  },
  // warnings
  () => [
    `[WARN]  High memory usage detected, triggering GC`,
    `[INFO]  GC complete, freed {freed} MB`,
  ],
  // checkpointing
  () => [
    `[INFO]  Checkpointing state to /tmp/checkpoint-{hash}`,
    `[INFO]  Checkpoint written`,
  ],
  // completion
  () => [
    `[INFO]  Processing complete`,
    `[INFO]  Writing results to /output/{hash}.json`,
    `[INFO]  Flushing buffers...`,
    `[INFO]  Exiting cleanly with code 0`,
  ],
];

function fillTemplate(tpl: string, rng: () => number, taskId: string): string {
  const h = (s: string) => {
    let v = 5381;
    for (const c of s) v = ((v * 33) ^ c.charCodeAt(0)) >>> 0;
    return v.toString(16).slice(0, 8);
  };
  return tpl
    .replace("{n}", String(20 + Math.floor(rng() * 980)))
    .replace("{sz}", String(Math.floor(100 + rng() * 9900)))
    .replace("{p}", String(1 + Math.floor(rng() * 7)))
    .replace("{items}", String(Math.floor(500 + rng() * 4500)))
    .replace("{freed}", String(Math.floor(50 + rng() * 450)))
    .replace("{hash}", h(taskId + String(rng())));
}

export function simulateStdoutLines(
  taskId: string,
  timings: TimingWindows,
  simTime?: Date
): StdoutLine[] {
  const { exec_started, exec_complete } = timings;
  if (!exec_started) return [];

  const rng = seededRng(taskId + ":stdout");
  const startMs = exec_started.getTime();
  const fallbackEnd =
    exec_complete ??
    (simTime && simTime > exec_started
      ? simTime
      : new Date(exec_started.getTime() + 5 * 60_000));
  const endMs = fallbackEnd.getTime();
  const cutoffMs = simTime?.getTime() ?? endMs;
  const duration = endMs - startMs;

  const lines: StdoutLine[] = [];

  function emit(t: number, texts: string[]) {
    for (const raw of texts) {
      if (t <= cutoffMs)
        lines.push({ time: t, text: fillTemplate(raw, rng, taskId) });
      t += 200 + Math.floor(rng() * 800);
    }
  }

  // Startup burst
  let t = startMs + Math.floor(rng() * 2000);
  emit(t, LOG_TEMPLATES[0](taskId));
  t += 3000 + Math.floor(rng() * 5000);
  emit(t, LOG_TEMPLATES[1](taskId));
  t += 1000 + Math.floor(rng() * 2000);
  emit(t, LOG_TEMPLATES[2](taskId));

  // Progress ticks through the execution window
  const numTicks = 8 + Math.floor(rng() * 8);
  for (let i = 0; i < numTicks; i++) {
    const progress = (i + 1) / (numTicks + 1);
    t = startMs + Math.floor(progress * duration);
    const pct = Math.round(progress * 100);
    emit(t, LOG_TEMPLATES[3](taskId, pct));

    // Occasionally emit a warning or checkpoint
    if (rng() < 0.3) {
      t += 500 + Math.floor(rng() * 2000);
      emit(t, LOG_TEMPLATES[4](taskId));
    } else if (rng() < 0.3) {
      t += 500 + Math.floor(rng() * 2000);
      emit(t, LOG_TEMPLATES[5](taskId));
    }
  }

  // Completion lines at the end (only if task reached exec_complete)
  if (exec_complete) {
    t = endMs - 4000;
    emit(t, LOG_TEMPLATES[6](taskId));
  }

  return lines.sort((a, b) => a.time - b.time);
}

// Deterministic fake properties from task ID hash
const COMMANDS = [
  "python run.py --input {task_id}",
  "bash /scripts/process.sh --task {task_id} --output /results",
  "java -jar worker.jar --task-id {task_id}",
  "node /app/worker.js --task {task_id}",
  "Rscript analysis.R --id {task_id}",
];
const IMAGES = [
  "us.gcr.io/sparkles/worker:2.1.4",
  "us.gcr.io/sparkles/python-runner:3.11",
  "us.gcr.io/sparkles/java-worker:17",
  "us.gcr.io/sparkles/node-runner:20",
  "us.gcr.io/sparkles/r-analysis:4.3",
];

function hashStr(s: string): number {
  let h = 5381;
  for (let i = 0; i < s.length; i++) h = ((h * 33) ^ s.charCodeAt(i)) >>> 0;
  return h;
}

export function simulatedProperties(taskId: string) {
  const h = hashStr(taskId);
  const cmd = COMMANDS[h % COMMANDS.length].replace("{task_id}", taskId);
  const image = IMAGES[(h >> 3) % IMAGES.length];
  return { command: cmd, dockerImage: image };
}
