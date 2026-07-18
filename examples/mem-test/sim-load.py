import argparse
import multiprocessing
import time

# Length of one busy/sleep duty cycle used by each cpu worker, in seconds. Short enough
# to give a fairly even, continuous cpu-percent reading rather than a coarse on/off pattern.
CYCLE_SECONDS = 0.1


def cpu_worker(duration, cpu_percent):
    busy_seconds = CYCLE_SECONDS * (cpu_percent / 100.0)
    sleep_seconds = CYCLE_SECONDS - busy_seconds

    end_time = time.perf_counter() + duration
    while time.perf_counter() < end_time:
        busy_until = time.perf_counter() + busy_seconds
        a = 0
        while time.perf_counter() < busy_until:
            a += 1
        if sleep_seconds > 0:
            time.sleep(sleep_seconds)


def main():
    parser = argparse.ArgumentParser(
        description="Simulate CPU load and memory usage, for testing sparkles resource monitoring."
    )
    parser.add_argument(
        "--cpus",
        type=int,
        default=0,
        help="Number of worker processes to spawn, each running a busy loop",
    )
    parser.add_argument(
        "--duration", type=float, default=10, help="How long to run, in seconds"
    )
    parser.add_argument(
        "--cpu-percent",
        type=float,
        default=100,
        help="Percent of the time each cpu worker should spend busy vs sleeping. "
        "100 = fully use the cpu, 50 = half of each cycle busy, half sleeping",
    )
    parser.add_argument(
        "--mem",
        type=int,
        default=0,
        help="How much memory to allocate, in MB, to simulate memory usage",
    )
    args = parser.parse_args()

    assert 0 <= args.cpu_percent <= 100, "--cpu-percent must be between 0 and 100"

    allocated = None
    if args.mem:
        print("Allocating {} MB...".format(args.mem))
        allocated = bytearray(args.mem * 1024 * 1024)
        # Touch each page (not every byte) to force the OS to actually back it with physical
        # memory, rather than relying on the zero-fill-on-demand page it started out as.
        page_size = 4096
        for offset in range(0, len(allocated), page_size):
            allocated[offset] = 1
        print("Allocated and touched {} bytes".format(len(allocated)))

    workers = []
    if args.cpus:
        print(
            "Starting {} cpu worker(s) at {}% for {} seconds...".format(
                args.cpus, args.cpu_percent, args.duration
            )
        )
        for _ in range(args.cpus):
            p = multiprocessing.Process(
                target=cpu_worker, args=(args.duration, args.cpu_percent)
            )
            p.start()
            workers.append(p)

    print("Sleeping for {} seconds...".format(args.duration))
    time.sleep(args.duration)

    for p in workers:
        p.join()

    print("Done.")


if __name__ == "__main__":
    main()
