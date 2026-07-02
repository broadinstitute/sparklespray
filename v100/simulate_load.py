#!/usr/bin/env python3
"""Simulate CPU, memory, runtime, and disk load for testing metrics collection."""

import argparse
import os
import time


def parse_size(value):
    value = value.strip().upper()
    multipliers = {"K": 1024, "M": 1024**2, "G": 1024**3}
    if value[-1] in multipliers:
        return int(float(value[:-1]) * multipliers[value[-1]])
    return int(value)


def parse_args():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--cpu_fraction",
        type=float,
        required=True,
        help="average %% of a cpu core to use per second (0-100)",
    )
    p.add_argument(
        "--min_mem",
        type=parse_size,
        required=True,
        help="minimum memory to allocate, in bytes (accepts K/M/G suffix)",
    )
    p.add_argument(
        "--max_mem",
        type=parse_size,
        required=True,
        help="peak memory to allocate, in bytes (accepts K/M/G suffix)",
    )
    p.add_argument(
        "--run_time", type=float, required=True, help="how long to run, in seconds"
    )
    p.add_argument(
        "--output_size",
        type=parse_size,
        required=True,
        help="bytes to write to output.blob (accepts K/M/G suffix)",
    )
    p.add_argument(
        "--period",
        type=float,
        required=True,
        help="seconds between simulated load changes",
    )
    return p.parse_args()


PAGE_SIZE = 4096
CPU_SLICE = 0.1  # seconds per cpu duty-cycle slice
STATUS_INTERVAL = 1.0


def touch_pages(buf, start, end):
    for i in range(start - (start % PAGE_SIZE), end, PAGE_SIZE):
        buf[i] = 1


def resize_buffer(buf, new_size):
    old_size = len(buf)
    if new_size > old_size:
        buf.extend(bytearray(new_size - old_size))
        touch_pages(buf, old_size, new_size)
    elif new_size < old_size:
        del buf[new_size:]
    return buf


def write_output_file(path, size):
    chunk_size = 1024 * 1024
    with open(path, "wb") as f:
        remaining = size
        while remaining > 0:
            n = min(chunk_size, remaining)
            f.write(os.urandom(n))
            remaining -= n


def triangle_wave(elapsed, period, low, high):
    period_index = int(elapsed // period)
    frac = (elapsed % period) / period
    if period_index % 2 == 0:
        start, end = low, high
    else:
        start, end = high, low
    return start + (end - start) * frac


def burn_cpu_slice(duration):
    end = time.perf_counter() + duration
    x = 0.0
    while time.perf_counter() < end:
        x = x * x + 1.0
        if x > 1e12:
            x = 0.0


def main():
    args = parse_args()

    print(f"Writing {args.output_size} bytes to output.blob")
    write_output_file("output.blob", args.output_size)

    low_cpu = min(max(args.cpu_fraction * 0.5, 0.0), 100.0) / 100.0
    high_cpu = min(max(args.cpu_fraction * 2.0, 0.0), 100.0) / 100.0

    buf = bytearray()
    resize_buffer(buf, args.min_mem)

    start_time = time.perf_counter()
    last_status = 0.0

    while True:
        elapsed = time.perf_counter() - start_time
        if elapsed >= args.run_time:
            break

        cpu_target = triangle_wave(elapsed, args.period, low_cpu, high_cpu)
        mem_target = int(
            triangle_wave(elapsed, args.period, args.min_mem, args.max_mem)
        )
        resize_buffer(buf, mem_target)

        if elapsed - last_status >= STATUS_INTERVAL:
            print(
                f"t={elapsed:6.1f}s cpu={cpu_target * 100:5.1f}% mem={len(buf)} bytes"
            )
            last_status = elapsed

        slice_len = min(CPU_SLICE, args.run_time - elapsed)
        busy_time = slice_len * cpu_target
        sleep_time = slice_len - busy_time
        burn_cpu_slice(busy_time)
        if sleep_time > 0:
            time.sleep(sleep_time)

    print("Done")


if __name__ == "__main__":
    main()
