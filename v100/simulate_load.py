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
        "--min_cpu",
        type=float,
        default=10.0,
        help="low end of the cpu wave, %% of a cpu core (0-100), default 10",
    )
    p.add_argument(
        "--max_cpu",
        type=float,
        default=90.0,
        help="high end of the cpu wave, %% of a cpu core (0-100), default 50",
    )
    p.add_argument(
        "--min_mem",
        type=parse_size,
        default="64M",
        help="minimum memory to allocate, in bytes (accepts K/M/G suffix), default 64M",
    )
    p.add_argument(
        "--max_mem",
        type=parse_size,
        default="256M",
        help="peak memory to allocate, in bytes (accepts K/M/G suffix), default 256M",
    )
    p.add_argument(
        "--run_time",
        type=float,
        default=60.0,
        help="how long to run, in seconds, default 60",
    )
    p.add_argument(
        "--output_size",
        type=parse_size,
        default="1M",
        help="bytes to write to output.blob (accepts K/M/G suffix), default 1M",
    )
    p.add_argument(
        "--period",
        type=float,
        default=10.0,
        help="seconds between simulated load changes, default 10",
    )
    p.add_argument(
        "--shape",
        choices=["triangle", "square"],
        default="triangle",
        help="waveform for the transition between low and high load each period "
        "(triangle: ramps linearly; square: steps instantly), default triangle",
    )
    p.add_argument(
        "--start",
        choices=["min", "max"],
        default="min",
        help="whether the first period starts at the min or the max cpu/memory, "
        "default min",
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


def _rising_phase(elapsed, period, start_high):
    """True during the low-to-high half of the wave. With start_high, the
    very first phase (elapsed < period) is high-to-low instead."""
    period_index = int(elapsed // period)
    return (period_index % 2 == 0) != start_high


def triangle_wave(elapsed, period, low, high, start_high=False):
    frac = (elapsed % period) / period
    if _rising_phase(elapsed, period, start_high):
        start, end = low, high
    else:
        start, end = high, low
    return start + (end - start) * frac


def square_wave(elapsed, period, low, high, start_high=False):
    return low if _rising_phase(elapsed, period, start_high) else high


WAVES = {"triangle": triangle_wave, "square": square_wave}


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

    low_cpu = min(max(args.min_cpu, 0.0), 100.0) / 100.0
    high_cpu = min(max(args.max_cpu, 0.0), 100.0) / 100.0
    wave = WAVES[args.shape]
    start_high = args.start == "max"

    buf = bytearray()
    resize_buffer(buf, args.max_mem if start_high else args.min_mem)

    start_time = time.perf_counter()
    last_status = 0.0

    while True:
        elapsed = time.perf_counter() - start_time
        if elapsed >= args.run_time:
            break

        cpu_target = wave(elapsed, args.period, low_cpu, high_cpu, start_high)
        mem_target = int(
            wave(elapsed, args.period, args.min_mem, args.max_mem, start_high)
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
