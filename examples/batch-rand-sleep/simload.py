#!/usr/bin/env python3
import time, random, sys, math

duration = random.uniform(10, 120) + 5 * 60  # 10s – 2min + 5min
steady_state_duration = 5  # since we report metrics every 10s make sure the cycle is > 10 seconds so things don't get averaged out


def alloc_random():
    # Allocate memory and keep it resident
    buf = bytearray(mem_mb * 1024 * 1024)
    # Touch every page so the OS actually allocates it
    for i in range(0, len(buf), 4096):
        buf[i] = i & 0xFF
    return buf


def log(msg):
    print(msg, flush=True)


def load_cpu(cpu_frac, duration):
    log(f"CPU load will be {cpu_frac} for {duration}s")

    cycle = 100 / 1000  # Each 50 ms have a cycle of cpu on/off based on cpu_frac
    deadline = time.monotonic() + duration

    while time.monotonic() < deadline:
        t0 = time.monotonic()
        # Burn CPU for cpu_frac of the cycle
        busy_until = t0 + cycle * cpu_frac

        x = 1.0
        while time.monotonic() < busy_until:
            x = math.sqrt(x + 1.234567)  # something the optimizer can't elide

        # Sleep for the rest
        sleep_for = cycle - (time.monotonic() - t0)
        if sleep_for > 0:
            time.sleep(sleep_for)


deadline = time.monotonic() + duration

bufs = []
while time.monotonic() < deadline:
    mem_mb = random.randint(5, 100)  # 5 – 100 MB
    cpu_frac = random.uniform(0.1, 0.9)  # 10 – 90% of one core

    new_buf = alloc_random()
    log(f"Allocated {len(new_buf)} bytes")
    bufs.append(new_buf)
    if len(bufs) > 3:
        allocated = sum([len(x) for x in bufs])
        log(f"Freeing {allocated} bytes")
        bufs = []

    load_cpu(cpu_frac, steady_state_duration)

sys.exit(0)
