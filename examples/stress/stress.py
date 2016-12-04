import argparse
import time
import sys

parser = argparse.ArgumentParser()
parser.add_argument("--allocate", type=int)
parser.add_argument("--runtime", type=int, default=1)
parser.add_argument("--writesize", type=int)
parser.add_argument("--writepath", default="output")

args = parser.parse_args()

allocated = None
if args.allocate:
    print("about to allocated memory ...")
    sys.stdout.flush()
    allocated = bytearray(args.allocate * 1024*1024)
    print("Allocated", len(allocated))
    for i in range(len(allocated)):
        allocated[i] = 1

if args.writesize:
    buf = bytearray(100*1024)
    for i in range(len(buf)):
        buf[i] = 1
    written = 0
    with open(args.writepath, "wb") as fd:
        for i in range(int((args.writesize + len(buf) - 1) / len(buf))):
            written += fd.write(buf)
    print("Wrote", written)

start = time.time()
while True:
    end = time.time()
    if end-start > args.runtime:
        break
    a = 0
    for i in range(10000):
        a += i
print("ran for", end-start, "seconds")