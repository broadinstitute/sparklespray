import argparse
import time
import sys
import subprocess

parser = argparse.ArgumentParser()
parser.add_argument("--allocate", type=float, help="in GB", default=0.001)
parser.add_argument("--runtime", type=int, default=1)
parser.add_argument("--writesize", type=int)
parser.add_argument("--writepath", default="output")
parser.add_argument("--childcount", default=0, type=int)
parser.add_argument("--childalloc", default=0.001, type=float)
parser.add_argument("--childruntime", default=1, type=int)
args = parser.parse_args()

for i in range(args.childcount):
    print("starting child {}".format(i))
    sys.stdout.flush()
    cmd=['python', __file__, "--allocate", str(args.childalloc), "--runtime", str(args.childruntime)]
    print(cmd)
    subprocess.Popen(cmd)

allocated = None
if args.allocate:
    print("about to allocated memory ...")
    sys.stdout.flush()
    allocated = bytearray(int(args.allocate * 1024 * 1024 * 1024))
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
