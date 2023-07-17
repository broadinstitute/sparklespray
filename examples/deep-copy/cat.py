import sys
import os

x = ""

for fn in sys.argv[1:]:
    with open(fn, "rt") as fd:
        x += fd.read()

with open("output.txt", "wt") as fd:
    fd.write(x)

os.makedirs("outdir/2")
with open("outdir/2/3.txt", "wt") as fd:
    fd.write(x)
