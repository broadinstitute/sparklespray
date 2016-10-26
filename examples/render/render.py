#!/usr/bin/python

import time

print("opening output.raw")
with open("output.raw", "wt") as fd:    
    fd.write(time.asctime())
print("done")
