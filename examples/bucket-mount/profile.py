import time


def timeit(msg, callback):
    start = time.time()
    callback()
    end = time.time()
    print(f"{msg}: {end-start}")


def seq_read_1():
    with open("/mnt/disks/gcs-bucket/50mb-1", "rb") as fd:
        buf = fd.read()
    return buf


def seq_random_access_2():
    with open("/mnt/disks/gcs-bucket/50mb-2", "rb") as fd:
        for i in range(1000):
            fd.seek(50 * 1000 * 1000 - i * 1000)
            buf = fd.read(1000)
    return buf


def seq_random_access_1():
    with open("/mnt/disks/gcs-bucket/50mb-1", "rb") as fd:
        for i in range(1000):
            fd.seek(50 * 1000 * 1000 - i * 1000)
            buf = fd.read(1000)
    return buf


timeit("read sequentially", seq_read_1)
timeit("read sequentially a second time", seq_read_1)
timeit("read random access", seq_random_access_2)
timeit("read random access a second time", seq_random_access_2)
timeit("read random access first file", seq_random_access_1)
