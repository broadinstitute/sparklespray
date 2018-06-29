def compute_hash(filename):
    m = hashlib.sha256()
    with open(filename, "rb") as fd:
        for chunk in iter(lambda: fd.read(10000), b""):
            m.update(chunk)
    return m.hexdigest()

def _join(*args):
    concated = args[0]
    for x in args[1:]:
        if concated[-1] == "/":
            concated = concated[:-1]
        concated += "/" + x
    return concated
