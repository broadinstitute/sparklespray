import random
import string
import datetime
import hashlib


def compute_hash(filename):
    m = hashlib.sha256()
    with open(filename, "rb") as fd:
        for chunk in iter(lambda: fd.read(10000), b""):
            m.update(chunk)
    return m.hexdigest()


def url_join(*args):
    concated = args[0]
    for x in args[1:]:
        if concated[-1] == "/":
            concated = concated[:-1]
        concated += "/" + x
    return concated


def random_string(length):
    alphabet = string.ascii_uppercase + string.digits
    return (''.join(random.choice(alphabet) for _ in range(length)))


def get_timestamp():
    d = datetime.datetime.now()
    return d.strftime("%Y%m%d-%H%M%S")
