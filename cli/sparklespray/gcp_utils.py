from typing import List, Dict, Any
from .model import MachineSpec, LOCAL_SSD, PersistentDiskMount, ExistingDiskMount
import re
import os

# and image which has curl and sh installed, used to prep the worker node
SETUP_IMAGE = "sequenceiq/alpine-curl"

max_label_len = 63


def normalize_label(label):
    label = label.lower()
    label = re.sub("[^a-z0-9]+", "-", label)
    if re.match("[^a-z].*", label) is not None:
        label = "x-" + label
    return label


def validate_label(label):
    assert len(label) <= max_label_len
    assert re.match("[a-z][a-z0-9-]*", label) is not None


def make_unique_label(label):
    import string
    import random

    suffix_len = 5
    new_label = f"{label[:max_label_len-suffix_len-1]}-{''.join(random.sample(list(string.ascii_lowercase) + list(string.digits), suffix_len))}"
    validate_label(new_label)
    return new_label


def get_region(zone):
    # drop the zone suffix to get the name of the region
    # that contains the zone
    # us-east1-b -> us-east1
    m = re.match("^([a-z0-9]+-[a-z0-9]+)-[a-z0-9]+$", zone)
    assert m, f"Zone doesn't look like a valid zone name: {zone}"
    return m.group(1)
