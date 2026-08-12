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


class UnknownMachineType(Exception):
    pass


# Machine series (the part of the machine type before the first "-") known to
# support/not support Hyperdisk Balanced, per
# https://docs.cloud.google.com/compute/docs/disks/hyperdisks
_HYPERDISK_BALANCED_SUPPORTED_FAMILIES = {
    "a3",
    "a4",
    "a4x",
    "c3",
    "c3d",
    "c4",
    "c4a",
    "c4d",
    "c4n",
    "g4",
    "h3",
    "h4d",
    # M1/M2 support per GCP docs is gated by vCPU count (e.g. M2 only at 64+
    # vCPUs) which this family-only check can't express; treated as
    # supported since the docs list them, but worth revisiting if we ever
    # hit a small M1/M2 shape.
    "m1",
    "m2",
    "m3",
    "m4",
    "m4n",
    "n4",
    "n4a",
    "n4d",
    "x4",
    "z3",
}

_HYPERDISK_BALANCED_UNSUPPORTED_FAMILIES = {
    "a2",
    "c2",
    "c2d",
    "e2",
    # G2 supports Hyperdisk ML/Throughput but not Hyperdisk Balanced.
    "g2",
    "n1",
    "n2",
    "n2d",
    "t2a",
    "t2d",
}


def supports_hyperdisk_balanced(machine_type: str) -> bool:
    family = machine_type.split("-")[0].lower()
    if family in _HYPERDISK_BALANCED_SUPPORTED_FAMILIES:
        return True
    if family in _HYPERDISK_BALANCED_UNSUPPORTED_FAMILIES:
        return False
    raise UnknownMachineType(f"Unknown machine type: {machine_type!r}")
