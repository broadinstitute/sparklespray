from pydantic import BaseModel, validator
from typing import List, Optional, Union, Dict

ALLOWED_DISK_TYPES = {"local-ssd", "pd-standard", "pd-balanced", "pd-ssd", "hyperdisk-balanced"}

DEFAULT_SSD_SIZE = (
    300  # slightly smaller than the 375 GB limit to avoid it allocating two volumes
)

class DiskMount(BaseModel):
    path: str



class ExistingDiskMount(DiskMount):
    name: str


class PersistentDiskMount(DiskMount):
    size_in_gb: int
    type: str

    @validator("type")
    def check_type(cls, v: str) -> str:
        if v not in ALLOWED_DISK_TYPES:
            raise ValueError(f"{v} was not one of {ALLOWED_DISK_TYPES}")
        return v

from typing import TypeVar
DiskMountT = Union[ExistingDiskMount, PersistentDiskMount] # TypeVar("DiskMountT", bound=DiskMount)


class SubmitConfig(BaseModel):
    service_account_email: str
    boot_volume_in_gb: int
    default_url_prefix: str
    machine_type: str
    image: str
    project: str
    monitor_port: int
    zones: List[str]
    mounts: List[DiskMountT]
    work_root_dir: str
    kubequeconsume_url: str
    kubequeconsume_md5: str
    target_node_count: int
    max_preemptable_attempts_scale: int


class MachineSpec(BaseModel):
    service_account_email: str
    boot_volume_in_gb: int
    mounts: List[DiskMountT]
    work_root_dir: str
    machine_type: str

    def as_dict(self):
        return self.dict()

LOCAL_SSD = "local-ssd"
