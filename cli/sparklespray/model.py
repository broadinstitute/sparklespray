from pydantic import BaseModel, validator
from typing import List, Optional, Union

ALLOWED_DISK_TYPES =  {"local-ssd", "pd-standard", "pd-balanced", "pd-ssd"}

DEFAULT_SSD_SIZE = 300 # slightly smaller than the 375 GB limit to avoid it allocating two volumes

class ExistingDiskMount(BaseModel):
    name: str
    path: str

class PersistentDiskMount(BaseModel):
    path: str
    size_in_gb: int
    type: str
    
    @validator("type")
    def check_type(cls, v: str) -> str:
        if v not in ALLOWED_DISK_TYPES:
            raise ValueError(f"{v} was not one of {ALLOWED_DISK_TYPES}")
        return v

DiskMount = Union[PersistentDiskMount, ExistingDiskMount]

class SubmitConfig(BaseModel):
    service_account_email: str
    boot_volume_in_gb: float
    default_url_prefix: str
    machine_type: str
    image: str
    project: str
    monitor_port: int
    zones: List[str]
    mounts: List[DiskMount]
    work_root_dir: str
    kubequeconsume_url: str
    kubequeconsume_md5: str
    target_node_count: int
    max_preemptable_attempts_scale: int

class MachineSpec(BaseModel):
    service_account_email: str
    boot_volume_in_gb: int
    mounts: List[DiskMount]
    work_root_dir: str
    machine_type: str

LOCAL_SSD = "local-ssd"
