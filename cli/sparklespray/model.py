from pydantic import BaseModel
from typing import List, Optional


class PersistentDiskMount(BaseModel):
    name: Optional[str]
    path: str
    size_in_gb: Optional[int]
    type: str


class SubmitConfig(BaseModel):
    service_account_email: str
    preemptible: bool
    boot_volume_in_gb: float
    default_url_prefix: str
    machine_type: str
    image: str
    project: str
    monitor_port: int
    zones: List[str]
    ssd_mount_points: List[str]
    pd_mount_points: List[PersistentDiskMount]
    work_root_dir: str
    kubequeconsume_url: str
    kubequeconsume_md5: str
    gpu_count: int
    gpu_type: Optional[str]
    target_node_count: int
    max_preemptable_attempts_scale: int


class MachineSpec(BaseModel):
    service_account_email: str
    boot_volume_in_gb: int
    ssd_mount_points: List[str]
    pd_mount_points: List[PersistentDiskMount]
    work_root_dir: str
    machine_type: str
    gpu_count: int
    gpu_type: Optional[str]

    def get_gpu(self):
        if self.gpu_count > 0:
            """Definition of GPU by version v2alpha1"""
            def_gpu = {"type": self.gpu_type, "count": self.gpu_count}
            return def_gpu
        else:
            return None
