from sparklespray.compute_service import ComputeService
c = ComputeService("broad-achilles")
print(c.volume_exists("us-central1-b", "cds-team"))
print(c.volume_exists("us-central1-b", "test-vol-01"))
print(c.create_volume("us-central1-b", "pd-standard", 5, "test-vol-05"))