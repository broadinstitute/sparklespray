from sparklespray.batch_api import *

if __name__ == "__main__":
    w = ClusterAPI()

    project="broad-achilles"
    location="us-central1"
    cluster_id = "cluster-test"
    
    w.stop_cluster(project, location, cluster_id)
    while True:
        node_reqs = w.get_node_reqs(project, location, cluster_id)
        if len(node_reqs) == 0:
            break
        else:
            print(f"waiting for nodes to disappear: {node_reqs}")
        print("Sleeping...")
        time.sleep(5)
    
    job = JobSpec(
        task_count="1",
        runnables=[Runnable(image="alpine", command=["sleep", "60"])],
        machine_type="n4-standard-2",
        preemptible=True,
        locations=["regions/us-central1"],
        network_tags=[],
        boot_disk=Disk(name="bootdisk", size_gb=40, type="hyperdisk-balanced", mount_path="/"),
        disks=[Disk(name="data", size_gb=50, type="hyperdisk-balanced", mount_path="/data")],
        sparkles_job="job-test",
        sparkles_cluster=cluster_id
    )

    print("creating job")
    op_id = w.create_job(project, location, job)
    
    print("Polling cluster")
    for i in range(20):
        node_reqs = w.get_node_reqs(project, location, cluster_id)
        print(node_reqs)
        time.sleep(5)
    # status = w.create_job(project, location, job)
    # name = status.name
    # printer = EventPrinter()
    # count = 0 
    # while not status.is_done():
    #     count += 1
    #     printer.print_new(status.events)
    #     print("sleeping")
    #     time.sleep(2)
    #     # if count == 10:
    #     #     print("cancelling")
    #     #     w.cancel(name)
    #     status = w.get_job_status(name)
    # print(status)    
