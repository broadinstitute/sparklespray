# Kubeque: Easy submission of batch jobs to kubernetes

kubeque sub ^render.py 0 0 1.0

kubeque sub ^render.py -c ^colors.txt 0 0 1.0

kubeque sub -p parameters.csv ^render.py {pos_x} {pos_y} {zoom}

kubeque sub ^render.py -c ^s3://bucket/obj {pos_x} {pos_y} {zoom}

kubeque sub -f jobspec.json

Goals:
Run all of the examples successfully.
    need a working minikube, right?
add support for GCP: add mypy annotations and implement as abstract classes?

Issues to fix:
Verify stdout.txt gets uploaded (where?)
    should fetch pull stdout.txt?
    How does retcode get reported?

* Missing support: Disk space for jobs:
No solution for ensuring sufficient diskspace for scheduling:
    - it appears there's no per-pod/container way to require sufficient space.
    - best bet might be to somehow control what type of instance or mounted volume while running.
    - having trouble finding how autoscaler determines which machine type to spawn
        - answer: uses nodepool/template/managed group

For now, probably best to take a parameter from config with diskspace required per node (optionally, could also 
take number of local SSDs to attach.  Local SSD are 375GB each and currently cost 0.11/month, contrast with equiv persistent disk 0.02/month )

* Missing support: Preemptable node support:
tutorial on creating pre-emptable nodepool
https://gist.github.com/rimusz/bd5c9fd9727522fd546329f61d4064a6
So, we should create two nodepools at startup.  One with standard VMs, and a second with preemptable nodes with autoscaling enabled.

* Missing support: un-claiming jobs when executing node goes away.
Best solution would be to have a kubernetes process run somewhere which polls kube for active nodes and reconcile 
those with the owners in the job table.  

First, need to fix setting of "owner"
http://stackoverflow.com/questions/35008011/kubernetes-how-do-i-know-what-node-im-on
try using hostname and see if that works

* Missing support: killing jobs
currently scales to 0

* Missing support: autoscaling
Currently manually enabled and handled out of band.  Also, autoscaling didn't appear to shrink cluster.  (perhaps due to the tiny VMs being used?)
Need to learn more about the default policy used to autoscale.

* Add export to timeline
y-axis by owner or by jobid
plotly?

* Use "downward" api to get pod name

Write service which watches pods.  On change, reconcile with tasks.
* handle OOM case
* handle FS exhausted case
(write test script to exercise these)
