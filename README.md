# Kubeque: Easy submission of batch jobs to kubernetes

# Getting started:

Create a google project.  In the below, we'll assume the project name is PROJECT_NAME.

Create a bucket for holding results and uploads.  In the following example, we'll assume the name of the bucket is BUCKET_NAME.

Create a config file "~/.kubeque" containing the following:

```
[config]
cas_url_prefix=gs://BUCKET_NAME/cas/
default_url_prefix=gs://BUCKET_NAME/kubeque/
project=PROJECT_NAME
cluster_name=kubeque-cluster
machine_type=g1-small
default_image=us.gcr.io/PROJECT_NAME/kubeque-example
default_resource_cpu=0.2
default_resource_memory=100M
```

Create an image to use by our job:
```
cd examples/docker
./prepare-image PROJECT_NAME
```

Create the cluster:
```
kubeque start 
```

Submit the sample job
```
cd examples/sample
kubeque sub '^render.py' 0 0
```

Stop the cluster:
```
kubeque stop
```


# Notes

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

* Missing support: autoscaling
Currently manually enabled and handled out of band.  Also, autoscaling didn't appear to shrink cluster.  (perhaps due to the tiny VMs being used?)
Need to learn more about the default policy used to autoscale.

* Add export to timeline
y-axis by owner or by jobid
plotly?

* Use "downward" api to get pod name
http://kubernetes.io/v1.1/docs/user-guide/downward-api.html

Write service which watches pods.  On change, reconcile with tasks.
* handle OOM case
* handle FS exhausted case
(Test script to exercise these is in examples/stress )

TODO: test, does a failed pod result in a new node with a new name being created, or does the pod keep its name when its created elsewhere?
This could be a problem when detecting failures of claimed tasks.  Would like to use UID but not exposed in downward API.  Can work around by
using volume export and adding uid label, but will be more work.

FIXME: upload resolve does not exclude files that were downloaded as part of staging
FIXME: multiple duplicate dl blocks 
FIXME: multiple duplicate ul blocks 
FIXME: when submitting job array, result json is not unique
