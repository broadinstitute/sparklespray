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

Add "watch" which polls periodically

r