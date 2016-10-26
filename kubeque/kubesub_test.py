from kubeque.main import submit, main
from kubeque.aws import IO, JobQueue
import pytest
import subprocess
import json
import time
import socket
from boto.s3.connection import S3Connection, OrdinaryCallingFormat

FAKES3PORT=3201
FAKEDYPORT=3202

def wait_for_ports():
    for port in [FAKEDYPORT, FAKES3PORT]:
        for i in range(1000):
            try:
                c = socket.create_connection( ("localhost", port))
                c.close()
                break
            except ConnectionRefusedError:
                time.sleep(0.1)

@pytest.fixture
def fakes3(tmpdir):
    root = str(tmpdir.join("fake3root"))
    cmd = ["fakes3", "--root", root, "--port", str(FAKES3PORT)]
    print("exec:", " ".join(cmd))
    p = subprocess.Popen(cmd)
    yield 
    p.kill()

@pytest.fixture
def fakedynamo():
    cmd = ["java", "-Djava.library.path=/Users/pmontgom/dev/dynamodb/DynamoDBLocal_lib",
        "-jar","/Users/pmontgom/dev/dynamodb/DynamoDBLocal.jar","-sharedDb","-inMemory","-port",str(FAKEDYPORT)]
    print("exec:", " ".join(cmd))
    p = subprocess.Popen(cmd)
    yield 
    p.kill()

@pytest.fixture
def config_file(tmpdir):
    config_file = str(tmpdir.join("config"))
    with open(config_file, "wt") as fd:
        fd.write("""
[config]
AWS_ACCESS_KEY_ID=fake
AWS_SECRET_ACCESS_KEY=fake
dynamodb_prefix=test.
dynamodb_region=us-east-1
dynamodb_host=http://localhost:3202
cas_url_prefix=s3://bucket/cas
fake_s3_port=3201
""")
    return config_file

def test_submit(tmpdir, config_file, fakedynamo, fakes3):    
    env_config = {
        'cas_url_prefix': "s3://bucket/cas",
        'fake_s3_port': "3201", 
        'dynamodb_prefix': "test.", 
        'dynamodb_region': "us-east-1", 
        'dynamodb_host': "http://localhost:3202"
    }

    spec_file = str(tmpdir.join("spec"))
    input_file = str(tmpdir.join("input"))

    with open(input_file, "wt") as fd:
        fd.write("hello")

    spec ={"common": {
                "log_path" : "output.log",
                "command_result_path" : "result.json",
                "command_result_url" : "s3://bucket/command_result.json",
                "downloads": [
                    {"src": input_file, "dst": "message"}
                ],
                "uploads": [
                    {"src": "output.log", "dst_url": "s3://bucket/output.log"},
                    {"src": "m2", "dst_url": "s3://bucket/m2"},
                    ],
            },
            "tasks" :[{"command": "bash -c 'cat message message > m2'"}]
            }
    with open(spec_file, "wt") as fd:
        fd.write(json.dumps(
            spec
        ))

    # TODO: add support for "tasks": {range: 100, task:{...}}
    # and "tasks": {values: ["a", "b"], task:{...}}
    # add support for url_prefix

    wait_for_ports()

    connection = S3Connection("", "", is_secure=False, port=FAKES3PORT, host='localhost', calling_format=OrdinaryCallingFormat())
    connection.create_bucket("bucket")

    config = ["--config", config_file]    

    jq = JobQueue(env_config['dynamodb_prefix'], env_config['dynamodb_region'], env_config['dynamodb_host'])
    io = IO("fake", "fake", env_config['cas_url_prefix'], env_config['fake_s3_port'])
    consume_config_url = submit(jq, io, "jobname", spec, False, env_config, skip_kube_submit=True)
    assert consume_config_url is not None

    main(config + ["consume", consume_config_url, "jobname", "testowner"])
    main(config + ["status", "jobname"])

    dest_path = str(tmpdir.mkdir("dest"))

    main(config + ["fetch", "jobname", dest_path])

    assert tmpdir.join("dest/m2").read() == "hellohello"

# def test_job_queue():
#     jq = JobQueue()

#     args = ["a", "b", "c"]
#     import uuid
#     job_id = uuid.uuid4().hex
#     print("job_id:", job_id)
#     jq.submit(job_id, args)

#     main_run_loop("owner", exec_task)

#     print("done")   
#     print("Jobs")
#     for job in Job.scan():
#         print(" ",job.dumps())
#     print("Tasks")
#     for task in Task.scan():
#         print(" ", task.dumps())
