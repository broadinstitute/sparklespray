from sparklespray.spec import make_spec_from_command
from sparklespray.submit import expand_tasks


def dummy_hash(text):
    return str(hash(text))


def test_expand_tasks():
    spec = {
        "tasks": [
            {
                "downloads": [
                    {
                        "dst": "mandelbrot.py",
                        "src_url": "gs://source1",
                        "executable": True,
                    }
                ],
                "command": "python3 mandelbrot.py",
                "uploads": {
                    "include_patterns": ["**"],
                    "exclude_patterns": [],
                    "dst_url": "s3://testjob/1",
                },
                "parameters": {},
            },
            {
                "downloads": [
                    {
                        "dst": "mandelbrot.py",
                        "src_url": "gs://source1",
                        "executable": True,
                    }
                ],
                "command": "python3 mandelbrot.py",
                "uploads": {
                    "include_patterns": ["**"],
                    "exclude_patterns": [],
                    "dst_url": "s3://testjob/2",
                },
                "parameters": {},
            },
        ],
        "image": "us.gcr.io/project/tag",
        "common": {"command_result_url": "result.json", "stdout_url": "stdout.txt"},
    }
    io = None
    default_url_prefix = "s3://testcas"
    default_job_url_prefix = "s3://testjob"
    expanded_spec = expand_tasks(spec, io, default_url_prefix, default_job_url_prefix)
    expected_tasks = [
        {
            "downloads": [
                {
                    "dst": "mandelbrot.py",
                    "src_url": "gs://source1",
                    "executable": True,
                    "is_cas_key": False,
                    "symlink_safe": False,
                }
            ],
            "uploads": {
                "include_patterns": ["**"],
                "exclude_patterns": [],
                "dst_url": "s3://testjob/1",
            },
            "command": "python3 mandelbrot.py",
            "command_result_url": "s3://testjob/1/result.json",
            "stdout_url": "s3://testjob/1/stdout.txt",
            "parameters": {},
        },
        {
            "downloads": [
                {
                    "dst": "mandelbrot.py",
                    "src_url": "gs://source1",
                    "executable": True,
                    "is_cas_key": False,
                    "symlink_safe": False,
                }
            ],
            "uploads": {
                "include_patterns": ["**"],
                "exclude_patterns": [],
                "dst_url": "s3://testjob/2",
            },
            "command": "python3 mandelbrot.py",
            "command_result_url": "s3://testjob/2/result.json",
            "stdout_url": "s3://testjob/2/stdout.txt",
            "parameters": {},
        },
    ]
    assert expanded_spec == expected_tasks  # ['tasks']


def test_simple_command():
    upload_mapping, spec = make_spec_from_command(
        ["bash", "-c", "date"],
        cas_url="s3://bucket/cas",
        docker_image="us.gcr.io/bucket/dockerimage",
        dest_url="s3://bucket/dest",
    )
    assert upload_mapping.map == {}
    assert spec == {
        "image": "us.gcr.io/bucket/dockerimage",
        "common": {
            "working_dir": ".",
            "command_result_url": "result.json",
            "stdout_url": "stdout.txt",
            "pre-exec-script": "ls -al",
            "post-exec-script": "ls -al",
        },
        "tasks": [
            {
                "downloads": [],
                "command": "bash -c date",
                "uploads": {
                    "include_patterns": ["**"],
                    "exclude_patterns": [],
                    "dst_url": "s3://bucket/dest/1",
                },
                "parameters": {},
            }
        ],
    }


def test_upload_files():
    script_hash = dummy_hash("script_to_run.py")

    upload_mapping, spec = make_spec_from_command(
        ["^script_to_run.py"],
        docker_image="us.gcr.io/bucket/dockerimage",
        dest_url="s3://bucket/dest",
        cas_url="s3://bucket/cas",
        hash_function=dummy_hash,
        is_executable_function=lambda fn: fn.startswith("script"),
    )
    assert upload_mapping.map == {
        "script_to_run.py": ("s3://bucket/cas/" + script_hash, False)
    }
    assert spec == {
        "image": "us.gcr.io/bucket/dockerimage",
        "common": {
            "working_dir": ".",
            "command_result_url": "result.json",
            "stdout_url": "stdout.txt",
            "pre-exec-script": "ls -al",
            "post-exec-script": "ls -al",
        },
        "tasks": [
            {
                "downloads": [
                    {
                        "src_url": "s3://bucket/cas/" + script_hash,
                        "dst": "script_to_run.py",
                        "executable": True,
                        "is_cas_key": True,
                    }
                ],
                "command": "script_to_run.py",
                "uploads": {
                    "include_patterns": ["**"],
                    "exclude_patterns": [],
                    "dst_url": "s3://bucket/dest/1",
                },
                "parameters": {},
            }
        ],
    }


def test_parameterized():
    script1_hash = dummy_hash("script1")
    script2_hash = dummy_hash("script2")

    upload_mapping, spec = make_spec_from_command(
        ["python", "^{script_name}", "{parameter}"],
        docker_image="us.gcr.io/bucket/dockerimage",
        dest_url="s3://bucket/dest",
        cas_url="s3://bucket/cas",
        parameters=[
            dict(script_name="script1", parameter="a"),
            dict(script_name="script2", parameter="b"),
        ],
        hash_function=dummy_hash,
        is_executable_function=lambda fn: fn.startswith("script"),
    )

    assert upload_mapping.map == {
        "script1": ("s3://bucket/cas/" + script1_hash, False),
        "script2": ("s3://bucket/cas/" + script2_hash, False),
    }
    assert spec == {
        "image": "us.gcr.io/bucket/dockerimage",
        "common": {
            "working_dir": ".",
            "command_result_url": "result.json",
            "stdout_url": "stdout.txt",
            "pre-exec-script": "ls -al",
            "post-exec-script": "ls -al",
        },
        "tasks": [
            {
                "downloads": [
                    {
                        "src_url": "s3://bucket/cas/" + script1_hash,
                        "dst": "script1",
                        "executable": True,
                        "is_cas_key": True,
                    }
                ],
                "command": "python script1 a",
                "uploads": {
                    "include_patterns": ["**"],
                    "exclude_patterns": [],
                    "dst_url": "s3://bucket/dest/1",
                },
                "parameters": {"script_name": "script1", "parameter": "a"},
            },
            {
                "downloads": [
                    {
                        "src_url": "s3://bucket/cas/" + script2_hash,
                        "dst": "script2",
                        "executable": True,
                        "is_cas_key": True,
                    }
                ],
                "command": "python script2 b",
                "uploads": {
                    "include_patterns": ["**"],
                    "exclude_patterns": [],
                    "dst_url": "s3://bucket/dest/2",
                },
                "parameters": {"script_name": "script2", "parameter": "b"},
            },
        ],
    }
