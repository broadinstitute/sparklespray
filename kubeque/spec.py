import re
import collections
import os

# def expand_specs(spec, default_url_prefix, default_job_url_prefix):
#     image = spec['image']
#     common = spec['common']
#     common['downloads'] = rewrite_downloads(io, common['downloads'], default_url_prefix)
#     common['uploads'] = rewrite_uploads(common['uploads'], default_job_url_prefix)

#     tasks = []
#     for task in spec['tasks']:
#         task = expand_task_spec(common, task)
#         task = rewrite_url_in_dict(task, "command_result_url", default_job_url_prefix)
#         task['downloads'] = rewrite_downloads(io, task['downloads'], default_url_prefix)
#         task['uploads'] = rewrite_uploads(task['uploads'], default_url_prefix)
#         tasks.append(task)

#     return tasks

def check_types(d, prop_types, required=False):
    errors = []
    for name, expected_type in prop_types.items():
        if name in d:
            v = d[name]
            if not isinstance(v, expected_type):
                errors.append("Expected {} to be of type {} but was {}".format(name, expected_type, type(v)))
        else:
            if required:
                errors.append("Missing '{}''".format(name))
    return errors

def validate_final_spec(spec):
    errors = check_types(spec, {"image": str, "tasks": list}, required=True)
    if len(errors) > 0:
        return errors
    
    tasks = spec["tasks"]
    for task in tasks:
        errors.extend(check_types(task, {"command": str}, required=True))
        errors.extend(check_types(task, {"downloads": dict, "uploads": dict}))
        if "downloads" in task:
            for dl in task['downloads']:
                if set(dl.keys()) == set(["src_url", "dst"]):
                    errors.extend(check_types({"src_url": str, "dst": str}))
                else:
                    errors.append( "Expected ('src_url', 'dst') but got ({})".format(", ".join(dl.keys())) )

        if "uploads" in task:
            for ul in task["uploads"]:
                if set(dl.keys()) == set(["src_wildcard", "dst_url_prefix"]):
                    errors.extend(check_types({"src_wildcard": str, "dst_url_prefix": str}))
                elif set(dl.keys()) == set(["src", "dst_url"]):
                    errors.extend(check_types({"src": str, dst_url: str}))
                else:
                    errors.append( "Expected either ('src_wildcard' and 'dst_url_prefix') or ('src' and 'dst_url') but got ({})".format(", ".join(dl.keys())) )

    return errors


def rewrite_argv_with_parameters(argv, parameters):
    l = []
    for task_params in parameters:
        def expand_parameters(x):
            m = re.match("(.*){([^}]+)}(.*)", x)
            if m == None:
                return x
            else:
                return m.group(1)+task_params[m.group(2)]+m.group(3)
        
        l.append([expand_parameters(x) for x in argv])
    return l

class Download:
    def __init__(self, src_url, dst, executable):
        self.src_url = src_url
        self.dst = dst
        self.executable = executable
        #log.debug("src_url", self.src_url, self.executable)
    def _asdict(self):
        d = dict(src_url=self.src_url, dst=self.dst)
        if self.executable:
            d["executable"] = self.executable
        return d

DownloadsAndCommand = collections.namedtuple("DownloadsAndCommand", "downloads command")
SrcDstPair = collections.namedtuple("SrcDstPair", "src dst")

def add_file_to_pull_to_wd(src_dst_pair, upload_map, hash_function, is_executable_function, cas_url, files_to_dl):
    assert isinstance(src_dst_pair, SrcDstPair)
    if src_dst_pair.src.startswith("gs://"):
        url = src_dst_pair.src
        executable_flag = False
    else:
        executable_flag = is_executable_function(src_dst_pair.src)
        if src_dst_pair.src in upload_map:
            url = upload_map[src_dst_pair.src]
        else:
            assert len(src_dst_pair.src) > 0
            url = add_file_to_upload_map(upload_map, hash_function, cas_url, src_dst_pair.src, src_dst_pair.src)

    files_to_dl.append( Download(url, src_dst_pair.dst, executable_flag) )

def add_file_to_upload_map(upload_map, hash_function, cas_url, filename, upload_map_key):
    h = hash_function(filename)
    url = cas_url + h
    upload_map[filename] = url
    return url

def rewrite_argvs_files_to_upload(list_of_argvs, cas_url, hash_function, is_executable_function, extra_files):
    assert cas_url is not None
    if not cas_url.endswith("/"):
        cas_url += "/"

    l = []
    upload_map = {}
    for argv in list_of_argvs:
        files_to_dl = []
        def rewrite_filenames(x):
            m = re.match("\\^(.*)", x)
            if m == None:
                return x
            else:
                filename = m.group(1)
                add_file_to_pull_to_wd(SrcDstPair(filename, filename), upload_map, hash_function, is_executable_function, cas_url, files_to_dl)
                return filename

        l.append(DownloadsAndCommand(files_to_dl, " ".join([rewrite_filenames(x) for x in argv])))

        for src_dst_pair in extra_files:
            add_file_to_pull_to_wd(src_dst_pair, upload_map, hash_function, is_executable_function, cas_url, files_to_dl)

    return upload_map, l

def is_executable(filename):
    return os.access(filename, os.X_OK)

from kubeque.gcp import _join

def make_spec_from_command(argv,
    docker_image,
    dest_url=None,
    cas_url=None,
    parameters=[{}],
    hash_function=None,
    is_executable_function=is_executable,
    resource_spec=None,
    extra_files=[],
    src_wildcards=None,
    pre_exec_script="ls -al",
    post_exec_script="ls -al",
    working_dir="."):

    if src_wildcards is None:
        src_wildcards = ["**"]

    list_of_argvs = rewrite_argv_with_parameters(argv, parameters)

    upload_map, list_of_dl_and_commands = rewrite_argvs_files_to_upload(list_of_argvs, cas_url, hash_function, is_executable_function, extra_files)

    # we're able to index into parameters and list_of_dl_and_commands because they should be in
    # the same order.
    assert len(list_of_dl_and_commands) == len(parameters)

    tasks = []
    for task_i, dl_and_command in enumerate(list_of_dl_and_commands):
        tasks.append(dict(
            downloads=[dict(d._asdict()) for d in dl_and_command.downloads], 
            command=dl_and_command.command,
            uploads=dict(include_patterns=src_wildcards, exclude_patterns=[], dst_url=_join(dest_url, str(task_i+1))),
            parameters=parameters[task_i]
        ))

    spec = {
            "image": docker_image,
            "resources": resource_spec,
            "common": {
                "working_dir": working_dir,
                "command_result_url": "result.json",
                "stdout_url": "stdout.txt",
                "pre-exec-script": pre_exec_script,
                "post-exec-script": post_exec_script
            },
            "tasks": tasks
        }

    return upload_map, spec

