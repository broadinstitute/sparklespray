import re
import collections
import os
from kubeque.gcp import _join

class UploadMap:
    def __init__(self):
        self.map = {}

    def get_dst_url(self, filename, must=False):
        if filename in self.map:
            return self.map[filename][0]
        else:
            if must:
                raise Exception("no path for {}".format(filename))
            else:
                return None

    def uploads(self):
        return [(k, v[0], v[1]) for k, v in self.map.items()]

    def add(self, hash_function, cas_url, filename, is_public=False):
        assert isinstance(is_public, bool)
        h = hash_function(filename)
        url = _join(cas_url, h)
        self.map[filename] = (url, is_public)
        return url


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

def _add_files_in_dir_to_pull_to_wd(src_dst_pair, upload_map, hash_function, is_executable_function, cas_url, files_to_dl):
    for filename in os.listdir(src_dst_pair.src):
        src_filename = os.path.join(src_dst_pair.src, filename)
        dst_filename = os.path.join(src_dst_pair.dst, filename)
        add_file_to_pull_to_wd(SrcDstPair(src=src_filename, dst=dst_filename), upload_map, hash_function, is_executable_function, cas_url, files_to_dl)

def add_file_to_pull_to_wd(src_dst_pair, upload_map, hash_function, is_executable_function, cas_url, files_to_dl):
    assert isinstance(src_dst_pair, SrcDstPair)
    if src_dst_pair.src.startswith("gs://"):
        url = src_dst_pair.src
        executable_flag = False
    else:
        if os.path.isdir(src_dst_pair.src):
            _add_files_in_dir_to_pull_to_wd(src_dst_pair, upload_map, hash_function, is_executable_function, cas_url, files_to_dl)
            return
        else:
            executable_flag = is_executable_function(src_dst_pair.src)
            url = upload_map.get_dst_url(src_dst_pair.src, must=False)
            if url is None:
                assert len(src_dst_pair.src) > 0
                url = upload_map.add(hash_function, cas_url, src_dst_pair.src)

    files_to_dl.append( Download(url, src_dst_pair.dst, executable_flag) )


def rewrite_argvs_files_to_upload(list_of_argvs, cas_url, hash_function, is_executable_function, extra_files):
    assert cas_url is not None
    if not cas_url.endswith("/"):
        cas_url += "/"

    l = []
    upload_map = UploadMap()
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

