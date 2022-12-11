import re
import collections
import os
from .util import url_join


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
        url = url_join(cas_url, h)
        self.map[filename] = (url, is_public)
        return url


def rewrite_argv_with_parameters(argv, parameters):
    l = []
    for task_params in parameters:

        def expand_parameters(x):
            while True:
                m = re.match("(.*){([^}]+)}(.*)", x)
                if m == None:
                    return x
                else:
                    x = m.group(1) + task_params[m.group(2)] + m.group(3)

        l.append([expand_parameters(x) for x in argv])
    return l


class Download:
    def __init__(self, src_url, dst, executable, is_cas_key, symlink_safe):
        self.src_url = src_url
        self.dst = dst
        self.executable = executable
        self.is_cas_key = is_cas_key
        self.symlink_safe = symlink_safe

    def _asdict(self):
        d = dict(src_url=self.src_url, dst=self.dst)
        if self.executable:
            d["executable"] = self.executable
        if self.is_cas_key:
            d["is_cas_key"] = self.is_cas_key
        #
        if self.symlink_safe:
            d["symlink_safe"] = self.symlink_safe
        return d


DownloadsAndCommand = collections.namedtuple("DownloadsAndCommand", "downloads command")
SrcDstPair = collections.namedtuple("SrcDstPair", "src dst")


def _add_files_in_dir_to_pull_to_wd(
    src_dst_pair,
    upload_map,
    hash_function,
    is_executable_function,
    cas_url,
    files_to_dl,
    allow_symlinks,
):
    for filename in os.listdir(src_dst_pair.src):
        src_filename = os.path.join(src_dst_pair.src, filename)
        dst_filename = os.path.join(src_dst_pair.dst, filename)
        add_file_to_pull_to_wd(
            SrcDstPair(src=src_filename, dst=dst_filename),
            upload_map,
            hash_function,
            is_executable_function,
            cas_url,
            files_to_dl,
            allow_symlinks,
        )


def add_file_to_pull_to_wd(
    src_dst_pair,
    upload_map,
    hash_function,
    is_executable_function,
    cas_url,
    files_to_dl,
    allow_symlinks,
):
    assert isinstance(src_dst_pair, SrcDstPair)
    if src_dst_pair.src.startswith("gs://"):
        url = src_dst_pair.src
        executable_flag = False
    else:
        if os.path.isdir(src_dst_pair.src):
            _add_files_in_dir_to_pull_to_wd(
                src_dst_pair,
                upload_map,
                hash_function,
                is_executable_function,
                cas_url,
                files_to_dl,
                allow_symlinks,
            )
            return
        else:
            executable_flag = is_executable_function(src_dst_pair.src)
            url = upload_map.get_dst_url(src_dst_pair.src, must=False)
            if url is None:
                assert len(src_dst_pair.src) > 0
                url = upload_map.add(hash_function, cas_url, src_dst_pair.src)

    is_cas_key = url.startswith(cas_url)
    # print("add_file_to_pull_to_wd url={} cas_url={}, is_cas_key={}".format(
    #     url, cas_url, is_cas_key))
    files_to_dl.append(
        Download(url, src_dst_pair.dst, executable_flag, is_cas_key, allow_symlinks)
    )


def rewrite_argvs_files_to_upload(
    list_of_argvs,
    cas_url,
    hash_function,
    is_executable_function,
    extra_files,
    allow_symlinks,
):
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
                add_file_to_pull_to_wd(
                    SrcDstPair(filename, filename),
                    upload_map,
                    hash_function,
                    is_executable_function,
                    cas_url,
                    files_to_dl,
                    allow_symlinks,
                )
                return filename

        l.append(
            DownloadsAndCommand(
                files_to_dl, " ".join([rewrite_filenames(x) for x in argv])
            )
        )

        for src_dst_pair in extra_files:
            add_file_to_pull_to_wd(
                src_dst_pair,
                upload_map,
                hash_function,
                is_executable_function,
                cas_url,
                files_to_dl,
                allow_symlinks,
            )

    return upload_map, l


def is_executable(filename):
    return os.access(filename, os.X_OK)


def make_spec_from_command(
    argv,
    docker_image,
    dest_url=None,
    cas_url=None,
    parameters=[{}],
    hash_function=None,
    is_executable_function=is_executable,
    extra_files=[],
    src_wildcards=None,
    pre_exec_script="ls -al",
    post_exec_script="ls -al",
    working_dir=".",
    allow_symlinks=False,
    exclude_patterns=None,
):

    if src_wildcards is None:
        src_wildcards = ["**"]

    if exclude_patterns is None:
        exclude_patterns = []

    list_of_argvs = rewrite_argv_with_parameters(argv, parameters)

    upload_map, list_of_dl_and_commands = rewrite_argvs_files_to_upload(
        list_of_argvs,
        cas_url,
        hash_function,
        is_executable_function,
        extra_files,
        allow_symlinks,
    )

    # we're able to index into parameters and list_of_dl_and_commands because they should be in
    # the same order.
    assert len(list_of_dl_and_commands) == len(parameters)

    tasks = []
    for task_i, dl_and_command in enumerate(list_of_dl_and_commands):
        tasks.append(
            dict(
                downloads=[dict(d._asdict()) for d in dl_and_command.downloads],
                command=dl_and_command.command,
                uploads=dict(
                    include_patterns=src_wildcards,
                    exclude_patterns=exclude_patterns,
                    dst_url=url_join(dest_url, str(task_i + 1)),
                ),
                parameters=parameters[task_i],
            )
        )

    spec = {
        "image": docker_image,
        "common": {
            "working_dir": working_dir,
            "command_result_url": "result.json",
            "stdout_url": "stdout.txt",
            "pre-exec-script": pre_exec_script,
            "post-exec-script": post_exec_script,
        },
        "tasks": tasks,
    }

    return upload_map, spec
