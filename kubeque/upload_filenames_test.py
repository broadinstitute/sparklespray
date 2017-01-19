from kubeque.main import expand_files_to_upload, SrcDstPair
import os

def test_simple_expand_files_for_upload():
    assert expand_files_to_upload(["a", "b"]) == [SrcDstPair("a", "a"), SrcDstPair("b", "b")]
    assert expand_files_to_upload(["a:b"]) == [SrcDstPair("a","b")]

def test_files_from_filename(tmpdir):
    fn = str(tmpdir.join("files"))
    with open(fn, "wt") as fd:
        fd.write("a\nb:c\n")

    assert expand_files_to_upload(["@"+fn]) == [SrcDstPair("a","a"), SrcDstPair("b", "c")]

def test_handling_of_abs_paths():
    assert expand_files_to_upload(["/root/filename"]) == [SrcDstPair("/root/filename", "filename")]

def test_dir_upload(tmpdir):
    dirname = str(tmpdir.join("files"))
    os.makedirs(dirname)

    with open(dirname+"/a", "wt") as fd:
        fd.write("a")
    with open(dirname+"/b", "wt") as fd:
        fd.write("b")

    assert expand_files_to_upload([dirname]) == [SrcDstPair(dirname+"/a", "a"), SrcDstPair(dirname+"/b", "b")]
    assert expand_files_to_upload([dirname+":x"]) == [SrcDstPair(dirname+"/a", "x/a"), SrcDstPair(dirname+"/b", "x/b")]


def test_skip_subdir_upload(tmpdir):
    dirname = str(tmpdir.join("files"))
    os.makedirs(dirname)
    os.makedirs(dirname+"/sub")

    with open(dirname+"/a", "wt") as fd:
        fd.write("a")
    with open(dirname+"/sub/b", "wt") as fd:
        fd.write("b")

    assert expand_files_to_upload([dirname]) == [SrcDstPair(dirname+"/a", "a")]
