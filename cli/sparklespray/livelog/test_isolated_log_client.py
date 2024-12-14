from .logclient import SafeRemoteCaller
import os
import time
import pytest

class MockMonitorDictAPI:
    def __init__(self, init_param):
        self.init_param = init_param

    def echo(self, obj):
        return {"success": True, "value": obj, "init_param": self.init_param}
        

def create_mock(init_params):
    return MockMonitorDictAPI(init_params)

def test_with_mock():
    # this doesn't really test grpc calls can be made. It just tests the plumbing. Creates a child process and makes sure it can 
    # communicate
    stub = SafeRemoteCaller(f"{__name__}:create_mock", 3)
    stub.start("my name is steve")
    result = stub._blocking_call("echo", ["hello"], {})
    assert result == {"success": True, "value": "hello", "init_param": "my name is steve"}
    result = stub._blocking_call("echo", ["bye"], {})
    assert result == {"success": True, "value": "bye", "init_param": "my name is steve"}
    stub.dispose()

class MockHangingMonitor:
    def __init__(self, blocking_filename):
        self.blocking_filename = blocking_filename

    def echo(self, obj):
        # if the filename exists, hang indefinitely
        if os.path.exists(self.blocking_filename):
            time.sleep(10000)

        return {"success": True, "value": obj}


def test_timeout(tmpdir):
    lock_file = tmpdir.join("lock")

    stub = SafeRemoteCaller(f"{__name__}:MockHangingMonitor", 0.5)
    stub.start(str(lock_file))
    
    # make a normal call
    result = stub._blocking_call("echo", ["hello"], {})
    assert result == {"success": True, "value": "hello"}

    # make a call that does not complete by first creating lock file
    lock_file.write("stop")
    with pytest.raises(TimeoutError):
        stub._blocking_call("echo", ["hello"], {})

    stub.dispose()
