from kubeque.pb_pb2_grpc import MonitorStub
from kubeque.pb_pb2 import ReadOutputRequest
import grpc
import sys

channel = grpc.insecure_channel('localhost:5099')
stub = MonitorStub(channel)
response = stub.ReadOutput(ReadOutputRequest(taskId="testjob.0", offset=0, size=10000))
print (response)