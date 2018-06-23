from kubeque.pb_pb2_grpc import MonitorStub
from kubeque.pb_pb2 import ReadOutputRequest
import grpc
import sys
from google.cloud import datastore

datastore_client = datastore.Client("broad-achilles", credentials=None)
entity_key = datastore_client.key("ClusterKeys", "sparklespray")
entity = datastore_client.get(entity_key)
cert = entity['cert']
private_key = entity['private_key']
shared_secret = entity['shared_secret']

creds = grpc.ssl_channel_credentials(cert)
channel = grpc.secure_channel('localhost:5099', creds, options=(('grpc.ssl_target_name_override', 'sparkles.server',),))
stub = MonitorStub(channel)
print("about to call")
response = stub.ReadOutput(ReadOutputRequest(taskId="testjob.0", offset=0, size=10000), metadata=[('shared-secret', shared_secret)])
print (response)


# def header_adder_interceptor(header, value):

#     def intercept_call(client_call_details, request_iterator, request_streaming,
#                        response_streaming):
#         metadata = []
#         if client_call_details.metadata is not None:
#             metadata = list(client_call_details.metadata)
#         metadata.append((
#             header,
#             value,
#         ))
#         client_call_details = _ClientCallDetails(
#             client_call_details.method, client_call_details.timeout, metadata,
#             client_call_details.credentials)
#         return client_call_details, request_iterator, None

# class AddSharedSecret(grpc.UnaryUnaryClientInterceptor):
#     def intercept_unary_unary(self, continuation, client_call_details, request):
#         new_details = _ClientCallDetails(
#             client_call_details.method, client_call_details.timeout, metadata,
#             client_call_details.credentials)
#         return continuation(new_details, next(new_request_iterator))
        
#     return generic_client_interceptor.create(intercept_call)