from google.protobuf import duration_pb2 as duration_proto

import grpc
import clientservice.request_pb2 as request_proto
import clientservice.request_pb2_grpc as request_grpc

import os.path
import sys
sys.path.append(os.path.abspath("../../build/tests/apollo/util/"))
import skvbc_messages

from util import skvbc as kvbc

class GrpcClient(object):
    """
    Client for gRPC functionality
    """

    def __init__(self):
        self.host = '0.0.0.0'
        self.server_port = 50051
        self.client_id = 100

        # instantiate a channel
        self.channel = grpc.insecure_channel(
            'localhost:50051', options=(('grpc.enable_http_proxy', 0),))

        # bind the client and the server
        self.request_stub = request_grpc.RequestServiceStub(self.channel)

    def sendRequest(self, request=b"csutil.py", timeout=duration_proto.Duration(seconds=5),
             read_only=False, pre_execute=False, correlation_id="grpcclient-cid"):
        
        req = request_proto.Request(
            raw_request=bytes(request),
            timeout=timeout,
            read_only=read_only,
            pre_execute=pre_execute,
            correlation_id=correlation_id
        )

        response =  self.request_stub.Send(req)

        return response.raw_response