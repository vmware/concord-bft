#!/usr/bin/env python3

# Manual testing via grpcurl, e.g.:
#
# $ grpcurl -plaintext -d '{"request":"asdf", "read_only":true}'
#   localhost:50505 vmware.concord.client.v1.RequestService/Send
#

from google.protobuf import duration_pb2 as duration_proto
import grpc

import os
import sys

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import request_pb2 as request_proto  # noqa: E402
import event_pb2 as event_proto  # noqa: E402
import state_snapshot_pb2 as state_snapshot_proto

import request_pb2_grpc as request_grpc  # noqa: E402
import event_pb2_grpc as event_grpc  # noqa: E402
import state_snapshot_pb2_grpc as state_snapshot_grpc


class Clientservice:
    def __init__(self, host="localhost", port="50505"):
        self.channel = grpc.insecure_channel("{}:{}".format(host, port))
        self.request_stub = request_grpc.RequestServiceStub(self.channel)
        self.event_stub = event_grpc.EventServiceStub(self.channel)
        self.state_snapshot_stub = state_snapshot_grpc.StateSnapshotServiceStub(self.channel)

    def __del__(self):
        self.channel.close()

    def send(self, request=b"csutil.py", timeout=duration_proto.Duration(seconds=5),
             read_only=False, pre_execute=False, correlation_id="csutil-cid"):
        req = request_proto.Request(
            request=request,
            timeout=timeout,
            read_only=read_only,
            pre_execute=pre_execute,
            correlation_id=correlation_id
        )
        return self.request_stub.Send(req)

    def subscribe(self, id, legacy=False):
        if legacy:
            req = event_proto.SubscribeRequest(
                events=event_proto.EventsRequest(block_id=id)
            )
        else:
            req = event_proto.SubscribeRequest(
                event_groups=event_proto.EventGroupsRequest(event_group_id=id)
            )
        return self.event_stub.Subscribe(req)

    def get_recent_snapshot(self):
        req = state_snapshot_proto.GetRecentSnapshotRequest()
        return self.state_snapshot_stub.GetRecentSnapshot(req)


    def stream_snapshot(self, id, last_recvd_key=""):
        req = state_snapshot_proto.StreamSnapshotRequest(snapshot_id=id, last_received_key=last_recvd_key)
        return self.state_snapshot_stub.StreamSnapshot(req)

    def read_as_of(self, id, keys_list=[""]):
        req = state_snapshot_proto.ReadAsOfRequest(snashot_id = id, keys = keys_list)