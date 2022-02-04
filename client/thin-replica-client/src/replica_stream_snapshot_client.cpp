// Concord
//
// Copyright (c) 2021-2022 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include "client/concordclient/concord_client_exceptions.hpp"
#include "client/thin-replica-client/replica_stream_snapshot_client.hpp"

using client::concordclient::GrpcConnection;
using concord::client::concordclient::RemoteData;
using concord::client::concordclient::SnapshotKVPair;
using concord::client::concordclient::UpdateNotFound;
using concord::client::concordclient::OutOfRangeSubscriptionRequest;
using concord::client::concordclient::InternalError;
using concord::client::concordclient::EndOfStream;
using concord::client::concordclient::StreamUnavailable;
using concord::client::concordclient::UpdateQueue;
using vmware::concord::replicastatesnapshot::StreamSnapshotRequest;
using vmware::concord::replicastatesnapshot::StreamSnapshotResponse;

namespace client::replica_state_snapshot_client {
void ReplicaStreamSnapshotClient::readSnapshotStream(const SnapshotRequest& request,
                                                     std::shared_ptr<UpdateQueue> remote_queue) {
  threadpool_.async([this, remote_queue, request]() {
    try {
      this->receiveSnapshot(request, remote_queue);
    } catch (...) {
      // Set exception and quit receiveUpdates
      remote_queue->setException(std::current_exception());
    }
  });
}

void ReplicaStreamSnapshotClient::receiveSnapshot(const SnapshotRequest& request,
                                                  std::shared_ptr<UpdateQueue> remote_queue) {
  ConcordAssert(config_->rss_conns.size() > 0);
  uint16_t replica_id = 0;
  GrpcConnection::Result result = GrpcConnection::Result::kUnknown;
  std::string last_read_key("");
  for (const auto& conn : config_->rss_conns) {
    concordclient::RequestId request_id = 0;
    vmware::concord::replicastatesnapshot::StreamSnapshotRequest stream_snapshot_request;
    stream_snapshot_request.set_snapshot_id(request.snapshot_id);
    if (last_read_key.empty()) {
      stream_snapshot_request.set_last_received_key(request.last_received_key);
    } else {
      stream_snapshot_request.set_last_received_key(last_read_key);
    }

    result = conn->openStateSnapshotStream(stream_snapshot_request, request_id);
    if (result != GrpcConnection::Result::kSuccess) {
      LOG_INFO(logger_, "Not able to open connection with replica id" << replica_id);
      replica_id++;
      continue;
    }
    bool is_reading = true;

    while (is_reading) {
      vmware::concord::replicastatesnapshot::StreamSnapshotResponse stream_snapshot_response;
      if (conn->hasStateSnapshotStream(request_id)) {
        result = conn->readStateSnapshot(request_id, &stream_snapshot_response);
      } else {
        result = GrpcConnection::Result::kFailure;
      }
      if (result == GrpcConnection::Result::kSuccess) {
        pushDatumToRemoteQueue(stream_snapshot_response, remote_queue, last_read_key);
      } else {
        is_reading = false;
        conn->cancelStateSnapshotStream(request_id);
      }
    }
    if (result == GrpcConnection::Result::kEndOfStream) {
      break;
    }
    replica_id++;
  }
  pushFinalStateToRemoteQueue(result);
}
void ReplicaStreamSnapshotClient::pushDatumToRemoteQueue(const StreamSnapshotResponse& datum,
                                                         std::shared_ptr<UpdateQueue> remote_queue,
                                                         std::string& last_key) {
  if (datum.has_key_value()) {
    auto snapshot_datum = std::make_unique<RemoteData>();
    SnapshotKVPair kv_pair;
    kv_pair.key = datum.key_value().key();
    last_key.assign(kv_pair.key);
    kv_pair.val = datum.key_value().value();
    snapshot_datum->emplace<SnapshotKVPair>(kv_pair);
    remote_queue->push(std::move(snapshot_datum));
  }
}

void ReplicaStreamSnapshotClient::pushFinalStateToRemoteQueue(const GrpcConnection::Result& result) {
  switch (result) {
    case GrpcConnection::Result::kEndOfStream:
      throw EndOfStream();
      break;
    case GrpcConnection::Result::kFailure:
      throw InternalError();
      break;
    case GrpcConnection::Result::kNotFound:
      throw UpdateNotFound();
      break;
    case GrpcConnection::Result::kOutOfRange:
      throw OutOfRangeSubscriptionRequest();
      break;
    case GrpcConnection::Result::kTimeout:
      throw StreamUnavailable();
      break;
    case GrpcConnection::Result::kUnknown:
      throw InternalError();
      break;
    case GrpcConnection::Result::kSuccess:  // fall through
    default:
      break;
  }
}

}  // namespace client::replica_state_snapshot_client
