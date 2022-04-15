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

#include "client/concordclient/client_health.hpp"
#include "client/concordclient/concord_client_exceptions.hpp"
#include "client/thin-replica-client/replica_state_snapshot_client.hpp"

using client::concordclient::GrpcConnection;
using concord::client::concordclient::SnapshotKVPair;
using concord::client::concordclient::UpdateNotFound;
using concord::client::concordclient::OutOfRangeSubscriptionRequest;
using concord::client::concordclient::InternalError;
using concord::client::concordclient::EndOfStream;
using concord::client::concordclient::StreamUnavailable;
using concord::client::concordclient::RequestOverload;
using concord::client::concordclient::SnapshotQueue;
using vmware::concord::replicastatesnapshot::StreamSnapshotRequest;
using vmware::concord::replicastatesnapshot::StreamSnapshotResponse;

using namespace concord::client::concordclient;

namespace client::replica_state_snapshot_client {

static inline const uint kNumOpsPerHealthCheck = 128;

void ReplicaStateSnapshotClient::readSnapshotStream(const SnapshotRequest& request,
                                                    std::shared_ptr<SnapshotQueue> remote_queue) {
  is_serving_ = false;
  updateOpCounter();
  if (count_of_concurrent_request_.load() > config_->concurrency_level) {
    remote_queue->setException(std::make_exception_ptr(RequestOverload()));
    updateOpErrorCounter();
    return;
  }
  ++count_of_concurrent_request_;

  threadpool_.async([this, remote_queue, request]() {
    is_serving_ = true;
    try {
      this->receiveSnapshot(request, remote_queue);
    } catch (...) {
      // Set exception and quit receiveUpdates
      remote_queue->setException(std::current_exception());
      is_serving_ = false;
      updateOpErrorCounter();
    }
    --(this->count_of_concurrent_request_);
  });
}

void ReplicaStateSnapshotClient::receiveSnapshot(const SnapshotRequest& request,
                                                 std::shared_ptr<SnapshotQueue> remote_queue) {
  ConcordAssertGT(config_->rss_conns.size(), 0);
  uint16_t replica_id = 0;
  GrpcConnection::Result result = GrpcConnection::Result::kUnknown;
  std::string last_read_key("");
  for (const auto& conn : config_->rss_conns) {
    concordclient::RequestId request_id = 0;
    vmware::concord::replicastatesnapshot::StreamSnapshotRequest stream_snapshot_request;
    stream_snapshot_request.set_snapshot_id(request.snapshot_id);
    if (last_read_key.empty()) {
      if (request.last_received_key.has_value()) {
        stream_snapshot_request.set_last_received_key(request.last_received_key.value());
      }
    } else {
      stream_snapshot_request.set_last_received_key(last_read_key);
    }

    updateOpCounter();
    result = conn->openStateSnapshotStream(stream_snapshot_request, request_id);
    if (result != GrpcConnection::Result::kSuccess) {
      LOG_INFO(logger_, "Not able to open connection with replica id" << replica_id);
      replica_id++;
      updateOpErrorCounter();
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
        updateOpErrorCounter();
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
void ReplicaStateSnapshotClient::pushDatumToRemoteQueue(const StreamSnapshotResponse& datum,
                                                        std::shared_ptr<SnapshotQueue> remote_queue,
                                                        std::string& last_key) {
  if (datum.has_key_value()) {
    last_key.assign(datum.key_value().key());
    auto snapshot_datum = std::unique_ptr<SnapshotKVPair>{new SnapshotKVPair{last_key, datum.key_value().value()}};
    remote_queue->push(std::move(snapshot_datum));
  }
}

void ReplicaStateSnapshotClient::pushFinalStateToRemoteQueue(const GrpcConnection::Result& result) {
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

void ReplicaStateSnapshotClient::updateOpErrorCounter() {
  if (!health_check_enabled_) {
    return;
  }
  op_error_counter_++;
  if (op_counter_ < kNumOpsPerHealthCheck) {
    return;
  }

  if (op_error_counter_ > (op_counter_ / 4)) {
    // More than 25% of the operations in the checking interval resulted in health errors;
    // we're sick.
    unhealthy_ = true;
    LOG_ERROR(logger_, "Too many read errors: " << op_error_counter_ << " reads out of " << op_counter_ << "reads");
  }

  LOG_DEBUG(logger_, "reads: " << op_counter_ << " read errors " << op_error_counter_);

  // We've reached the end of our measurement window; start over.
  op_error_counter_ = 0;
  op_counter_ = 0;
}

// TODO(scramer): is this trivial method necessary?
void ReplicaStateSnapshotClient::updateOpCounter() {
  if (!health_check_enabled_) {
    return;
  }
  op_error_counter_++;
}

void ReplicaStateSnapshotClient::setHealthCheckEnabled(bool is_enabled) { health_check_enabled_ = is_enabled; }

ClientHealth ReplicaStateSnapshotClient::getClientHealth() {
  if (!health_check_enabled_) {
    return ClientHealth::Healthy;
  }
  if (unhealthy_) {
    return ClientHealth::Unhealthy;
  } else {
    return ClientHealth::Healthy;
  }
}

void ReplicaStateSnapshotClient::setClientHealth(ClientHealth health) {
  if (!health_check_enabled_) {
    return;
  }
  if (health == ClientHealth::Healthy) {
    unhealthy_ = false;
    op_error_counter_ = 0;
    op_counter_ = 0;
  } else {
    unhealthy_ = true;
  }
}

}  // namespace client::replica_state_snapshot_client
