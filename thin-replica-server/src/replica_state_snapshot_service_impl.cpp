// Concord
//
// Copyright (c) 2022 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include "thin-replica-server/replica_state_snapshot_service_impl.hpp"

#include "Logger.hpp"
#include "kvbc_adapter/replica_adapter.hpp"

#include <stdexcept>
#include <string>

namespace concord::thin_replica {

using vmware::concord::replicastatesnapshot::StreamSnapshotRequest;
using vmware::concord::replicastatesnapshot::StreamSnapshotResponse;

using bftEngine::impl::DbCheckpointManager;
using storage::rocksdb::NativeClient;
using concord::kvbc::adapter::ReplicaBlockchain;

grpc::Status ReplicaStateSnapshotServiceImpl::StreamSnapshot(grpc::ServerContext* context,
                                                             const StreamSnapshotRequest* request,
                                                             grpc::ServerWriter<StreamSnapshotResponse>* writer) {
  const auto snapshot_id_str = std::to_string(request->snapshot_id());
  if (!overriden_path_for_test_.has_value()) {
    const auto checkpoint_state = overriden_checkpoint_state_for_test_.has_value()
                                      ? *overriden_checkpoint_state_for_test_
                                      : DbCheckpointManager::instance().getCheckpointState(request->snapshot_id());
    switch (checkpoint_state) {
      case DbCheckpointManager::CheckpointState::kNonExistent: {
        const auto msg = "State Snapshot ID = " + snapshot_id_str + " doesn't exist";
        LOG_INFO(STATE_SNAPSHOT, msg);
        return grpc::Status{grpc::StatusCode::NOT_FOUND, msg};
      }
      case DbCheckpointManager::CheckpointState::kPending: {
        const auto msg = "State Snapshot ID = " + snapshot_id_str + " is pending creation";
        LOG_INFO(STATE_SNAPSHOT, msg);
        return grpc::Status{grpc::StatusCode::UNAVAILABLE, msg};
      }
      case DbCheckpointManager::CheckpointState::kCreated:
        LOG_INFO(STATE_SNAPSHOT, "Starting streaming of State Snapshot ID = " + snapshot_id_str);
        break;
    }
  }

  try {
    if (throw_exception_for_test_) {
      throw std::runtime_error{"test exception - only thrown in tests"};
    }

    const auto snapshot_path = overriden_path_for_test_.has_value()
                                   ? *overriden_path_for_test_
                                   : DbCheckpointManager::instance().getPathForCheckpoint(request->snapshot_id());
    const auto read_only = true;
    const auto link_st_chain = false;
    auto db_client = NativeClient::newClient(snapshot_path, read_only, NativeClient::DefaultOptions{});
    auto kvbc_state_snapshot_ = std::make_unique<ReplicaBlockchain>(db_client, link_st_chain);

    const auto iterate = [&](std::string&& key, std::string&& value) {
      auto resp = StreamSnapshotResponse{};
      auto kv = resp.mutable_key_value();
      auto resp_key = kv->mutable_key();
      auto resp_value = kv->mutable_value();
      *resp_key = std::move(key);
      *resp_value = state_value_converter_(std::move(value));
      if (!writer->Write(resp)) {
        const auto err =
            "Streaming of State Snapshot ID = " + snapshot_id_str + " failed, reason = gRPC:Write() failure";
        LOG_ERROR(STATE_SNAPSHOT, err);
        throw std::runtime_error{err};
      }
    };

    if (request->has_last_received_key()) {
      if (!kvbc_state_snapshot_->iteratePublicStateKeyValues(iterate, request->last_received_key())) {
        const auto msg =
            "Streaming of State Snapshot ID = " + snapshot_id_str + " failed, reason = last_received_key not found";
        LOG_INFO(STATE_SNAPSHOT, msg);
        return grpc::Status{grpc::StatusCode::INVALID_ARGUMENT, msg};
      }
    } else {
      kvbc_state_snapshot_->iteratePublicStateKeyValues(iterate);
    }
  } catch (const std::exception& e) {
    const auto err = "Streaming of State Snapshot ID = " + snapshot_id_str + " failed, reason = " + e.what();
    LOG_ERROR(STATE_SNAPSHOT, err);
    return grpc::Status{grpc::StatusCode::UNKNOWN, err};
  }

  const auto success_msg = "Streaming of State Snapshot ID = " + snapshot_id_str + " succeeded";
  LOG_INFO(STATE_SNAPSHOT, success_msg);
  return grpc::Status{grpc::StatusCode::OK, success_msg};
}

}  // namespace concord::thin_replica
