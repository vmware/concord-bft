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

#pragma once

#include "replica_state_snapshot.grpc.pb.h"

#include "bftengine/DbCheckpointManager.hpp"
#include "kvbc_app_filter/value_from_kvbc_proto.h"
#include "blockchain_misc.hpp"

#include <optional>
#include <string>
#include <memory>

namespace concord::thin_replica {

// A service that streams state snapshot key-values. By default, `kvbc` values are assumed to be in a
// `com::vmware::concord::kvbc::ValueWithTrids` format and the value is extracted from it. Users can specify different
// convertors, if needed.
class ReplicaStateSnapshotServiceImpl
    : public vmware::concord::replicastatesnapshot::ReplicaStateSnapshotService::Service {
 public:
  // Streams the state snapshot requested in `request` in the form of a finite stream of key-values.
  // See `replica_state_snapshot.proto` for the possible return values and the data structures.
  ::grpc::Status StreamSnapshot(
      ::grpc::ServerContext* context,
      const ::vmware::concord::replicastatesnapshot::StreamSnapshotRequest* request,
      ::grpc::ServerWriter< ::vmware::concord::replicastatesnapshot::StreamSnapshotResponse>* writer) override;

  // Allows users to convert state values to any format that is appropriate.
  void setStateValueConverter(const concord::kvbc::Converter& c) { state_value_converter_ = c; }

  // Following methods are used for testing only. Please do not use in production.
  void overrideCheckpointPathForTest(const std::string& path) { overriden_path_for_test_ = path; }
  void overrideCheckpointStateForTest(bftEngine::impl::DbCheckpointManager::CheckpointState state) {
    overriden_checkpoint_state_for_test_ = state;
  }
  void throwExceptionForTest() { throw_exception_for_test_ = true; }

 private:
  std::optional<std::string> overriden_path_for_test_;
  std::optional<bftEngine::impl::DbCheckpointManager::CheckpointState> overriden_checkpoint_state_for_test_;
  bool throw_exception_for_test_{false};
  // Allows users to convert state values to any format that is appropriate.
  // The default converter extracts the value from the ValueWithTrids protobuf type.
  concord::kvbc::Converter state_value_converter_{kvbc::valueFromKvbcProto};
};

}  // namespace concord::thin_replica
