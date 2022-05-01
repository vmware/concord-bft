// Concord
//
// Copyright (c) 2022 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License"). You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#pragma once

#include <vector>
#include <mutex>
#include <grpcpp/grpcpp.h>
#include "state_snapshot.grpc.pb.h"
#include "sha_hash.hpp"
#include "Logger.hpp"
#include "concord.cmf.hpp"
#include "client/concordclient/concord_client.hpp"

namespace concord::client::clientservice {

class StateSnapshotServiceImpl final
    : public vmware::concord::client::statesnapshot::v1::StateSnapshotService::Service {
 public:
  StateSnapshotServiceImpl(std::shared_ptr<concord::client::concordclient::ConcordClient> client)
      : logger_(logging::getLogger("concord.client.clientservice.statesnapshot")), client_(client){};
  grpc::Status GetRecentSnapshot(
      grpc::ServerContext* context,
      const vmware::concord::client::statesnapshot::v1::GetRecentSnapshotRequest* request,
      vmware::concord::client::statesnapshot::v1::GetRecentSnapshotResponse* response) override;
  grpc::Status StreamSnapshot(
      grpc::ServerContext* context,
      const vmware::concord::client::statesnapshot::v1::StreamSnapshotRequest* request,
      grpc::ServerWriter<vmware::concord::client::statesnapshot::v1::StreamSnapshotResponse>* stream) override;
  grpc::Status ReadAsOf(grpc::ServerContext* context,
                        const vmware::concord::client::statesnapshot::v1::ReadAsOfRequest* request,
                        vmware::concord::client::statesnapshot::v1::ReadAsOfResponse* response) override;

 private:
  void isHashValid(uint64_t snapshot_id,
                   const concord::util::SHA3_256::Digest& final_hash,
                   const std::chrono::milliseconds& timeout,
                   grpc::Status& return_status);

  void compareWithRsiAndSetReadAsOfResponse(
      const std::unique_ptr<concord::messages::StateSnapshotReadAsOfResponse>& bft_readasof_response,
      const vmware::concord::client::statesnapshot::v1::ReadAsOfRequest* const proto_request,
      vmware::concord::client::statesnapshot::v1::ReadAsOfResponse*& response,
      grpc::Status& return_status) const;

  void clearAllPrevDoneCallbacksAndAdd(std::shared_ptr<bool> condition,
                                       std::shared_ptr<bftEngine::RequestCallBack> callback);

  std::chrono::milliseconds setTimeoutFromDeadline(grpc::ServerContext* context);

  logging::Logger logger_;
  static const int32_t MAX_TIMEOUT_MS = 600000;  // 10 mins
  std::shared_ptr<concord::client::concordclient::ConcordClient> client_;
  std::map<std::shared_ptr<bool>, std::shared_ptr<bftEngine::RequestCallBack>> callbacks_for_cleanup_;
  std::mutex cleanup_mutex_;
};

}  // namespace concord::client::clientservice
