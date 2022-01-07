// Concord
//
// Copyright (c) 2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License"). You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include <grpcpp/grpcpp.h>
#include "state_snapshot.grpc.pb.h"

#include "Logger.hpp"
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
  logging::Logger logger_;
  std::shared_ptr<concord::client::concordclient::ConcordClient> client_;
};

}  // namespace concord::client::clientservice
