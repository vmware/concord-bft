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

#include "client/clientservice/state_snapshot_service.hpp"

using grpc::Status;
using grpc::ServerContext;
using grpc::ServerWriter;

using vmware::concord::client::statesnapshot::v1::GetRecentSnapshotRequest;
using vmware::concord::client::statesnapshot::v1::GetRecentSnapshotResponse;
using vmware::concord::client::statesnapshot::v1::StreamSnapshotRequest;
using vmware::concord::client::statesnapshot::v1::StreamSnapshotResponse;
using vmware::concord::client::statesnapshot::v1::ReadAsOfRequest;
using vmware::concord::client::statesnapshot::v1::ReadAsOfResponse;

namespace concord::client::clientservice {

Status StateSnapshotServiceImpl::GetRecentSnapshot(ServerContext* context,
                                                   const GetRecentSnapshotRequest* proto_request,
                                                   GetRecentSnapshotResponse* response) {
  // TODO: Add implementation
  return grpc::Status(grpc::StatusCode::UNIMPLEMENTED, "GetRecentSnapshot");
}

Status StateSnapshotServiceImpl::StreamSnapshot(ServerContext* context,
                                                const StreamSnapshotRequest* proto_request,
                                                ServerWriter<StreamSnapshotResponse>* stream) {
  // TODO: Add implementation
  return grpc::Status(grpc::StatusCode::UNIMPLEMENTED, "StreamSnapshot");
}

Status StateSnapshotServiceImpl::ReadAsOf(ServerContext* context,
                                          const ReadAsOfRequest* proto_request,
                                          ReadAsOfResponse* response) {
  // TODO: Add implementation
  return grpc::Status(grpc::StatusCode::UNIMPLEMENTED, "ReadAsOf");
}

}  // namespace concord::client::clientservice
