// Concord
//
// Copyright (c) 2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include "thin-replica-server/grpc_services.hpp"

using grpc::ServerContext;
using grpc::ServerWriter;

using com::vmware::concord::thin_replica::BlockId;
using com::vmware::concord::thin_replica::Data;
using com::vmware::concord::thin_replica::Hash;
using com::vmware::concord::thin_replica::ReadStateHashRequest;
using com::vmware::concord::thin_replica::ReadStateRequest;
using com::vmware::concord::thin_replica::SubscriptionRequest;

// NOTE: Make sure that all the logic is located in the implementation

namespace concord {
namespace thin_replica {

grpc::Status ThinReplicaService::ReadState(ServerContext* context,
                                           const ReadStateRequest* request,
                                           ServerWriter<Data>* stream) {
  diagnostics::TimeRecorder scoped_timer(*histograms_.readState);
  return impl_->ReadState(context, request, stream);
}

grpc::Status ThinReplicaService::ReadStateHash(ServerContext* context,
                                               const ReadStateHashRequest* request,
                                               Hash* hash) {
  diagnostics::TimeRecorder scoped_timer(*histograms_.readStateHash);
  return impl_->ReadStateHash(context, request, hash);
}

grpc::Status ThinReplicaService::AckUpdate(ServerContext* context,
                                           const BlockId* block_id,
                                           google::protobuf::Empty* empty) {
  diagnostics::TimeRecorder scoped_timer(*histograms_.ackUpdate);
  return impl_->AckUpdate(context, block_id, empty);
}

grpc::Status ThinReplicaService::SubscribeToUpdates(ServerContext* context,
                                                    const SubscriptionRequest* request,
                                                    ServerWriter<Data>* stream) {
  diagnostics::TimeRecorder scoped_timer(*histograms_.subscribeToUpdates);
  return impl_->SubscribeToUpdates<ServerContext, ServerWriter<Data>, Data>(context, request, stream);
}

grpc::Status ThinReplicaService::SubscribeToUpdateHashes(ServerContext* context,
                                                         const SubscriptionRequest* request,
                                                         ServerWriter<Hash>* stream) {
  diagnostics::TimeRecorder scoped_timer(*histograms_.subscribeToUpdateHashes);
  return impl_->SubscribeToUpdates<ServerContext, ServerWriter<Hash>, Hash>(context, request, stream);
}

grpc::Status ThinReplicaService::Unsubscribe(ServerContext* context,
                                             const google::protobuf::Empty* request,
                                             google::protobuf::Empty* response) {
  diagnostics::TimeRecorder scoped_timer(*histograms_.unsubscribe);
  return impl_->Unsubscribe(context, request, response);
}

}  // namespace thin_replica
}  // namespace concord
