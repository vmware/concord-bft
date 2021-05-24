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

#ifndef CONCORD_THIN_REPLICA_GRPC_SERVICES_HPP_
#define CONCORD_THIN_REPLICA_GRPC_SERVICES_HPP_

#include <grpcpp/grpcpp.h>
#include <grpcpp/impl/codegen/sync_stream.h>

#include <memory>

#include "diagnostics.h"
#include "performance_handler.h"
#include "thin_replica.grpc.pb.h"

#ifdef RUN_PERF_TRS_TRC_TOOL
#include "thin_replica_impl_perf.hpp"
#else
#include "thin_replica_impl.hpp"
#endif

namespace concord {
namespace thin_replica {

class ThinReplicaService final : public com::vmware::concord::thin_replica::ThinReplica::Service {
 public:
  explicit ThinReplicaService(std::unique_ptr<ThinReplicaImpl>&& impl) : impl_(std::move(impl)) {}

  grpc::Status ReadState(grpc::ServerContext* context,
                         const com::vmware::concord::thin_replica::ReadStateRequest* request,
                         grpc::ServerWriter<com::vmware::concord::thin_replica::Data>* stream) override;

  grpc::Status ReadStateHash(grpc::ServerContext* context,
                             const com::vmware::concord::thin_replica::ReadStateHashRequest* request,
                             com::vmware::concord::thin_replica::Hash* hash) override;

  grpc::Status AckUpdate(grpc::ServerContext* context,
                         const com::vmware::concord::thin_replica::BlockId* block_id,
                         google::protobuf::Empty* empty) override;

  grpc::Status SubscribeToUpdates(grpc::ServerContext* context,
                                  const com::vmware::concord::thin_replica::SubscriptionRequest* request,
                                  grpc::ServerWriter<com::vmware::concord::thin_replica::Data>* stream) override;

  grpc::Status SubscribeToUpdateHashes(grpc::ServerContext* context,
                                       const com::vmware::concord::thin_replica::SubscriptionRequest* request,
                                       grpc::ServerWriter<com::vmware::concord::thin_replica::Hash>* stream) override;

  grpc::Status Unsubscribe(grpc::ServerContext* context,
                           const google::protobuf::Empty* request,
                           google::protobuf::Empty* response) override;

 private:
  std::unique_ptr<ThinReplicaImpl> impl_;

  static constexpr int64_t MAX_VALUE_MILLISECONDS = 1000 * 60;  // 60 Seconds

  struct Recorders {
    Recorders() {
      auto& registrar = concord::diagnostics::RegistrarSingleton::getInstance();
      registrar.perf.registerComponent(
          "trs", {readState, readStateHash, ackUpdate, subscribeToUpdates, subscribeToUpdateHashes, unsubscribe});
    }

    ~Recorders() {
      auto& registrar = concord::diagnostics::RegistrarSingleton::getInstance();
      registrar.perf.unRegisterComponent("trs");
    }

    DEFINE_SHARED_RECORDER(readState, 1, MAX_VALUE_MILLISECONDS, 3, concord::diagnostics::Unit::MILLISECONDS);
    DEFINE_SHARED_RECORDER(readStateHash, 1, MAX_VALUE_MILLISECONDS, 3, concord::diagnostics::Unit::MILLISECONDS);
    DEFINE_SHARED_RECORDER(ackUpdate, 1, MAX_VALUE_MILLISECONDS, 3, concord::diagnostics::Unit::MILLISECONDS);
    DEFINE_SHARED_RECORDER(subscribeToUpdates, 1, MAX_VALUE_MILLISECONDS, 3, concord::diagnostics::Unit::MILLISECONDS);
    DEFINE_SHARED_RECORDER(
        subscribeToUpdateHashes, 1, MAX_VALUE_MILLISECONDS, 3, concord::diagnostics::Unit::MILLISECONDS);
    DEFINE_SHARED_RECORDER(unsubscribe, 1, MAX_VALUE_MILLISECONDS, 3, concord::diagnostics::Unit::MILLISECONDS);
  };
  Recorders histograms_;
};

}  // namespace thin_replica
}  // namespace concord

#endif  // CONCORD_THIN_REPLICA_GRPC_SERVICES_HPP_
