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

#include <map>
#include <mutex>

#include <grpcpp/server_context.h>
#include <grpcpp/support/status.h>
#include "client/concordclient/concord_client.hpp"

#include <health.grpc.pb.h>

namespace concord::client::clientservice {

using grpc::health::v1::HealthCheckRequest;
using grpc::health::v1::HealthCheckResponse;
using grpc::health::v1::HealthCheckResponse_ServingStatus;

// A sync implementation of the health checking service.
class HealthCheckServiceImpl : public grpc::health::v1::Health::Service {
 public:
  HealthCheckServiceImpl(std::shared_ptr<concord::client::concordclient::ConcordClient> client)
      : logger_(logging::getLogger("concord.client.clientservice.event")), client_(client){};
  grpc::Status Check(grpc::ServerContext* context,
                     const HealthCheckRequest* request,
                     HealthCheckResponse* response) override;
  grpc::Status Watch(grpc::ServerContext* context,
                     const HealthCheckRequest* request,
                     grpc::ServerWriter<HealthCheckResponse>* writer) override;
  void SetStatus(const std::string& service_name, HealthCheckResponse::ServingStatus status);
  void SetAll(HealthCheckResponse::ServingStatus status);

  void Shutdown();

 private:
  // Queries the health status of the client_pool via the concord client and returns true if
  // atleast one client in the client pool is ready to serve requests
  HealthCheckResponse_ServingStatus getRequeserviceHealthStatus();

  // updates the overall clientservice health, i.e., reports healthy
  // iff all the services in clientservice are healthy
  void updateAggregateHealth();

 public:
  const std::string kRequestService{"vmware.concord.client.request.v1.RequestService"};
  const std::string kEventService{"vmware.concord.client.event.v1.EventService"};
  const std::string kStateSnapshotService{"vmware.concord.client.statesnapshot.v1.StateSnapshotService"};

 private:
  logging::Logger logger_;
  std::mutex mtx_;
  bool shutdown_ = false;
  std::shared_ptr<concord::client::concordclient::ConcordClient> client_;
  std::map<const std::string, HealthCheckResponse::ServingStatus> status_map_;
};

class HealthCheckService : public grpc::HealthCheckServiceInterface {
 public:
  explicit HealthCheckService(HealthCheckServiceImpl* impl) : impl_(impl) {
    impl_->SetStatus("", HealthCheckResponse::SERVING);
  }
  void SetServingStatus(const std::string& service_name, bool serving) override {
    impl_->SetStatus(service_name, serving ? HealthCheckResponse::SERVING : HealthCheckResponse::NOT_SERVING);
  }

  void SetServingStatus(bool serving) override {
    impl_->SetAll(serving ? HealthCheckResponse::SERVING : HealthCheckResponse::NOT_SERVING);
  }

  void Shutdown() override { impl_->Shutdown(); }

 private:
  std::shared_ptr<HealthCheckServiceImpl> impl_;  // not owned
};
}  // namespace concord::client::clientservice