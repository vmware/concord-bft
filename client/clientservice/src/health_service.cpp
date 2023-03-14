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

#include "client/clientservice/health_service.hpp"

namespace concord::client::clientservice {

grpc::Status HealthCheckServiceImpl::Check(grpc::ServerContext* /*context*/,
                                           const HealthCheckRequest* request,
                                           HealthCheckResponse* response) {
  std::lock_guard<std::mutex> lock(mtx_);
  auto iter = status_map_.find(request->service());
  if (iter == status_map_.end()) {
    return grpc::Status(grpc::StatusCode::NOT_FOUND, "");
  }
  if (iter->first == kRequestService) {
    auto rs_status = getRequeserviceHealthStatus();
    response->set_status(rs_status);
    iter->second = rs_status;
  } else {
    updateAggregateHealth();
    // TODO: Add implementation for event service and state snapshot service
    response->set_status(iter->second);
  }
  return grpc::Status::OK;
}

// TODO: Make Watch asynchronous instead of using a polling mechanism for efficiency
grpc::Status HealthCheckServiceImpl::Watch(grpc::ServerContext* context,
                                           const HealthCheckRequest* request,
                                           grpc::ServerWriter<HealthCheckResponse>* writer) {
  auto last_state = HealthCheckResponse::UNKNOWN;
  while (!context->IsCancelled()) {
    {
      std::lock_guard<std::mutex> lock(mtx_);
      HealthCheckResponse response;
      auto iter = status_map_.find(request->service());
      if (iter == status_map_.end()) {
        response.set_status(response.SERVICE_UNKNOWN);
      } else if (iter->first == kRequestService) {
        auto rs_status = getRequeserviceHealthStatus();
        response.set_status(rs_status);
        iter->second = rs_status;
      } else {
        updateAggregateHealth();
        // TODO: Add implementation for event service and state snapshot service
        response.set_status(iter->second);
      }
      if (response.status() != last_state) {
        writer->Write(response, grpc::WriteOptions());
        last_state = response.status();
      }
    }
    gpr_sleep_until(gpr_time_add(gpr_now(GPR_CLOCK_MONOTONIC), gpr_time_from_seconds(30, GPR_TIMESPAN)));
  }
  return grpc::Status::OK;
}

void HealthCheckServiceImpl::SetStatus(const std::string& service_name, HealthCheckResponse::ServingStatus status) {
  std::lock_guard<std::mutex> lock(mtx_);
  if (shutdown_) {
    status = HealthCheckResponse::NOT_SERVING;
  }
  status_map_[service_name] = status;
}

void HealthCheckServiceImpl::SetAll(HealthCheckResponse::ServingStatus status) {
  std::lock_guard<std::mutex> lock(mtx_);
  if (shutdown_) {
    return;
  }
  for (auto iter = status_map_.begin(); iter != status_map_.end(); ++iter) {
    iter->second = status;
  }
}

void HealthCheckServiceImpl::Shutdown() {
  std::lock_guard<std::mutex> lock(mtx_);
  if (shutdown_) {
    return;
  }
  shutdown_ = true;
  for (auto iter = status_map_.begin(); iter != status_map_.end(); ++iter) {
    iter->second = HealthCheckResponse::NOT_SERVING;
  }
}

HealthCheckResponse_ServingStatus HealthCheckServiceImpl::getRequeserviceHealthStatus() {
  if (!client_) return HealthCheckResponse::NOT_SERVING;
  return (client_->isClientPoolHealthy()) ? HealthCheckResponse::SERVING : HealthCheckResponse::NOT_SERVING;
}

void HealthCheckServiceImpl::updateAggregateHealth() {
  bool aggregate_health = true;  // reports healthy iff all services are healthy
  for (auto& [k, v] : status_map_) {
    if (k == "") continue;
    if (k == kRequestService) {
      v = getRequeserviceHealthStatus();
    }
    aggregate_health = aggregate_health && (v == HealthCheckResponse::SERVING);
  }
  status_map_[""] = aggregate_health ? HealthCheckResponse::SERVING : HealthCheckResponse::NOT_SERVING;
}

}  // namespace concord::client::clientservice