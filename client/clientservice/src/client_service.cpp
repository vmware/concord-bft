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

#include "client_service.hpp"

#include <concord_client.grpc.pb.h>
#include <grpcpp/grpcpp.h>

namespace concord::client::clientservice {

ClientService::ClientService() {
  request_service_ = std::make_unique<RequestServiceImpl>();
  event_service_ = std::make_unique<EventServiceImpl>();
}

void ClientService::start(const std::string& addr) {
  grpc::EnableDefaultHealthCheckService(true);

  grpc::ServerBuilder builder;
  builder.AddListeningPort(addr, grpc::InsecureServerCredentials());
  builder.RegisterService(request_service_.get());
  builder.RegisterService(event_service_.get());

  auto clientservice_server = std::unique_ptr<grpc::Server>(builder.BuildAndStart());
  clientservice_server->Wait();
}

}  // namespace concord::client::clientservice
