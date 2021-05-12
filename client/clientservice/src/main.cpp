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

#include <iostream>
#include <memory>
#include <grpcpp/grpcpp.h>

#include <concord_client.grpc.pb.h>
#include "event_service.hpp"
#include "request_service.hpp"

using concord::client::RequestServiceImpl;
using concord::client::EventServiceImpl;

static std::unique_ptr<grpc::Server> clientservice_server;

void runGrpcServer() {
  grpc::EnableDefaultHealthCheckService(true);

  auto request_service = std::make_unique<RequestServiceImpl>();
  auto event_service = std::make_unique<EventServiceImpl>();

  grpc::ServerBuilder builder;
  builder.AddListeningPort("localhost:1337", grpc::InsecureServerCredentials());
  builder.RegisterService(request_service.get());
  builder.RegisterService(event_service.get());

  clientservice_server = std::unique_ptr<grpc::Server>(builder.BuildAndStart());
  clientservice_server->Wait();
}

int main(int argc, char** argv) {
  std::cout << "Clientservice started" << std::endl;

  runGrpcServer();

  return 0;
}
