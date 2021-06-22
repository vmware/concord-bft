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

#include "Logger.hpp"
#include "client_service.hpp"
#include "client/concordclient/concord_client.hpp"

using concord::client::clientservice::ClientService;
using concord::client::concordclient::ConcordClient;
using concord::client::concordclient::ConcordClientConfig;

int main(int argc, char** argv) {
  // TODO: Use config file and watch thread
  auto logger = logging::getLogger("concord.client.clientservice.main");

  std::string addr = "localhost:1337";
  LOG_INFO(logger, "Starting clientservice at " << addr);

  // TODO: Setup ConcordClientConfig and read from config file
  ConcordClientConfig config{};
  auto concord_client = std::make_unique<ConcordClient>(config);
  ClientService service(std::move(concord_client));
  service.start(addr);

  return 0;
}
