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

#include <memory>
#include <string>
#include <grpcpp/grpcpp.h>
#include <boost/program_options.hpp>
#include <yaml-cpp/yaml.h>

#include "client/clientservice/client_service.hpp"
#include "client/clientservice/configuration.hpp"
#include "client/concordclient/concord_client.hpp"
#include "Logger.hpp"
#include "Metrics.hpp"

using concord::client::clientservice::ClientService;
using concord::client::concordclient::ConcordClient;
using concord::client::concordclient::ConcordClientConfig;
using concord::client::clientservice::parseConfigFile;

namespace po = boost::program_options;

po::variables_map parseCmdLine(int argc, char** argv) {
  po::options_description desc;
  // clang-format off
  desc.add_options()
    ("server-port", po::value<int>()->default_value(1337), "Clientservice gRPC service port")
    ("config", po::value<std::string>()->required(), "YAML configuration file for the RequestService")
    ("event-service-id", po::value<std::string>()->required(), "ID used to subscribe to replicas for data/hashes")
  ;
  // clang-format on
  po::variables_map opts;
  po::store(po::parse_command_line(argc, argv, desc), opts);
  po::notify(opts);

  return opts;
}

int main(int argc, char** argv) {
  // TODO: Use config file and watch thread for logger
  auto logger = logging::getLogger("concord.client.clientservice.main");

  auto opts = parseCmdLine(argc, argv);

  ConcordClientConfig config;
  try {
    auto yaml = YAML::LoadFile(opts["config"].as<std::string>());
    parseConfigFile(config, yaml);
    config.subscribe_config.id = opts["event-service-id"].as<std::string>();
    // TODO: Read TLS certs and fill config struct
    // TODO: Configure TRS endpoints
  } catch (std::exception& e) {
    LOG_ERROR(logger, "Failed to configure ConcordClient: " << e.what());
    return 1;
  }

  auto concord_client = std::make_unique<ConcordClient>(config);
  auto metrics = std::make_shared<concordMetrics::Aggregator>();
  concord_client->setMetricsAggregator(metrics);
  ClientService service(std::move(concord_client));

  auto server_addr = std::string("localhost:") + std::to_string(opts["server-port"].as<int>());
  LOG_INFO(logger, "Starting clientservice at " << server_addr);
  service.start(server_addr);

  return 0;
}
