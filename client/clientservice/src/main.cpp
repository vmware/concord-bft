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

#include <concord_prometheus_metrics.hpp>
#include <memory>
#include <string>
#include <vector>
#include <chrono>
#include <grpcpp/grpcpp.h>
#include <boost/program_options.hpp>
#include <yaml-cpp/yaml.h>

#include "client/clientservice/client_service.hpp"
#include "client/clientservice/configuration.hpp"
#include "client/concordclient/concord_client.hpp"
#include "Logger.hpp"
#include "Metrics.hpp"
#include "secret_retriever.hpp"
#include <jaegertracing/Tracer.h>

using concord::client::clientservice::ClientService;
using concord::client::clientservice::configureSubscription;
using concord::client::clientservice::configureTransport;
using concord::client::clientservice::parseConfigFile;

using concord::client::concordclient::ConcordClient;
using concord::client::concordclient::ConcordClientConfig;

namespace po = boost::program_options;

const static int kLogConfigRefreshIntervalInMs = 60 * 1000;

const static char* getLog4CplusConfigLocation() {
  auto log_location = std::getenv("LOG4CPLUS_CONFIGURATION");
  if (!log_location) std::cerr << "ClientService log4cplus configuration file was not set" << std::endl;
  return log_location ? log_location : "LOG4CPLUS_CONFIGURATION_NOT_SET";
}

po::variables_map parseCmdLine(int argc, char** argv) {
  po::options_description desc;
  // clang-format off
  desc.add_options()
    ("config", po::value<std::string>()->required(), "YAML configuration file for the RequestService")
    ("host", po::value<std::string>()->default_value("0.0.0.0"), "Clientservice gRPC service host")
    ("port", po::value<int>()->default_value(50505), "Clientservice gRPC service port")
    ("bft-batching", po::value<bool>()->default_value(false), "Enable batching requests before sending to replicas")
    ("tr-id", po::value<std::string>()->required(), "ID used to subscribe to replicas for data/hashes")
    ("tr-insecure", po::value<bool>()->default_value(false), "Testing only: Allow insecure connection with TRS on replicas")
    ("tr-tls-path", po::value<std::string>()->default_value(""), "Path to thin replica TLS certificates")
    ("metrics-port", po::value<int>()->default_value(9891), "Prometheus port to query clientservice metrics")
    ("secrets-url", po::value<std::string>(), "URL to decrypt private keys")
    ("jaeger", po::value<std::string>(), "Push trace data to this Jaeger Agent")
  ;
  // clang-format on
  po::variables_map opts;
  po::store(po::parse_command_line(argc, argv, desc), opts);
  po::notify(opts);

  return opts;
}

std::tuple<std::shared_ptr<concord::utils::PrometheusRegistry>,
           std::shared_ptr<concord::utils::ConcordBftPrometheusCollector>>
initPrometheus(int port) {
  std::string bind_address = "0.0.0.0:" + std::to_string(port);
  auto registry = std::make_shared<concord::utils::PrometheusRegistry>(bind_address);
  auto collector = std::make_shared<concord::utils::ConcordBftPrometheusCollector>();
  registry->scrapeRegistry(collector);
  return std::make_tuple(registry, collector);
}

class JaegerLogger : public jaegertracing::logging::Logger {
 private:
  logging::Logger logger = logging::getLogger("concord.client.clientservice.jaeger");

 public:
  void error(const std::string& message) override { LOG_ERROR(logger, message); }
  void info(const std::string& message) override { LOG_INFO(logger, message); }
};

void initJaeger(const std::string& addr, const std::string& id) {
  // No sampling for now - report all traces
  jaegertracing::samplers::Config sampler_config(jaegertracing::kSamplerTypeConst, 1.0);
  jaegertracing::reporters::Config reporter_config(jaegertracing::reporters::Config::kDefaultQueueSize,
                                                   jaegertracing::reporters::Config::defaultBufferFlushInterval(),
                                                   false /* false=don't log spans, true=JaegerUI */,
                                                   addr);
  jaegertracing::propagation::HeadersConfig headers_config(jaegertracing::kJaegerDebugHeader,
                                                           jaegertracing::kJaegerBaggageHeader,
                                                           jaegertracing::kTraceContextHeaderName,
                                                           jaegertracing::kTraceBaggageHeaderPrefix);
  jaegertracing::baggage::RestrictionsConfig baggage_restrictions(false, "", std::chrono::steady_clock::duration());
  std::string trace_proc_name = "clientservice-" + id;
  jaegertracing::Config config(false,
                               false,
                               sampler_config,
                               reporter_config,
                               headers_config,
                               baggage_restrictions,
                               trace_proc_name,
                               std::vector<jaegertracing::Tag>(),
                               jaegertracing::propagation::Format::W3C);
  auto tracer = jaegertracing::Tracer::make(
      trace_proc_name, config, std::unique_ptr<jaegertracing::logging::Logger>(new JaegerLogger()));
  opentracing::Tracer::InitGlobal(std::static_pointer_cast<opentracing::Tracer>(tracer));
}

int main(int argc, char** argv) {
  LOG_CONFIGURE_AND_WATCH(getLog4CplusConfigLocation(), kLogConfigRefreshIntervalInMs);
  auto logger = logging::getLogger("concord.client.clientservice.main");
  po::variables_map opts;
  try {
    opts = parseCmdLine(argc, argv);
  } catch (const boost::bad_lexical_cast& e) {
    LOG_ERROR(logger, "Failed to parse command line arguments: " << e.what());
    return 1;
  }

  ConcordClientConfig config;
  try {
    auto yaml = YAML::LoadFile(opts["config"].as<std::string>());
    parseConfigFile(config, yaml);
    std::optional<std::string> secrets_url = std::nullopt;
    if (opts.count("secrets-url") && config.topology.encrypted_config_enabled) {
      secrets_url = {opts["secrets-url"].as<std::string>()};
      if (secrets_url) {
        config.transport.secret_data = concord::secretsmanager::secretretriever::retrieveSecret(*secrets_url);
      }
    }
    configureSubscription(config,
                          opts["tr-id"].as<std::string>(),
                          opts["tr-insecure"].as<bool>(),
                          opts["tr-tls-path"].as<std::string>(),
                          secrets_url);
    configureTransport(config, opts["tr-insecure"].as<bool>(), opts["tr-tls-path"].as<std::string>());
  } catch (std::exception& e) {
    LOG_ERROR(logger, "Failed to configure ConcordClient: " << e.what());
    return 1;
  }
  LOG_INFO(logger, "ConcordClient configured");

  // Metrics
  auto port = opts["metrics-port"].as<int>();
  const auto& ptuple = initPrometheus(port);
  const auto& metrics_collector = std::get<1>(ptuple);
  LOG_INFO(logger, "Prometheus metrics available on port " << port);

  // Tracing
  if (opts.count("jaeger")) {
    auto jaeger_addr = opts["jaeger"].as<std::string>();
    initJaeger(jaeger_addr, opts["tr-id"].as<std::string>());
    LOG_INFO(logger, "Sending trace data to Jaeger Agent at " << jaeger_addr);
  } else {
    LOG_INFO(logger, "No Jaeger Agent address provided");
  }

  auto concord_client = std::make_unique<ConcordClient>(config, metrics_collector->getAggregator());
  ClientService service(std::move(concord_client));

  auto server_addr = opts["host"].as<std::string>() + ":" + std::to_string(opts["port"].as<int>());
  LOG_INFO(logger, "Starting clientservice at " << server_addr);
  service.start(server_addr);

  return 0;
}
