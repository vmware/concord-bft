// Copyright (c) 2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the sub-component's license, as noted in the LICENSE
// file.

#include <chrono>
#include <thread>

#include "assertUtils.hpp"
#include "secret_retriever.hpp"
#include "secrets_manager_enc.h"
#include "secrets_manager_plain.h"
#include "client/concordclient/concord_client.hpp"
#include "client/thin-replica-client/thin_replica_client.hpp"

using ::client::thin_replica_client::ThinReplicaClient;
using ::client::thin_replica_client::ThinReplicaClientConfig;
using ::client::thin_replica_client::TrsConnection;
using concord::config_pool::ConcordClientPoolConfig;
using concord::config_pool::Replica;
using concord::config_pool::ExternalClient;
using concord::config_pool::ParticipantNode;
using ::client::thin_replica_client::TrsConnectionConfig;

namespace concord::client::concordclient {

ConcordClient::ConcordClient(const ConcordClientConfig& config, std::shared_ptr<concordMetrics::Aggregator> aggregator)
    : logger_(logging::getLogger("concord.client.concordclient")), config_(config), metrics_(aggregator) {
  ConcordClientPoolConfig client_pool_config = createClientPoolStruct(config);
  client_pool_ = std::make_unique<concord::concord_client_pool::ConcordClientPool>(client_pool_config, metrics_);
  while (client_pool_->HealthStatus() == concord::concord_client_pool::PoolStatus::NotServing) {
    LOG_INFO(logger_, "Waiting for client pool to connect");
    std::this_thread::sleep_for(std::chrono::seconds(2));
  }
}

ConcordClientPoolConfig ConcordClient::createClientPoolStruct(const ConcordClientConfig& config) {
  ConcordClientPoolConfig client_pool_config;
  int id = 0;
  for (const auto& replica : config_.topology.replicas) {
    Replica client_pool_replica;
    auto replica_id = replica.id.val;
    client_pool_replica.principal_id = replica_id;
    client_pool_replica.replica_host = replica.host;
    client_pool_replica.replica_port = replica.bft_port;
    client_pool_config.node[id] = client_pool_replica;
    id++;
  }

  ParticipantNode client_pool_pn;
  id = 0;
  for (const auto& bft_client : config_.bft_clients) {
    ExternalClient client_pool_ec;
    auto external_client_id = bft_client.id.val;
    client_pool_ec.principal_id = external_client_id;
    client_pool_ec.client_port = bft_client.port;
    client_pool_pn.externalClients[id] = client_pool_ec;
    id++;
  }
  client_pool_pn.participant_node_host = config_.bft_clients[0].host;
  client_pool_config.participant_nodes.push_back(client_pool_pn);
  client_pool_config.clients_per_participant_node = config_.num_of_used_bft_clients;

  client_pool_config.f_val = config.topology.f_val;
  client_pool_config.c_val = config.topology.c_val;
  client_pool_config.client_initial_retry_timeout_milli =
      config.topology.client_retry_config.initial_retry_timeout.count();
  client_pool_config.client_min_retry_timeout_milli = config.topology.client_retry_config.min_retry_timeout.count();
  client_pool_config.client_max_retry_timeout_milli = config.topology.client_retry_config.max_retry_timeout.count();
  client_pool_config.client_number_of_standard_deviations_to_tolerate =
      config.topology.client_retry_config.number_of_standard_deviations_to_tolerate;
  client_pool_config.client_samples_per_evaluation = config.topology.client_retry_config.samples_per_evaluation;
  client_pool_config.client_samples_until_reset = config.topology.client_retry_config.samples_until_reset;
  client_pool_config.client_sends_request_to_all_replicas_first_thresh =
      config.topology.client_sends_request_to_all_replicas_first_thresh;
  client_pool_config.client_sends_request_to_all_replicas_period_thresh =
      config.topology.client_sends_request_to_all_replicas_period_thresh;
  client_pool_config.num_replicas = config.topology.replicas.size();
  client_pool_config.client_proxies_per_replica = config.topology.client_proxies_per_replica;
  client_pool_config.signing_key_path = config.topology.signing_key_path;
  client_pool_config.external_requests_queue_size = config.topology.external_requests_queue_size;
  client_pool_config.encrypted_config_enabled = config.topology.encrypted_config_enabled;
  client_pool_config.transaction_signing_enabled = config.topology.transaction_signing_enabled;
  client_pool_config.with_cre = config.topology.with_cre;
  client_pool_config.client_batching_enabled = config.topology.client_batching_enabled;
  client_pool_config.client_batching_max_messages_nbr = config.topology.client_batching_max_messages_nbr;
  client_pool_config.client_batching_flush_timeout_ms = config.topology.client_batching_flush_timeout_ms;

  client_pool_config.comm_to_use = config.transport.comm_type == TransportConfig::Invalid
                                       ? "Invalid"
                                       : config.transport.comm_type == TransportConfig::TlsTcp ? "tls" : "udp";
  client_pool_config.concord_bft_communication_buffer_length = std::to_string(config.transport.buffer_length);
  client_pool_config.tls_certificates_folder_path = config.transport.tls_cert_root_path;
  client_pool_config.tls_cipher_suite_list = config.transport.tls_cipher_suite;
  client_pool_config.enable_mock_comm = config.transport.enable_mock_comm;

  return client_pool_config;
}

void ConcordClient::send(const bft::client::ReadConfig& config,
                         bft::client::Msg&& msg,
                         const std::unique_ptr<opentracing::Span>& parent_span,
                         const std::function<void(SendResult&&)>& callback) {
  LOG_INFO(logger_, "Log message until config is used f=" << config_.topology.f_val);
  client_pool_->SendRequest(config, std::forward<bft::client::Msg>(msg), callback);
}

void ConcordClient::send(const bft::client::WriteConfig& config,
                         bft::client::Msg&& msg,
                         const std::unique_ptr<opentracing::Span>& parent_span,
                         const std::function<void(SendResult&&)>& callback) {
  client_pool_->SendRequest(config, std::forward<bft::client::Msg>(msg), callback);
}

void ConcordClient::subscribe(const SubscribeRequest& sub_req,
                              std::shared_ptr<UpdateQueue>& queue,
                              const std::unique_ptr<opentracing::Span>& parent_span) {
  bool expected = false;
  if (!active_subscription_.compare_exchange_weak(expected, true)) {
    LOG_ERROR(logger_, "subscription already in progress - unsubscribe first");
    throw SubscriptionExists();
  }

  std::vector<std::unique_ptr<::client::thin_replica_client::TrsConnection>> trs_connections;
  std::unique_ptr<TrsConnectionConfig> trsc_config;
  for (const auto& replica : config_.topology.replicas) {
    auto addr = replica.host + ":" + std::to_string(replica.event_port);
    auto trsc = std::make_unique<TrsConnection>(addr, config_.subscribe_config.id, /* TODO */ 3, /* TODO */ 3);

    // TODO: Adapt TRC API to support PEM buffers
    if (config_.subscribe_config.use_tls) {
      std::string trc_tls_key =
          decryptPK(config_.subscribe_config.secrets_url, config_.subscribe_config.trsc_tls_cert_path);
      LOG_INFO(
          logger_,
          "TLS for thin replica client is enabled, certificate path: " << config_.subscribe_config.trsc_tls_cert_path);
      const std::string client_cert_path = config_.subscribe_config.trsc_tls_cert_path + "/client.cert";
      const std::string server_cert_path = config_.subscribe_config.trsc_tls_cert_path + "/server.cert";

      std::string trc_cert, root_cert;
      readCert(client_cert_path, trc_cert);

      // server_cert_path specifies the path to a composite cert file i.e., a
      // concatentation of the certificates of all known servers
      readCert(server_cert_path, root_cert);

      std::string cert_trc_id = getClientIdFromClientCert(client_cert_path);
      trsc_config = std::make_unique<TrsConnectionConfig>(
          config_.subscribe_config.use_tls, trc_tls_key, trc_cert, root_cert, cert_trc_id);
    } else {
      trsc_config = std::make_unique<TrsConnectionConfig>(config_.subscribe_config.use_tls);
    }

    trsc->connect(trsc_config);
    trs_connections.push_back(std::move(trsc));
  }

  auto trc_config = std::make_unique<ThinReplicaClientConfig>(
      config_.subscribe_config.id, queue, config_.topology.f_val, std::move(trs_connections));
  trc_ = std::make_unique<ThinReplicaClient>(std::move(trc_config), metrics_);

  if (std::holds_alternative<EventGroupRequest>(sub_req.request)) {
    ::client::thin_replica_client::SubscribeRequest trc_request;
    trc_request.event_group_id = std::get<EventGroupRequest>(sub_req.request).event_group_id;
    trc_->Subscribe(trc_request);
  } else if (std::holds_alternative<LegacyEventRequest>(sub_req.request)) {
    trc_->Subscribe(std::get<LegacyEventRequest>(sub_req.request).block_id);
  } else {
    ConcordAssert(false);
  }
}

const std::string ConcordClient::decryptPK(const std::optional<std::string>& secrets_url, const std::string& path) {
  std::string pkpath;
  std::unique_ptr<concord::secretsmanager::ISecretsManagerImpl> secrets_manager;
  if (secrets_url) {
    auto secret_data = concord::secretsmanager::secretretriever::retrieveSecret(*secrets_url);
    pkpath = path + "/pk.pem.enc";
    secrets_manager.reset(new concord::secretsmanager::SecretsManagerEnc(secret_data));
  } else {
    pkpath = path + "/pk.pem";
    secrets_manager.reset(new concord::secretsmanager::SecretsManagerPlain());
  }

  auto decrypted_data = secrets_manager->decryptFile(pkpath);
  if (!decrypted_data) {
    throw std::runtime_error("Error loading " + pkpath);
  }

  return *decrypted_data;
}

void ConcordClient::readCert(const std::string& input_filename, std::string& out_data) {
  std::ifstream input_file(input_filename.c_str(), std::ios::in);

  if (!input_file.is_open()) {
    LOG_FATAL(logger_, "Failed to construct concord client.");
    throw std::runtime_error(__PRETTY_FUNCTION__ + std::string(": Could not open the input file (") + input_filename +
                             std::string(") to establish TLS connection with the thin replica server."));
  } else {
    try {
      std::stringstream read_buffer;
      read_buffer << input_file.rdbuf();
      input_file.close();
      out_data = read_buffer.str();
      LOG_INFO(logger_, "Successfully loaded the contents of " + input_filename);
    } catch (std::exception& e) {
      LOG_FATAL(logger_, "Failed to construct concord client.");
      throw std::runtime_error(__PRETTY_FUNCTION__ +
                               std::string(": An exception occurred while trying to read the input file (") +
                               input_filename + std::string("): ") + std::string(e.what()));
    }
  }
  return;
}

std::string ConcordClient::getClientIdFromClientCert(const std::string& client_cert_path) {
  std::array<char, 128> buffer;
  std::string client_id;

  // check if client cert can be opened
  std::ifstream input_file(client_cert_path.c_str(), std::ios::in);

  if (!input_file.is_open()) {
    throw std::runtime_error("Could not open the input file (" + client_cert_path + ") at the thin replica client.");
  }

  // The cmd string is used to get the subject in the client cert.
  std::string cmd =
      "openssl crl2pkcs7 -nocrl -certfile " + client_cert_path + " | openssl pkcs7 -print_certs -noout | grep .";
  std::unique_ptr<FILE, decltype(&pclose)> pipe(popen(cmd.c_str(), "r"), pclose);
  if (!pipe) {
    throw std::runtime_error("Failed to read subject fields from client cert - popen() failed!");
  }
  if (fgets(buffer.data(), buffer.size(), pipe.get()) != nullptr) {
    // parse the OU field i.e., the client id from the subject field
    client_id = parseClientIdFromSubject(buffer.data());
  }
  return client_id;
}

// Parses the value of the OU field i.e., the client id from the subject
// string
std::string ConcordClient::parseClientIdFromSubject(const std::string& subject_str) {
  std::string delim = "OU = ";
  size_t start = subject_str.find(delim) + delim.length();
  size_t end = subject_str.find(',', start);
  std::string raw_str = subject_str.substr(start, end - start);
  size_t fstart = 0;
  size_t fend = raw_str.length();
  // remove surrounding whitespaces and newlines
  if (raw_str.find_first_not_of(' ') != std::string::npos) fstart = raw_str.find_first_not_of(' ');
  if (raw_str.find_last_not_of(' ') != std::string::npos) fend = raw_str.find_last_not_of(' ');
  raw_str.erase(std::remove(raw_str.begin(), raw_str.end(), '\n'), raw_str.end());
  return raw_str.substr(fstart, fend - fstart + 1);
}

void ConcordClient::unsubscribe() {
  if (active_subscription_) {
    LOG_INFO(logger_, "Closing subscription. Waiting for subscriber to finish.");
    trc_->Unsubscribe();
    trc_.reset();
    active_subscription_ = false;
    LOG_INFO(logger_, "Subscriber finished.");
  }
}

}  // namespace concord::client::concordclient
