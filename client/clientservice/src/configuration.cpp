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

#include "client/clientservice/configuration.hpp"

#include <chrono>
#include <string>
#include <sstream>

#include "assertUtils.hpp"
#include "secret_retriever.hpp"
#include "secrets_manager_enc.h"
#include "secrets_manager_plain.h"
#include "client/clientservice/client_service.hpp"

using concord::client::concordclient::ConcordClientConfig;

static auto logger = logging::getLogger("concord.client.clientservice.configuration");

namespace concord::client::clientservice {

// Copy a value from the YAML node to `out`.
// Throws and exception if no value could be read but the value is required.
template <typename T>
static void readYamlField(const YAML::Node& yaml, const std::string& index, T& out, bool value_required = true) {
  try {
    out = yaml[index].as<T>();
  } catch (const std::exception& e) {
    if (value_required) {
      // We ignore the YAML excpetions because they aren't useful
      std::ostringstream msg;
      msg << "Failed to read \"" << index << "\"";
      throw std::runtime_error(msg.str().data());
    } else {
      LOG_INFO(logger, "No value found for \"" << index << "\"");
    }
  }
}
static void readYamlField(const YAML::Node& yaml,
                          const std::string& index,
                          std::chrono::milliseconds& out,
                          bool value_required = true) {
  try {
    out = std::chrono::milliseconds(yaml[index].as<uint64_t>());
  } catch (const std::exception& e) {
    if (value_required) {
      // We ignore the YAML excpetions because they aren't useful
      std::ostringstream msg;
      msg << "Failed to read milliseconds \"" << index << "\"";
      throw std::runtime_error(msg.str().data());
    } else {
      LOG_INFO(logger, "No value found for \"" << index << "\"");
    }
  }
}

void parseConfigFile(ConcordClientConfig& config, const YAML::Node& yaml) {
  readYamlField(yaml, "f_val", config.topology.f_val);
  readYamlField(yaml, "c_val", config.topology.c_val);
  readYamlField(yaml,
                "client_sends_request_to_all_replicas_first_thresh",
                config.topology.client_sends_request_to_all_replicas_first_thresh);
  readYamlField(yaml,
                "client_sends_request_to_all_replicas_period_thresh",
                config.topology.client_sends_request_to_all_replicas_period_thresh);
  readYamlField(yaml, "signing_key_path", config.topology.signing_key_path);
  readYamlField(yaml, "encrypted_config_enabled", config.topology.encrypted_config_enabled);
  readYamlField(yaml, "transaction_signing_enabled", config.topology.transaction_signing_enabled);
  readYamlField(yaml, "with_cre", config.topology.with_cre);
  readYamlField(yaml, "client_batching_enabled", config.topology.client_batching_enabled);
  readYamlField(yaml, "client_batching_max_messages_nbr", config.topology.client_batching_max_messages_nbr);
  readYamlField(yaml, "client_batching_flush_timeout_ms", config.topology.client_batching_flush_timeout_ms);

  ConcordAssert(yaml["node"].IsSequence());
  for (const auto& node : yaml["node"]) {
    ConcordAssert(node.IsMap());
    ConcordAssert(node["replica"].IsSequence());
    ConcordAssert(node["replica"][0].IsMap());
    auto replica = node["replica"][0];

    concord::client::concordclient::ReplicaInfo ri;
    readYamlField(replica, "principal_id", ri.id.val);
    readYamlField(replica, "replica_host", ri.host);
    readYamlField(replica, "replica_port", ri.bft_port);
    readYamlField(replica, "event_port", ri.event_port);

    config.topology.replicas.push_back(ri);
  }

  readYamlField(yaml, "client_initial_retry_timeout_milli", config.topology.client_retry_config.initial_retry_timeout);
  readYamlField(yaml, "client_min_retry_timeout_milli", config.topology.client_retry_config.min_retry_timeout);
  readYamlField(yaml, "client_max_retry_timeout_milli", config.topology.client_retry_config.max_retry_timeout);
  readYamlField(yaml, "client_samples_per_evaluation", config.topology.client_retry_config.samples_per_evaluation);
  readYamlField(yaml,
                "client_number_of_standard_deviations_to_tolerate",
                config.topology.client_retry_config.number_of_standard_deviations_to_tolerate);
  readYamlField(yaml, "client_samples_until_reset", config.topology.client_retry_config.samples_until_reset);
  readYamlField(yaml, "enable_mock_comm", config.transport.enable_mock_comm);

  readYamlField(yaml, "concord-bft_communication_buffer_length", config.transport.buffer_length);

  concord::client::concordclient::TransportConfig::CommunicationType comm_type;
  std::string comm;
  readYamlField(yaml, "comm_to_use", comm);
  if (comm == "tls") {
    comm_type = concord::client::concordclient::TransportConfig::TlsTcp;
    readYamlField(yaml, "tls_certificates_folder_path", config.transport.tls_cert_root_path);
    readYamlField(yaml, "tls_cipher_suite_list", config.transport.tls_cipher_suite);
  } else if (comm == "udp") {
    comm_type = concord::client::concordclient::TransportConfig::PlainUdp;
  } else {
    comm_type = concord::client::concordclient::TransportConfig::Invalid;
  }
  config.transport.comm_type = comm_type;

  auto node = yaml["participant_nodes"][0];
  ConcordAssert(node.IsMap());
  ConcordAssert(node["participant_node"].IsSequence());
  ConcordAssert(node["participant_node"][0].IsMap());
  ConcordAssert(node["participant_node"][0]["external_clients"].IsSequence());
  for (const auto& item : node["participant_node"][0]["external_clients"]) {
    ConcordAssert(item.IsMap());
    ConcordAssert(item["client"].IsSequence());
    ConcordAssert(item["client"][0].IsMap());
    auto client = item["client"][0];

    concord::client::concordclient::BftClientInfo ci;
    readYamlField(client, "principal_id", ci.id.val);
    readYamlField(client, "client_port", ci.port);
    readYamlField(node["participant_node"][0], "participant_node_host", ci.host);
    config.bft_clients.push_back(ci);
  }

  config.num_of_used_bft_clients = yaml["clients_per_participant_node"].as<int16_t>();
}

void configureSubscription(concord::client::concordclient::ConcordClientConfig& config,
                           const std::string& tr_id,
                           bool is_insecure,
                           const std::string& tls_path,
                           const std::optional<std::string>& secrets_url) {
  config.subscribe_config.id = tr_id;
  config.subscribe_config.use_tls = not is_insecure;

  if (config.subscribe_config.use_tls) {
    LOG_INFO(logger, "TLS for thin replica client is enabled, certificate path: " << tls_path);
    const std::string client_cert_path = tls_path + "/client.cert";

    readCert(client_cert_path, config.subscribe_config.pem_cert_chain);

    config.subscribe_config.pem_private_key = decryptPrivateKey(secrets_url, tls_path);

    std::string cert_client_id = getClientIdFromClientCert(client_cert_path);
    // The client cert must have the client ID in the OU field, because the TRS obtains
    // the client_id from the certificate of the connecting client.
    if (cert_client_id.empty()) {
      LOG_FATAL(logger, "Failed to construct concord client.");
      throw std::runtime_error("The OU field in client certificate is empty. It must contain the client ID.");
    }
    // cert_client_id in client cert should match the client_id if TLS is
    // enabled for TRC-TRS connection. Since the TRS reads the client id from
    // the connecting client cert, and the value of the TRID specified by the
    // user for TRC initialization can be used by TRC's client application to
    // generate requests, if they do not match, the TRS will filter out all the
    // key value pairs meant for the requesting client.
    if (cert_client_id.compare(config.subscribe_config.id) != 0) {
      LOG_FATAL(logger, "Failed to construct concord client.");
      throw std::runtime_error("The client ID in the OU field of the client certificate (" + cert_client_id +
                               ") does not match the client ID in the environment variable (" +
                               config.subscribe_config.id + ").");
    }
  } else {
    LOG_WARN(logger,
             "TLS for thin replica client is disabled, falling back to "
             "insecure channel");
  }
}

void configureTransport(concord::client::concordclient::ConcordClientConfig& config,
                        bool is_insecure,
                        const std::string& tls_path) {
  if (not is_insecure) {
    const std::string server_cert_path = tls_path + "/server.cert";
    // read server TLS certs for this TRC instance
    // server_cert_path specifies the path to a composite cert file i.e., a
    // concatentation of the certificates of all known servers
    readCert(server_cert_path, config.transport.event_pem_certs);
  }
}

const std::string decryptPrivateKey(const std::optional<std::string>& secrets_url, const std::string& path) {
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

void readCert(const std::string& input_filename, std::string& out_data) {
  std::ifstream input_file(input_filename.c_str(), std::ios::in);

  if (!input_file.is_open()) {
    LOG_FATAL(logger, "Failed to construct concord client.");
    throw std::runtime_error(__PRETTY_FUNCTION__ + std::string(": Could not open the input file (") + input_filename +
                             std::string(") to establish TLS connection with thin replica server."));
  }
  try {
    std::stringstream read_buffer;
    read_buffer << input_file.rdbuf();
    input_file.close();
    out_data = read_buffer.str();
    LOG_INFO(logger, "Successfully loaded the contents of " + input_filename);
  } catch (std::exception& e) {
    LOG_FATAL(logger, "Failed to construct concord client.");
    throw std::runtime_error(__PRETTY_FUNCTION__ +
                             std::string(": An exception occurred while trying to read the input file (") +
                             input_filename + std::string("): ") + std::string(e.what()));
  }
}

std::string getClientIdFromClientCert(const std::string& client_cert_path) {
  std::array<char, 128> buffer;
  std::string client_id;

  // check if client cert can be opened
  std::ifstream input_file(client_cert_path.c_str(), std::ios::in);

  if (!input_file.is_open()) {
    throw std::runtime_error("Could not open the input file (" + client_cert_path + ") at the concord client.");
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
std::string parseClientIdFromSubject(const std::string& subject_str) {
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

}  // namespace concord::client::clientservice
