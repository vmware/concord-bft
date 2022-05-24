// Concord
//
// Copyright (c) 2018 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include <fstream>
#include <iostream>
#include <regex>
#include <string>
#include <unordered_map>
#include <string.hpp>
#include <exception>
#include "KeyfileIOUtils.hpp"
#include "yaml_utils.hpp"
#include "util/filesystem.hpp"

void outputReplicaKeyfile(uint16_t numReplicas,
                          uint16_t numRoReplicas,
                          bftEngine::ReplicaConfig& config,
                          const std::string& outputFilename,
                          Cryptosystem* commonSys) {
  std::cout << "Outputting key file: " << fs::absolute(outputFilename) << std::endl;

  std::ofstream output(outputFilename);
  if ((3 * config.fVal + 2 * config.cVal + 1) != numReplicas)
    throw std::runtime_error("F, C, and number of replicas do not agree for requested output.");

  output << "# Concord-BFT replica keyfile " << outputFilename << ".\n"
         << "# For replica " << config.replicaId << " in a " << numReplicas << "-replica + " << numRoReplicas
         << "-read-only-replica cluster.\n\n"
         << "num_replicas: " << numReplicas << "\n"
         << "num_ro_replicas: " << numRoReplicas << "\n"
         << "f_val: " << config.fVal << "\n"
         << "c_val: " << config.cVal << "\n"
         << "replica_id: " << config.replicaId << "\n"
         << "read-only: " << config.isReadOnly << "\n\n"
         << "# RSA non-threshold replica public keys\n"
         << "rsa_public_keys:\n";

  for (auto& v : config.publicKeysOfReplicas) output << "  - " << v.second << "\n";
  output << "\n";

  output << "rsa_private_key: " << config.replicaPrivateKey << "\n";

  if (commonSys) commonSys->writeConfiguration(output, "common", config.replicaId);
}

static void validateRSAPublicKey(const std::string& key) {
  const size_t rsaPublicKeyHexadecimalLength = 584;
  if (!(key.length() == rsaPublicKeyHexadecimalLength) && (std::regex_match(key, std::regex("[0-9A-Fa-f]+"))))
    throw std::runtime_error("Invalid RSA public key: " + key);
}

static void validateRSAPrivateKey(const std::string& key) {
  // Note we do not verify the length of RSA private keys because their length
  // actually seems to vary a little in the output; it hovers around 2430
  // characters but often does not exactly match that number.

  if (!std::regex_match(key, std::regex("[0-9A-Fa-f]+"))) throw std::runtime_error("Invalid RSA private key: " + key);
}

Cryptosystem* inputReplicaKeyfileMultisig(const std::string& filename, bftEngine::ReplicaConfig& config) {
  using namespace concord::util;

  std::ifstream input(filename);
  if (!input.is_open()) throw std::runtime_error(__PRETTY_FUNCTION__ + std::string(": can't open ") + filename);

  config.numReplicas = yaml::readValue<std::uint16_t>(input, "num_replicas");
  config.numRoReplicas = yaml::readValue<std::uint16_t>(input, "num_ro_replicas");
  config.fVal = yaml::readValue<std::uint16_t>(input, "f_val");
  config.cVal = yaml::readValue<std::uint16_t>(input, "c_val");
  config.replicaId = yaml::readValue<std::uint16_t>(input, "replica_id");
  config.isReadOnly = yaml::readValue<bool>(input, "read-only");

  // Note we validate the number of replicas using 32-bit integers in case
  // (3 * f + 2 * c + 1) overflows a 16-bit integer.
  uint32_t predictedNumReplicas = 3 * (uint32_t)config.fVal + 2 * (uint32_t)config.cVal + 1;
  if (predictedNumReplicas != (uint32_t)config.numReplicas)
    throw std::runtime_error("num_replicas must be equal to (3 * f_val + 2 * c_val + 1)");

  if (config.replicaId >= config.numReplicas + config.numRoReplicas)
    throw std::runtime_error("replica IDs must be in the range [0, num_replicas + num_ro_replicas]");

  std::vector<std::string> rsaPublicKeys = yaml::readCollection<std::string>(input, "rsa_public_keys");

  if (rsaPublicKeys.size() != config.numReplicas + config.numRoReplicas)
    throw std::runtime_error("number of public RSA keys must match num_replicas");

  config.publicKeysOfReplicas.clear();
  for (size_t i = 0; i < config.numReplicas + config.numRoReplicas; ++i) {
    validateRSAPublicKey(rsaPublicKeys[i]);
    config.publicKeysOfReplicas.insert(std::pair<uint16_t, std::string>(i, rsaPublicKeys[i]));
  }

  config.replicaPrivateKey = yaml::readValue<std::string>(input, "rsa_private_key");
  validateRSAPrivateKey(config.replicaPrivateKey);

  if (config.isReadOnly) return nullptr;

  return Cryptosystem::fromConfiguration(input,
                                         "common",
                                         config.replicaId + 1,
                                         config.thresholdSystemType_,
                                         config.thresholdSystemSubType_,
                                         config.thresholdPrivateKey_,
                                         config.thresholdPublicKey_,
                                         config.thresholdVerificationKeys_);
}
