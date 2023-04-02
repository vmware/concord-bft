// Concord
//
// Copyright (c) 2022 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include "reconfiguration/reconfiguration.hpp"
#include "crypto/factory.hpp"
#include <fstream>
#include <string>
namespace concord::reconfiguration {
OperatorCommandsReconfigurationHandler::OperatorCommandsReconfigurationHandler(
    const std::string& path_to_operator_pub_key, concord::crypto::SignatureAlgorithm sig_type) {
  if (path_to_operator_pub_key.empty()) {
    LOG_WARN(getLogger(),
             "The operator public key is missing, the reconfiguration handler won't be able to execute the requests");
    return;
  }
  std::ifstream key_content;
  key_content.open(path_to_operator_pub_key);
  if (!key_content) {
    LOG_WARN(getLogger(), "unable to read the operator public key file");
    return;
  }
  auto key_str = std::string{};
  auto buf = std::string(4096, '\0');
  while (key_content.read(&buf[0], 4096)) {
    key_str.append(buf, 0, key_content.gcount());
  }
  key_str.append(buf, 0, key_content.gcount());
  verifier_ = crypto::Factory::getVerifier(key_str, sig_type, crypto::KeyFormat::PemFormat);
}

bool OperatorCommandsReconfigurationHandler::verifySignature(uint32_t sender_id,
                                                             const std::string& data,
                                                             const std::string& signature) const {
  if (verifier_ == nullptr) return false;
  return verifier_->verify(data, signature);
}
}  // namespace concord::reconfiguration