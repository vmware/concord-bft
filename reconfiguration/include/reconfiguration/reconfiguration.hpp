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

#pragma once
#include "ireconfiguration.hpp"
#include "crypto/verifier.hpp"
#include "crypto/crypto.hpp"
namespace concord::reconfiguration {
class OperatorCommandsReconfigurationHandler : public IReconfigurationHandler {
 public:
  OperatorCommandsReconfigurationHandler(const std::string &path_to_operator_pub_key,
                                         concord::crypto::SignatureAlgorithm sig_type);
  bool verifySignature(uint32_t sender_id, const std::string &data, const std::string &signature) const override;

 protected:
  std::unique_ptr<concord::crypto::IVerifier> verifier_;
};
}  // namespace concord::reconfiguration
