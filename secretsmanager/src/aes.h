// Concord
//
// Copyright (c) 2020 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the
// "License").  You may not use this product except in compliance with the
// Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.
//
// This convenience header combines different block implementations.

#pragma once

#include <vector>
#include <string>

#include "crypto/crypto.hpp"
#include "key_params.h"

namespace concord::secretsmanager {

class AES_CBC {
 public:
  AES_CBC(const KeyParams& params,
          concord::crypto::SignatureAlgorithm algo = concord::crypto::SignatureAlgorithm::EdDSA)
      : params_{params}, algo_{algo} {}
  std::vector<uint8_t> encrypt(const std::string& input);
  std::string decrypt(const std::vector<uint8_t>& cipher);

 private:
  KeyParams params_;
  concord::crypto::SignatureAlgorithm algo_;
};

}  // namespace concord::secretsmanager
