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

#include <string>
#include <vector>

namespace concord::secretsmanager {

struct SaltedCipher {
  std::vector<uint8_t> salt;
  std::vector<uint8_t> cipher_text;
};

std::string base64Enc(const std::vector<uint8_t>& salt, const std::vector<uint8_t>& cipher_text);
std::string base64Enc(const std::vector<uint8_t>& cipher_text);
SaltedCipher base64Dec(const std::string& input);
std::vector<uint8_t> base64DecNoSalt(const std::string& input);

}  // namespace concord::secretsmanager