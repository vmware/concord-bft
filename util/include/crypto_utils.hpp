// Concord
//
// Copyright (c) 2020 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the sub-component's license, as noted in the LICENSE
// file.
#pragma once

#include <utility>
#include <string>
#include <memory>
namespace concord::util {
class Crypto {
 public:
  enum class KeyFormat : std::uint16_t { HexaDecimalStrippedFormat, PemFormat };
  Crypto& instance() {
    static Crypto crypto;
    return crypto;
  }

  Crypto();
  std::pair<std::string, std::string> generateRsaKeyPairs(uint32_t sig_length, KeyFormat fmt);
  std::pair<std::string, std::string> hexToPem(const std::pair<std::string, std::string> key_pair);

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};
}  // namespace concord::util