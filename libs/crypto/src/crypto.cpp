// Concord
//
// Copyright (c) 2022 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the sub-component's license, as noted in the LICENSE
// file.

#include <utility>

#include "crypto/crypto.hpp"

#include "log/logger.hpp"
#include "util/assertUtils.hpp"
#include "util/hex_tools.hpp"
#include "util/string.hpp"
#include "util/types.hpp"
#ifdef USE_OPENSSL
#include "crypto/openssl/EdDSA.hpp"
#include "crypto/openssl/crypto.hpp"
#endif
namespace concord::crypto {

std::pair<std::string, std::string> generateEdDSAKeyPair(const KeyFormat fmt) {
#ifdef USE_OPENSSL
  return openssl::generateEdDSAKeyPair(fmt);
#else
#error "Openssl is the only implementation available. Use \"USE_OPENSSL\" flag"
#endif
}

std::pair<std::string, std::string> EdDSAHexToPem(const std::pair<std::string, std::string>& hex_key_pair) {
#ifdef USE_OPENSSL
  return openssl::EdDSAHexToPem(hex_key_pair);
#else
#error "Openssl is the only implementation available. Use \"USE_OPENSSL\" flag"
#endif
}

KeyFormat getFormat(const std::string& key) {
  return (key.find("BEGIN") != std::string::npos) ? KeyFormat::PemFormat : KeyFormat::HexaDecimalStrippedFormat;
}

bool isValidKey(const std::string& keyName, const std::string& key, size_t expectedSize) {
  auto isValidHex = util::isValidHexString(key);
  if ((expectedSize == 0 or (key.length() == expectedSize)) and isValidHex) {
    return true;
  }
  throw std::runtime_error("Invalid " + keyName + " key (" + key + ") of size " + std::to_string(expectedSize) +
                           " bytes.");
}
}  // namespace concord::crypto
