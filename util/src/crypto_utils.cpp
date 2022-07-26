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

#include "crypto_utils.hpp"
#include "string.hpp"

namespace concord::util::crypto {

bool isValidKey(const std::string& keyName, const std::string& key, size_t expectedSize) {
  auto isValidHex = isValidHexString(key);
  if ((expectedSize == 0 or (key.length() == expectedSize)) and isValidHex) {
    return true;
  }
  throw std::runtime_error("Invalid " + keyName + " key (" + key + ") of size " + std::to_string(expectedSize) +
                           " bytes.");
}
}  // namespace concord::util::crypto
