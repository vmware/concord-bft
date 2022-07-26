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

#pragma once

#include <stdint.h>
#include <string>
#include <vector>

namespace concord::util::crypto {
enum class KeyFormat : uint16_t { HexaDecimalStrippedFormat, PemFormat };
enum class CurveType : uint16_t { secp256k1, secp384r1 };

/**
 * @brief Validates the key.
 *
 * @param keyType Key type to be validated.
 * @param key Key to be validate.
 * @param expectedSize Size of the key to be validated.
 * @return Validation result.
 */
bool isValidKey(const std::string& keyType, const std::string& key, size_t expectedSize);
  // valid field_name: "C"/"L"/"ST"/"O"/"OU"/"CN"
  static std::string getSubjectFieldByName(const std::string& cert_path, const std::string& attribute_name);
  // This function accepts path to a cert bundle
  static std::vector<std::string> getSubjectFieldListByName(const std::string& cert_bundle_path,
                                                            const std::string& attribute_name);

}  // namespace concord::util::crypto
