// Concord
//
// Copyright (c) 2022-2023 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the
// "License").  You may not use this product except in compliance with the
// Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#pragma once

#include <string>
#include <functional>

#include "kv_types.hpp"
#include "assertUtils.hpp"

namespace concord::kvbc {
enum BLOCKCHAIN_VERSION { CATEGORIZED_BLOCKCHAIN = 1, V4_BLOCKCHAIN = 4, INVALID_BLOCKCHAIN_VERSION };

inline std::string V4Version() { return "V4_BLOCKCHAIN"; }

using version_type = uint16_t;
enum class block_version : version_type { V1 = 0x1 };
const size_t BLOCK_VERSION_SIZE = sizeof(version_type);

class BlockVersion {
 public:
  static block_version getBlockVersion(const concord::kvbc::RawBlock& raw_block_ser);
  static block_version getBlockVersion(const std::string_view& raw_block_ser);

 private:
  static block_version getBlockVersion(const char* raw_block_ser, size_t len);
};

// Key or value converter interface.
// Allows users to convert keys or values to any format that is appropriate.
using Converter = std::function<std::string(std::string&&)>;

namespace bcutil {
class BlockChainUtils {
 public:
  static std::string publicStateHashKey();
};
}  // namespace bcutil
}  // end namespace concord::kvbc
