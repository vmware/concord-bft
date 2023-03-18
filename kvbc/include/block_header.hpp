// Copyright (c) 2023 VMware, Inc. All Rights Reserved.
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

#pragma once
#include <vector>
#include <string>
#include "util/serializable.hpp"
#include "crypto/digest.hpp"
#include "kvbc_app_filter/kvbc_key_types.h"
#include "categorized_kvbc_msgs.cmf.hpp"

namespace concord::kvbc {

using concord::kvbc::categorization::BlockHeaderData;
using uint256be_t = std::array<uint8_t, 32>;
using nonce_t = std::array<uint8_t, 8>;
using address_t = std::array<uint8_t, 20>;

class BlockHeader {
 public:
  BlockHeader(void) : data({0}) {
    data.transactions.resize(0);
    data.version = kBlockStorageVersion;
  }

  static const std::string blockNumAsKeyToBlockHash(const uint8_t *bytes, size_t length);
  static const std::string blockNumAsKeyToBlockHeader(const uint8_t *bytes, size_t length);
  static std::string blockHashAsKeyToBlockNum(const uint8_t *bytes, size_t length);
  uint256be_t hash() const;
  std::string serialize() const;
  static BlockHeader deserialize(const std::string &input);

 public:
  static constexpr int64_t kHashSizeInBytes = 32;
  static constexpr int64_t kBlockStorageVersion = 1;
  BlockHeaderData data;
};

}  // namespace concord::kvbc