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

#include "Digest.hpp"
#include "blockchain_misc.hpp"

namespace concord::kvbc::adapter::v4blockchain::utils {

class V4BlockUtils {
 public:
  static concord::util::digest::BlockDigest getparentDigest(const std::string_view& block_ser) {
    return getparentDigest(block_ser.data(), block_ser.size());
  }
  static concord::util::digest::BlockDigest getparentDigest(const concord::kvbc::RawBlock& block_ser) {
    return getparentDigest(block_ser.data(), block_ser.size());
  }

 private:
  static concord::util::digest::BlockDigest getparentDigest(const char* raw_block_ser, size_t len) {
    ConcordAssertGE(len, concord::kvbc::BLOCK_VERSION_SIZE + DIGEST_SIZE);
    return *reinterpret_cast<const concord::util::digest::BlockDigest*>(raw_block_ser +
                                                                        concord::kvbc::BLOCK_VERSION_SIZE);
  }
};

}  // namespace concord::kvbc::adapter::v4blockchain::utils
