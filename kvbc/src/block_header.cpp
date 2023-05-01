// Concord
//
// Copyright (c) 2023 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the
// LICENSE file.

#include <string.h>

#include "block_header.hpp"
#include "log/logger.hpp"
#include "kvbc_key_types.hpp"
#include "categorized_kvbc_msgs.cmf.hpp"

using namespace concord::serialize;
using concord::kvbc::keyTypes::kKvbKeyBlockHeaderHash;

namespace concord::kvbc {

std::string BlockHeader::serialize() const {
  std::string serialized_buffer;
  categorization::serialize(serialized_buffer, data);
  ConcordAssert(serialized_buffer.size() > 0);
  return serialized_buffer;
}

BlockHeader BlockHeader::deserialize(const std::string &input) {
  BlockHeader outblk;
  categorization::deserialize(input, outblk.data);
  if (outblk.data.version != kBlockStorageVersion) {
    LOG_ERROR(V4_BLOCK_LOG, "Unknown block storage version " << outblk.data.version);
    throw std::runtime_error("Unkown block storage version");
  }
  return outblk;
}

const std::string BlockHeader::blockNumAsKeyToBlockHash(const uint8_t *bytes, size_t length) {
  std::string ret;
  ret.push_back((char)kKvbKeyBlockHeaderHash);
  ret.append(reinterpret_cast<const char *>(bytes), length);
  return ret;
}

const std::string BlockHeader::blockNumAsKeyToBlockHeader(const uint8_t *bytes, size_t length) {
  std::string ret;
  ret.push_back((char)kKvbKeyEthBlockHash);
  ret.append(reinterpret_cast<const char *>(bytes), length);
  return ret;
}

std::string BlockHeader::blockHashAsKeyToBlockNum(const uint8_t *bytes, size_t length) {
  std::string ret;
  ret.push_back((char)kKvbKeyEthBlock);
  ret.append(reinterpret_cast<const char *>(bytes), length);
  return ret;
}

uint256be_t BlockHeader::hash() const {
  static_assert(sizeof(crypto::BlockDigest) == sizeof(uint256be_t), "hash size should be same");
  auto serialized_header = serialize();
  crypto::DigestGenerator digest_generator;
  uint256be_t hash{0};
  digest_generator.update(reinterpret_cast<const char *>(serialized_header.c_str()), serialized_header.size());
  digest_generator.writeDigest(reinterpret_cast<char *>(hash.data()));
  return hash;
}

}  // namespace concord::kvbc