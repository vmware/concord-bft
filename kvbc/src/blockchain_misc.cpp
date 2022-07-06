// Concord
//
// Copyright (c) 2022-2023 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include "blockchain_misc.hpp"
#include "storage/merkle_tree_key_manipulator.h"
#include "categorization/blocks.h"

namespace concord::kvbc {

// Local function to this c++ file
template <typename T>
bool isV1(const T& raw_block_ser) {
  try {
    concord::kvbc::categorization::RawBlock::deserialize(raw_block_ser);
    return true;
  } catch (const std::runtime_error& re) {
  }
  return false;
}

block_version BlockVersion::getBlockVersion(const concord::kvbc::RawBlock& raw_block_ser) {
  return isV1(raw_block_ser) ? concord::kvbc::block_version::V1
                             : getBlockVersion(raw_block_ser.data(), raw_block_ser.size());
}
block_version BlockVersion::getBlockVersion(const std::string_view& raw_block_ser) {
  return isV1(raw_block_ser) ? concord::kvbc::block_version::V1
                             : getBlockVersion(raw_block_ser.data(), raw_block_ser.size());
}
block_version BlockVersion::getBlockVersion(const char* raw_block_ser, size_t len) {
  ConcordAssertGE(len, BLOCK_VERSION_SIZE);
  switch (*(reinterpret_cast<const block_version*>(raw_block_ser))) {
    case block_version::V1:
      LOG_FATAL(GL, "V1 Version is never set in the block.");
      ConcordAssert(false);
      return block_version::V1;
    case block_version::V4:
      return block_version::V4;
  }
  LOG_FATAL(GL, "Version is not valid.");
  ConcordAssert(false);
  return block_version::V1;
}

namespace bcutil {
static const auto kPublicStateHashKey = concord::storage::v2MerkleTree::detail::serialize(
    concord::storage::v2MerkleTree::detail::EBFTSubtype::PublicStateHashAtDbCheckpoint);

std::string BlockChainUtils::publicStateHashKey() { return kPublicStateHashKey; }
}  // end of namespace bcutil
}  // end of namespace concord::kvbc
