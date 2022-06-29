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

namespace concord::kvbc {

block_version BlockVersion::getBlockVersion(const concord::kvbc::RawBlock& raw_block_ser) {
  return getBlockVersion(raw_block_ser.data(), raw_block_ser.size());
}
block_version BlockVersion::getBlockVersion(const std::string_view& raw_block_ser) {
  return getBlockVersion(raw_block_ser.data(), raw_block_ser.size());
}
block_version BlockVersion::getBlockVersion(const char* raw_block_ser, size_t len) {
  ConcordAssertGE(len, BLOCK_VERSION_SIZE);
  return *(reinterpret_cast<const block_version*>(raw_block_ser));
}

namespace bcutil {
static const auto kPublicStateHashKey = concord::storage::v2MerkleTree::detail::serialize(
    concord::storage::v2MerkleTree::detail::EBFTSubtype::PublicStateHashAtDbCheckpoint);

std::string BlockChainUtils::publicStateHashKey() { return kPublicStateHashKey; }
}  // end of namespace bcutil
}  // end of namespace concord::kvbc
