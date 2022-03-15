// Concord
//
// Copyright (c) 2020 VMware, Inc. All Rights Reserved.
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

#include "storage/test/storage_test_common.h"

#include "kv_types.hpp"
#include "sparse_merkle/base_types.h"
#include "Digest.hpp"
#include "SimpleBCStateTransfer.hpp"

using concord::util::digest::BlockDigest;

inline BlockDigest blockDigest(concord::kvbc::BlockId blockId, const concordUtils::Sliver &block) {
  return ::bftEngine::bcst::computeBlockDigest(blockId, block.data(), block.length());
}

inline auto getHash(const std::string &str) {
  auto hasher = ::concord::kvbc::sparse_merkle::Hasher{};
  return hasher.hash(str.data(), str.size());
}

inline auto getHash(const concordUtils::Sliver &sliver) {
  auto hasher = ::concord::kvbc::sparse_merkle::Hasher{};
  return hasher.hash(sliver.data(), sliver.length());
}

inline auto getBlockDigest(const std::string &data) { return getHash(data).dataArray(); }

inline const auto defaultBlockId = ::concord::kvbc::BlockId{42};
inline const auto maxNumKeys = 16u;
