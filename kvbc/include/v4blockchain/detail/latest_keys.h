// Concord
//
// Copyright (c) 2022 VMware, Inc. All Rights Reserved.
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

#include "rocksdb/native_client.h"
#include <memory>
#include <unordered_map>
#include "categorization/updates.h"
#include "v4blockchain/detail/categories.h"

namespace concord::kvbc::v4blockchain::detail {

/*
The latest keys are the state of the blockchain.
It's implemented as a column family, where all the keys of a block that is being added are added to it.
It uses the RocksDb timestamp API to mark the version of the key.
A key can be:
- newly added - in this case it has a single version i.e. the block that it was added.
- updated - a key may have several versions (accessible) until we mark its history as save to delete.
- deleted - trying to access with the deletion version or higher will return nullopt.

For backward compatibility with the previous categorized implementation, a key belongs to a category.
the category is represented by a prefix, the following properties of categories will be honored by this
implementation as well:
- version category : stale on update i.e. a keys is prunable although it's the latest version when its block is deleted.
- immutable - updating an immutable key is an error.
*/
class LatestKeys {
 public:
  using Flags = std::array<char, 1>;
  // E.L need to be used with compaction filter
  static constexpr Flags STALE_ON_UPDATE = {0x1};
  LatestKeys(const std::shared_ptr<concord::storage::rocksdb::NativeClient>&,
             const std::optional<std::map<std::string, concord::kvbc::categorization::CATEGORY_TYPE>>&);
  void addBlockKeys(const concord::kvbc::categorization::Updates&,
                    BlockId block_id,
                    storage::rocksdb::NativeWriteBatch&);

  void handleCategoryUpdates(const std::string& block_version,
                             const std::string& category_id,
                             const concord::kvbc::categorization::BlockMerkleInput&,
                             concord::storage::rocksdb::NativeWriteBatch&);
  void handleCategoryUpdates(const std::string& block_version,
                             const std::string& category_id,
                             const concord::kvbc::categorization::VersionedInput&,
                             concord::storage::rocksdb::NativeWriteBatch&);
  void handleCategoryUpdates(const std::string& block_version,
                             const std::string& category_id,
                             const concord::kvbc::categorization::ImmutableInput&,
                             concord::storage::rocksdb::NativeWriteBatch&);
  const std::string& getCategoryPrefix(const std::string& category) const {
    return category_mapping_.categoryPrefix(category);
  }
  // E.L need to call with min of last_reachable and state snop shot point.
  void trimHistoryUntil(BlockId block_id);

 private:
  std::shared_ptr<concord::storage::rocksdb::NativeClient> native_client_;
  v4blockchain::detail::Categories category_mapping_;
};

}  // namespace concord::kvbc::v4blockchain::detail
