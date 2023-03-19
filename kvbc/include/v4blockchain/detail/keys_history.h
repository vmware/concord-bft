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
#include "categorization/updates.h"
#include "v4blockchain/detail/categories.h"
#include <rocksdb/compaction_filter.h>

namespace concord::kvbc::v4blockchain::detail {

class KeysHistory {
 public:
  KeysHistory(const std::shared_ptr<concord::storage::rocksdb::NativeClient>&,
              const std::optional<std::map<std::string, concord::kvbc::categorization::CATEGORY_TYPE>>&);

  // Add the keys of the block to the keys history column family
  void addBlockKeys(const concord::kvbc::categorization::Updates&, const BlockId, storage::rocksdb::NativeWriteBatch&);

  // Return the id of the block where the key was last updated to its value at given version
  std::optional<BlockId> getVersionFromHistory(const std::string& category_id,
                                               const std::string& key,
                                               const BlockId version) const;

  // Delete the last added block keys
  void revertLastBlockKeys(const concord::kvbc::categorization::Updates&,
                           const BlockId,
                           storage::rocksdb::NativeWriteBatch&);

  struct KHCompactionFilter : ::rocksdb::CompactionFilter {
    static ::rocksdb::CompactionFilter* getFilter() {
      static KHCompactionFilter instance;
      return &instance;
    }
    KHCompactionFilter() {}
    const char* Name() const override { return "KeysHistoryCompactionFilter"; }
    bool Filter(int /*level*/,
                const ::rocksdb::Slice& key,
                const ::rocksdb::Slice& /*val*/,
                std::string* /*new_value*/,
                bool* /*value_changed*/) const override;
  };

 private:
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

  template <typename UPDATES>
  void handleUpdatesImp(const std::string& block_version,
                        const std::string& category_id,
                        const UPDATES& updates_kv,
                        concord::storage::rocksdb::NativeWriteBatch& write_batch);

  template <typename DELETES>
  void handleDeletesUpdatesImp(const std::string& block_version,
                               const std::string& category_id,
                               const DELETES& deletes,
                               concord::storage::rocksdb::NativeWriteBatch& write_batch);

  void revertCategoryKeys(const std::string& category_id,
                          const categorization::BlockMerkleInput& updates,
                          const std::string& block_version,
                          concord::storage::rocksdb::NativeWriteBatch& write_batch);
  void revertCategoryKeys(const std::string& category_id,
                          const categorization::VersionedInput& updates,
                          const std::string& block_version,
                          concord::storage::rocksdb::NativeWriteBatch& write_batch);
  void revertCategoryKeys(const std::string& category_id,
                          const categorization::ImmutableInput& updates,
                          const std::string& block_version,
                          concord::storage::rocksdb::NativeWriteBatch& write_batch);

  template <typename UPDATES>
  void revertKeysImp(const std::string& category_id,
                     const UPDATES& updates_kv,
                     const std::string& block_version,
                     concord::storage::rocksdb::NativeWriteBatch& write_batch);

  template <typename DELETES>
  void revertDeletedKeysImp(const std::string& category_id,
                            const DELETES& deletes,
                            const std::string& block_version,
                            concord::storage::rocksdb::NativeWriteBatch& write_batch);

  void revertOneKeyAtVersion(const std::string& category_id,
                             const std::string& block_id,
                             const std::string& prefix,
                             const std::string& key,
                             concord::storage::rocksdb::NativeWriteBatch& write_batch);

 private:
  std::shared_ptr<concord::storage::rocksdb::NativeClient> native_client_;
  v4blockchain::detail::Categories category_mapping_;
};

}  // namespace concord::kvbc::v4blockchain::detail
