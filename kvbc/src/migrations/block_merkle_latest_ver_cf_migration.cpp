// Concord
//
// Copyright (c) 2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the sub-component's license, as noted in the
// LICENSE file.

#include "migrations/block_merkle_latest_ver_cf_migration.h"

#include "assertUtils.hpp"
#include "thread_pool.hpp"
#include "categorization/blockchain.h"
#include "categorization/column_families.h"
#include "categorization/db_categories.h"
#include "categorization/details.h"
#include "kv_types.hpp"
#include "storage/db_types.h"
#include "string.hpp"

#include <stdexcept>
#include <vector>

#include "util/filesystem.hpp"
#include <rocksdb/options.h>

namespace concord::kvbc::migrations {

using namespace concord::storage::rocksdb;

using kvbc::INITIAL_GENESIS_BLOCK_ID;
using kvbc::categorization::BlockMerkleOutput;
using kvbc::categorization::kExecutionProvableCategory;
using kvbc::categorization::detail::Blockchain;
using kvbc::categorization::detail::BLOCK_MERKLE_LATEST_KEY_VERSION_CF;
using kvbc::categorization::detail::hash;
using kvbc::categorization::Hash;
using util::toChar;
using util::ThreadPool;

using ::rocksdb::Checkpoint;
using ::rocksdb::ColumnFamilyDescriptor;
using ::rocksdb::ColumnFamilyOptions;
using ::rocksdb::ExportImportFilesMetaData;
using ::rocksdb::ImportColumnFamilyOptions;

// 2 bytes: EDBKeyType::Migration followed by EMigrationSubType::BlockMerkleLatestVerCfState .
const auto kMigrationKey =
    std::string{toChar(storage::v2MerkleTree::detail::EDBKeyType::Migration),
                toChar(storage::v2MerkleTree::detail::EMigrationSubType::BlockMerkleLatestVerCfState)};
const std::string& BlockMerkleLatestVerCfMigration::migrationKey() { return kMigrationKey; }

const auto kTempCf = BLOCK_MERKLE_LATEST_KEY_VERSION_CF + "_temp";
const std::string& BlockMerkleLatestVerCfMigration::temporaryColumnFamily() { return kTempCf; }

BlockMerkleLatestVerCfMigration::BlockMerkleLatestVerCfMigration(const std::string& db_path,
                                                                 const std::string& export_path,
                                                                 const size_t iteration_batch_size)
    : db_path_{db_path},
      export_path_{export_path},
      iteration_batch_size_((iteration_batch_size <= 0) ? 1 : iteration_batch_size) {
  const auto read_only = false;
  db_ = NativeClient::newClient(db_path, read_only, NativeClient::DefaultOptions{});
}

void BlockMerkleLatestVerCfMigration::removeExportDir() { fs::remove_all(export_path_); }

void BlockMerkleLatestVerCfMigration::checkpointDB() {
  Checkpoint* checkpoint{nullptr};
  if (!Checkpoint::Create(&db_->rawDB(), &checkpoint).ok()) {
    throw std::runtime_error{"Failed to create a RocksDB checkpoint for DB path = " + db_path_};
  }
  checkpoint_.reset(checkpoint);
}

void BlockMerkleLatestVerCfMigration::exportLatestVerCf() {
  ExportImportFilesMetaData* export_metadata{nullptr};
  const auto status = checkpoint_->ExportColumnFamily(
      db_->columnFamilyHandle(BLOCK_MERKLE_LATEST_KEY_VERSION_CF), export_path_, &export_metadata);
  if (!status.ok()) {
    throw std::runtime_error{"Failed to export " + BLOCK_MERKLE_LATEST_KEY_VERSION_CF +
                             " column family, reason: " + status.ToString()};
  }
  export_metadata_.reset(export_metadata);
}

void BlockMerkleLatestVerCfMigration::importTempLatestVerCf() {
  auto import_opts = ImportColumnFamilyOptions{};
  import_opts.move_files = true;
  db_->createColumnFamilyWithImport(kTempCf, import_opts, *export_metadata_);
  db_->put(kMigrationKey, kStateImportedTempCf);
}

void BlockMerkleLatestVerCfMigration::clearExistingLatestVerCf() {
  // First, get the CF options from the CF descriptor.
  auto cf = db_->columnFamilyHandle(BLOCK_MERKLE_LATEST_KEY_VERSION_CF);
  auto cf_desc = ColumnFamilyDescriptor{};
  const auto status = cf->GetDescriptor(&cf_desc);
  if (!status.ok()) {
    throw std::runtime_error{"Failed to get CF descriptor, reason: " + status.ToString()};
  }

  // Then, drop and create the CF.
  db_->dropColumnFamily(BLOCK_MERKLE_LATEST_KEY_VERSION_CF);
  db_->createColumnFamily(BLOCK_MERKLE_LATEST_KEY_VERSION_CF, cf_desc.options);
}

void BlockMerkleLatestVerCfMigration::iterateAndMigrate() {
  auto blockchain = Blockchain{db_};
  ThreadPool tp(iteration_batch_size_);
  for (auto block_id = INITIAL_GENESIS_BLOCK_ID; block_id <= blockchain.getLastReachableBlockId();
       block_id += iteration_batch_size_) {
    std::vector<std::future<void>> tasks;
    std::vector<NativeWriteBatch> batches(iteration_batch_size_, NativeWriteBatch(db_));
    size_t read_offset = 0;
    for (auto batched_block_id = block_id; (batched_block_id < (block_id + iteration_batch_size_)) &&
                                           (batched_block_id <= blockchain.getLastReachableBlockId());
         ++batched_block_id) {
      tasks.push_back(tp.async([&batches, read_offset, &blockchain, batched_block_id, this]() -> void {
        const auto block = blockchain.getBlock(batched_block_id);
        if (!block.has_value()) {
          throw std::runtime_error{"Failed to load block ID = " + std::to_string(batched_block_id)};
        }
        std::vector<Hash> key_hashes;
        std::vector<::rocksdb::PinnableSlice> values;
        std::vector<::rocksdb::Status> statuses;
        auto it = block->data.categories_updates_info.find(kExecutionProvableCategory);
        if (it != block->data.categories_updates_info.cend()) {
          const auto block_merkle_output = std::get_if<BlockMerkleOutput>(&it->second);
          ConcordAssertNE(block_merkle_output, nullptr);
          for (const auto& [key, _] : block_merkle_output->keys) {
            (void)_;
            key_hashes.push_back(hash(key));
          }
          auto key_it = block_merkle_output->keys.cbegin();
          db_->multiGet(kTempCf, key_hashes, values, statuses);
          ConcordAssertEQ(key_hashes.size(), values.size());
          ConcordAssertEQ(key_hashes.size(), statuses.size());
          for (auto i = 0ull; i < values.size(); ++i) {
            const auto& value = values[i];
            const auto& status = statuses[i];
            if (status.ok()) {
              (batches[read_offset]).put(BLOCK_MERKLE_LATEST_KEY_VERSION_CF, key_it->first, value);
            } else {
              // We expect that no pruning has occurred. Therefore, the latest version of the key cannot be missing.
              throw std::runtime_error{"multiGet() failed, reason: " + status.ToString()};
            }
            ++key_it;
          }
        }
      }));
      read_offset++;
    }
    read_offset = 0;
    for (const auto& t : tasks) {
      t.wait();
      db_->write(std::move(batches[read_offset]));
      read_offset++;
    }
  }
  db_->put(kMigrationKey, kStateMigrated);
}

void BlockMerkleLatestVerCfMigration::dropTempLatestVerCf() { db_->dropColumnFamily(kTempCf); }

void BlockMerkleLatestVerCfMigration::commitComplete() { db_->put(kMigrationKey, kStateMigrationNotNeededOrCompleted); }

BlockMerkleLatestVerCfMigration::ExecutionStatus BlockMerkleLatestVerCfMigration::execute() {
  auto migrate = [this]() {
    clearExistingLatestVerCf();
    iterateAndMigrate();
  };

  auto cleanup_and_complete = [this]() {
    removeExportDir();
    dropTempLatestVerCf();
    commitComplete();
  };

  const auto state = db_->get(kMigrationKey);
  // If we've already completed or we are already at the new DB format, there's nothing for us to do.
  if (state && *state == kStateMigrationNotNeededOrCompleted) {
    return ExecutionStatus::kNotNeededOrAlreadyExecuted;
  }

  // Start by removing the export dir.
  removeExportDir();

  if (state && *state == kStateImportedTempCf) {
    // If we've successfully imported the temporary CF, we can migrate.
    migrate();
  } else if (state && *state == kStateMigrated) {
    // If we've already migrated, we can just cleanup and complete.
  } else {
    // We need to execute all the steps to migrate.
    dropTempLatestVerCf();
    checkpointDB();
    exportLatestVerCf();
    importTempLatestVerCf();
    migrate();
  }
  cleanup_and_complete();

  return ExecutionStatus::kExecuted;
}

}  // namespace concord::kvbc::migrations
