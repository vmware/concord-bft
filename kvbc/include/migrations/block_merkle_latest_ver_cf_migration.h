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

#pragma once

#include "rocksdb/native_client.h"

#include <rocksdb/utilities/checkpoint.h>

#include <exception>
#include <memory>
#include <optional>
#include <string>
#include <string_view>

namespace concord::kvbc::migrations {

// Migrates a RocksDB DB from using key hashes in the `block_merkle_latest_key_version` to using raw keys.
// Keeps track of what steps have been executed such that if the migration process crashes, it can start fresh or
// continue from where it left off.
class BlockMerkleLatestVerCfMigration {
 public:
  // Note: `export_path` must be on the same filesystem as `db_path`.
  BlockMerkleLatestVerCfMigration(const std::string& db_path,
                                  const std::string& export_path,
                                  const size_t iteration_batch_size);

 public:
  static const std::string& temporaryColumnFamily();
  static const std::string& migrationKey();

  // Migration states.
  static inline const std::string kStateImportedTempCf{"imported-temp-cf"};
  static inline const std::string kStateMigrated{"migrated"};
  static inline const std::string kStateMigrationNotNeededOrCompleted{"migration-not-needed-or-completed"};

  enum class ExecutionStatus {
    kExecuted,                    // executed as part of this call
    kNotNeededOrAlreadyExecuted,  // migration is not needed or already executed and, therefore, nothing done in this
                                  // call
  };

 public:
  // Executes the migration, throwing on error.
  // If execute() returns, it is always a success. The ExecutionStatus gives indication as to what the actual outcome
  // is.
  ExecutionStatus execute();

  std::shared_ptr<storage::rocksdb::NativeClient> db() { return db_; }

  // Following methods are used for testing purposes only. Do not use in production.
 public:
  void removeExportDir();
  void checkpointDB();
  void exportLatestVerCf();
  void importTempLatestVerCf();
  void clearExistingLatestVerCf();
  void iterateAndMigrate();
  void dropTempLatestVerCf();
  void commitComplete();

 private:
  const std::string db_path_;
  const std::string export_path_;
  const size_t iteration_batch_size_;
  std::shared_ptr<storage::rocksdb::NativeClient> db_;
  std::unique_ptr<::rocksdb::Checkpoint> checkpoint_;
  std::unique_ptr<::rocksdb::ExportImportFilesMetaData> export_metadata_;
};

}  // namespace concord::kvbc::migrations
