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

#ifdef USE_ROCKSDB

#include "details.h"
#include "native_iterator.h"
#include "native_write_batch.h"
#include "rocksdb_exception.h"
#include "rocksdb/snapshot.h"
#include "endianness.hpp"
#include "client.h"

#include <rocksdb/slice.h>
#include <rocksdb/utilities/options_util.h>
#include <rocksdb/utilities/transaction_db.h>

#include <memory>
#include <optional>
#include <string>
#include <unordered_set>
#include <vector>

// Wrapper around the RocksDB library that aims at improving:
//  * memory management
//  * error reporting - done via exceptions instead of status codes
//
// The Span template parameters to methods allow conversion from any type that has data() and size()
// members. The data() member should return `const char *` or `const std::uint8_t*`. That includes std::string,
// std::string_view, std::vector<char/std::uint8_t>, std::span<char/std::uint8_t>, Sliver, ::rocksdb::Slice, etc. Raw
// pointers without size are not supported.
//
// Note1: All methods on all classes throw on errors.
// Note2: All methods without a column family parameter work with the default column family.
namespace concord::storage::rocksdb {

class NativeClient : public std::enable_shared_from_this<NativeClient> {
 public:
  // User-supplied options.
  struct UserOptions {
    std::string filepath;

    // Any RocksDB customization that cannot be completed in an init file can be done here.
    std::function<void(::rocksdb::Options &, std::vector<::rocksdb::ColumnFamilyDescriptor> &)> completeInit;
  };

  // Default RocksDB options.
  struct DefaultOptions {};

  // Existing options either in rocksdb_client.cpp or in the OPTIONS file.
  struct ExistingOptions {};

  // All methods create the DB if it is missing, irrespective of the `create_if_missing` option supplied.
  static std::shared_ptr<NativeClient> newClient(const std::string &path, bool readOnly, const DefaultOptions &);
  static std::shared_ptr<NativeClient> newClient(const std::string &path, bool readOnly, const ExistingOptions &);
  static std::shared_ptr<NativeClient> newClient(const std::string &path, bool readOnly, const UserOptions &);

  // Convert from an IDBClient that points to a RocksDB instance into a NativeClient.
  // Throws std::bad_cast if the IDBClient doesn't point to a RocksDB instance.
  static std::shared_ptr<NativeClient> fromIDBClient(const std::shared_ptr<IDBClient> &);

  // Use this native client through the IDBClient interface.
  std::shared_ptr<IDBClient> asIDBClient() const;

  // Return a reference to the underlying db.
  ::rocksdb::DB &rawDB() const;

  // Column family single key read-write interface.
  template <typename KeySpan, typename ValueSpan>
  void put(const std::string &cFamily, const KeySpan &key, const ValueSpan &value);
  // Returns nullopt if the key is not found.
  template <typename KeySpan>
  std::optional<std::string> get(const std::string &cFamily, const KeySpan &key) const;
  template <typename KeySpan>
  std::optional<std::string> get(const std::string &cFamily, const KeySpan &key, ::rocksdb::ReadOptions ro) const;
  // Returns nullopt if the key is not found.
  template <typename KeySpan>
  std::optional<::rocksdb::PinnableSlice> getSlice(const std::string &cFamily, const KeySpan &key) const;
  // Deleting a key that doesn't exist is not an error.
  template <typename KeySpan>
  void del(const std::string &cFamily, const KeySpan &key);

  // Single key read-write interface for the default column family.
  template <typename KeySpan, typename ValueSpan>
  void put(const KeySpan &key, const ValueSpan &value);
  // Returns nullopt if the key is not found.
  template <typename KeySpan>
  std::optional<std::string> get(const KeySpan &key) const;
  // Returns nullopt if the key is not found.
  template <typename KeySpan>
  std::optional<::rocksdb::PinnableSlice> getSlice(const KeySpan &key) const;
  // Deleting a key that doesn't exist is not an error.
  template <typename KeySpan>
  void del(const KeySpan &key);

  // Batching interface.
  NativeWriteBatch getBatch(size_t reserved_bytes = 0) const;
  NativeWriteBatch getBatch(std::string &&data) const;
  void write(NativeWriteBatch &&);

  // MultiGet interface
  //
  // Return values in the same order of keys. All keys reside in the same column family. There
  // aren't really any benefits to using multiget across column families, except consistency, which
  // we do not require, since our keys are versioned or immutable in all cases.
  //
  // We don't use exceptions here, and stick to standard RocksDB status types because:
  //   1. Each key has a distinct status.
  //   2. Performance - We want to minimize allocation and copying.
  //
  // ContiguousStatusContainer must always contain type ::rocksdb::Status
  template <typename KeySpan>
  void multiGet(const std::string &cFamily,
                const std::vector<KeySpan> &keys,
                std::vector<::rocksdb::PinnableSlice> &values,
                std::vector<::rocksdb::Status> &statuses) const;

  template <typename KeySpan>
  void multiGet(const std::string &cFamily,
                const std::vector<KeySpan> &keys,
                std::vector<::rocksdb::PinnableSlice> &values,
                std::vector<::rocksdb::Status> &statuses,
                ::rocksdb::ReadOptions ro) const;

  // Iterator interface.
  // Iterators initially don't point to a key value, i.e. they convert to false.
  // Important note - RocksDB requires that iterators are destroyed before the DB client that created them.
  //
  // Get an iterator into the default column family.
  NativeIterator getIterator() const;
  // Get an iterator into a column family
  NativeIterator getIterator(const std::string &cFamily) const;
  // Get iterators from a consistent database state across multiple column families. The order of the returned iterators
  // match the families input.
  std::vector<NativeIterator> getIterators(const std::vector<std::string> &cFamilies) const;

  ::rocksdb::Options options() const;

  // Return the DB path.
  const std::string &path() const { return client_->m_dbPath; }

  // On-disk column family management.
  static std::string defaultColumnFamily();
  // Return the column families in the DB pointed to by `path`.
  static std::unordered_set<std::string> columnFamilies(const std::string &path);

  // Client instance column management. Methods below only operate on column families this client is aware of. The
  // actual column families on-disk can be different if this client is in read-only mode as another read-write client
  // might have modified them. Return the column families this client is aware of.
  std::unordered_set<std::string> columnFamilies() const;
  // Checks if the client has a column family.
  bool hasColumnFamily(const std::string &cFamily) const;
  // Throws if the column family already exists.
  void createColumnFamily(const std::string &cFamily,
                          const ::rocksdb::ColumnFamilyOptions &options = ::rocksdb::ColumnFamilyOptions{});
  // Create a column family by importing previously exported SST files.
  void createColumnFamilyWithImport(const std::string &cFamily,
                                    const ::rocksdb::ImportColumnFamilyOptions &importOpts,
                                    const ::rocksdb::ExportImportFilesMetaData &metadata,
                                    const ::rocksdb::ColumnFamilyOptions &cfOpts = ::rocksdb::ColumnFamilyOptions{});
  bool createColumnFamilyIfNotExisting(const std::string &cf, const ::rocksdb::CompactionFilter *filter = nullptr);
  // Return the column family options for an existing column family in this client.
  ::rocksdb::ColumnFamilyOptions columnFamilyOptions(const std::string &cFamily) const;
  // Drops a column family and its data. It is not an error if the column family doesn't exist or if the client is not
  // aware of it.
  void dropColumnFamily(const std::string &cFamily);

  ::rocksdb::ColumnFamilyHandle *defaultColumnFamilyHandle() const;
  ::rocksdb::ColumnFamilyHandle *columnFamilyHandle(const std::string &cFamily) const;

  void createCheckpointNative(const uint64_t &checkPointId);
  std::vector<uint64_t> getListOfCreatedCheckpointsNative() const;
  void removeCheckpointNative(const uint64_t &checkPointId) const;
  void setCheckpointDirNative(const std::string &path);

 private:
  NativeClient(const std::string &path, bool readOnly, const DefaultOptions &);
  NativeClient(const std::string &path, bool readOnly, const ExistingOptions &);
  NativeClient(const std::string &path, bool readOnly, const UserOptions &);
  NativeClient(const std::shared_ptr<Client> &);
  NativeClient(const NativeClient &) = delete;
  NativeClient(NativeClient &&) = delete;
  NativeClient &operator=(const NativeClient &) = delete;
  NativeClient &operator=(NativeClient &&) = delete;

  Client::CfUniquePtr createColumnFamilyHandle(const std::string &cFamily,
                                               const ::rocksdb::ColumnFamilyOptions &options);

 private:
  std::shared_ptr<Client> client_;
  static const bool applyOptimizationsOnDefaultOpts_ = false;
  friend class NativeWriteBatch;
};

}  // namespace concord::storage::rocksdb

#include "native_write_batch.ipp"
#include "native_client.ipp"

#endif  // USE_ROCKSDB
