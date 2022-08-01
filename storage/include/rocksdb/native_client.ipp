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

#include "client.h"
#include "details.h"

#include <rocksdb/options.h>

#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_set>
#include <utility>
#include <vector>

namespace concord::storage::rocksdb {

using namespace std::string_view_literals;

inline std::shared_ptr<NativeClient> NativeClient::newClient(const std::string &path,
                                                             bool readOnly,
                                                             const DefaultOptions &opts) {
  return std::shared_ptr<NativeClient>{new NativeClient{path, readOnly, opts}};
}

inline std::shared_ptr<NativeClient> NativeClient::newClient(const std::string &path,
                                                             bool readOnly,
                                                             const ExistingOptions &opts) {
  return std::shared_ptr<NativeClient>{new NativeClient{path, readOnly, opts}};
}

inline std::shared_ptr<NativeClient> NativeClient::newClient(const std::string &path,
                                                             bool readOnly,
                                                             const UserOptions &opts) {
  return std::shared_ptr<NativeClient>{new NativeClient{path, readOnly, opts}};
}

inline std::shared_ptr<NativeClient> NativeClient::fromIDBClient(const std::shared_ptr<IDBClient> &idb) {
  auto rocksDbClient = std::dynamic_pointer_cast<Client>(idb);
  if (!rocksDbClient) {
    throw std::bad_cast{};
  }
  return std::shared_ptr<NativeClient>(new NativeClient{rocksDbClient});
}

inline NativeClient::NativeClient(const std::string &path, bool readOnly, const DefaultOptions &)
    : client_{std::make_shared<Client>(path)} {
  client_->initDB(readOnly, std::nullopt, applyOptimizationsOnDefaultOpts_);
}

inline NativeClient::NativeClient(const std::string &path, bool readOnly, const ExistingOptions &)
    : client_{std::make_shared<Client>(path)} {
  client_->initDB(readOnly, std::nullopt, applyOptimizationsOnDefaultOpts_);
}

inline NativeClient::NativeClient(const std::string &path, bool readOnly, const UserOptions &userOpts)
    : client_{std::make_shared<Client>(path)} {
  auto options = Client::Options{userOpts.filepath, userOpts.completeInit};
  client_->initDBFromFile(readOnly, options);
}

inline NativeClient::NativeClient(const std::shared_ptr<Client> &client) : client_{client} {}

inline std::shared_ptr<IDBClient> NativeClient::asIDBClient() const { return client_; }

inline ::rocksdb::DB &NativeClient::rawDB() const { return *client_->dbInstance_; }

template <typename KeySpan, typename ValueSpan>
void NativeClient::put(const std::string &cFamily, const KeySpan &key, const ValueSpan &value) {
  auto s = client_->dbInstance_->Put(
      ::rocksdb::WriteOptions{}, columnFamilyHandle(cFamily), detail::toSlice(key), detail::toSlice(value));
  detail::throwOnError("put() failed"sv, std::move(s));
}

template <typename KeySpan>
std::optional<std::string> NativeClient::get(const std::string &cFamily, const KeySpan &key) const {
  return get(cFamily, detail::toSlice(key), ::rocksdb::ReadOptions{});
}

template <typename KeySpan>
std::optional<std::string> NativeClient::get(const std::string &cFamily,
                                             const KeySpan &key,
                                             ::rocksdb::ReadOptions ro) const {
  auto value = std::string{};
  auto s = client_->dbInstance_->Get(ro, columnFamilyHandle(cFamily), detail::toSlice(key), &value);
  if (s.IsNotFound()) {
    return std::nullopt;
  }
  detail::throwOnError("get() failed"sv, std::move(s));
  return value;
}

template <typename KeySpan>
std::optional<::rocksdb::PinnableSlice> NativeClient::getSlice(const std::string &cFamily, const KeySpan &key) const {
  auto slice = ::rocksdb::PinnableSlice{};
  auto s =
      client_->dbInstance_->Get(::rocksdb::ReadOptions{}, columnFamilyHandle(cFamily), detail::toSlice(key), &slice);
  if (s.IsNotFound()) {
    return std::nullopt;
  }
  detail::throwOnError("get() failed"sv, std::move(s));
  return std::move(slice);
}

template <typename KeySpan>
void NativeClient::del(const std::string &cFamily, const KeySpan &key) {
  auto s = client_->dbInstance_->Delete(::rocksdb::WriteOptions{}, columnFamilyHandle(cFamily), detail::toSlice(key));
  detail::throwOnError("del() failed"sv, std::move(s));
}

template <typename KeySpan, typename ValueSpan>
void NativeClient::put(const KeySpan &key, const ValueSpan &value) {
  put(defaultColumnFamily(), key, value);
}

template <typename KeySpan>
std::optional<std::string> NativeClient::get(const KeySpan &key) const {
  return get(defaultColumnFamily(), key);
}

template <typename KeySpan>
std::optional<::rocksdb::PinnableSlice> NativeClient::getSlice(const KeySpan &key) const {
  return getSlice(defaultColumnFamily(), key);
}

template <typename KeySpan>
void NativeClient::del(const KeySpan &key) {
  del(defaultColumnFamily(), key);
}

template <typename KeySpan>
void NativeClient::multiGet(const std::string &cFamily,
                            const std::vector<KeySpan> &keys,
                            std::vector<::rocksdb::PinnableSlice> &values,
                            std::vector<::rocksdb::Status> &statuses) const {
  multiGet(cFamily, keys, values, statuses, ::rocksdb::ReadOptions{});
}

template <typename KeySpan>
void NativeClient::multiGet(const std::string &cFamily,
                            const std::vector<KeySpan> &keys,
                            std::vector<::rocksdb::PinnableSlice> &values,
                            std::vector<::rocksdb::Status> &statuses,
                            ::rocksdb::ReadOptions ro) const {
  if (values.size() < keys.size()) {
    values.resize(keys.size());
  }
  if (statuses.size() < keys.size()) {
    statuses.resize(keys.size());
  }
  // TODO: We may be able to eliminate this allocation with a thread local pre-allocated vector or array.
  // We don't want to force users to pass through ::RocksDB slices, so we are unlikely to get rid of this loop.
  auto key_slices = std::vector<::rocksdb::Slice>();
  key_slices.reserve(keys.size());
  for (const auto &k : keys) {
    key_slices.emplace_back(detail::toSlice(k));
  }
  client_->dbInstance_->MultiGet(
      ro, columnFamilyHandle(cFamily), key_slices.size(), key_slices.data(), values.data(), statuses.data());
}

inline NativeWriteBatch NativeClient::getBatch(size_t reserved_bytes) const {
  return reserved_bytes == 0 ? NativeWriteBatch{shared_from_this()}
                             : NativeWriteBatch{shared_from_this(), reserved_bytes};
}

inline NativeWriteBatch NativeClient::getBatch(std::string &&data) const {
  return NativeWriteBatch{shared_from_this(), std::move(data)};
}

inline NativeIterator NativeClient::getIterator() const {
  return std::unique_ptr<::rocksdb::Iterator>{client_->dbInstance_->NewIterator(::rocksdb::ReadOptions{})};
}

inline NativeIterator NativeClient::getIterator(const std::string &cFamily) const {
  return std::unique_ptr<::rocksdb::Iterator>{
      client_->dbInstance_->NewIterator(::rocksdb::ReadOptions{}, columnFamilyHandle(cFamily))};
}

inline std::vector<NativeIterator> NativeClient::getIterators(const std::vector<std::string> &cFamilies) const {
  auto cfHandles = std::vector<::rocksdb::ColumnFamilyHandle *>{};
  for (const auto &cf : cFamilies) {
    cfHandles.push_back(columnFamilyHandle(cf));
  }

  auto rawPtrIterators = std::vector<::rocksdb::Iterator *>{};
  auto status = client_->dbInstance_->NewIterators(::rocksdb::ReadOptions{}, cfHandles, &rawPtrIterators);

  // Wrap RocksDB iterators in unique pointers so that they are freed, irrespective of the returned status.
  // Note: NewIterators()'s interface is bad and the caller doesn't have enough info by just looking at it. One has to
  // look into the RocksDB implementation to see that no iterators will be returned if the status is not OK. That,
  // however, might change, but the interface will stay the same...
  auto ret = std::vector<NativeIterator>{};
  for (auto iter : rawPtrIterators) {
    ret.push_back(std::unique_ptr<::rocksdb::Iterator>{iter});
  }
  detail::throwOnError("get multiple iterators failed"sv, std::move(status));
  return ret;
}

inline void NativeClient::write(NativeWriteBatch &&b) {
  auto s = client_->dbInstance_->Write(::rocksdb::WriteOptions{}, &b.batch_);
  detail::throwOnError("write(batch) failed"sv, std::move(s));
}

inline std::unordered_set<std::string> NativeClient::columnFamilies(const std::string &path) {
  auto families = std::vector<std::string>{};
  auto status = ::rocksdb::DB::ListColumnFamilies(::rocksdb::DBOptions{}, path, &families);
  detail::throwOnError("list column families failed"sv, std::move(status));
  auto ret = std::unordered_set<std::string>{};
  for (auto &f : families) {
    ret.emplace(std::move(f));
  }
  return ret;
}

inline std::string NativeClient::defaultColumnFamily() { return ::rocksdb::kDefaultColumnFamilyName; }

inline std::unordered_set<std::string> NativeClient::columnFamilies() const {
  auto ret = std::unordered_set<std::string>{};
  for (const auto &cFamilyToHandle : client_->cf_handles_) {
    ret.insert(cFamilyToHandle.first);
  }
  return ret;
}

inline bool NativeClient::hasColumnFamily(const std::string &cFamily) const {
  return (client_->cf_handles_.find(cFamily) != client_->cf_handles_.cend());
}

inline void NativeClient::createColumnFamily(const std::string &cFamily,
                                             const ::rocksdb::ColumnFamilyOptions &options) {
  auto handle = createColumnFamilyHandle(cFamily, options);
  client_->cf_handles_[cFamily] = std::move(handle);
}

inline void NativeClient::createColumnFamilyWithImport(const std::string &cFamily,
                                                       const ::rocksdb::ImportColumnFamilyOptions &importOpts,
                                                       const ::rocksdb::ExportImportFilesMetaData &metadata,
                                                       const ::rocksdb::ColumnFamilyOptions &cfOpts) {
  ::rocksdb::ColumnFamilyHandle *handlePtr{nullptr};
  auto s = rawDB().CreateColumnFamilyWithImport(cfOpts, cFamily, importOpts, metadata, &handlePtr);
  auto handleUniquePtr = Client::CfUniquePtr{handlePtr, Client::CfDeleter{client_.get()}};
  detail::throwOnError("failed to import column family"sv, cFamily, std::move(s));
  client_->cf_handles_[cFamily] = std::move(handleUniquePtr);
}

inline bool NativeClient::createColumnFamilyIfNotExisting(const std::string &cf,
                                                          const ::rocksdb::CompactionFilter *filter) {
  if (!hasColumnFamily(cf)) {
    auto cf_options = ::rocksdb::ColumnFamilyOptions{};
    if (filter) {
      cf_options.compaction_filter = filter;
    }
    createColumnFamily(cf, cf_options);
    return true;
  }
  return false;
}
inline ::rocksdb::ColumnFamilyOptions NativeClient::columnFamilyOptions(const std::string &cFamily) const {
  auto family = columnFamilyHandle(cFamily);
  auto descriptor = ::rocksdb::ColumnFamilyDescriptor{};
  auto s = family->GetDescriptor(&descriptor);
  detail::throwOnError("getting column family options failed"sv, std::move(s));
  return descriptor.options;
}

inline void NativeClient::dropColumnFamily(const std::string &cFamily) {
  auto it = client_->cf_handles_.find(cFamily);
  if (it == client_->cf_handles_.cend()) {
    return;
  }
  auto s = client_->dbInstance_->DropColumnFamily(it->second.get());
  detail::throwOnError("failed to drop column family"sv, cFamily, std::move(s));
  // std::map::erase(iterator) cannot throw.
  client_->cf_handles_.erase(it);
}

inline ::rocksdb::Options NativeClient::options() const { return client_->dbInstance_->GetOptions(); }

inline ::rocksdb::ColumnFamilyHandle *NativeClient::defaultColumnFamilyHandle() const {
  return client_->dbInstance_->DefaultColumnFamily();
}

inline ::rocksdb::ColumnFamilyHandle *NativeClient::columnFamilyHandle(const std::string &cFamily) const {
  auto it = client_->cf_handles_.find(cFamily);
  if (it == client_->cf_handles_.cend()) {
    detail::throwOnError("no such column family"sv, cFamily, ::rocksdb::Status::ColumnFamilyDropped());
  }
  return it->second.get();
}

inline Client::CfUniquePtr NativeClient::createColumnFamilyHandle(const std::string &cFamily,
                                                                  const ::rocksdb::ColumnFamilyOptions &options) {
  ::rocksdb::ColumnFamilyHandle *cf{nullptr};
  auto s = client_->dbInstance_->CreateColumnFamily(options, cFamily, &cf);
  // Make sure we delete any returned column family handle, irrespective of the returned status.
  // Note: CreateColumnFamily()'s interface is bad and the caller doesn't have enough info by just looking at it. One
  // has to look into the RocksDB implementation to see that a nullptr handle will be returned if the status is not OK.
  // That, however, might change, but the interface will stay the same...
  auto ret = Client::CfUniquePtr{cf, Client::CfDeleter{client_.get()}};
  detail::throwOnError("cannot create column family handle"sv, cFamily, std::move(s));
  return ret;
}

inline void NativeClient::createCheckpointNative(const uint64_t &checkPointId) {
  client_->createCheckpoint(checkPointId);
}
inline std::vector<uint64_t> NativeClient::getListOfCreatedCheckpointsNative() const {
  return client_->getListOfCreatedCheckpoints();
}

inline void NativeClient::removeCheckpointNative(const uint64_t &checkPointId) const {
  client_->removeCheckpoint(checkPointId);
}
inline void NativeClient::setCheckpointDirNative(const std::string &path) { client_->setCheckpointPath(path); }

}  // namespace concord::storage::rocksdb

#endif  // USE_ROCKSDB
