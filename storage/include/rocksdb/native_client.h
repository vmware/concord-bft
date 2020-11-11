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

#include "client.h"
#include "storage/db_interface.h"

#include <rocksdb/slice.h>
#include <rocksdb/status.h>
#include <rocksdb/write_batch.h>

#include <exception>
#include <map>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>
#include <unordered_set>
#include <utility>
#include <vector>

// Wrapper around the RocksDB library that aims at improving:
//  * memory management
//  * error reporting - done via exceptions instead of status codes
//
// The Span template parameters to methods allow conversion from any type that has data() and size()
// members. The data() member should return a char pointer. That includes std::string, std::string_view,
// std::vector<char>, std::span<char>, Sliver, etc. Raw pointers without size are not supported.
//
// Note1: All methods on all classes throw on errors.
// Note2: All methods without a column family parameter work with the default column family.
namespace concord::storage::rocksdb {

using namespace std::string_view_literals;

struct RocksDBException : std::runtime_error {
  RocksDBException(const std::string &what, ::rocksdb::Status &&s) : std::runtime_error{what}, status{std::move(s)} {}
  const ::rocksdb::Status status;
};

class NativeClient;

class WriteBatch {
 public:
  WriteBatch(const std::shared_ptr<const NativeClient> &);

  template <typename KeySpan, typename ValueSpan>
  void put(const std::string &cFamily, const KeySpan &key, const ValueSpan &value);
  template <typename KeySpan, typename ValueSpan>
  void put(const KeySpan &key, const ValueSpan &value);

  // Deleting a key that doesn't exist is not an error.
  template <typename KeySpan>
  void del(const std::string &cFamily, const KeySpan &key);
  template <typename KeySpan>
  void del(const KeySpan &key);

  // Remove the DB entries in the range [beginKey, endKey).
  template <typename BeginSpan, typename EndSpan>
  void delRange(const std::string &cFamily, const BeginSpan &beginKey, const EndSpan &endKey);
  template <typename BeginSpan, typename EndSpan>
  void delRange(const BeginSpan &beginKey, const EndSpan &endKey);

 private:
  std::shared_ptr<const NativeClient> client_;
  ::rocksdb::WriteBatch batch_;
  friend class NativeClient;
};

// Put a container of key-value pair spans.
template <typename Container>
void putInBatch(WriteBatch &batch, const std::string &cFamily, const Container &cont);
template <typename Container>
void putInBatch(WriteBatch &batch, const Container &cont);

// Delete a container of key spans.
template <typename Container>
void delInBatch(WriteBatch &batch, const std::string &cFamily, const Container &cont);
template <typename Container>
void delInBatch(WriteBatch &batch, const Container &cont);

class Iterator {
 public:
  // Returns true if the iterator points to a key-value pair and false otherwise. Initially, an iterator doesn't point
  // to anything. A false return from this method is not an error - it might mean there are no more keys or a key wasn't
  // found.
  operator bool() const;

  // Methods that move the iterator.
  // Precondition: the iterator points to a key-value pair.
  void first();
  void last();
  void next();
  void prev();

  // Seek for a key that is at or past target.
  template <typename KeySpan>
  void seekAtLeast(const KeySpan &target);

  // Seek for a key that is at or before target.
  template <typename KeySpan>
  void seekAtMost(const KeySpan &target);

  // Copy the key and value from the iterator.
  // Precondition: the iterator points to a key-value pair.
  std::string key() const;
  std::string value() const;

  // A view into the key and value inside the iterator. The return value is valid until the iterator is modified.
  // Precondition: the iterator points to a key-value pair.
  std::string_view keyView() const;
  std::string_view valueView() const;

 private:
  Iterator(std::unique_ptr<::rocksdb::Iterator> iter);
  static std::string sliceToString(const ::rocksdb::Slice &);
  static std::string_view sliceToStringView(const ::rocksdb::Slice &);

 private:
  std::unique_ptr<::rocksdb::Iterator> iter_;
  friend class NativeClient;
};

class NativeClient : public std::enable_shared_from_this<NativeClient> {
 public:
  static std::shared_ptr<NativeClient> newClient(const std::string &path, bool readOnly);

  // Use this native client through the IDBClient interface.
  std::shared_ptr<IDBClient> asIDBClient() const;

  // Column family single key read-write interface.
  template <typename KeySpan, typename ValueSpan>
  void put(const std::string &cFamily, const KeySpan &key, const ValueSpan &value);
  // Returns nullopt if the key is not found.
  template <typename KeySpan>
  std::optional<std::string> get(const std::string &cFamily, const KeySpan &key) const;
  // Deleting a key that doesn't exist is not an error.
  template <typename KeySpan>
  void del(const std::string &cFamily, const KeySpan &key);

  // Single key read-write interface for the default column family.
  template <typename KeySpan, typename ValueSpan>
  void put(const KeySpan &key, const ValueSpan &value);
  // Returns nullopt if the key is not found.
  template <typename KeySpan>
  std::optional<std::string> get(const KeySpan &key) const;
  // Deleting a key that doesn't exist is not an error.
  template <typename KeySpan>
  void del(const KeySpan &key);

  // Batching interface.
  WriteBatch getBatch() const;
  void write(WriteBatch &&);

  // Iterator interface.
  // Iterators initially don't point to a key value, i.e. they convert to false.
  // Important note - RocksDB requires that iterators are destroyed before the DB client that created them.
  //
  // Get an iterator into the default column family.
  Iterator getIterator() const;
  // Get an iterator into a column family
  Iterator getIterator(const std::string &cFamily) const;
  // Get iterators from a consistent database state across multiple column families. The order of the returned iterators
  // match the families input.
  std::vector<Iterator> getIterators(const std::vector<std::string> &cFamilies) const;

  // Column family management.
  static std::string defaultColumnFamily();
  static std::unordered_set<std::string> columnFamilies(const std::string &path);
  std::unordered_set<std::string> columnFamilies() const;
  // If is not an error if it already exists.
  void createColumnFamily(const std::string &cFamily);
  // Drops a column family and its data. It is not an error if the column family doesn't exist.
  void dropColumnFamily(const std::string &cFamily);

 private:
  NativeClient(const std::string &path, bool readOnly);

  // Make sure we only allow types that have data() and size() members. That excludes raw pointers without corresponding
  // size.
  template <
      typename Span,
      std::enable_if_t<std::is_convertible_v<decltype(std::declval<Span>().size()), std::size_t> &&
                           std::is_pointer_v<decltype(std::declval<Span>().data())> &&
                           std::is_convertible_v<std::remove_pointer_t<decltype(std::declval<Span>().data())> (*)[],
                                                 const char (*)[]>,
                       int> = 0>
  static ::rocksdb::Slice toSlice(const Span &span) noexcept {
    return ::rocksdb::Slice{span.data(), span.size()};
  }

  static void throwOnError(std::string_view msg1, std::string_view msg2, ::rocksdb::Status &&);
  static void throwOnError(std::string_view msg, ::rocksdb::Status &&);

  // For use in column family unique pointers that are managed solely by NativeClient. This allows us to use a raw
  // pointer in CfDeleter as we know the column family unique pointer will not be moved out of NativeClient.
  struct CfDeleter {
    void operator()(::rocksdb::ColumnFamilyHandle *h) const noexcept {
      if (!client_->dbInstance_->DestroyColumnFamilyHandle(h).ok()) {
        std::terminate();
      }
    }
    Client *client_{nullptr};
  };

  using CfUniquePtr = std::unique_ptr<::rocksdb::ColumnFamilyHandle, CfDeleter>;

  ::rocksdb::ColumnFamilyHandle *defaultColumnFamilyHandle() const;
  ::rocksdb::ColumnFamilyHandle *columnFamilyHandle(const std::string &cFamily) const;
  CfUniquePtr createColumnFamilyHandle(const std::string &cFamily);

 private:
  std::string path_;
  // Keep cf_handles_ after the client_ as column family handles need to be deleted before deleting the client. See
  // CfDeleter.
  std::shared_ptr<Client> client_;
  std::map<std::string, CfUniquePtr> cf_handles_;
  friend class WriteBatch;
  friend class Iterator;
};

inline WriteBatch::WriteBatch(const std::shared_ptr<const NativeClient> &client) : client_{client} {}

template <typename KeySpan, typename ValueSpan>
void WriteBatch::put(const std::string &cFamily, const KeySpan &key, const ValueSpan &value) {
  auto s = batch_.Put(client_->columnFamilyHandle(cFamily), NativeClient::toSlice(key), NativeClient::toSlice(value));
  NativeClient::throwOnError("batch put failed"sv, std::move(s));
}

template <typename KeySpan, typename ValueSpan>
void WriteBatch::put(const KeySpan &key, const ValueSpan &value) {
  return put(NativeClient::defaultColumnFamily(), key, value);
}

template <typename KeySpan>
void WriteBatch::del(const std::string &cFamily, const KeySpan &key) {
  auto s = batch_.Delete(client_->columnFamilyHandle(cFamily), NativeClient::toSlice(key));
  NativeClient::throwOnError("batch del failed"sv, std::move(s));
}

template <typename KeySpan>
void WriteBatch::del(const KeySpan &key) {
  return del(NativeClient::defaultColumnFamily(), key);
}

template <typename BeginSpan, typename EndSpan>
void WriteBatch::delRange(const std::string &cFamily, const BeginSpan &beginKey, const EndSpan &endKey) {
  auto s = batch_.DeleteRange(
      client_->columnFamilyHandle(cFamily), NativeClient::toSlice(beginKey), NativeClient::toSlice(endKey));
  NativeClient::throwOnError("delRange failed", std::move(s));
}

template <typename BeginSpan, typename EndSpan>
void WriteBatch::delRange(const BeginSpan &beginKey, const EndSpan &endKey) {
  return delRange(client_->defaultColumnFamily(), beginKey, endKey);
}

template <typename Container>
void putInBatch(WriteBatch &batch, const std::string &cFamily, const Container &cont) {
  for (auto &[key, value] : cont) {
    batch.put(cFamily, key, value);
  }
}

template <typename Container>
void putInBatch(WriteBatch &batch, const Container &cont) {
  return putInBatch(batch, NativeClient::defaultColumnFamily(), cont);
}

template <typename Container>
void delInBatch(WriteBatch &batch, const std::string &cFamily, const Container &cont) {
  for (auto &key : cont) {
    batch.del(cFamily, key);
  }
}

template <typename Container>
void delInBatch(WriteBatch &batch, const Container &cont) {
  return delInBatch(batch, NativeClient::defaultColumnFamily(), cont);
}

inline Iterator::Iterator(std::unique_ptr<::rocksdb::Iterator> iter) : iter_{std::move(iter)} {}

inline std::string Iterator::sliceToString(const ::rocksdb::Slice &s) { return std::string{s.data(), s.size()}; }

inline std::string_view Iterator::sliceToStringView(const ::rocksdb::Slice &s) {
  return std::string_view{s.data(), s.size()};
}

inline Iterator::operator bool() const { return iter_->Valid(); }

inline void Iterator::first() {
  iter_->SeekToFirst();
  NativeClient::throwOnError("iterator first failed"sv, iter_->status());
}

inline void Iterator::last() {
  iter_->SeekToLast();
  NativeClient::throwOnError("iterator last failed"sv, iter_->status());
}

inline void Iterator::next() {
  if (!iter_->Valid()) {
    NativeClient::throwOnError("next on invalid iterator"sv, ::rocksdb::Status::InvalidArgument());
  }
  iter_->Next();
  auto s = iter_->status();
  NativeClient::throwOnError("iterator next failed"sv, iter_->status());
}

inline void Iterator::prev() {
  if (!iter_->Valid()) {
    NativeClient::throwOnError("prev on invalid iterator"sv, ::rocksdb::Status::InvalidArgument());
  }
  iter_->Prev();
  auto s = iter_->status();
  NativeClient::throwOnError("iterator prev failed"sv, iter_->status());
}

template <typename KeySpan>
void Iterator::seekAtLeast(const KeySpan &target) {
  iter_->Seek(NativeClient::toSlice(target));
  NativeClient::throwOnError("iterator seekAtLeast failed"sv, iter_->status());
}

template <typename KeySpan>
void Iterator::seekAtMost(const KeySpan &target) {
  iter_->SeekForPrev(NativeClient::toSlice(target));
  NativeClient::throwOnError("iterator seekAtMost failed"sv, iter_->status());
}

inline std::string Iterator::key() const {
  if (!iter_->Valid()) {
    NativeClient::throwOnError("key on invalid iterator"sv, ::rocksdb::Status::InvalidArgument());
  }
  return sliceToString(iter_->key());
}

inline std::string Iterator::value() const {
  if (!iter_->Valid()) {
    NativeClient::throwOnError("value on invalid iterator"sv, ::rocksdb::Status::InvalidArgument());
  }
  return sliceToString(iter_->value());
}

inline std::string_view Iterator::keyView() const {
  if (!iter_->Valid()) {
    NativeClient::throwOnError("keyView on invalid iterator"sv, ::rocksdb::Status::InvalidArgument());
  }
  return sliceToStringView(iter_->key());
}

inline std::string_view Iterator::valueView() const {
  if (!iter_->Valid()) {
    NativeClient::throwOnError("valueView on invalid iterator"sv, ::rocksdb::Status::InvalidArgument());
  }
  return sliceToStringView(iter_->value());
}

inline std::shared_ptr<NativeClient> NativeClient::newClient(const std::string &path, bool readOnly) {
  return std::shared_ptr<NativeClient>{new NativeClient{path, readOnly}};
}

inline NativeClient::NativeClient(const std::string &path, bool readOnly)
    : path_{path}, client_{std::make_shared<Client>(path)} {
  client_->init(readOnly);

  const auto families = NativeClient::columnFamilies(path);
  for (auto &f : families) {
    if (defaultColumnFamily() != f) {
      cf_handles_.emplace(f, createColumnFamilyHandle(f));
    }
  }
}

inline std::shared_ptr<IDBClient> NativeClient::asIDBClient() const { return client_; }

template <typename KeySpan, typename ValueSpan>
void NativeClient::put(const std::string &cFamily, const KeySpan &key, const ValueSpan &value) {
  auto s =
      client_->dbInstance_->Put(::rocksdb::WriteOptions{}, columnFamilyHandle(cFamily), toSlice(key), toSlice(value));
  throwOnError("put() failed"sv, std::move(s));
  client_->storage_metrics_.tryToUpdateMetrics();
}

template <typename KeySpan>
std::optional<std::string> NativeClient::get(const std::string &cFamily, const KeySpan &key) const {
  auto value = std::string{};
  auto s = client_->dbInstance_->Get(::rocksdb::ReadOptions{}, columnFamilyHandle(cFamily), toSlice(key), &value);
  if (s.IsNotFound()) {
    return std::nullopt;
  }
  throwOnError("get() failed"sv, std::move(s));
  client_->storage_metrics_.tryToUpdateMetrics();
  return value;
}

template <typename KeySpan>
void NativeClient::del(const std::string &cFamily, const KeySpan &key) {
  auto s = client_->dbInstance_->Delete(::rocksdb::WriteOptions{}, columnFamilyHandle(cFamily), toSlice(key));
  throwOnError("del() failed"sv, std::move(s));
  client_->storage_metrics_.tryToUpdateMetrics();
}

template <typename KeySpan, typename ValueSpan>
void NativeClient::put(const KeySpan &key, const ValueSpan &value) {
  return put(defaultColumnFamily(), key, value);
}

template <typename KeySpan>
std::optional<std::string> NativeClient::get(const KeySpan &key) const {
  return get(defaultColumnFamily(), key);
}

template <typename KeySpan>
void NativeClient::del(const KeySpan &key) {
  return del(defaultColumnFamily(), key);
}

inline WriteBatch NativeClient::getBatch() const { return WriteBatch{shared_from_this()}; }

inline Iterator NativeClient::getIterator() const {
  return std::unique_ptr<::rocksdb::Iterator>{client_->dbInstance_->NewIterator(::rocksdb::ReadOptions{})};
}

inline Iterator NativeClient::getIterator(const std::string &cFamily) const {
  return std::unique_ptr<::rocksdb::Iterator>{
      client_->dbInstance_->NewIterator(::rocksdb::ReadOptions{}, columnFamilyHandle(cFamily))};
}

inline std::vector<Iterator> NativeClient::getIterators(const std::vector<std::string> &cFamilies) const {
  auto cfHandles = std::vector<::rocksdb::ColumnFamilyHandle *>{};
  for (auto &cf : cFamilies) {
    cfHandles.push_back(columnFamilyHandle(cf));
  }
  auto rawPtrIterators = std::vector<::rocksdb::Iterator *>{};
  auto uniquePtrIterators = std::vector<std::unique_ptr<::rocksdb::Iterator>>{};
  auto status = client_->dbInstance_->NewIterators(::rocksdb::ReadOptions{}, cfHandles, &rawPtrIterators);
  // Are we supposed to delete the iterators if error is returned...
  for (auto iter : rawPtrIterators) {
    uniquePtrIterators.push_back(std::unique_ptr<::rocksdb::Iterator>{iter});
  }
  throwOnError("get multiple iterators failed"sv, std::move(status));
  auto ret = std::vector<Iterator>{};
  for (auto &iter : uniquePtrIterators) {
    ret.push_back(std::move(iter));
  }
  return ret;
}

inline void NativeClient::write(WriteBatch &&b) {
  auto s = client_->dbInstance_->Write(::rocksdb::WriteOptions{}, &b.batch_);
  throwOnError("write(batch) failed"sv, std::move(s));
  client_->storage_metrics_.tryToUpdateMetrics();
}

inline std::unordered_set<std::string> NativeClient::columnFamilies(const std::string &path) {
  auto families = std::vector<std::string>{};
  auto status = ::rocksdb::DB::ListColumnFamilies(::rocksdb::DBOptions{}, path, &families);
  throwOnError("list column families failed"sv, std::move(status));
  auto ret = std::unordered_set<std::string>{};
  for (auto &f : families) {
    ret.emplace(std::move(f));
  }
  return ret;
}

inline std::string NativeClient::defaultColumnFamily() { return ::rocksdb::kDefaultColumnFamilyName; }

inline std::unordered_set<std::string> NativeClient::columnFamilies() const { return columnFamilies(path_); }

inline void NativeClient::createColumnFamily(const std::string &cFamily) {
  if (cf_handles_.find(cFamily) != cf_handles_.cend()) {
    return;
  }
  cf_handles_.emplace(cFamily, createColumnFamilyHandle(cFamily));
}

inline void NativeClient::dropColumnFamily(const std::string &cFamily) {
  auto it = cf_handles_.find(cFamily);
  if (it == cf_handles_.cend()) {
    return;
  }
  auto s = client_->dbInstance_->DropColumnFamily(it->second.get());
  throwOnError("failed to drop column family"sv, cFamily, std::move(s));
  // std::map::erase(iterator) cannot throw.
  cf_handles_.erase(it);
}

inline void NativeClient::throwOnError(std::string_view msg1, std::string_view msg2, ::rocksdb::Status &&s) {
  if (!s.ok()) {
    auto rocksdbMsg = std::string{"RocksDB: "};
    rocksdbMsg.append(msg1);
    rocksdbMsg.append(": ");
    rocksdbMsg.append(msg2);
    LOG_ERROR(Client::logger(), rocksdbMsg << ", status = " << s.ToString());
    throw RocksDBException{rocksdbMsg, std::move(s)};
  }
}

inline void NativeClient::throwOnError(std::string_view msg, ::rocksdb::Status &&s) {
  return throwOnError(msg, ""sv, std::move(s));
}

inline ::rocksdb::ColumnFamilyHandle *NativeClient::defaultColumnFamilyHandle() const {
  return client_->dbInstance_->DefaultColumnFamily();
}

inline ::rocksdb::ColumnFamilyHandle *NativeClient::columnFamilyHandle(const std::string &cFamily) const {
  if (defaultColumnFamily() == cFamily) {
    return defaultColumnFamilyHandle();
  }
  auto it = cf_handles_.find(cFamily);
  if (it == cf_handles_.cend()) {
    throwOnError("no such column family"sv, cFamily, ::rocksdb::Status::ColumnFamilyDropped());
  }
  return it->second.get();
}

inline NativeClient::CfUniquePtr NativeClient::createColumnFamilyHandle(const std::string &cFamily) {
  ::rocksdb::ColumnFamilyHandle *cf{nullptr};
  auto s = client_->dbInstance_->CreateColumnFamily(::rocksdb::ColumnFamilyOptions{}, cFamily, &cf);
  // Are we supposed to delete the handle if error is returned...
  auto ret = CfUniquePtr{cf, CfDeleter{client_.get()}};
  throwOnError("cannot create column family handle"sv, cFamily, std::move(s));
  return ret;
}

}  // namespace concord::storage::rocksdb
