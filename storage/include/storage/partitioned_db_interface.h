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

#include "db_interface.h"

#include <cstring>
#include <memory>
#include <set>
#include <utility>

namespace concord::storage {

// A transaction in a partitioned DB. Introduces a DB `partition` notion while keeping the existing ITransaction
// interface. Calling ITransaction methods on a IPartitionedTransaction instance is equivalent to using the default
// partition for the client in use.
//
// See IPartitionedDBClient for more information.
class IPartitionedTransaction : public ITransaction {
 public:
  using ITransaction::put;
  using ITransaction::get;
  using ITransaction::del;

  IPartitionedTransaction(ITransaction::ID id) : ITransaction{id} {}

  virtual void put(const std::string& partition, const Sliver& key, const Sliver& value) = 0;
  virtual std::string get(const std::string& partition, const Sliver& key) = 0;
  virtual void del(const std::string& partition, const Sliver& key) = 0;

  virtual ~IPartitionedTransaction() = default;

  // Takes ownership of the passed transaction. Commits the transaction when destructed.
  class Guard {
   public:
    Guard(IPartitionedTransaction* txn) : txn_{txn}, guard_{txn} {}
    Guard(std::unique_ptr<IPartitionedTransaction>&& txn) : txn_{txn.get()}, guard_{std::move(txn)} {}
    Guard(const Guard&) = delete;
    Guard& operator=(const Guard&) = delete;
    Guard(Guard&&) = default;
    Guard& operator=(Guard&&) = default;

    IPartitionedTransaction* operator->() const noexcept { return txn_; }

   private:
    // The transaction is managed by guard_. Store the pointer with its real type here too, in order to avoid a
    // dynamic_cast on every dereference in operator->().
    IPartitionedTransaction* txn_{nullptr};

    ITransaction::Guard guard_;
  };
};

// A client to a partitioned DB. Introduces a DB `partition` notion while mimicking the existing IDBClient interface as
// closely as possible.
//
// A `partition` can be thought of as a key namespace. Two or more partitions can contain the same key and its value can
// be different across them. Partitions can be useful for logically grouping keys in a database. In addition, certain
// implementations can provide optimizations as compared to using key prefixing.
//
// Finally, calling IDBClient methods on a IPartitionedDBClient instance is equivalent to using the default partition.
// Default partition names can vary across implementations and users are required to accomodate that via the
// getDetaultPartition() method.
class IPartitionedDBClient : public IDBClient, public std::enable_shared_from_this<IPartitionedDBClient> {
 public:
  using IDBClient::get;
  using IDBClient::has;
  using IDBClient::put;
  using IDBClient::del;
  using IDBClient::multiGet;
  using IDBClient::multiPut;
  using IDBClient::multiDel;
  using IDBClient::rangeDel;
  using IDBClient::getIterator;

  virtual std::string defaultPartition() const = 0;
  virtual std::set<std::string> partitions() const = 0;
  virtual bool hasPartition(const std::string& partition) const = 0;
  // Adds a partition.
  // Fails with Status::InvalidArgument if the partition already exists.
  virtual Status addPartition(const std::string& partition) = 0;
  // Drops a partition, destroying all key-values inside of it.
  // Fails with Status::InvalidArgument if the partition doesn't exist.
  // Fails with Status::IllegalOperation if the given partition is the default one as it cannot be deleted.
  virtual Status dropPartition(const std::string& partition) = 0;

  virtual Status get(const std::string& partition, const Sliver& key, Sliver& outValue) const = 0;
  virtual Status has(const std::string& partition, const Sliver& key) const = 0;
  virtual Status put(const std::string& partition, const Sliver& key, const Sliver& value) = 0;
  virtual Status del(const std::string& partition, const Sliver& key) = 0;
  virtual Status multiGet(const std::string& partition, const KeysVector& keys, ValuesVector& values) = 0;
  virtual Status multiPut(const std::string& partition, const SetOfKeyValuePairs& keyValues) = 0;
  virtual Status multiDel(const std::string& partition, const KeysVector& keys) = 0;
  // A version of IDBClient::rangeDel() that works on a single partition. See IDBClient::rangeDel() for more
  // information.
  virtual Status rangeDel(const std::string& partition, const Sliver& beginKey, const Sliver& endKey) = 0;

  // Get an iterator to the given partition. Returns a nullptr if the partition doesn't exist.
  virtual std::unique_ptr<IDBClientIterator> getIterator(const std::string& partition) const = 0;

  // Starts a partitioned transaction. Can be upgraded to an IPartitionedTransaction::Guard in order to commit
  // automatically on destruction.
  virtual std::unique_ptr<IPartitionedTransaction> startPartitionedTransaction() = 0;

  // Get a IDBClient instance that works for the given partition. Returns a nullptr if the partition doesn't exist.
  std::unique_ptr<IDBClient> getPartitionClient(const std::string& partition);

  virtual ~IPartitionedDBClient() = default;
};

// A transaction that operates on a single partition that is potentially different than the default one. Allows usage of
// partitions as IDBClient instances (see PartitionDBClient).
class PartitionTransaction : public ITransaction {
 public:
  PartitionTransaction(const std::string& partition, std::unique_ptr<IPartitionedTransaction>&& txn)
      : ITransaction{txn->getId()}, partition_{partition}, txn_{std::move(txn)} {}

  void commit() override { return txn_->commit(); }
  void rollback() override { return txn_->rollback(); }
  void put(const Sliver& key, const Sliver& value) override { return txn_->put(partition_, key, value); }
  std::string get(const Sliver& key) override { return txn_->get(partition_, key); }
  void del(const Sliver& key) override { return txn_->del(partition_, key); }

 private:
  std::string partition_;
  std::unique_ptr<IPartitionedTransaction> txn_;
};

// A client to a DB partition that behaves as a normal IDBClient. All operations are executed in the passed partition as
// if it was the default one.
class PartitionDBClient : public IDBClient {
 public:
  PartitionDBClient(const std::string& partition, const std::shared_ptr<IPartitionedDBClient>& db)
      : partition_{partition}, db_{db} {}

  void init(bool) override {}
  Status get(const Sliver& key, Sliver& outValue) const override { return db_->get(partition_, key, outValue); }
  Status get(const Sliver& key, char*& buf, uint32_t bufSize, uint32_t& size) const override {
    auto outValue = Sliver{};
    const auto s = get(key, outValue);
    if (!s.isOK()) {
      return s;
    }
    if (bufSize < outValue.length()) {
      return Status::GeneralError("Key value is bigger than the output buffer");
    }
    size = outValue.length();
    std::memcpy(buf, outValue.data(), size);
    return Status::OK();
  }
  Status has(const Sliver& key) const override { return db_->has(partition_, key); }
  Status put(const Sliver& key, const Sliver& value) override { return db_->put(partition_, key, value); }
  Status del(const Sliver& key) override { return db_->del(partition_, key); }
  Status multiGet(const KeysVector& keys, ValuesVector& values) override {
    return db_->multiGet(partition_, keys, values);
  }
  Status multiPut(const SetOfKeyValuePairs& keyValues) override { return db_->multiPut(partition_, keyValues); }
  Status multiDel(const KeysVector& keys) override { return db_->multiDel(partition_, keys); }
  Status rangeDel(const Sliver& beginKey, const Sliver& endKey) override {
    return db_->rangeDel(partition_, beginKey, endKey);
  }
  bool isNew() override { return db_->isNew(); }
  ITransaction* beginTransaction() override {
    return new PartitionTransaction{partition_, db_->startPartitionedTransaction()};
  }
  std::unique_ptr<ITransaction> startTransaction() override {
    return std::unique_ptr<ITransaction>{beginTransaction()};
  }
  // TODO - should we overwrite the parent aggregator or just ignore this call?
  void setAggregator(std::shared_ptr<concordMetrics::Aggregator>) override {}
  std::unique_ptr<IDBClientIterator> getIterator() const override { return db_->getIterator(partition_); }

 private:
  std::string partition_;
  std::shared_ptr<IPartitionedDBClient> db_;
};

inline std::unique_ptr<IDBClient> IPartitionedDBClient::getPartitionClient(const std::string& partition) {
  if (!hasPartition(partition)) {
    return nullptr;
  }
  return std::make_unique<PartitionDBClient>(partition, shared_from_this());
}

}  // namespace concord::storage
