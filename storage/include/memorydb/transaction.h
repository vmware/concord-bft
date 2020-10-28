// Copyright 2020 VMware, all rights reserved

#pragma once

#include "client.h"
#include "sliver.hpp"
#include "storage/db_interface.h"

#include <cstdlib>
#include <exception>
#include <iterator>
#include <string>
#include <unordered_map>

namespace concord {
namespace storage {
namespace memorydb {

// Provides transaction support for memorydb. Since memorydb only supports single-thread operations, Transaction takes
// advantage of that and implements commit() trivially.
class Transaction : public IPartitionedTransaction {
 public:
  using IPartitionedTransaction::get;
  using IPartitionedTransaction::put;
  using IPartitionedTransaction::del;

  Transaction(Client& client, ITransaction::ID id) : IPartitionedTransaction{id}, client_{client} {}

  void commit() override { commitImpl(); }

  void rollback() override { partitionOps_.clear(); }

  void put(const std::string& partition, const Sliver& key, const concordUtils::Sliver& value) override {
    partitionOps_[partition].emplace(key, WriteOperation{false, value});
  }

  void put(const Sliver& key, const concordUtils::Sliver& value) override {
    return put(Client::kDefaultPartition, key, value);
  }

  std::string get(const std::string& partition, const concordUtils::Sliver& key) override {
    // Try the transaction first.
    auto partitionIt = partitionOps_.find(partition);
    if (partitionIt != std::cend(partitionOps_)) {
      const auto& writeOps = partitionIt->second;
      auto kvIt = writeOps.find(key);
      if (kvIt != std::cend(writeOps)) {
        if (kvIt->second.isDelete) {
          return std::string{};
        }
        return kvIt->second.value.toString();
      }
    }

    // If not found in the transaction, try to get from storage.
    concordUtils::Sliver val;
    if (!client_.get(partition, key, val).isOK()) {
      throw std::runtime_error{"memorydb::Transaction: Failed to get key"};
    }
    return val.toString();
  }

  std::string get(const concordUtils::Sliver& key) override { return get(Client::kDefaultPartition, key); }

  void del(const std::string& partition, const concordUtils::Sliver& key) override {
    partitionOps_[partition].emplace(key, WriteOperation{true, concordUtils::Sliver{}});
  }

  void del(const concordUtils::Sliver& key) override { return del(Client::kDefaultPartition, key); }

 private:
  struct WriteOperation {
    bool isDelete{false};
    concordUtils::Sliver value;
  };

  // Make sure the commit operation cannot throw. If it does, terminate the program.
  void commitImpl() noexcept {
    for (const auto& [partition, ops] : partitionOps_) {
      for (const auto& [key, op] : ops) {
        auto status = concordUtils::Status::OK();
        if (op.isDelete) {
          status = client_.del(partition, key);
        } else {
          status = client_.put(partition, key, op.value);
        }

        if (!status.isOK()) {
          std::terminate();
        }
      }
    }
    partitionOps_.clear();
  }

  Client& client_;

  // Maps a key to a write operation.
  using WriteOperations = std::unordered_map<concordUtils::Sliver, WriteOperation>;

  // Maps a partition to a set of write operations for it.
  std::map<std::string, WriteOperations> partitionOps_;
};

}  // namespace memorydb
}  // namespace storage
}  // namespace concord
