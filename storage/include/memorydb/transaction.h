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
class Transaction : public ITransaction {
 public:
  Transaction(Client& client, ITransaction::ID id) : ITransaction{id}, client_{client} {}

  void commit() override { commitImpl(); }

  void rollback() override { updates_.clear(); }

  void put(const concordUtils::Sliver& key, const concordUtils::Sliver& value) override {
    updates_.emplace(key, WriteOperation{false, value});
  }

  std::string get(const concordUtils::Sliver& key) override {
    // Try the transaction first.
    auto it = updates_.find(key);
    if (it != std::cend(updates_)) {
      if (it->second.isDelete) {
        return std::string{};
      }
      return it->second.value.toString();
    }

    // If not found in the transaction, try to get from storage.
    concordUtils::Sliver val;
    if (!client_.get(key, val).isOK()) {
      throw std::runtime_error{"memorydb::Transaction: Failed to get key"};
    }
    return val.toString();
  }

  void del(const concordUtils::Sliver& key) override {
    updates_.emplace(key, WriteOperation{true, concordUtils::Sliver{}});
  }

 private:
  struct WriteOperation {
    bool isDelete{false};
    concordUtils::Sliver value;
  };

  // Make sure the commit operation cannot throw. If it does, abort the program.
  void commitImpl() noexcept {
    for (const auto& update : updates_) {
      auto status = concordUtils::Status::OK();
      if (update.second.isDelete) {
        status = client_.del(update.first);
      } else {
        status = client_.put(update.first, update.second.value);
      }

      if (!status.isOK()) {
        std::abort();
      }
    }
    updates_.clear();
  }

  Client& client_;

  // Maps a key to a write operation.
  std::unordered_map<concordUtils::Sliver, WriteOperation> updates_;
};

}  // namespace memorydb
}  // namespace storage
}  // namespace concord
