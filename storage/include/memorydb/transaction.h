// Copyright 2020 VMware, all rights reserved

#pragma once

#include "client.h"
#include "kv_types.hpp"
#include "storage/db_interface.h"

#include <cstdlib>
#include <exception>
#include <string>
#include <vector>

namespace concord {
namespace storage {
namespace memorydb {

// Provides transaction support for memorydb. Since memorydb only supports single-thread operations, Transaction takes
// advantage of that and implements commit() trivially.
class Transaction : public ITransaction {
 public:
  Transaction(Client& client, ITransaction::ID id) : ITransaction{id}, client_{client} {}

  void commit() override { commitImpl(); }

  void rollback() override { ops_.clear(); }

  void put(const concordUtils::Sliver& key, const concordUtils::Sliver& value) override {
    ops_.push_back(Operation{false, key, value});
  }

  std::string get(const concordUtils::Sliver& key) override {
    concordUtils::Sliver val;
    if (!client_.get(key, val).isOK()) {
      throw std::runtime_error{"memorydb::Transaction: Failed to get key"};
    }
    return val.toString();
  }

  void del(const concordUtils::Sliver& key) override { ops_.push_back(Operation{true, key, concordUtils::Value{}}); }

 private:
  struct Operation {
    bool isDelete{false};
    concordUtils::Key key;
    concordUtils::Value value;
  };

  // Make sure the commit operation cannot throw. If it does, abort the program.
  void commitImpl() noexcept {
    for (const auto& op : ops_) {
      auto status = concordUtils::Status::OK();
      if (op.isDelete) {
        status = client_.del(op.key);
      } else {
        status = client_.put(op.key, op.value);
      }

      if (!status.isOK()) {
        std::abort();
      }
    }
    ops_.clear();
  }

  Client& client_;
  std::vector<Operation> ops_;
};

}  // namespace memorydb
}  // namespace storage
}  // namespace concord
