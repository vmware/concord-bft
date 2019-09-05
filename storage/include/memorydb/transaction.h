// Concord
//
// Copyright (c) 2019 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the sub-component's license, as noted in the
// LICENSE file.

#pragma once

#include <sstream>

#include "storage/db_interface.h"
#include "client.h"

namespace concord {
namespace storage {
namespace memorydb {

#define MEMORYDB_THROW(action, status)                                                                \
  std::ostringstream os;                                                                              \
  os << "memorydb error: action: " << action << ", txn id[" << getIdStr() << "], reason: " << status; \
  throw std::runtime_error(os.str())

// In-memory transactions are just a thin wrapper around a client.
// A transaction already holds a lock when it gets created so it has exclusive
// control over the client for the lifetime of the transaction.
//
// All writes are put into a shadow map and are only applied on commit.
// This allows the transaction to abort in case of an exception. Reads first
// check the shadow map, then look in the underlying map accessed via the client
// if the transaction hasn't updated the requested key.
class Transaction : public ITransaction {
 private:
  enum OpType { Put, Remove };
  struct WriteOp {
    OpType type_;
    Sliver key;
    Sliver value;
  };

 public:
  Transaction(Client* client, ID id, std::unique_lock<std::recursive_mutex> client_lock_guard)
      : ITransaction(id),
        client_(client),
        client_lock_guard_(std::move(client_lock_guard)),
        updates_(client->getMap().key_comp()) {}

  // Apply all write operations to the memorydb via client_ 
  void commit() override {
    for (auto&& kv : updates_) {
      auto op = kv.second;
      if (op.type_ == Put) {
        auto status = client_->put(op.key, op.value);
        if (!status.isOK()) {
          MEMORYDB_THROW("Put", status);
        }
      } else {
        auto status = client_->del(op.key);
        if (!status.isOK()) {
          MEMORYDB_THROW("Remove", status);
        }
      }
    }
  }

  void rollback() override {
    // NOOP
  }

  void put(const Sliver& key, const Sliver& value) override { updates_[key] = WriteOp{Put, key, value}; }

  // Return an empty string if the key is not found
  std::string get(const Sliver& key) override {
    try {
      // Look in the shadow map to see if the key was updated during the
      // transaction.
      WriteOp value = updates_.at(key);
      if (value.type_ == Put) {
        return std::string((char*)value.value.data(), value.value.length());
      } else {
        return std::string();
      }
    } catch (std::out_of_range&) {
      // The value wasn't in the shadow map. Look in the memorydb via the
      // client.
      Sliver out;
      auto status = client_->get(key, out);
      if (!status.isOK() && !status.isNotFound()) {
        MEMORYDB_THROW("Get", status);
      }
      return std::string((char*)out.data(), out.length());
    }
  }

  void remove(const Sliver& key) override { updates_[key] = WriteOp{Remove, key}; }

 private:
  // A Client is expected to live for the lifetime of the program
  Client* client_;

  // A lock around the client is held for the duration of the transaction
  std::unique_lock<std::recursive_mutex> client_lock_guard_;

  // All writes that take place during the transaction occur in this shadow
  // map. In transaction gets read from the shadow map first, and then read from
  // the memorydb client if the key has not been touched.
  std::map<Sliver, WriteOp, Compare> updates_;
};

}  // namespace memorydb
}  // namespace storage
}  // namespace concord
