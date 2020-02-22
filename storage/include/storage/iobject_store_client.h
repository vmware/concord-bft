// Copyright 2020 VMware, all rights reserved

#pragma once

#include "sliver.hpp"
#include "status.hpp"
#include "db_interface.h"

#define OUT

namespace concord::storage {

using concordUtils::Sliver;
using concordUtils::Status;

// The interface for object store clients
class IObjectStoreClient {
  virtual Status get(const Sliver& key, OUT Sliver& outValue) = 0;
  virtual Status put(const Sliver& key, const Sliver& value) = 0;
  virtual Status del(const Sliver& key) = 0;

  // We don't just return bool, because we need to know if the operation fails
  // due to object store unavailability.
  virtual Status objectExists(const Sliver& key) = 0;
};

// An implementation of IObjectStoreClient for a database backed by an IDBClient
class DbObjectStoreClient : public IObjectStoreClient {
 public:
  DbObjectStoreClient(const std::shared_ptr<IDBClient>& db) : db_(db) {}

  Status get(const Sliver& key, OUT Sliver& outValue) override { return db_->get(key, outValue); }
  Status put(const Sliver& key, const Sliver& value) override { return db_->put(key, value); }
  Status del(const Sliver& key) override { return db_->del(key); }
  Status objectExists(const Sliver& key) override {
    Sliver unused;
    return db_->get(key, unused);
  }

 private:
  std::shared_ptr<IDBClient> db_;
};

}  // namespace concord::storage