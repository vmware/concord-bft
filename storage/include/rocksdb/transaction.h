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
#include <rocksdb/utilities/transaction.h>
#include "storage/db_interface.h"
#include "client.h"
#pragma once

namespace concord {
namespace storage {
namespace rocksdb {


#define ROCKSDB_THROW(action, status) \
  throw std::runtime_error("rocksdb error: action: " action ", txn id[" + getIdStr() + std::string("], reason: ") + status.ToString())

class Transaction: public ITransaction {
 public:
  Transaction(::rocksdb::Transaction* txn, ID id): ITransaction(id), txn_(txn){}

  void commit() override {
    ::rocksdb::Status s = txn_->Commit();
    if (!s.ok())
      ROCKSDB_THROW("Commit", s);
  }
  void rollback() override {
    ::rocksdb::Status s = txn_->Rollback();
    if (!s.ok())
      ROCKSDB_THROW("Rollback", s);

  }
  void put(const Sliver& key, const Sliver& value)  override {
    ::rocksdb::Status s = txn_->Put(toRocksdbSlice(key), toRocksdbSlice(value));
    if (!s.ok() )
      ROCKSDB_THROW("Put", s);
  }
  std::string get(const Sliver& key) override {
    std::string val;
    ::rocksdb::Status s = txn_->Get(::rocksdb::ReadOptions(), toRocksdbSlice(key), &val);
    if (!s.ok() && !s.IsNotFound())
      ROCKSDB_THROW("Get", s);
    return val;
  }
  void remove(const Sliver& key) override {
    ::rocksdb::Status s =txn_->Delete(toRocksdbSlice(key));
    if (!s.ok())
      ROCKSDB_THROW("Delete", s);
  }

 protected:
  std::unique_ptr<::rocksdb::Transaction> txn_;
};

}
}
}
