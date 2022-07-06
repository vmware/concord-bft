// Concord
//
// Copyright (c) 2022-2023 VMware, Inc. All Rights Reserved.
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

#include <string>

#include "db_interfaces.h"
#include "v4blockchain/v4_blockchain.h"

using concord::storage::rocksdb::NativeClient;

namespace concord::kvbc::adapter::v4blockchain {

class BlocksAdderAdapter : public IBlockAdder {
 public:
  virtual ~BlocksAdderAdapter() { kvbc_ = nullptr; }
  explicit BlocksAdderAdapter(std::shared_ptr<concord::kvbc::v4blockchain::KeyValueBlockchain> &kvbc)
      : kvbc_{kvbc.get()} {}

  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  // IBlockAdder
  BlockId add(concord::kvbc::categorization::Updates &&updates) override final {
    return kvbc_->add(std::move(updates));
  }
  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

 private:
  concord::kvbc::v4blockchain::KeyValueBlockchain *kvbc_{nullptr};
};

}  // namespace concord::kvbc::adapter::v4blockchain
