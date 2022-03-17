// Concord
//
// Copyright (c) 2018-2020 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#pragma once

#include <vector>
#include <map>
#include <set>
#include <string>
#include <optional>
#include <exception>

#include "transactions.hpp"

class Account {
 public:
  Account(std::string id) : id_(std::move(id)) {}

  const std::string getId() const { return id_; }
  int getBalancePublic() const { return publicBalance_; }

  void depositPublic(int val);
  int withdrawPublic(int val);

 private:
  std::string id_;
  int publicBalance_ = 0;
  // To-Do: add optional UTT wallet
};

using BlockId = std::uint64_t;

struct Block {
  Block() : id_(0) {}
  Block(Tx tx) : id_(0), tx_(std::move(tx)) {}
  Block(BlockId id, Tx tx) : id_(id), tx_(std::move(tx)) {}

  BlockId id_ = 0;
  std::optional<Tx> tx_;
};
std::ostream& operator<<(std::ostream& os, const Block& b);

struct AppState {
  AppState();

  void setLastKnownBlockId(BlockId id);
  BlockId getLastKnowBlockId() const;
  const Block* getBlockById(BlockId id) const;

  std::optional<BlockId> sync();    // Returns missing block id if unknown blocks exist
  BlockId appendBlock(Block&& bl);  // Return the id of the appended block

  const std::map<std::string, Account> GetAccounts() const;
  const std::vector<Block>& GetBlocks() const;

  const Account* getAccountById(const std::string& id) const;
  Account* getAccountById(const std::string& id);

  void validateTx(const Tx& tx) const;  // throws std::domain_error
  void executeTx(const Tx& tx);

 private:
  std::map<std::string, Account> accounts_;
  std::vector<Block> blocks_;
  BlockId lastExecutedBlockId_ = 0;
  BlockId lastKnownBlockId_ = 0;
};
