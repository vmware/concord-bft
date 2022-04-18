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
#include "utt_config.hpp"

#include <utt/RegAuth.h>
#include <utt/Params.h>
#include <utt/Wallet.h>

class Account {
 public:
  Account(std::string id, int pubBalance = 0) : id_(std::move(id)), publicBalance_{pubBalance} {}

  const std::string& getId() const;

  int getPublicBalance() const { return publicBalance_; }
  void publicDeposit(int val);
  int publicWithdraw(int val);

 private:
  std::string id_;
  int publicBalance_ = 0;
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

class UTTBlockchainApp {
 public:
  static void initUTTLibrary();

  UTTBlockchainApp();
  virtual ~UTTBlockchainApp() = default;

  void setLastKnownBlockId(BlockId id);
  BlockId getLastKnownBlockId() const;
  const Block* getBlockById(BlockId id) const;

  void appendBlock(Block&& bl);            // Returns the id of the appended block
  std::optional<BlockId> executeBlocks();  // Returns the next missing block id if unknown blocks exist

  void addAccount(Account&& acc);

  const std::map<std::string, Account>& GetAccounts() const;
  std::map<std::string, Account>& GetAccounts();

  const std::vector<Block>& GetBlocks() const;

  const Account* getAccountById(const std::string& id) const;
  Account* getAccountById(const std::string& id);

  void addNullifier(std::string nullifier);
  bool hasNullifier(const std::string& nullifier) const;

 protected:
  virtual void executeTx(const Tx& tx);

  std::map<std::string, Account> accounts_;
  std::vector<Block> blocks_;
  std::set<std::string> nullset_;
  BlockId lastExecutedBlockId_ = 0;
  BlockId lastKnownBlockId_ = 0;
};
