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

struct Block {
  int id_ = 0;
  std::map<std::string, Tx> tx_;
  std::set<std::string> nullifiers_;
};
std::ostream& operator<<(std::ostream& os, const Block& b);

struct AppState {
  AppState();

  bool validateTx(const Tx& tx) const;
  void executeTx(const Tx& tx);
  void printLedger(std::optional<int> from = 1, std::optional<int> to = std::nullopt) const;

  std::vector<Account> accounts_;
  std::vector<Block> blocks_;
};
