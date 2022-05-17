// Concord
//
// Copyright (c) 2018-2022 VMware, Inc. All Rights Reserved.
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

#include <sstream>

#include "Logger.hpp"
#include "utt_blockchain_app.hpp"

/////////////////////////////////////////////////////////////////////////////////////////////////////
class UTTClientApp : public UTTBlockchainApp {
 public:
  UTTClientApp(logging::Logger& logger, uint16_t walletId);

  const std::string& getMyPid() const;
  const Account& getMyAccount() const;

  const libutt::Wallet& getMyUttWallet() const;
  const std::set<std::string>& getOtherPids() const;

  size_t getUttBalance() const;
  size_t getUttBudget() const;

  // Prints the state of the client given a selector that
  // represents a hierarchical path to some part of the state
  // Examples:
  // default: prints the account balance
  // 'wallet' - prints the content of the utt wallet
  // 'wallet/coins' - prints a list of the wallet coins
  // 'wallet/coins/0' - prints details about the first coin
  // 'ledger' - prints the contents of the ledger
  // 'ledger/0' - prints the contents of the first block of the ledger

  void printState(const std::string& selector = "") const;

  template <typename T>
  std::string fmtCurrency(T val) const {
    return "$" + std::to_string(val);
  }

 private:
  static std::string extractToken(std::stringstream& ss);

  void executeTx(const Tx& tx) override;
  void pruneSpentCoins();
  void tryClaimCoins(const TxUtt& tx);

  void printOtherPids() const;

  void printWallet(const libutt::Wallet& wallet, std::string& path, std::stringstream& ss) const;
  void printCoins(const libutt::Wallet& wallet, std::string& path, std::stringstream& ss) const;
  void printCoin(const libutt::Coin& coin, std::string& path, std::stringstream& ss) const;

  void printLedger(std::string& path, std::stringstream& ss) const;
  void printBlock(BlockId blockId, std::string& path, std::stringstream& ss) const;
  void printUttTx(const libutt::Tx& tx, std::string& path, std::stringstream& ss) const;

  logging::Logger& logger_;
  std::string myPid_;
  std::set<std::string> otherPids_;
  libutt::Wallet wallet_;
};