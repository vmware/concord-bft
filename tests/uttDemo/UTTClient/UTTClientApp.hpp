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
#include <queue>

#include "Logger.hpp"
#include "utt_blockchain_app.hpp"

/////////////////////////////////////////////////////////////////////////////////////////////////////
class UTTClientApp : public UTTBlockchainApp {
 public:
  UTTClientApp(logging::Logger& logger, uint16_t walletId);

  uint16_t getNumReplicas() const { return numReplicas_; }
  uint16_t getSigThresh() const { return sigThresh_; }

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

  void printState(const std::string& path = "") const;

  template <typename T>
  std::string fmtCurrency(T val) const {
    return "$" + std::to_string(val);
  }

 private:
  static std::optional<std::string> extractPathToken(std::stringstream& ss);
  static size_t getValidIdx(size_t size, const std::string& object, const std::string& tokenIdx);

  void executeTx(const Tx& tx) override;
  void pruneSpentCoins();
  void tryClaimCoins(const TxUtt& tx);

  struct PrintContext;

  void printBalance(PrintContext& ctx) const;
  void printAccounts(PrintContext& ctx) const;

  void printWallet(PrintContext& ctx, std::stringstream& ss) const;
  void printCoin(PrintContext& ctx, const libutt::Coin& coin) const;

  void printLedger(PrintContext& ctx, std::stringstream& ss) const;
  void printBlock(PrintContext& ctx, const Block& block, std::stringstream& ss) const;
  void printUttTx(PrintContext& ctx, const libutt::Tx& tx, std::stringstream& ss) const;
  void printTxIn(PrintContext& ctx, const libutt::TxIn& txi) const;
  void printTxOut(PrintContext& ctx, const libutt::TxOut& txo) const;

  logging::Logger& logger_;
  std::string myPid_;
  std::set<std::string> otherPids_;
  libutt::Wallet wallet_;

  // [TODO-UTT] Check these assumptions when loading a config
  uint16_t numReplicas_ = 4;
  uint16_t sigThresh_ = 2;  // F + 1
};