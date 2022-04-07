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
// This module creates an instance of ClientImp class using input
// parameters and launches a bunch of tests created by TestsBuilder towards
// concord::consensus::ReplicaImp objects.

#include "app_state.hpp"

#include <iostream>
#include <sstream>
#include <algorithm>

#include <utt/NtlLib.h>
#include <utt/RangeProof.h>

////////////////////////////////////////////////////////////////////////////////////////////////////
const std::string& Account::getId() const { return wallet_ ? wallet_->getUserPid() : id_; }

void Account::publicDeposit(int val) { publicBalance_ += val; }

int Account::publicWithdraw(int val) {
  val = std::min<int>(publicBalance_, val);
  publicBalance_ -= val;
  return val;
}

libutt::Wallet* Account::getWallet() { return wallet_ ? &(*wallet_) : nullptr; }
const libutt::Wallet* Account::getWallet() const { return wallet_ ? &(*wallet_) : nullptr; }

int Account::getUttBalance() const {
  if (!wallet_) throw std::runtime_error("UTT Wallet is unavalable!");
  int balance = 0;
  for (const auto& c : wallet_->coins) balance += static_cast<int>(c.getValue());
  return balance;
}

int Account::getUttBudget() const {
  if (!wallet_) throw std::runtime_error("UTT Wallet is unavalable!");
  return wallet_->budgetCoin ? static_cast<int>(wallet_->budgetCoin->getValue()) : 0;
}

std::ostream& operator<<(std::ostream& os, const Block& b) {
  os << b.id_ << " | ";
  b.tx_ ? os << *b.tx_ : os << "(Empty)";
  return os;
}

////////////////////////////////////////////////////////////////////////////////////////////////////
void AppState::initUTTLibrary() {
  unsigned char* randSeed = nullptr;  // TODO: initialize entropy source
  int size = 0;                       // TODO: initialize entropy source

  // Apparently, libff logs some extra info when computing pairings
  libff::inhibit_profiling_info = true;

  // AB: We _info disables printing of information and _counters prevents tracking of profiling information. If we
  // are using the code in parallel, disable both the logs.
  libff::inhibit_profiling_counters = true;

  // Initializes the default EC curve, so as to avoid "surprises"
  libff::default_ec_pp::init_public_params();

  // Initializes the NTL finite field
  NTL::ZZ p = NTL::conv<ZZ>("21888242871839275222246405745257275088548364400416034343698204186575808495617");
  NTL::ZZ_p::init(p);

  NTL::SetSeed(randSeed, size);

  libutt::RangeProof::Params::initializeOmegas();
}

AppState::AppState() {
  blocks_.emplace_back();  // Genesis block
}

void AppState::addAccount(Account&& acc) {
  auto it = accounts_.find(acc.getId());
  if (it != accounts_.end()) throw std::runtime_error("Account already exists: " + acc.getId());
  accounts_.emplace(acc.getId(), std::move(acc));
}

void AppState::setLastKnownBlockId(BlockId id) { lastKnownBlockId_ = std::max<int>(lastKnownBlockId_, id); }

BlockId AppState::getLastKnownBlockId() const { return lastKnownBlockId_; }

const Block* AppState::getBlockById(BlockId id) const {
  if (id >= blocks_.size()) return nullptr;
  return &blocks_[id];
}

void AppState::appendBlock(Block&& bl) {
  const BlockId nextBlockId = blocks_.size();
  bl.id_ = nextBlockId;
  blocks_.emplace_back(std::move(bl));
  lastKnownBlockId_ = std::max<int>(lastKnownBlockId_, nextBlockId);
}

std::optional<BlockId> AppState::executeBlocks() {
  for (int i = lastExecutedBlockId_ + 1; i < (int)blocks_.size(); ++i)
    if (blocks_[i].tx_) executeTx(*blocks_[i].tx_);

  lastExecutedBlockId_ = blocks_.size() - 1;

  return lastExecutedBlockId_ < lastKnownBlockId_ ? std::optional<int>(lastExecutedBlockId_ + 1) : std::nullopt;
}

const std::map<std::string, Account>& AppState::GetAccounts() const { return accounts_; }

const std::vector<Block>& AppState::GetBlocks() const { return blocks_; }

const Account* AppState::getAccountById(const std::string& id) const {
  auto it = accounts_.find(id);
  return it != accounts_.end() ? &(it->second) : nullptr;
}

Account* AppState::getAccountById(const std::string& id) {
  auto it = accounts_.find(id);
  return it != accounts_.end() ? &(it->second) : nullptr;
}

const std::set<std::string>& AppState::getNullset() const { return nullset_; }

bool AppState::canExecuteTx(const Tx& tx, std::string& err, const IUTTConfig& cfg) const {
  struct Visitor {
    const AppState& state_;
    const IUTTConfig& uttCfg_;
    Visitor(const AppState& state, const IUTTConfig& cfg) : state_{state}, uttCfg_{cfg} {}

    void operator()(const TxPublicDeposit& tx) const {
      if (tx.amount_ <= 0) throw std::domain_error("Public deposit amount must be positive!");
      auto acc = state_.getAccountById(tx.toAccountId_);
      if (!acc) throw std::domain_error("Unknown account for public deposit!");
    }

    void operator()(const TxPublicWithdraw& tx) const {
      if (tx.amount_ <= 0) throw std::domain_error("Public withdraw amount must be positive!");
      auto acc = state_.getAccountById(tx.toAccountId_);
      if (!acc) throw std::domain_error("Unknown account for public withdraw!");
      if (tx.amount_ > acc->getPublicBalance()) throw std::domain_error("Account has insufficient public balance!");
    }

    void operator()(const TxPublicTransfer& tx) const {
      if (tx.amount_ <= 0) throw std::domain_error("Public transfer amount must be positive!");
      auto sender = state_.getAccountById(tx.fromAccountId_);
      if (!sender) throw std::domain_error("Unknown sender account for public transfer!");
      auto receiver = state_.getAccountById(tx.toAccountId_);
      if (!receiver) throw std::domain_error("Unknown receiver account for public transfer!");
      if (tx.amount_ > sender->getPublicBalance()) throw std::domain_error("Sender has insufficient public balance!");
    }

    void operator()(const TxUttTransfer& tx) const {
      // [TODO--UTT] Validate takes the bank public key, but that's used for quickPay validation
      // which we aren't using in the demo
      if (!tx.uttTx_.validate(uttCfg_.getParams(), uttCfg_.getBankPK(), uttCfg_.getRegAuthPK()))
        throw std::domain_error("Invalid utt transfer tx!");

      // [TODO--UTT] Does a copy of nullifiers
      const auto& nullset = state_.getNullset();
      for (const auto& n : tx.uttTx_.getNullifiers()) {
        if (nullset.count(n) > 0) throw std::domain_error("Input coin already spent!");
      }
    }
  };

  try {
    std::visit(Visitor{*this, cfg}, tx);
  } catch (const std::domain_error& e) {
    err = e.what();
    return false;
  }
  return true;
}

void AppState::executeTx(const Tx& tx) {
  struct Visitor {
    AppState& state_;

    Visitor(AppState& state) : state_{state} {}

    void operator()(const TxPublicDeposit& tx) {
      auto acc = state_.getAccountById(tx.toAccountId_);
      if (acc) acc->publicDeposit(tx.amount_);
    }

    void operator()(const TxPublicWithdraw& tx) {
      auto acc = state_.getAccountById(tx.toAccountId_);
      if (acc) acc->publicWithdraw(tx.amount_);
    }

    void operator()(const TxPublicTransfer& tx) {
      auto sender = state_.getAccountById(tx.fromAccountId_);
      auto receiver = state_.getAccountById(tx.toAccountId_);
      if (sender) sender->publicWithdraw(tx.amount_);
      if (receiver) receiver->publicDeposit(tx.amount_);
    }

    void operator()(const TxUttTransfer& tx) {
      // To-Do
    }
  };

  std::visit(Visitor{*this}, tx);
}
