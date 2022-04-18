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

#include "utt_blockchain_app.hpp"

#include <iostream>
#include <sstream>
#include <algorithm>

#include <utt/NtlLib.h>
#include <utt/RangeProof.h>

#include <utt/Client.h>

////////////////////////////////////////////////////////////////////////////////////////////////////
const std::string& Account::getId() const { return id_; }

void Account::publicDeposit(int val) { publicBalance_ += val; }

int Account::publicWithdraw(int val) {
  val = std::min<int>(publicBalance_, val);
  publicBalance_ -= val;
  return val;
}

std::ostream& operator<<(std::ostream& os, const Block& b) {
  os << b.id_ << " | ";
  if (!b.tx_)
    os << "(Empty)";
  else if (const auto* txUtt = std::get_if<TxUtt>(&(*b.tx_)))
    os << "UTT Tx: " << txUtt->utt_.getHashHex();
  else
    os << *b.tx_;  // Public Tx
  return os;
}

////////////////////////////////////////////////////////////////////////////////////////////////////
void UTTBlockchainApp::initUTTLibrary() {
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
  // [TODO-UTT] What is this value for?
  NTL::ZZ p = NTL::conv<ZZ>("21888242871839275222246405745257275088548364400416034343698204186575808495617");
  NTL::ZZ_p::init(p);

  NTL::SetSeed(randSeed, size);

  libutt::RangeProof::Params::initializeOmegas();
}

UTTBlockchainApp::UTTBlockchainApp() {
  blocks_.emplace_back();  // Genesis block
}

void UTTBlockchainApp::addAccount(Account&& acc) {
  auto it = accounts_.find(acc.getId());
  if (it != accounts_.end()) throw std::runtime_error("Account already exists: " + acc.getId());
  accounts_.emplace(acc.getId(), std::move(acc));
}

void UTTBlockchainApp::setLastKnownBlockId(BlockId id) { lastKnownBlockId_ = std::max<int>(lastKnownBlockId_, id); }

BlockId UTTBlockchainApp::getLastKnownBlockId() const { return lastKnownBlockId_; }

const Block* UTTBlockchainApp::getBlockById(BlockId id) const {
  if (id >= blocks_.size()) return nullptr;
  return &blocks_[id];
}

void UTTBlockchainApp::appendBlock(Block&& bl) {
  const BlockId nextBlockId = blocks_.size();
  bl.id_ = nextBlockId;
  blocks_.emplace_back(std::move(bl));
  lastKnownBlockId_ = std::max<int>(lastKnownBlockId_, nextBlockId);
}

std::optional<BlockId> UTTBlockchainApp::executeBlocks() {
  for (int i = lastExecutedBlockId_ + 1; i < (int)blocks_.size(); ++i)
    if (blocks_[i].tx_) executeTx(*blocks_[i].tx_);

  lastExecutedBlockId_ = blocks_.size() - 1;

  return lastExecutedBlockId_ < lastKnownBlockId_ ? std::optional<int>(lastExecutedBlockId_ + 1) : std::nullopt;
}

const std::map<std::string, Account>& UTTBlockchainApp::GetAccounts() const { return accounts_; }
std::map<std::string, Account>& UTTBlockchainApp::GetAccounts() { return accounts_; }

const std::vector<Block>& UTTBlockchainApp::GetBlocks() const { return blocks_; }

const Account* UTTBlockchainApp::getAccountById(const std::string& id) const {
  auto it = accounts_.find(id);
  return it != accounts_.end() ? &(it->second) : nullptr;
}

Account* UTTBlockchainApp::getAccountById(const std::string& id) {
  auto it = accounts_.find(id);
  return it != accounts_.end() ? &(it->second) : nullptr;
}

void UTTBlockchainApp::addNullifier(std::string nullifier) { nullset_.emplace(std::move(nullifier)); }

bool UTTBlockchainApp::hasNullifier(const std::string& nullifier) const { return nullset_.count(nullifier) > 0; }

void UTTBlockchainApp::executeTx(const Tx& tx) {
  struct Visitor {
    UTTBlockchainApp& state_;

    Visitor(UTTBlockchainApp& state) : state_{state} {}

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

    void operator()(const TxUtt& tx) {
      // Add nullifiers
      const auto& txNullifiers = tx.utt_.getNullifiers();
      for (const auto& n : txNullifiers) {
        state_.addNullifier(n);
      }
    }
  };

  std::visit(Visitor{*this}, tx);
}
