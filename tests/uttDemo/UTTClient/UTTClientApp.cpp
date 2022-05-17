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

#include "UTTClientApp.hpp"

#include <fstream>

#include <utt/Client.h>

/////////////////////////////////////////////////////////////////////////////////////////////////////
UTTClientApp::UTTClientApp(logging::Logger& logger, uint16_t walletId) : logger_{logger} {
  if (walletId == 0) throw std::runtime_error("wallet id must be a positive value!");

  const std::string fileName = "config/utt_wallet_" + std::to_string(walletId);
  std::ifstream ifs(fileName);
  if (!ifs.is_open()) throw std::runtime_error("Missing config: " + fileName);

  UTTClientConfig cfg;
  ifs >> cfg;

  myPid_ = cfg.wallet_.getUserPid();
  if (myPid_.empty()) throw std::runtime_error("Empty wallet pid!");

  for (auto& pid : cfg.pids_) {
    if (pid == myPid_) continue;
    otherPids_.emplace(std::move(pid));
  }
  if (otherPids_.empty()) throw std::runtime_error("Other pids are empty!");

  LOG_INFO(logger_, "Successfully loaded UTT wallet with pid '" << myPid_);

  addAccount(Account{myPid_, cfg.initPublicBalance_});
  wallet_ = std::move(cfg.wallet_);
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
const std::string& UTTClientApp::getMyPid() const { return myPid_; }

/////////////////////////////////////////////////////////////////////////////////////////////////////
const Account& UTTClientApp::getMyAccount() const { return *getAccountById(myPid_); }

/////////////////////////////////////////////////////////////////////////////////////////////////////
const libutt::Wallet& UTTClientApp::getMyUttWallet() const { return wallet_; }

/////////////////////////////////////////////////////////////////////////////////////////////////////
const std::set<std::string>& UTTClientApp::getOtherPids() const { return otherPids_; }

/////////////////////////////////////////////////////////////////////////////////////////////////////
size_t UTTClientApp::getUttBalance() const {
  size_t balance = 0;
  for (const auto& c : wallet_.coins) balance += c.getValue();
  return balance;
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
size_t UTTClientApp::getUttBudget() const { return wallet_.budgetCoin ? wallet_.budgetCoin->getValue() : 0; }

/////////////////////////////////////////////////////////////////////////////////////////////////////
void UTTClientApp::executeTx(const Tx& tx) {
  UTTBlockchainApp::executeTx(tx);  // Common logic for tx execution

  // Client removes spent coins and attempts to claim output coins
  if (const auto* txUtt = std::get_if<TxUtt>(&tx)) {
    std::cout << "Applying UTT tx " << txUtt->utt_.getHashHex() << '\n';
    pruneSpentCoins();
    tryClaimCoins(*txUtt);
    std::cout << '\n';
  }
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
void UTTClientApp::pruneSpentCoins() {
  auto result = libutt::Client::pruneSpentCoins(wallet_, nullset_);

  for (const size_t value : result.spentCoins_)
    std::cout << " - \'" << wallet_.getUserPid() << "' removes spent " << fmtCurrency(value) << " normal coin.\n";

  if (result.spentBudgetCoin_)
    std::cout << " - \'" << wallet_.getUserPid() << "' removes spent " << fmtCurrency(*result.spentBudgetCoin_)
              << " budget coin.\n";
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
void UTTClientApp::tryClaimCoins(const TxUtt& tx) {
  // Add any new coins
  const size_t n = 4;  // [TODO-UTT] Get from config
  if (!tx.sigShares_) throw std::runtime_error("Missing sigShares in utt tx!");
  const auto& sigShares = *tx.sigShares_;

  size_t numTxo = tx.utt_.outs.size();
  if (numTxo != sigShares.sigShares_.size())
    throw std::runtime_error("Number of output coins differs from provided sig shares!");

  for (size_t i = 0; i < numTxo; ++i) {
    auto result = libutt::Client::tryClaimCoin(wallet_, tx.utt_, i, sigShares.sigShares_[i], sigShares.signerIds_, n);
    if (result) {
      std::cout << " + \'" << myPid_ << "' claims " << fmtCurrency(result->value_)
                << (result->isBudgetCoin_ ? " budget" : " normal") << " coin.\n";
    }
  }
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
std::string UTTClientApp::extractToken(std::stringstream& ss) {
  std::string token;
  getline(ss, token, '/');
  return token;
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
void UTTClientApp::printState(const std::string& selector) const {
  if (selector.empty()) {
    const auto& myAccount = getMyAccount();
    std::cout << "Account summary:\n";
    std::cout << "  Public balance:\t" << fmtCurrency(myAccount.getPublicBalance()) << '\n';
    std::cout << "  UTT wallet balance:\t" << fmtCurrency(getUttBalance()) << '\n';
    std::cout << "  UTT wallet coins:\t[";
    if (!getMyUttWallet().coins.empty()) {
      for (int i = 0; i < (int)getMyUttWallet().coins.size() - 1; ++i)
        std::cout << fmtCurrency(getMyUttWallet().coins[i].getValue()) << ", ";
      std::cout << fmtCurrency(getMyUttWallet().coins.back().getValue());
    }
    std::cout << "]\n";
    std::cout << "  Anonymous budget:\t" << fmtCurrency(getUttBudget()) << '\n';
    return;
  }

  std::stringstream ss(selector);
  std::string token = extractToken(ss);
  if (token == "accounts") {
    printOtherPids();
  } else if (token == "ledger") {
    printLedger(ss);
  } else {
    throw std::domain_error("Unexpected token in selector '" + token + "'");
  }
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
void UTTClientApp::printOtherPids() const {
  std::cout << "My account: " << getMyPid() << '\n';
  for (const auto& pid : getOtherPids()) {
    std::cout << pid << '\n';
  }
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
void UTTClientApp::printLedger(std::stringstream& ss) const {
  auto token = extractToken(ss);
  if (token.empty()) {
    for (const auto& block : GetBlocks()) {
      std::cout << block << '\n';
    }
  } else {
    int blockId = std::atoi(token.c_str());
    if (blockId >= 0) printBlock(static_cast<BlockId>(blockId), ss);
    // Index from the back, example: -1 references the last block
    else
      printBlock(static_cast<BlockId>(blocks_.size() + blockId), ss);
  }
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
void UTTClientApp::printBlock(BlockId blockId, std::stringstream& ss) const {
  const auto* block = getBlockById(blockId);
  if (!block) {
    std::cout << "The block does not exist yet.\n";
    return;
  }

  std::cout << "Block " << block->id_ << " details:\n";
  if (block->id_ == 0) std::cout << "  Genesis block.\n";
  if (block->tx_) {
    if (const auto* txUtt = std::get_if<TxUtt>(&(*block->tx_))) {
      printUttTx(txUtt->utt_, ss);
    } else {
      std::cout << "  Public transaction: " << *block->tx_ << '\n';
    }
  } else {
    std::cout << "  Empty.\n";
  }
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
void UTTClientApp::printUttTx(const libutt::Tx& tx, std::stringstream& ss) const {
  auto coinTypeToStr = [](const libutt::Fr& type) -> const char* {
    if (type == libutt::Coin::NormalType()) return "normal";
    if (type == libutt::Coin::BudgetType()) return "budget";
    return "INVALID coin type!\n";
  };

  std::cout << "  ========================== UnTracable Transaction (UTT) ==========================\n";
  std::cout << "  hash: " << tx.getHashHex() << '\n';
  std::cout << "  isSplitOwnCoins: " << std::boolalpha << tx.isSplitOwnCoins << std::noboolalpha << "\n";
  std::cout << "  rcm: " << tx.rcm.toString() << '\n';
  std::cout << "  regsig: <...>\n";
  std::cout << "  budgetProof: " << (tx.budget_pi ? "\n" : "<Empty>\n");
  if (tx.budget_pi) {
    std::cout << "    forMeTxos: {";
    for (const auto& txo : tx.budget_pi->forMeTxos) std::cout << txo << ", ";
    std::cout << "}\n";
    std::cout << "    alpha: [";
    for (const auto& fr : tx.budget_pi->alpha) std::cout << fr.as_ulong() << ", ";
    std::cout << "]\n";
    std::cout << "    beta: [";
    for (const auto& fr : tx.budget_pi->alpha) std::cout << fr.as_ulong() << ", ";
    std::cout << "]\n";
    std::cout << "    e: " << tx.budget_pi->e.as_ulong() << '\n';
  }

  std::cout << "  ------------------------------- Input Tx(s) -------------------------------\n";
  for (size_t i = 0; i < tx.ins.size(); ++i) {
    const auto& txi = tx.ins[i];
    std::cout << "  TxIn " << i << ":\n";
    std::cout << "    coin_type: " << txi.coin_type.as_ulong() << " (" << coinTypeToStr(txi.coin_type) << ")\n";
    std::cout << "    exp_date: " << txi.exp_date.as_ulong() << '\n';
    std::cout << "    nullifier: " << txi.null.toUniqueString() << '\n';
    std::cout << "    vcm: " << txi.vcm.toString() << '\n';
    std::cout << "    ccm: " << txi.ccm.toString() << '\n';
    std::cout << "    coinsig: <...>\n";
    std::cout << "    split proof: <...>\n";
  }

  std::cout << "  ------------------------------- Output Tx(s) -------------------------------\n";
  for (size_t i = 0; i < tx.outs.size(); ++i) {
    const auto& txo = tx.outs[i];
    std::cout << "  TxOut " << i << ":\n";
    std::cout << "    coin_type: " << txo.coin_type.as_ulong() << " (" << coinTypeToStr(txo.coin_type) << ")\n";
    std::cout << "    exp_date: " << txo.exp_date.as_ulong() << '\n';
    // std::cout << "    H: " << (txo.H ? "..." : "<Empty>") << '\n';
    std::cout << "    vcm_1: " << txo.vcm_1.toString() << '\n';
    std::cout << "    range proof: " << (txo.range_pi ? "..." : "<Empty>") << '\n';
    // std::cout << "    d: " << txo.d.as_ulong() << '\n';
    std::cout << "    vcm_2: " << txo.vcm_2.toString() << '\n';
    std::cout << "    vcm_eq_pi: <...>\n";
    // std::cout << "    t: " << txo.t.as_ulong() << '\n';
    std::cout << "    icm: " << txo.icm.toString() << '\n';
    std::cout << "    icm_pok: " << (txo.icm_pok ? "<...>" : "<Empty>") << '\n';
    std::cout << "    ctxt: <...>\n";
  }
}