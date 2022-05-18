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
#define THROW_UNEXPECTED_PRINT_TOKEN() throw std::domain_error("Unexpected token in selector: " + token)

/////////////////////////////////////////////////////////////////////////////////////////////////////
struct UTTClientApp::PrintContext {
  PrintContext() = default;
  PrintContext(const PrintContext& parent, size_t idx) : PrintContext(parent, std::to_string(idx)) {}
  PrintContext(const PrintContext& parent, std::string name)
      : indent_{parent.indent_ + 2}, path_{parent.path_ + "/" + name} {}

  void printCurrentPath() const { std::cout << std::setw(indent_) << '[' << path_ << "]\n"; }
  void printComment(const char* comment) const { std::cout << std::setw(indent_) << "# " << comment << '\n'; }
  void printKey(const char* key) const { std::cout << std::setw(indent_) << key << ": <...>\n"; }

  template <typename T>
  void printValue(const T& value) const {
    std::cout << std::setw(indent_) << value << '\n';
  }

  template <typename T>
  void printKeyValue(const char* key, const T& value) const {
    std::cout << std::setw(indent_) << key << ':' << value << '\n';
  }

  size_t indent_ = 0;
  std::string path_;
};

/////////////////////////////////////////////////////////////////////////////////////////////////////
template <>
void UTTClientApp::PrintContext::printKeyValue(const char* key, const std::vector<std::string>& v) const {
  std::cout << std::setw(indent_) << key << ": [";
  if (!v.empty()) {
    for (int i = 0; i < (int)v.size() - 1; ++i) std::cout << v[i] << ", ";
    std::cout << v.back();
  }
  std::cout << "]\n";
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
namespace {
auto coinTypeToStr = [](const libutt::Fr& type) -> const char* {
  if (type == libutt::Coin::NormalType()) return "NORMAL";
  if (type == libutt::Coin::BudgetType()) return "BUDGET";
  return "INVALID coin type!\n";
};
}

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
  PrintContext ctx;

  if (selector.empty()) {
    const auto& myAccount = getMyAccount();
    ctx.printComment("Account summary");
    ctx.printKeyValue("Public balance", fmtCurrency(myAccount.getPublicBalance()));
    ctx.printKeyValue("UTT wallet balance", fmtCurrency(getUttBalance()));

    std::vector<std::string> coinValues;
    for (int i = 0; i < (int)getMyUttWallet().coins.size() - 1; ++i)
      coinValues.emplace_back(fmtCurrency(getMyUttWallet().coins[i].getValue()));
    ctx.printKeyValue("UTT wallet coins", coinValues);

    ctx.printKeyValue("Anonymous budget", fmtCurrency(getUttBudget()));
  } else {
    std::stringstream ss(selector);
    std::string token = extractToken(ss);

    if (token == "accounts") {
      printOtherPids(PrintContext(ctx, token), ss);
    } else if (token == "ledger") {
      printLedger(PrintContext(ctx, token), ss);
    } else if (token == "wallet") {
      printWallet(PrintContext(ctx, token), ss);
    } else {
      THROW_UNEXPECTED_PRINT_TOKEN();
    }
  }
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
void UTTClientApp::printOtherPids(const PrintContext& ctx, std::stringstream& ss) const {
  ctx.printComment("Accounts");
  ctx.printKeyValue("My account", getMyPid());
  for (const auto& pid : getOtherPids()) {
    ctx.printValue(pid);
  }
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
void UTTClientApp::printWallet(const PrintContext& ctx, std::stringstream& ss) const {
  auto token = extractToken(ss);
  if (token.empty()) {
    ctx.printComment("UTT Wallet");
    ctx.printKeyValue("pid", wallet_.getUserPid());
    printCoins(PrintContext(ctx, "coins"), ss);
  } else if (token == "coins") {
    printCoins(PrintContext(ctx, "coins"), ss);
  } else {
    THROW_UNEXPECTED_PRINT_TOKEN();
  }
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
void UTTClientApp::printCoins(const PrintContext& ctx, std::stringstream& ss) const {
  auto token = extractToken(ss);
  if (token.empty()) {
    ctx.printCurrentPath();
    ctx.printComment("Normal Coins");
    for (size_t i = 0; i < wallet_.coins.size(); ++i) printCoin(PrintContext(ctx, i), ss, wallet_.coins[i]);
    ctx.printComment("Budget Coin");
    if (wallet_.budgetCoin) printCoin(PrintContext(ctx, 0), ss, *wallet_.budgetCoin);

  } else {
    int idx = std::atoi(token.c_str());
    size_t coinIdx = static_cast<size_t>(idx < 0 ? wallet_.coins.size() + idx : idx);
    if (coinIdx >= wallet_.coins.size()) throw std::domain_error("The coin does not exist!");
    printCoin(PrintContext(ctx, coinIdx), ss, wallet_.coins.at(coinIdx), true /*verbose*/);
  }
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
void UTTClientApp::printCoin(const PrintContext& ctx,
                             std::stringstream& ss,
                             const libutt::Coin& coin,
                             bool verbose) const {
  if (!verbose) {
    std::cout << "<type:" << coinTypeToStr(coin.type) << " value:" << coin.getValue() << " ...>\n";
  } else {
    ctx.printComment("Coin commitment key needed for re-randomization\n");
    ctx.printKey("ck");

    ctx.printComment("Owner's PID hash");
    ctx.printKeyValue("pid_hash", coin.pid_hash.as_ulong());

    ctx.printComment("Serial Number");
    ctx.printKeyValue("sn", coin.sn.as_ulong());

    ctx.printComment("Denomination");
    ctx.printKeyValue("val", coin.val.as_ulong());

    // // TODO: turn CoinType and ExpirationDate into their own class with an toFr() method, or risk getting burned!
    // Fr type;      // either Coin::NormalType() or Coin::BudgetType()
    // Fr exp_date;  // expiration date; TODO: Fow now, using 32/64-bit UNIX time, since we never check expiration in
    // the
    //               // current prototype

    // Fr r;  // randomness used to commit to the coin

    // RandSig sig;  // signature on coin commitment from bank

    // //
    // // NOTE: We pre-compute these when we create/claim a coin, to make spending it faster!
    // //
    // Fr t;            // randomness for nullifier proof
    // Nullifier null;  // nullifier for this coin, pre-computed for convenience

    // Comm vcm;  // value commitment for this coin, pre-computed for convenience
    // Fr z;      // value commitment randomness
  }
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
void UTTClientApp::printLedger(const PrintContext& ctx, std::stringstream& ss) const {
  // PRINT_PUSH();
  // PRINT_CURRENT_PATH();
  // auto token = extractToken(ss);
  // if (token.empty()) {
  //   for (const auto& block : GetBlocks()) {
  //     std::cout << block << '\n';
  //   }
  // } else {
  //   int idx = std::atoi(token.c_str());
  //   BlockId blockId = static_cast<BlockId>(idx < 0 ? blocks_.size() + idx : idx);
  //   printBlock(blockId, path, ss);
  // }
  // PRINT_POP();
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
void UTTClientApp::printBlock(const PrintContext& ctx, std::stringstream& ss, BlockId blockId) const {
  // const auto* block = getBlockById(blockId);
  // if (!block) {
  //   std::cout << "The block does not exist yet.\n";
  //   return;
  // }

  // PRINT_PUSH();
  // if (block->id_ == 0) {
  //   std::cout << "This is the genesis block.\n";
  // }
  // if (block->tx_) {
  //   if (const auto* txUtt = std::get_if<TxUtt>(&(*block->tx_))) {
  //     printUttTx(txUtt->utt_, path, ss);
  //   } else {
  //     std::cout << "# Public transaction\n";
  //     std::cout << *block->tx_ << '\n';
  //   }
  // } else {
  //   std::cout << "No transaction.\n";
  // }
  // PRINT_POP();
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
void UTTClientApp::printUttTx(const PrintContext& ctx, std::stringstream& ss, const libutt::Tx& tx) const {
  // PRINT_COMMENT("UnTraceable Transaction (UTT)");
  // std::cout << "hash: " << tx.getHashHex() << '\n';
  // std::cout << "isSplitOwnCoins: " << std::boolalpha << tx.isSplitOwnCoins << std::noboolalpha << "\n";
  // std::cout << "rcm: " << tx.rcm.toString() << '\n';
  // std::cout << "regsig: <...>\n";
  // std::cout << "budgetProof: " << (tx.budget_pi ? "\n" : "<Empty>\n");
  // if (tx.budget_pi) {
  //   std::cout << "    forMeTxos: {";
  //   for (const auto& txo : tx.budget_pi->forMeTxos) std::cout << txo << ", ";
  //   std::cout << "}\n";
  //   std::cout << "    alpha: [";
  //   for (const auto& fr : tx.budget_pi->alpha) std::cout << fr.as_ulong() << ", ";
  //   std::cout << "]\n";
  //   std::cout << "    beta: [";
  //   for (const auto& fr : tx.budget_pi->alpha) std::cout << fr.as_ulong() << ", ";
  //   std::cout << "]\n";
  //   std::cout << "    e: " << tx.budget_pi->e.as_ulong() << '\n';
  // }

  // std::cout << "# Input transactions\n";
  // for (size_t i = 0; i < tx.ins.size(); ++i) {
  //   const auto& txi = tx.ins[i];
  //   std::cout << "  /" << i << ":\n";
  //   std::cout << "    coin_type: " << txi.coin_type.as_ulong() << " (" << coinTypeToStr(txi.coin_type) << ")\n";
  //   // std::cout << "    exp_date: " << txi.exp_date.as_ulong() << '\n';
  //   std::cout << "...\n";
  //   // std::cout << "    nullifier: " << txi.null.toUniqueString() << '\n';
  //   // std::cout << "    vcm: " << txi.vcm.toString() << '\n';
  //   // std::cout << "    ccm: " << txi.ccm.toString() << '\n';
  //   // std::cout << "    coinsig: <...>\n";
  //   // std::cout << "    split proof: <...>\n";
  // }

  // std::cout << "# Output transactions\n";
  // for (size_t i = 0; i < tx.outs.size(); ++i) {
  //   const auto& txo = tx.outs[i];
  //   std::cout << "  /" << i << ":\n";
  //   std::cout << "    coin_type: " << txo.coin_type.as_ulong() << " (" << coinTypeToStr(txo.coin_type) << ")\n";
  //   // std::cout << "    exp_date: " << txo.exp_date.as_ulong() << '\n';
  //   // std::cout << "    H: " << (txo.H ? "..." : "<Empty>") << '\n';
  //   // std::cout << "    vcm_1: " << txo.vcm_1.toString() << '\n';
  //   // std::cout << "    range proof: " << (txo.range_pi ? "..." : "<Empty>") << '\n';
  //   // std::cout << "    d: " << txo.d.as_ulong() << '\n';
  //   // std::cout << "    vcm_2: " << txo.vcm_2.toString() << '\n';
  //   // std::cout << "    vcm_eq_pi: <...>\n";
  //   // std::cout << "    t: " << txo.t.as_ulong() << '\n';
  //   // std::cout << "    icm: " << txo.icm.toString() << '\n';
  //   // std::cout << "    icm_pok: " << (txo.icm_pok ? "<...>" : "<Empty>") << '\n';
  //   // std::cout << "    ctxt: <...>\n";
  //   std::cout << "...\n";
  // }
}