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
#include <exception>

#include <assertUtils.hpp>

#include <utt/Client.h>

#define INDENT(width) std::setw(width) << ' '

/////////////////////////////////////////////////////////////////////////////////////////////////////
namespace {
auto coinTypeToStr = [](const libutt::Fr& type) -> const char* {
  if (type == libutt::Coin::NormalType()) return "NORMAL";
  if (type == libutt::Coin::BudgetType()) return "BUDGET";
  return "INVALID coin type!\n";
};

std::string preview(const libutt::Coin& coin) {
  std::stringstream ss;
  ss << "<";
  ss << "type: " << coinTypeToStr(coin.type) << ", val:" << coin.val.as_ulong() << ", sn:" << coin.sn.as_ulong();
  ss << " ...>";
  return ss.str();
}

std::string preview(const Block& block) {
  if (block.tx_) {
    std::stringstream ss;
    ss << "<"
       << "id:" << block.id_ << ", ";
    if (const auto* txUtt = std::get_if<TxUtt>(&(*block.tx_))) {
      ss << "UTT Tx: " << txUtt->utt_.getHashHex();
    } else {
      ss << *block.tx_;
    }
    ss << " ...>";
    return ss.str();
  } else {
    return "<Empty>";
  }
}
}  // namespace

/////////////////////////////////////////////////////////////////////////////////////////////////////
struct PrintContextError : std::runtime_error {
  PrintContextError(const std::string& msg) : std::runtime_error(msg) {}
};

struct UnexpectedPathTokenError : PrintContextError {
  UnexpectedPathTokenError(const std::string& token) : PrintContextError("Unexpected path token: " + token) {}
};

struct IndexEmptyObjectError : PrintContextError {
  IndexEmptyObjectError(const std::string& object) : PrintContextError("Indexed object is empty: " + object) {}
};

struct IndexOutOfBoundsError : PrintContextError {
  IndexOutOfBoundsError(const std::string& object) : PrintContextError("Index out of bounds for object: " + object) {}
};

/////////////////////////////////////////////////////////////////////////////////////////////////////
struct UTTClientApp::PrintContext {
  static void printList(const std::vector<std::string>& v, const std::string& delim = ", ") {
    if (!v.empty()) {
      for (int i = 0; i < (int)v.size() - 1; ++i) std::cout << v[i] << delim;
      std::cout << v.back();
    }
  }

  PrintContext() = default;

  PrintContext& push(std::string token, std::string trace = "") {
    path_.emplace_back(std::move(token));
    trace_.emplace_back(std::move(trace));
    return *this;
  }

  PrintContext& push(size_t idx, std::string trace = "") {
    path_.emplace_back(std::to_string(idx));
    trace_.emplace_back(std::move(trace));
    return *this;
  }

  PrintContext& pop() {
    path_.pop_back();
    trace_.pop_back();
    return *this;
  }

  size_t getIndent() const { return path_.size() * 2; }

  void printTrace() const {
    if (path_.size() != trace_.size()) throw std::runtime_error("Inconsistent path and trace sizes!");
    // Pick the trace element (if non-empty) otherwise use the element in path
    for (size_t i = 0; i < trace_.size(); ++i) {
      std::cout << INDENT(i * 2) << "-> " << (trace_[i].empty() ? path_[i] : trace_[i]) << '\n';
    }
  }

  void printComment(const char* comment) const { std::cout << INDENT(getIndent()) << "# " << comment << '\n'; }

  void printLink(const std::string& to, const std::string& preview = "<...>") const {
    std::cout << INDENT(getIndent()) << "- " << to << ": " << preview << " [";
    printList(path_, "/");
    std::cout << '/' << to << "]\n";
  }

  void printKey(const char* key) const { std::cout << INDENT(getIndent()) << " - " << key << ":\n"; }

  template <typename T>
  void printValue(const T& value) const {
    std::cout << INDENT(getIndent()) << "- " << value << '\n';
  }

  template <typename T>
  void printKeyValue(const char* key, const T& value) const {
    std::cout << INDENT(getIndent()) << "- " << key << ": " << value << '\n';
  }

  std::vector<std::string> path_;
  std::vector<std::string> trace_;
};

/////////////////////////////////////////////////////////////////////////////////////////////////////
template <>
void UTTClientApp::PrintContext::printKeyValue(const char* key, const std::vector<std::string>& v) const {
  std::cout << INDENT(getIndent()) << " - " << key << ": [";
  printList(v);
  std::cout << "]\n";
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
    std::cout << "\nApplying UTT tx " << txUtt->utt_.getHashHex() << '\n';
    pruneSpentCoins();
    tryClaimCoins(*txUtt);
    std::cout << '\n';
  } else if (const auto* txMint = std::get_if<TxMint>(&tx)) {  // Client claims minted coins
    if (txMint->pid_ == myPid_) {
      ConcordAssert(txMint->sigShares_.has_value());
      ConcordAssert(txMint->sigShares_->sigShares_.size() == 1);
      std::cout << "\nApplying Mint tx: " << txMint->op_.getHashHex() << '\n';
      auto coin = txMint->op_.claimCoin(wallet_.p,
                                        wallet_.ask,
                                        numReplicas_,
                                        txMint->sigShares_->sigShares_[0],
                                        txMint->sigShares_->signerIds_,
                                        wallet_.bpk);

      std::cout << " + '" << myPid_ << "' claims " << fmtCurrency(coin.getValue())
                << (coin.isBudget() ? " budget" : " normal") << " coin.\n";
      wallet_.addCoin(coin);
    }
  } else if (const auto* txBurn = std::get_if<TxBurn>(&tx)) {  // Client removes burned coins
    if (txBurn->op_.getOwnerPid() == myPid_) {
      std::cout << "\nApplying Burn tx: " << txBurn->op_.getHashHex() << '\n';
      pruneSpentCoins();
    }
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
  if (!tx.sigShares_) throw std::runtime_error("Missing sigShares in utt tx!");
  const auto& sigShares = *tx.sigShares_;

  size_t numTxo = tx.utt_.outs.size();
  if (numTxo != sigShares.sigShares_.size())
    throw std::runtime_error("Number of output coins differs from provided sig shares!");

  for (size_t i = 0; i < numTxo; ++i) {
    auto result =
        libutt::Client::tryClaimCoin(wallet_, tx.utt_, i, sigShares.sigShares_[i], sigShares.signerIds_, numReplicas_);
    if (result) {
      std::cout << " + \'" << myPid_ << "' claims " << fmtCurrency(result->value_)
                << (result->isBudgetCoin_ ? " budget" : " normal") << " coin.\n";
    }
  }
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
std::optional<std::string> UTTClientApp::extractPathToken(std::stringstream& ss) {
  std::string token;
  getline(ss, token, '/');
  if (token.empty()) return std::nullopt;
  return token;
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
size_t UTTClientApp::getValidIdx(size_t size, const std::string& object, const std::string& tokenIdx) {
  if (!size) throw IndexEmptyObjectError(object);
  int offset = std::atoi(tokenIdx.c_str());
  size_t idx = static_cast<size_t>(offset < 0 ? size + offset : offset);
  if (idx >= size) throw IndexOutOfBoundsError(object);
  return idx;
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
void UTTClientApp::printState(const std::string& path) const {
  PrintContext ctx;
  std::stringstream ss(path);

  try {
    if (auto next = extractPathToken(ss)) {
      ctx.push(*next);
      if (next == "accounts")
        printAccounts(ctx);
      else if (next == "ledger")
        printLedger(ctx, ss);
      else if (next == "wallet")
        printWallet(ctx, ss);
      else
        throw UnexpectedPathTokenError(*next);
    } else {
      ctx.push("balance");
      printBalance(ctx);
    }
  } catch (const PrintContextError& e) {
    std::cout << e.what() << '\n';
  }
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
void UTTClientApp::printBalance(PrintContext& ctx) const {
  ctx.printTrace();

  const auto& myAccount = getMyAccount();
  ctx.printComment("Account summary");
  ctx.printKeyValue("Public balance", fmtCurrency(myAccount.getPublicBalance()));
  ctx.printKeyValue("UTT wallet balance", fmtCurrency(getUttBalance()));

  std::vector<std::string> coinValues;
  for (int i = 0; i < (int)getMyUttWallet().coins.size() - 1; ++i)
    coinValues.emplace_back(fmtCurrency(getMyUttWallet().coins[i].getValue()));
  ctx.printKeyValue("UTT wallet coins", coinValues);

  ctx.printKeyValue("Anonymous budget", fmtCurrency(getUttBudget()));

  ctx.printComment("Links");
  ctx.printLink("accounts");
  ctx.printLink("ledger");
  ctx.printLink("wallet");
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
void UTTClientApp::printAccounts(PrintContext& ctx) const {
  ctx.printTrace();
  ctx.printKeyValue("My account", getMyPid());
  for (const auto& pid : getOtherPids()) {
    ctx.printValue(pid);
  }
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
void UTTClientApp::printWallet(PrintContext& ctx, std::stringstream& ss) const {
  if (auto token = extractPathToken(ss)) {
    if (token == "coins") {
      if (auto tokenIdx = extractPathToken(ss)) {
        auto idx = getValidIdx(wallet_.coins.size(), *token, *tokenIdx);
        ctx.push("coins").push(idx);
        printCoin(ctx, wallet_.coins[idx]);
      } else {
        throw PrintContextError("Expected a coin index");
      }
    } else {
      throw UnexpectedPathTokenError(*token);
    }
  } else {
    ctx.printTrace();
    ctx.printComment("UTT Wallet");
    ctx.printKeyValue("pid", wallet_.getUserPid());

    ctx.printComment("Normal Coins");
    ctx.printKey("coins");
    ctx.push("coins");
    if (wallet_.coins.empty()) ctx.printComment("No coins");
    for (size_t i = 0; i < wallet_.coins.size(); ++i) {
      ctx.printLink(std::to_string(i), preview(wallet_.coins[i]));
    }
    ctx.pop();  // coins

    ctx.printComment("Budget Coin");
    if (wallet_.budgetCoin)
      ctx.printLink("budgetCoin", preview(*wallet_.budgetCoin));
    else
      ctx.printKeyValue("budgetCoin", "<Empty>");
  }
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
void UTTClientApp::printCoin(PrintContext& ctx, const libutt::Coin& coin) const {
  ctx.printTrace();
  ctx.printComment("Coin commitment key needed for re-randomization\n");
  ctx.printLink("ck");
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

/////////////////////////////////////////////////////////////////////////////////////////////////////
void UTTClientApp::printLedger(PrintContext& ctx, std::stringstream& ss) const {
  if (auto tokenIdx = extractPathToken(ss)) {
    auto idx = getValidIdx(blocks_.size(), "ledger", *tokenIdx);
    ctx.push(idx, preview(blocks_[idx]));
    printBlock(ctx, blocks_[idx], ss);
  } else {
    for (size_t i = 0; i < blocks_.size(); ++i) ctx.printLink(std::to_string(i), preview(blocks_[i]));
  }
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
void UTTClientApp::printBlock(PrintContext& ctx, const Block& block, std::stringstream& ss) const {
  if (block.id_ == 0) ctx.printComment("Genesis block");
  if (block.tx_) {
    if (const auto* txUtt = std::get_if<TxUtt>(&(*block.tx_))) {
      printUttTx(ctx, txUtt->utt_, ss);
    } else {
      ctx.printTrace();
      ctx.printComment("Public transaction");
      std::cout << *block.tx_ << '\n';
    }
  } else {
    std::cout << "No transaction.\n";
  }
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
void UTTClientApp::printUttTx(PrintContext& ctx, const libutt::Tx& tx, std::stringstream& ss) const {
  if (auto token = extractPathToken(ss)) {
    if (token == "ins") {
      if (auto tokenIdx = extractPathToken(ss)) {
        size_t idx = getValidIdx(tx.ins.size(), *token, *tokenIdx);
        ctx.push("ins").push(idx);
        printTxIn(ctx, tx.ins[idx]);
      } else {
        throw PrintContextError("Expected input tx index");
      }
    } else if (token == "outs") {
      if (auto tokenIdx = extractPathToken(ss)) {
        size_t idx = getValidIdx(tx.ins.size(), *token, *tokenIdx);
        ctx.push("outs").push(idx);
        printTxOut(ctx, tx.outs[idx]);
      } else {
        throw PrintContextError("Expected output tx index");
      }
    } else
      throw UnexpectedPathTokenError(*token);
  } else {
    ctx.printTrace();
    ctx.printComment("UnTraceable Transaction");
    ctx.printKeyValue("hash", tx.getHashHex());
    ctx.printKeyValue("isSplitOwnCoins", tx.isSplitOwnCoins);
    ctx.printComment("Commitment to sending user's registration");
    ctx.printKeyValue("rcm", tx.rcm.toString());
    ctx.printComment("Signature on the registration commitment 'rcm'");
    ctx.printLink("regsig");

    ctx.printComment("Proof that (1) output budget coin has same pid as input coins and (2) that");
    ctx.printComment("this might be a \"self-payment\" TXN, so budget should be saved");
    ctx.printLink("budget_pi");

    ctx.printComment("Input transactions");
    ctx.printKey("ins");
    ctx.push("ins");
    for (size_t i = 0; i < tx.ins.size(); ++i) {
      ctx.printLink(std::to_string(i));
    }
    ctx.pop();

    ctx.printComment("Output transactions");
    ctx.printKey("outs");
    ctx.push("outs");
    for (size_t i = 0; i < tx.outs.size(); ++i) {
      ctx.printLink(std::to_string(i));
    }
    ctx.pop();
  }
}
/////////////////////////////////////////////////////////////////////////////////////////////////////
void UTTClientApp::printTxIn(PrintContext& ctx, const libutt::TxIn& txi) const {
  ctx.printTrace();
  ctx.printKeyValue("coin_type", coinTypeToStr(txi.coin_type));
  ctx.printKeyValue("exp_date", txi.exp_date.as_ulong());
  ctx.printKeyValue("nullifier", txi.null.toUniqueString());
  // std::cout << "    vcm: " << txi.vcm.toString() << '\n';
  // std::cout << "    ccm: " << txi.ccm.toString() << '\n';
  // std::cout << "    coinsig: <...>\n";
  // std::cout << "    split proof: <...>\n";
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
void UTTClientApp::printTxOut(PrintContext& ctx, const libutt::TxOut& txo) const {
  ctx.printTrace();
  ctx.printKeyValue("coin_type", coinTypeToStr(txo.coin_type));
  ctx.printKeyValue("exp_date", txo.exp_date.as_ulong());
  // std::cout << "    exp_date: " << txo.exp_date.as_ulong() << '\n';
  // std::cout << "    H: " << (txo.H ? "..." : "<Empty>") << '\n';
  // std::cout << "    vcm_1: " << txo.vcm_1.toString() << '\n';
  // std::cout << "    range proof: " << (txo.range_pi ? "..." : "<Empty>") << '\n';
  // std::cout << "    d: " << txo.d.as_ulong() << '\n';
  // std::cout << "    vcm_2: " << txo.vcm_2.toString() << '\n';
  // std::cout << "    vcm_eq_pi: <...>\n";
  // std::cout << "    t: " << txo.t.as_ulong() << '\n';
  // std::cout << "    icm: " << txo.icm.toString() << '\n';
  // std::cout << "    icm_pok: " << (txo.icm_pok ? "<...>" : "<Empty>") << '\n';
  // std::cout << "    ctxt: <...>\n";
}