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

#include "UTTDataViewer.hpp"

#include <memory>
#include <fstream>
#include <exception>
#include <regex>

#include <assertUtils.hpp>

#include <utt/Client.h>

/////////////////////////////////////////////////////////////////////////////////////////////////////
using DataViewPtr = std::unique_ptr<struct DataView>;
struct DataView {
  static constexpr const char* const s_PrefixComment = "# ";
  static constexpr const char* const s_PrefixList = "- ";
  static constexpr const char* const s_InfixKV = ": ";

  const DataView* prev_ = nullptr;
  std::string key_ = "undefined";
  mutable std::map<std::string, DataViewPtr> subViews_;

  DataView(const DataView* prev, std::string key) : prev_{prev}, key_{std::move(key)} {}
  virtual ~DataView() = default;
  DataView(DataView&& other) = default;
  DataView& operator=(DataView&& other) = default;

  const DataView* next(const std::string& key) const {
    if (subViews_.find(key) == subViews_.end()) {
      auto subView = makeSubView(key);
      if (!subView) subView = makeSubView(std::atoi(key.c_str()));
      if (!subView) throw UnexpectedPathTokenError(key);
      subViews_.emplace(key, std::move(subView));
    }
    return subViews_.at(key).get();
  }

  virtual void print() const { throw UTTDataViewerError("Visualization not implemented!"); }
  virtual DataViewPtr makeSubView(const std::string& key) const { return nullptr; }
  virtual DataViewPtr makeSubView(size_t idx) const { return nullptr; }

  static void list(const std::vector<std::string>& v, const std::string& delim = ", ") {
    if (!v.empty()) {
      for (int i = 0; i < (int)v.size() - 1; ++i) std::cout << v[i] << delim;
      std::cout << v.back();
    }
  }

  void title(const char* title) const {
    std::cout << "----------------------\n" << title << "\n----------------------\n";
  }

  void comment(const char* comment) const { std::cout << s_PrefixComment << comment << '\n'; }

  void key(const std::string& key) const { std::cout << s_PrefixList << key << s_InfixKV << "<...>\n"; }

  template <typename T>
  void keyValue(const std::string& key, const T& value) const {
    std::cout << s_PrefixList << key << s_InfixKV << value << '\n';
  }
};

/////////////////////////////////////////////////////////////////////////////////////////////////////
template <>
void DataView::keyValue(const std::string& key, const std::vector<std::string>& v) const {
  std::cout << " - " << key << ": [";
  list(v);
  std::cout << "]\n";
}

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
  ss << "type:" << coinTypeToStr(coin.type) << ", val:" << coin.val.as_ulong() << ", sn:" << coin.sn.as_ulong();
  ss << " ...>";
  return ss.str();
}

std::string preview(const libutt::Tx& tx) {
  std::stringstream ss;
  ss << "<UTT Tx: " << tx.getHashHex() << " ...>";
  return ss.str();
}

std::string preview(const Block& block) {
  if (block.tx_) {
    std::stringstream ss;
    ss << "<"
       << "id:" << block.id_ << ", ";
    if (const auto* txUtt = std::get_if<TxUtt>(&(*block.tx_))) {
      ss << "UTT Tx: " << txUtt->utt_.getHashHex();
    } else if (const auto* txMint = std::get_if<TxMint>(&(*block.tx_))) {
      ss << "Mint Tx: " << txMint->op_.getHashHex();
    } else if (const auto* txBurn = std::get_if<TxBurn>(&(*block.tx_))) {
      ss << "Burn Tx: " << txBurn->op_.getHashHex();
    } else {
      ss << *block.tx_;
    }
    ss << " ...>";
    return ss.str();
  } else {
    return "<Empty>";
  }
}

std::string preview(const libutt::TxIn& txi) {
  std::stringstream ss;
  ss << "<";
  ss << "type:" << coinTypeToStr(txi.coin_type);
  ss << " ...>";
  return ss.str();
}

std::string preview(const libutt::TxOut& txo) {
  std::stringstream ss;
  ss << "<";
  ss << "type:" << coinTypeToStr(txo.coin_type);
  ss << " ...>";
  return ss.str();
}
}  // namespace

/////////////////////////////////////////////////////////////////////////////////////////////////////
struct ParamsView : DataView {
  const libutt::Params& params_;

  ParamsView(const DataView* prev, std::string key, const libutt::Params& params)
      : DataView(prev, std::move(key)), params_{params} {}

  void print() const override {
    title("UTT Parameters");

    comment("Coin commitment key");
    key("ck_coin");

    comment("Registration commitment key");
    key("ck_reg");

    comment("Value commitment key");
    key("ck_val");

    comment("PRF public params");
    key("null");

    comment("IBE public params");
    key("ibe");

    comment("RangeProof params");
    key("rpp");

    comment("The number of sub-messages that compose a coin, which we commit to via Pedersen:");
    comment("(pid, sn, v, type, expdate) -> 5");
    keyValue("NumMessages", 5);
  }
};

/////////////////////////////////////////////////////////////////////////////////////////////////////
struct AddrSKView : DataView {
  const libutt::AddrSK& addrSK_;

  AddrSKView(const DataView* prev, std::string key, const libutt::AddrSK& addrSK)
      : DataView(prev, std::move(key)), addrSK_{addrSK} {}

  void print() const override {
    title("Address");

    comment("Public identifier of the user, e.g. alice@wonderland.com");
    keyValue("pid", addrSK_.pid);

    comment("The public identifier as hash of the pid string into a field element");
    keyValue("pid_hash", addrSK_.pid_hash);

    comment("PRF secret key");
    keyValue("s", addrSK_.s);

    comment("Encryption secret key");
    key("e");

    comment("Registration commitment to (pid, s) with randomness 0 (always re-randomized during a TXN)");
    key("rcm");

    comment("Registration signature on rcm");
    key("rs");
  }
};

/////////////////////////////////////////////////////////////////////////////////////////////////////
struct CoinView : DataView {
  const libutt::Coin& coin_;

  CoinView(const DataView* prev, std::string key, const libutt::Coin& coin)
      : DataView(prev, std::move(key)), coin_{coin} {}

  void print() const override {
    title("UTT Coin");

    comment("Coin commitment key needed for re-randomization");
    key("ck");

    comment("Owner's PID hash");
    keyValue("pid_hash", coin_.pid_hash);

    comment("Serial Number");
    keyValue("sn", coin_.sn);

    comment("Denomination");
    keyValue("val", coin_.val);

    comment("Coin type");
    keyValue("type", coin_.type);

    comment("Expiration time (UNIX time)");
    keyValue("exp_date", coin_.exp_date);

    comment("Randomness used to commit to the coin");
    keyValue("r", coin_.r);

    comment("Signature on coin commitment from the bank");
    key("sig");

    comment("Randomness for nullifier proof");
    keyValue("t", coin_.t);

    comment("Nullifier for this coin, pre-computed for convenience");
    keyValue("null", coin_.null.toUniqueString());

    comment("Value commitment for this coin, pre-computed for convenience");
    key("vcm");

    comment("Value commitment randomness");
    keyValue("z", coin_.z);

    comment("Partial coin commitment to (pid, sn, val) with randomness r");
    key("ccm_txn");
  }
};

/////////////////////////////////////////////////////////////////////////////////////////////////////
struct NormalCoinsView : DataView {
  const std::vector<libutt::Coin>& coins_;

  NormalCoinsView(const DataView* prev, std::string key, const std::vector<libutt::Coin>& coins)
      : DataView(prev, std::move(key)), coins_{coins} {}

  void print() const override { throw UTTDataViewerError("Expected valid normal coin index!"); }

  DataViewPtr makeSubView(size_t idx) const override {
    if (idx >= coins_.size()) throw IndexOutOfBoundsError(key_);
    return std::make_unique<CoinView>(this, std::to_string(idx), coins_[idx]);
  }
};

/////////////////////////////////////////////////////////////////////////////////////////////////////
struct WalletView : DataView {
  const libutt::Wallet& wallet_;

  WalletView(const DataView* prev, std::string key, const libutt::Wallet& wallet)
      : DataView(prev, std::move(key)), wallet_{wallet} {}

  void print() const override {
    title("UTT Wallet");

    comment("Parameters for UTT");
    key("p");

    comment("Address");
    key("ask");
    key("rpk");
    key("bpk");

    // Coins
    comment("Normal Coins");
    key("coins");
    // push("coins");
    if (wallet_.coins.empty()) comment("No coins");
    for (size_t i = 0; i < wallet_.coins.size(); ++i) {
      keyValue(std::to_string(i), preview(wallet_.coins[i]));
    }
    // pop();

    // Budget Coin
    comment("Budget Coin");
    if (wallet_.budgetCoin)
      keyValue("budgetCoin", preview(*wallet_.budgetCoin));
    else
      keyValue("budgetCoin", "<Empty>");
  }

  DataViewPtr makeSubView(const std::string& key) const override {
    if (key == "coins") return std::make_unique<NormalCoinsView>(this, "coins", wallet_.coins);
    if (key == "budgetCoin") {
      if (!wallet_.budgetCoin) throw UTTDataViewerError("Missing budget coin!");
      return std::make_unique<CoinView>(this, "budgetCoin", *wallet_.budgetCoin);
    }
    if (key == "p") return std::make_unique<ParamsView>(this, "p", wallet_.p);
    if (key == "ask") return std::make_unique<AddrSKView>(this, "ask", wallet_.ask);
    return nullptr;
  }
};

/////////////////////////////////////////////////////////////////////////////////////////////////////
struct UttTxInView : DataView {
  const libutt::TxIn& txi_;
  UttTxInView(const DataView* prev, std::string key, const libutt::TxIn& txi)
      : DataView(prev, std::move(key)), txi_{txi} {}

  void print() const override {
    title("UTT Tx Input");

    keyValue("coin_type", coinTypeToStr(txi_.coin_type));
    keyValue("exp_date", txi_.exp_date.as_ulong());
    keyValue("nullifier", txi_.null.toUniqueString());

    comment("Commitment to coin's value");
    key("vcm");

    comment("Commitment to coin (except coin type and expiration date)");
    key("ccm");

    comment("Signature on the coin commitment 'ccm'");
    key("coinsig");

    comment("Proof that 'ccm' was correctly split into 'null' and 'vcm'");
    key("pi");
  }
};

/////////////////////////////////////////////////////////////////////////////////////////////////////
struct UttTxOutView : DataView {
  const libutt::TxOut& txo_;
  UttTxOutView(const DataView* prev, std::string key, const libutt::TxOut& txo)
      : DataView(prev, std::move(key)), txo_{txo} {}

  void print() const override {
    title("UTT Tx Output");

    keyValue("coin_type", coinTypeToStr(txo_.coin_type));
    keyValue("exp_date", txo_.exp_date.as_ulong());
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
};

/////////////////////////////////////////////////////////////////////////////////////////////////////
struct UttTxInsView : DataView {
  const std::vector<libutt::TxIn>& ins_;
  UttTxInsView(const DataView* prev, std::string key, const std::vector<libutt::TxIn>& ins)
      : DataView(prev, std::move(key)), ins_{ins} {}

  DataViewPtr makeSubView(size_t idx) const override {
    if (idx >= ins_.size()) throw IndexOutOfBoundsError("UTT input transactions");
    return std::make_unique<UttTxInView>(this, std::to_string(idx), ins_[idx]);
  }
};

/////////////////////////////////////////////////////////////////////////////////////////////////////
struct UttTxOutsView : DataView {
  const std::vector<libutt::TxOut>& outs_;
  UttTxOutsView(const DataView* prev, std::string key, const std::vector<libutt::TxOut>& outs)
      : DataView(prev, std::move(key)), outs_{outs} {}

  DataViewPtr makeSubView(size_t idx) const override {
    if (idx >= outs_.size()) throw IndexOutOfBoundsError("UTT output transactions");
    return std::make_unique<UttTxOutView>(this, std::to_string(idx), outs_[idx]);
  }
};

/////////////////////////////////////////////////////////////////////////////////////////////////////
struct UttTxView : DataView {
  const libutt::Tx& tx_;

  UttTxView(const DataView* prev, std::string key, const libutt::Tx& tx) : DataView(prev, std::move(key)), tx_{tx} {}

  void print() const override {
    title("UnTraceable Transaction");

    keyValue("hash", tx_.getHashHex());
    keyValue("isSplitOwnCoins", tx_.isSplitOwnCoins);
    comment("Commitment to sending user's registration");
    keyValue("rcm", tx_.rcm.toString());
    comment("Signature on the registration commitment 'rcm'");
    key("regsig");

    comment("Proof that (1) output budget coin has same pid as input coins and (2) that");
    comment("this might be a \"self-payment\" TXN, so budget should be saved");
    key("budget_pi");

    comment("Input transactions");
    key("ins");
    // push("ins");
    for (size_t i = 0; i < tx_.ins.size(); ++i) {
      keyValue(std::to_string(i), preview(tx_.ins[i]));
    }
    // pop();

    comment("Output transactions");
    key("outs");
    // push("outs");
    for (size_t i = 0; i < tx_.outs.size(); ++i) {
      keyValue(std::to_string(i), preview(tx_.outs[i]));
    }
    // pop();
  }

  DataViewPtr makeSubView(const std::string& key) const override {
    if (key == "ins") return std::make_unique<UttTxInsView>(this, "ins", tx_.ins);
    if (key == "outs") return std::make_unique<UttTxOutsView>(this, "outs", tx_.outs);
    return nullptr;
  }
};

/////////////////////////////////////////////////////////////////////////////////////////////////////
struct BlockView : DataView {
  const Block& block_;

  BlockView(const DataView* prev, std::string key, const Block& block)
      : DataView(prev, std::move(key)), block_{block} {}

  void print() const override {
    title("Block");

    if (block_.id_ == 0) comment("Genesis block");
    keyValue("id", block_.id_);
    if (block_.tx_) {
      if (const auto* txUtt = std::get_if<TxUtt>(&(*block_.tx_))) {
        keyValue("utt", preview(txUtt->utt_));
        if (txUtt->sigShares_)
          key("sigShares");
        else
          keyValue("sigShares", "<Empty>");
      } else {
        comment("Public transaction");
        std::cout << *block_.tx_ << '\n';
      }
    } else {
      std::cout << "No transaction.\n";
    }
  }

  DataViewPtr makeSubView(const std::string& key) const override {
    if (key == "utt") {
      const auto* txUtt = std::get_if<TxUtt>(&(*block_.tx_));
      if (!txUtt) throw UTTDataViewerError("Expected a utt tx in block!");
      return std::make_unique<UttTxView>(this, "utt", txUtt->utt_);
    }
    return nullptr;
  }
};

/////////////////////////////////////////////////////////////////////////////////////////////////////
struct LedgerView : DataView {
  static constexpr const char* const s_key = "ledger";

  const std::vector<Block>& blocks_;

  LedgerView(const DataView* prev, std::string key, const std::vector<Block>& blocks)
      : DataView(prev, std::move(key)), blocks_{blocks} {}

  void print() const override {
    title("Ledger");
    for (size_t i = 0; i < blocks_.size(); ++i) keyValue(std::to_string(i), preview(blocks_[i]));
  }

  DataViewPtr makeSubView(size_t idx) const override {
    if (idx >= blocks_.size()) throw IndexOutOfBoundsError("Missing block");
    return std::make_unique<BlockView>(this, std::to_string(idx), blocks_[idx]);
  }
};

/////////////////////////////////////////////////////////////////////////////////////////////////////
struct RootView : DataView {
  const UTTClientApp& app_;

  RootView(const UTTClientApp& app) : DataView(nullptr, ""), app_{app} {}

  void print() const override {
    title("UTT Application");
    key("wallet");
    key("ledger");
  }

  DataViewPtr makeSubView(const std::string& key) const override {
    if (key == "wallet") return std::make_unique<WalletView>(this, "wallet", app_.getMyUttWallet());
    if (key == "ledger") return std::make_unique<LedgerView>(this, "ledger", app_.getBlocks());
    return nullptr;
  }
};

/////////////////////////////////////////////////////////////////////////////////////////////////////
UTTDataViewer::UTTDataViewer(const UTTClientApp& app) : root_{new RootView(app)}, current_{root_.get()} {}

/////////////////////////////////////////////////////////////////////////////////////////////////////
UTTDataViewer::~UTTDataViewer() = default;
UTTDataViewer::UTTDataViewer(UTTDataViewer&& other) = default;
UTTDataViewer& UTTDataViewer::operator=(UTTDataViewer&& other) = default;

/////////////////////////////////////////////////////////////////////////////////////////////////////
std::string UTTDataViewer::getCurrentPath() const {
  if (!current_) throw std::runtime_error("UTTDataViewer: current view is empty!");

  std::vector<std::string> keys;
  const DataView* view = current_;
  while (view) {
    keys.emplace_back(view->key_);
    view = view->prev_;
  }

  std::stringstream path;
  for (int i = keys.size() - 1; i > 0; --i) {
    path << keys[i] << '/';
  }
  path << keys[0];
  return path.str();
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
void UTTDataViewer::handleCommand(const std::string& cmd) {
  const DataView* newView = current_;
  if (cmd == "~") {  // Go to the root
    newView = root_.get();
  } else if (!cmd.empty()) {  // Interpret a relative path
    std::stringstream ss(cmd);
    std::string token;
    while (getline(ss, token, '/')) {
      if (token == "..") {
        if (newView == root_.get()) throw UTTDataViewerError("You cannot go back.");
        newView = newView->prev_;  // Go back
      } else if (!token.empty()) {
        newView = newView->next(token);
      }
    }
  }
  if (!newView) throw std::runtime_error("UTTDataViewer produced an empty view!");
  current_ = newView;
  current_->print();
}