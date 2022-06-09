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
namespace {
auto coinTypeToStr = [](const libutt::Fr& type) -> const char* {
  if (type == libutt::Coin::NormalType()) return "NORMAL";
  if (type == libutt::Coin::BudgetType()) return "BUDGET";
  return "INVALID coin type!\n";
};

template <typename T>
std::string preview(const T& value) {
  return "<...>";
}

template <>
std::string preview(const libutt::Coin& coin) {
  std::stringstream ss;
  ss << "<";
  ss << "type:" << coinTypeToStr(coin.type) << ", val:" << coin.val.as_ulong() << ", sn:" << coin.sn.as_ulong();
  ss << " ...>";
  return ss.str();
}

template <>
std::string preview(const libutt::Tx& tx) {
  std::stringstream ss;
  ss << "<UTT Tx: " << tx.getHashHex() << " ...>";
  return ss.str();
}

template <>
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

template <>
std::string preview(const libutt::TxIn& txi) {
  std::stringstream ss;
  ss << "<type:" << coinTypeToStr(txi.coin_type) << " ..>";
  return ss.str();
}

template <>
std::string preview(const libutt::TxOut& txo) {
  std::stringstream ss;
  ss << "<type:" << coinTypeToStr(txo.coin_type) << " ..>";
  return ss.str();
}

template <>
std::string preview(const libutt::RandSigShare& sigShare) {
  std::stringstream ss;
  ss << "<s1:" << sigShare.s1 << ", s2:" << sigShare.s2 << '>';
  return ss.str();
}

}  // namespace

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
  void keyValue(const std::string& key, const T& value, size_t indent = 0) const {
    if (indent > 0) std::cout << std::setw(indent) << ' ';
    std::cout << s_PrefixList << key << s_InfixKV << value << '\n';
  }

  template <typename T>
  void keyValue(const std::string& key, const std::optional<T>& value, size_t indent = 0) const {
    if (indent > 0) std::cout << std::setw(indent) << ' ';
    if (value.has_value())
      keyValue(key, *value, indent);
    else
      std::cout << s_PrefixList << key << s_InfixKV << "<NotSet>\n";
  }

  template <typename T>
  void keyValue(const std::string& key, const std::vector<T>& v, size_t indent = 0) const {
    if (indent > 0) std::cout << std::setw(indent) << ' ';
    std::cout << s_PrefixList << key << s_InfixKV << '[';
    if (!v.empty()) {
      for (size_t i = 0; i < v.size() - 1; ++i) std::cout << v[i] << ", ";
      std::cout << v.back();
    }
    std::cout << "]\n";
  }

  template <typename T>
  void keyValuePreview(const std::string& key, const T& value, size_t indent = 0) const {
    if (indent > 0) std::cout << std::setw(indent) << ' ';
    std::cout << s_PrefixList << key << s_InfixKV << preview(value) << '\n';
  }

  template <typename T>
  void keyValuePreview(const std::string& key, const std::optional<T>& value, size_t indent = 0) const {
    if (indent > 0) std::cout << std::setw(indent) << ' ';
    if (value.has_value())
      keyValue(key, preview(*value), indent);
    else
      std::cout << s_PrefixList << key << s_InfixKV << "<NotSet>\n";
  }

  template <typename T>
  void keyValuePreview(const std::string& key, const std::vector<T>& v, size_t indent = 0) const {
    if (indent > 0) std::cout << std::setw(indent) << ' ';
    if (v.empty()) {
      std::cout << s_PrefixList << key << s_InfixKV << "[]\n";
      return;
    }
    std::cout << s_PrefixList << key << s_InfixKV << '\n';
    for (size_t i = 0; i < v.size(); ++i) keyValuePreview(std::to_string(i), v[i], indent + 2);
  }
};

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

  void print() const override {
    title("Coins");
    keyValuePreview("coins", coins_);
  }

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
    key("ask");
    key("rpk");
    key("bpk");

    comment("Normal Coins");
    keyValuePreview("coins", wallet_.coins);

    comment("Budget Coin");
    keyValuePreview("budgetCoin", wallet_.budgetCoin);
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

    comment("Commitment to coin value transferred to the recipient");
    keyValuePreview("vcm_1", txo_.vcm_1);

    comment("Range proof for the coin value in 'vcm_1'");
    keyValuePreview("range_pi", txo_.range_pi);

    comment("Randomness for vcm_2 (is *never* serialized!)");
    keyValue("d", txo_.d);

    comment("Re-commitment under ck_tx");
    keyValuePreview("vcm_2", txo_.vcm_2);

    comment("Proof that vcm_1 and vcm_2 commit to the same value");
    keyValuePreview("vcm_eq_pi", txo_.vcm_eq_pi);

    comment("Randomness for 'icm' (is *never* serialized!)");
    keyValue("t", txo_.t);

    comment("Commitment to identity of the recipient, under ck_tx");
    keyValuePreview("icm", txo_.icm);

    comment("ZKPoK of opening for icm");
    keyValuePreview("icm_pok", txo_.icm_pok);

    comment("Encryption of coin value and coin commitment randomness");
    keyValuePreview("ctxt", txo_.ctxt);
  }
};

/////////////////////////////////////////////////////////////////////////////////////////////////////
struct UttTxInsView : DataView {
  const std::vector<libutt::TxIn>& ins_;
  UttTxInsView(const DataView* prev, std::string key, const std::vector<libutt::TxIn>& ins)
      : DataView(prev, std::move(key)), ins_{ins} {}

  void print() const override {
    comment("UTT Tx Inputs");
    keyValuePreview("ins", ins_);
  }

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

  void print() const override {
    comment("UTT Tx Outputs");
    keyValuePreview("outs", outs_);
  }

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
    keyValuePreview("ins", tx_.ins);

    comment("Output transactions");
    keyValuePreview("outs", tx_.outs);
  }

  DataViewPtr makeSubView(const std::string& key) const override {
    if (key == "ins") return std::make_unique<UttTxInsView>(this, "ins", tx_.ins);
    if (key == "outs") return std::make_unique<UttTxOutsView>(this, "outs", tx_.outs);
    return nullptr;
  }
};

/////////////////////////////////////////////////////////////////////////////////////////////////////
struct UttSigSharesView : DataView {
  const UttSigShares& sigShares_;

  UttSigSharesView(const DataView* prev, std::string key, const UttSigShares& sigShares)
      : DataView(prev, key), sigShares_{sigShares} {}

  void print() const override {
    title("Utt Signature Shares");
    comment("Replica ids that provided the shares");
    keyValue("signerIds", sigShares_.signerIds_);
    comment("Signer shares per coin");
    keyValuePreview("signerShares", sigShares_.signerShares_);
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
    if (block_.tx_) {
      if (const auto* txUtt = std::get_if<TxUtt>(&(*block_.tx_))) {
        comment("UTT Txn Block");
        keyValue("id", block_.id_);
        keyValuePreview("utt", txUtt->utt_);
        keyValuePreview("sigShares", txUtt->sigShares_);
      } else if (const auto* txMint = std::get_if<TxMint>(&(*block_.tx_))) {
        comment("Mint Txn Block");
        keyValue("id", block_.id_);
        keyValue("pid", txMint->pid_);
        keyValue("mintSeqNum", txMint->mintSeqNum_);
        keyValue("amount", txMint->amount_);
        keyValuePreview("op", txMint->op_);
        keyValuePreview("sigShares", txMint->sigShares_);
      } else if (const auto* txBurn = std::get_if<TxBurn>(&(*block_.tx_))) {
        comment("Burn Txn Block");
        keyValuePreview("op", txBurn->op_);
      } else {
        comment("Public Txn Block");
        keyValue("id", block_.id_);
        keyValue("tx", *block_.tx_);
      }
    } else {
      keyValue("tx", "<Empty>");
    }
  }

  DataViewPtr makeSubView(const std::string& key) const override {
    if (key == "utt") {
      const auto* txUtt = std::get_if<TxUtt>(&(*block_.tx_));
      if (!txUtt) throw UTTDataViewerError("Expected a utt tx in block!");
      return std::make_unique<UttTxView>(this, "utt", txUtt->utt_);
    }
    if (key == "sigShares") {
      const UttSigShares* sigShares = nullptr;
      if (const auto* txUtt = std::get_if<TxUtt>(&(*block_.tx_)))
        sigShares = txUtt->sigShares_ ? &(*txUtt->sigShares_) : nullptr;
      else if (const auto* txMint = std::get_if<TxMint>(&(*block_.tx_)))
        sigShares = txMint->sigShares_ ? &(*txUtt->sigShares_) : nullptr;
      if (!sigShares) throw UTTDataViewerError("Expected non-empty sig shares!");
      return std::make_unique<UttSigSharesView>(this, "sigShares", *sigShares);
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
    for (size_t i = 0; i < blocks_.size(); ++i) keyValuePreview(std::to_string(i), blocks_[i]);
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
void UTTDataViewer::prompt() const {
  std::cout << "\nYou are navigating the application state (type '.h' for commands, '.q' to exit):\n";
  std::cout << "view " << getCurrentPath() << "> ";
}

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
  if (cmd == ".h") {
    // [TODO-UTT] View help
    std::cout << "NYI\n";
    return;
  }

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