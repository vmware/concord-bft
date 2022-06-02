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

#include "PrintState.hpp"

#include <memory>
#include <fstream>
#include <exception>
#include <regex>

#include <assertUtils.hpp>

#include <utt/Client.h>

/////////////////////////////////////////////////////////////////////////////////////////////////////
using StateViewPtr = std::unique_ptr<struct StateView>;
struct StateView {
  const StateView* prev_ = nullptr;
  std::map<std::string, StateViewPtr> subViews_;

  StateView(const StateView* prev) : prev_{prev} {}
  virtual ~StateView() = default;
  StateView(StateView&& other) = default;
  StateView& operator=(StateView&& other) = default;

  const StateView* next(size_t idx) {
    std::string name = std::to_string(idx);
    if (subViews_.find(name) == subViews_.end()) {
      auto subView = makeSubView(idx);
      if (!subView) throw PrintContextError("Cannot be indexed!");
      subViews_.emplace(name, std::move(subView));
    }
    return subViews_.at(name).get();
  }

  const StateView* next(const std::string& name) {
    if (subViews_.find(name) == subViews_.end()) {
      auto subView = makeSubView(name);
      if (!subView) throw UnexpectedPathTokenError(name);
      subViews_.emplace(name, std::move(subView));
    }
    return subViews_.at(name).get();
  }

  virtual void print() const { throw PrintContextError("Visualization not implemented!"); }
  virtual StateViewPtr makeSubView(const std::string& name) { return nullptr; }
  virtual StateViewPtr makeSubView(size_t idx) { return nullptr; }
};

/////////////////////////////////////////////////////////////////////////////////////////////////////
struct ParamsView : StateView {
  const libutt::Params& params_;

  ParamsView(const StateView* prev, const libutt::Params& params) : StateView(prev), params_{params} {}

  void print() const override {
    // printComment("Coin commitment key");
    // printKeyValue("ck_coin");

    // printComment("Registration commitment key");
    // printKeyValue("ck_reg");

    // printComment("Value commitment key");
    // printKeyValue("ck_val");

    // printComment("PRF public params");
    // printKeyValue("null");

    // printComment("IBE public params");
    // printKeyValue("ibe");

    // // // range proof public parameters
    // // RangeProof::Params rpp;
    // printComment("RangeProof params");
    // printKeyValue("rpp");

    // printComment("The number of sub-messages that compose a coin, which we commit to via Pedersen:");
    // printComment("(pid, sn, v, type, expdate) -> 5");
    // printKeyValue("NumMessages", 5);
  }
};

/////////////////////////////////////////////////////////////////////////////////////////////////////
struct AddrSKView : StateView {
  const libutt::AddrSK& addrSK_;

  AddrSKView(const StateView* prev, const libutt::AddrSK& addrSK) : StateView(prev), addrSK_{addrSK} {}

  void print() const override {
    // printComment("Public identifier of the user, e.g. alice@wonderland.com");
    // printKeyValue("pid", ask.pid);

    // printComment("The public identifier as hash of the pid string into a field element");
    // printKeyValue("pid_hash", ask.pid_hash);

    // printComment("PRF secret key");
    // printKeyValue("s", ask.s);

    // printComment("Encryption secret key");
    // printKey("e");

    // printComment("Registration commitment to (pid, s) with randomness 0 (always re-randomized during a TXN)");
    // printKey("rcm");

    // printComment("Registration signature on rcm");
    // printKey("rs");
  }
};

/////////////////////////////////////////////////////////////////////////////////////////////////////
struct CoinView : StateView {
  const libutt::Coin& coin_;

  CoinView(const StateView* prev, const libutt::Coin& coin) : StateView(prev), coin_{coin} {}

  void print() const override {
    // printComment("Coin commitment key needed for re-randomization");
    // printLink("ck");

    // printComment("Owner's PID hash");
    // printKeyValue("pid_hash", coin.pid_hash);

    // printComment("Serial Number");
    // printKeyValue("sn", coin.sn);

    // printComment("Denomination");
    // printKeyValue("val", coin.val);

    // printComment("Coin type");
    // printKeyValue("type", coin.type);

    // printComment("Expiration time (UNIX time)");
    // printKeyValue("exp_date", coin.exp_date);

    // printComment("Randomness used to commit to the coin");
    // printKeyValue("r", coin.r);

    // printComment("Signature on coin commitment from the bank");
    // printLink("sig");

    // printComment("Randomness for nullifier proof");
    // printKeyValue("t", coin.t);

    // printComment("Nullifier for this coin, pre-computed for convenience");
    // printKeyValue("null", coin.null.toUniqueString());

    // printComment("Value commitment for this coin, pre-computed for convenience");
    // printLink("vcm");

    // printComment("Value commitment randomness");
    // printKeyValue("z", coin.z);

    // printComment("Partial coin commitment to (pid, sn, val) with randomness r");
    // printLink("ccm_txn");
  }
};

/////////////////////////////////////////////////////////////////////////////////////////////////////
struct NormalCoinsView : StateView {
  const std::vector<libutt::Coin>& coins_;

  NormalCoinsView(const StateView* prev, const std::vector<libutt::Coin>& coins) : StateView(prev), coins_{coins} {}

  void print() const override { throw PrintContextError("Expected valid normal coin index!"); }

  StateViewPtr makeSubView(size_t idx) override {
    if (idx >= coins_.size()) throw IndexOutOfBoundsError("coins");
    return std::make_unique<CoinView>(this, coins_[idx]);
  }
};

/////////////////////////////////////////////////////////////////////////////////////////////////////
struct WalletView : StateView {
  const libutt::Wallet& wallet_;

  WalletView(const StateView* prev, const libutt::Wallet& wallet) : StateView(prev), wallet_{wallet} {}

  void print() const override {
    // printComment("Parameters for UTT");
    // printLink("p");

    // printComment("Address");
    // printLink("ask");
    // printKeyValue("rpk");
    // printKeyValue("bpk");

    // // Coins
    // printComment("Normal Coins");
    // printKey("coins");
    // // push("coins");
    // if (wallet.coins.empty()) printComment("No coins");
    // for (size_t i = 0; i < wallet.coins.size(); ++i) {
    //   printLink(std::to_string(i), preview(wallet.coins[i]));
    // }
    // // pop();

    // // Budget Coin
    // printComment("Budget Coin");
    // if (wallet.budgetCoin)
    //   printLink("budgetCoin", preview(*wallet.budgetCoin));
    // else
    //   printKeyValue("budgetCoin", "<Empty>");
  }

  StateViewPtr makeSubView(const std::string& name) override {
    if (name == "coins") return std::make_unique<NormalCoinsView>(this, wallet_.coins);
    if (name == "budgetCoin") {
      if (!wallet_.budgetCoin) throw PrintContextError("Missing budget coin!");
      return std::make_unique<CoinView>(this, *wallet_.budgetCoin);
    }
    if (name == "p") return std::make_unique<ParamsView>(this, wallet_.p);
    if (name == "ask") return std::make_unique<AddrSKView>(this, wallet_.ask);
    return nullptr;
  }
};

/////////////////////////////////////////////////////////////////////////////////////////////////////
struct UttTxInView : StateView {
  const libutt::TxIn& txIn_;
  UttTxInView(const StateView* prev, const libutt::TxIn& txIn) : StateView(prev), txIn_{txIn} {}

  void print() const override {
    // printKeyValue("coin_type", coinTypeToStr(txi.coin_type));
    // printKeyValue("exp_date", txi.exp_date.as_ulong());
    // printKeyValue("nullifier", txi.null.toUniqueString());

    // printComment("Commitment to coin's value");
    // printKeyValue("vcm");

    // printComment("Commitment to coin (except coin type and expiration date)");
    // printKeyValue("ccm");

    // printComment("Signature on the coin commitment 'ccm'");
    // printKeyValue("coinsig");

    // printComment("Proof that 'ccm' was correctly split into 'null' and 'vcm'");
    // printKeyValue("pi");
  }
};

/////////////////////////////////////////////////////////////////////////////////////////////////////
struct UttTxOutView : StateView {
  const libutt::TxOut& txOut_;
  UttTxOutView(const StateView* prev, const libutt::TxOut& txOut) : StateView(prev), txOut_{txOut} {}

  void print() const override {
    // printKeyValue("coin_type", coinTypeToStr(txo.coin_type));
    // printKeyValue("exp_date", txo.exp_date.as_ulong());
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
struct UttTxInsView : StateView {
  const std::vector<libutt::TxIn>& ins_;
  UttTxInsView(const StateView* prev, const std::vector<libutt::TxIn>& ins) : StateView(prev), ins_{ins} {}

  StateViewPtr makeSubView(size_t idx) override {
    if (idx >= ins_.size()) throw IndexOutOfBoundsError("UTT input transactions");
    return std::make_unique<UttTxInView>(this, ins_[idx]);
  }
};

/////////////////////////////////////////////////////////////////////////////////////////////////////
struct UttTxOutsView : StateView {
  const std::vector<libutt::TxOut>& outs_;
  UttTxOutsView(const StateView* prev, const std::vector<libutt::TxOut>& outs) : StateView(prev), outs_{outs} {}

  StateViewPtr makeSubView(size_t idx) override {
    if (idx >= outs_.size()) throw IndexOutOfBoundsError("UTT output transactions");
    return std::make_unique<UttTxOutView>(this, outs_[idx]);
  }
};

/////////////////////////////////////////////////////////////////////////////////////////////////////
struct UttTxView : StateView {
  const libutt::Tx& tx_;

  UttTxView(const StateView* prev, const libutt::Tx& tx) : StateView(prev), tx_{tx} {}

  void print() const override {
    // printComment("UnTraceable Transaction");
    // printKeyValue("hash", tx.getHashHex());
    // printKeyValue("isSplitOwnCoins", tx.isSplitOwnCoins);
    // printComment("Commitment to sending user's registration");
    // printKeyValue("rcm", tx.rcm.toString());
    // printComment("Signature on the registration commitment 'rcm'");
    // printLink("regsig");

    // printComment("Proof that (1) output budget coin has same pid as input coins and (2) that");
    // printComment("this might be a \"self-payment\" TXN, so budget should be saved");
    // printLink("budget_pi");

    // printComment("Input transactions");
    // printKey("ins");
    // // push("ins");
    // for (size_t i = 0; i < tx.ins.size(); ++i) {
    //   printLink(std::to_string(i), preview(tx.ins[i]));
    // }
    // // pop();

    // printComment("Output transactions");
    // printKey("outs");
    // // push("outs");
    // for (size_t i = 0; i < tx.outs.size(); ++i) {
    //   printLink(std::to_string(i), preview(tx.outs[i]));
    // }
    // // pop();
  }

  StateViewPtr makeSubView(const std::string& name) override {
    if (name == "ins") return std::make_unique<UttTxInsView>(this, tx_.ins);
    if (name == "outs") return std::make_unique<UttTxOutsView>(this, tx_.outs);
    return nullptr;
  }
};

/////////////////////////////////////////////////////////////////////////////////////////////////////
struct BlockView : StateView {
  const Block& block_;

  BlockView(const StateView* prev, const Block& block) : StateView(prev), block_{block} {}

  void print() const override {
    // if (block.id_ == 0) printComment("Genesis block");
    // if (block.tx_) {
    //   if (const auto* txUtt = std::get_if<TxUtt>(&(*block.tx_))) {
    //     printUttTx(txUtt->utt_);
    //   } else {
    //     printComment("Public transaction");
    //     std::cout << *block.tx_ << '\n';
    //   }
    // } else {
    //   std::cout << "No transaction.\n";
    // }
  }
};

/////////////////////////////////////////////////////////////////////////////////////////////////////
struct LedgerView : StateView {
  const std::vector<Block>& blocks_;

  LedgerView(const StateView* prev, const std::vector<Block>& blocks) : StateView(prev), blocks_{blocks} {}

  void print() const override {
    // for (size_t i = 0; i < blocks_.size(); ++i)
    //   printLink(std::to_string(i), preview(blocks_[i]));
  }

  StateViewPtr makeSubView(size_t idx) override {
    if (idx >= blocks_.size()) throw IndexOutOfBoundsError("Missing block");
    return std::make_unique<BlockView>(this, blocks_[idx]);
  }
};

/////////////////////////////////////////////////////////////////////////////////////////////////////
namespace {
// auto coinTypeToStr = [](const libutt::Fr& type) -> const char* {
//   if (type == libutt::Coin::NormalType()) return "NORMAL";
//   if (type == libutt::Coin::BudgetType()) return "BUDGET";
//   return "INVALID coin type!\n";
// };

// std::string preview(const libutt::Coin& coin) {
//   std::stringstream ss;
//   ss << "<";
//   ss << "type:" << coinTypeToStr(coin.type) << ", val:" << coin.val.as_ulong() << ", sn:" << coin.sn.as_ulong();
//   ss << " ...>";
//   return ss.str();
// }

// std::string preview(const Block& block) {
//   if (block.tx_) {
//     std::stringstream ss;
//     ss << "<"
//        << "id:" << block.id_ << ", ";
//     if (const auto* txUtt = std::get_if<TxUtt>(&(*block.tx_))) {
//       ss << "UTT Tx: " << txUtt->utt_.getHashHex();
//     } else if (const auto* txMint = std::get_if<TxMint>(&(*block.tx_))) {
//       ss << "Mint Tx: " << txMint->op_.getHashHex();
//     } else if (const auto* txBurn = std::get_if<TxBurn>(&(*block.tx_))) {
//       ss << "Burn Tx: " << txBurn->op_.getHashHex();
//     } else {
//       ss << *block.tx_;
//     }
//     ss << " ...>";
//     return ss.str();
//   } else {
//     return "<Empty>";
//   }
// }

// std::string preview(const libutt::TxIn& txi) {
//   std::stringstream ss;
//   ss << "<";
//   ss << "type:" << coinTypeToStr(txi.coin_type);
//   ss << " ...>";
//   return ss.str();
// }

// std::string preview(const libutt::TxOut& txo) {
//   std::stringstream ss;
//   ss << "<";
//   ss << "type:" << coinTypeToStr(txo.coin_type);
//   ss << " ...>";
//   return ss.str();
// }
}  // namespace

/////////////////////////////////////////////////////////////////////////////////////////////////////
void PrintContext::handleCommand(const UTTClientApp& app, const std::vector<std::string>& cmd) {
  if (cmd.empty()) return;

  // if (cmd[0] == "show") {
  //   if (cmd.size() != 2) throw UnexpectedPathTokenError("Expected a path argument!");
  //   path_ = cmd[1];
  // } else {
  //   if (cmd.size() != 1) throw UnexpectedPathTokenError("Expected a single token!");
  //   if (cmd[1] == "..") {
  //     auto pos = path_.find_last_of('/');
  //     if (pos == std::string::npos) throw UnexpectedPathTokenError("Cannot go back!");
  //     path_ = path_.substr(0, pos);
  //   } else {
  //     path_ = path_ + "/" + cmd[1];
  //   }
  // }
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
std::optional<std::string> PrintContext::extractPathToken(std::stringstream& ss) {
  std::string token;
  getline(ss, token, '/');
  if (token.empty()) return std::nullopt;
  return token;
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
size_t PrintContext::getValidIdx(size_t size, const std::string& object, const std::string& tokenIdx) {
  if (!size) throw IndexEmptyObjectError(object);
  int offset = std::atoi(tokenIdx.c_str());
  size_t idx = static_cast<size_t>(offset < 0 ? size + offset : offset);
  if (idx >= size) throw IndexOutOfBoundsError(object);
  return idx;
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
template <>
void PrintContext::printKeyValue(const char* key, const std::vector<std::string>& v) const {
  std::cout << INDENT(getIndent()) << " - " << key << ": [";
  printList(v);
  std::cout << "]\n";
}