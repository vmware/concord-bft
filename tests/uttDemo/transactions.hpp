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

#pragma once

#include <set>
#include <string>
#include <variant>
#include <optional>
#include <vector>

#include <utt/RandSig.h>
#include <utt/Tx.h>

std::vector<uint8_t> StrToBytes(const std::string& str);
std::string BytesToStr(const std::vector<uint8_t>& bytes);

struct ReplicaSigShares {
  std::vector<size_t> signerIds_;
  std::vector<std::vector<libutt::RandSigShare>> sigShares_;  // Signiture shares for each output coin
};

struct TxPublicDeposit {
  TxPublicDeposit(std::string accId, int amount) : amount_{amount}, toAccountId_{std::move(accId)} {}

  int amount_ = 0;
  std::string toAccountId_;
};
std::ostream& operator<<(std::ostream& os, const TxPublicDeposit& tx);

struct TxPublicWithdraw {
  TxPublicWithdraw(std::string accId, int amount) : amount_{amount}, toAccountId_{std::move(accId)} {}

  int amount_ = 0;
  std::string toAccountId_;
};
std::ostream& operator<<(std::ostream& os, const TxPublicWithdraw& tx);

struct TxPublicTransfer {
  TxPublicTransfer(std::string fromAccId, std::string toAccId, int amount)
      : amount_{amount}, fromAccountId_{std::move(fromAccId)}, toAccountId_{std::move(toAccId)} {}

  int amount_ = 0;
  std::string fromAccountId_;
  std::string toAccountId_;
};
std::ostream& operator<<(std::ostream& os, const TxPublicTransfer& tx);

struct TxUtt {
  TxUtt(libutt::Tx&& utt) : utt_{std::move(utt)} {}
  libutt::Tx utt_;
  // [TODO-UTT] Added here for convenience for the client to fill in before execution
  // could be moved somewhere else
  std::optional<ReplicaSigShares> sigShares_;
};
std::ostream& operator<<(std::ostream& os, const TxUtt& tx);

using Tx = std::variant<TxPublicDeposit, TxPublicWithdraw, TxPublicTransfer, TxUtt>;

std::ostream& operator<<(std::ostream& os, const Tx& tx);

std::optional<Tx> parseTx(const std::string& str);