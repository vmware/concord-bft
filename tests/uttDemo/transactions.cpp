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

#include "transactions.hpp"

#include <iostream>
#include <vector>
#include <sstream>

std::vector<uint8_t> StrToBytes(const std::string& str) { return std::vector<uint8_t>(str.begin(), str.end()); }
std::string BytesToStr(const std::vector<uint8_t>& bytes) { return std::string{bytes.begin(), bytes.end()}; }

std::ostream& operator<<(std::ostream& os, const TxPublicDeposit& tx) {
  os << "deposit " << tx.toAccountId_ << ' ' << tx.amount_;
  return os;
}

std::ostream& operator<<(std::ostream& os, const TxPublicWithdraw& tx) {
  os << "withdraw " << tx.toAccountId_ << ' ' << tx.amount_;
  return os;
}

std::ostream& operator<<(std::ostream& os, const TxPublicTransfer& tx) {
  os << "transfer " << tx.fromAccountId_ << ' ' << tx.toAccountId_ << ' ' << tx.amount_;
  return os;
}

std::ostream& operator<<(std::ostream& os, const TxUtt& tx) {
  os << "utt ";  // Notice the space after utt (this is expected by the parser)
  os << tx.utt_;
  return os;
}

std::ostream& operator<<(std::ostream& os, const TxMint& tx) {
  os << "mint ";  // Notice the space (this is expected by the parser)
  os << tx.pid_ << ' ';
  os << tx.mintSeqNum_ << ' ';
  os << tx.amount_ << ' ';
  os << tx.op_;
  return os;
}

std::ostream& operator<<(std::ostream& os, const TxBurn& tx) {
  os << "burn ";  // Notice the space (this is expected by the parser)
  os << tx.op_;
  return os;
}

std::ostream& operator<<(std::ostream& os, const Tx& tx) {
  std::visit([&os](const auto& tx) { os << tx; }, tx);
  return os;
}

std::optional<Tx> parseTx(const std::string& str) {
  std::stringstream ss(str);
  std::string token;

  std::getline(ss, token, ' ');  // getline extracts the delimiter
  if (token == "utt") {
    return TxUtt(libutt::Tx(ss));
  } else if (token == "mint") {
    // Extract the tokens pid, mintSeqNum, value and pass the rest to deserialize the mint op
    std::string pid;
    uint32_t mintSeqNum = 0;
    uint32_t amount = 0;
    ss >> pid >> mintSeqNum >> amount;
    ss.ignore(1, ' ');
    return TxMint(std::move(pid), mintSeqNum, amount, libutt::MintOp(ss));
  } else if (token == "burn") {
    return TxBurn(libutt::BurnOp(ss));
  } else {
    // Keep parsing for public tx
    std::vector<std::string> tokens;
    tokens.emplace_back(std::move(token));

    while (std::getline(ss, token, ' ')) tokens.emplace_back(std::move(token));

    if (tokens.size() == 3) {
      if (tokens[0] == "deposit")
        return TxPublicDeposit(std::move(tokens[1]), std::atoi(tokens[2].c_str()));
      else if (tokens[0] == "withdraw")
        return TxPublicWithdraw(std::move(tokens[1]), std::atoi(tokens[2].c_str()));
    } else if (tokens.size() == 4) {
      if (tokens[0] == "transfer")
        return TxPublicTransfer(std::move(tokens[1]), std::move(tokens[2]), std::atoi(tokens[3].c_str()));
    }
  }

  return std::nullopt;
}
