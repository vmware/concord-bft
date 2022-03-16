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

void Account::depositPublic(int val) { publicBalance_ += val; }

int Account::withdrawPublic(int val) {
  val = std::min<int>(publicBalance_, val);
  publicBalance_ -= val;
  return val;
}

std::ostream& operator<<(std::ostream& os, const Block& b) {
  os << "Block " << b.id_ << "\n";
  os << "---------------------------\n";
  for (const auto& kvp : b.tx_) os << kvp.first << " : " << kvp.second << '\n';
  // To-Do: print nullifiers?
  return os;
}

AppState::AppState() {
  accounts_.emplace_back("A");
  accounts_.emplace_back("B");
  accounts_.emplace_back("C");
  blocks_.emplace_back();  // Genesis block
}

bool AppState::validateTx(const Tx& tx) const { return true; }

void AppState::executeTx(const Tx& tx) {}

void AppState::printLedger(std::optional<int> from, std::optional<int> to) const {
  int first = from ? *from : 0;
  int last = to ? *to + 1 : blocks_.size();

  first = std::clamp<int>(first, 0, blocks_.size());
  last = std::clamp<int>(last, 0, blocks_.size());

  if (first > last) std::swap(first, last);

  for (int i = first; i < last; ++i) std::cout << blocks_[i] << '\n';
}
