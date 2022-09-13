// UTT Client API
//
// Copyright (c) 2020-2022 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the sub-component's license, as noted in the LICENSE
// file.

#include "UTTClientApi.hpp"

namespace utt::client {

std::vector<uint8_t> User::getRegistrationInput() const {
  // [TODO-UTT] User::getRegistrationInput
  return std::vector<uint8_t>{};
}

bool User::useRegistration(const std::string& pk, const std::vector<uint8_t>& rs, const std::vector<uint8_t>& s2) {
  // [TODO-UTT] User::useRegistration
  (void)pk;
  (void)rs;
  (void)s2;
  return false;
}

bool User::useBudgetCoin(const std::vector<uint8_t>& budgetCoin) {
  // [TODO-UTT] User::useBudgetCoin
  (void)budgetCoin;
  return false;
}

bool User::useBudgetCoinSig(const std::vector<uint8_t>& sig) {
  // [TODO-UTT] User::useBudgetCoinSig
  (void)sig;
  return false;
}

uint64_t User::getBalance() const {
  // [TODO-UTT] User::getBalance
  return 0;
}

uint64_t User::getBudget() const {
  // [TODO-UTT] User::getBudget
  return 0;
}

const std::string& User::getUserId() const {
  // [TODO-UTT] User::getUserId
  static const std::string& s_Undefined = "Undefined";
  return s_Undefined;
}

const std::string& User::getPK() const {
  // [TODO-UTT] User::getPK
  static const std::string& s_Undefined = "Undefined";
  return s_Undefined;
}

uint64_t User::getLastExecutedTxNum() const {
  // [TODO-UTT] User::getLastExecutedTxNum
  return 0;
}

bool User::update(uint64_t txNum, const Tx& tx, const std::vector<std::vector<uint8_t>>& sigs) {
  // [TODO-UTT] User::update
  (void)txNum;
  (void)tx;
  (void)sigs;
  return false;
}

void User::update(uint64_t txNum) {
  // [TODO-UTT] User::update no-op
  (void)txNum;
}

BurnResult User::burn(uint64_t amount) const {
  // [TODO-UTT] User::burn
  (void)amount;
  return BurnResult{};
}

TransferResult User::transfer(const std::string& userId, const std::string& destPK, uint64_t amount) const {
  // [TODO-UTT] User::transfer
  (void)userId;
  (void)destPK;
  (void)amount;
  return TransferResult{};
}

}  // namespace utt::client