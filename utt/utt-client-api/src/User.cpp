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

#include "utt-client-api/User.hpp"

// libutt
#include <utt/Params.h>
#include <utt/RegAuth.h>
#include <utt/RandSig.h>
#include <utt/Address.h>
#include <utt/Coin.h>

// libutt new interface
#include <UTTParams.hpp>
#include <coin.hpp>
#include <client.hpp>

namespace utt::client {

struct Impl {
  std::string sk_;                     // User's secret key
  std::string pk_;                     // User's public key
  libutt::api::types::CurvePoint s1_;  // User's secret part of the PRF key
  libutt::api::UTTParams params_;
  std::unique_ptr<libutt::api::Client> client_;  // User's credentials

  uint16_t lastExecutedTxNum_ = 0;
  std::vector<libutt::api::Coin> coins_;         // User's unspent UTT coins (tokens)
  std::optional<libutt::api::Coin> budgetCoin_;  // User's current UTT budget coin (token)
};

std::unique_ptr<User> User::createInitial(const std::string& userId,
                                          const PublicParams& params,
                                          IUserPKInfrastructure& pki,
                                          IUserStorage& storage) {
  (void)pki;
  (void)storage;
  if (userId.empty()) throw std::runtime_error("UserId cannot be empty!");
  if (params.empty()) throw std::runtime_error("UTT instance public params cannot be empty!");

  // [TODO-UTT] Maybe we do something with pki and storage here before we try to create the user.
  // - Ask pki to create a new public/private key pair?
  // - Ask storage to allocate X amount of storage?
  // - Generate s1

  // [TODO-UTT] To create a user we need to successfully generate and persist to storage the user's secret key
  // and PRF secret s1.

  // std::unique_ptr<User> user;

  // Create a client object with an RSA based PKI
  // user.pImpl_->client_ = libutt::api::Client(userId, )

  return std::make_unique<User>();
}

std::unique_ptr<User> User::createFromStorage(IUserStorage& storage) {
  (void)storage;
  return nullptr;
}

User::User() : pImpl_{new Impl{}} {}
User::~User() = default;

std::vector<uint8_t> User::getRegistrationInput() const {
  // [TODO-UTT] Implement User::getRegistrationInput
  return std::vector<uint8_t>{};
}

bool User::useRegistration(const std::string& pk, const std::vector<uint8_t>& rs, const std::vector<uint8_t>& s2) {
  // [TODO-UTT] Implement User::useRegistration
  (void)pk;
  (void)rs;
  (void)s2;
  return false;
}

bool User::useBudgetCoin(const std::vector<uint8_t>& budgetCoin) {
  // [TODO-UTT] Implement User::useBudgetCoin
  (void)budgetCoin;
  return false;
}

bool User::useBudgetCoinSig(const std::vector<uint8_t>& sig) {
  // [TODO-UTT] Implement User::useBudgetCoinSig
  (void)sig;
  return false;
}

uint64_t User::getBalance() const {
  // [TODO-UTT] Implement User::getBalance
  return 0;
}

uint64_t User::getPrivacyBudget() const {
  // [TODO-UTT] Implement User::getPrivacyBudget
  return 0;
}

const std::string& User::getUserId() const {
  static std::string s_userId = "undefined";
  return s_userId;
}

const std::string& User::getPK() const {
  // [TODO-UTT] Implement User::getPK
  static const std::string& s_Undefined = "Undefined";
  return s_Undefined;
}

uint64_t User::getLastExecutedTxNum() const {
  // [TODO-UTT] Implement User::getLastExecutedTxNum
  return 0;
}

bool User::update(uint64_t txNum, const Tx& tx, const std::vector<std::vector<uint8_t>>& sigs) {
  // [TODO-UTT] Implement User::update
  (void)txNum;
  (void)tx;
  (void)sigs;
  return false;
}

void User::update(uint64_t txNum) {
  // [TODO-UTT] Implement User::update no-op
  (void)txNum;
}

BurnResult User::burn(uint64_t amount) const {
  // [TODO-UTT] Implement User::burn
  (void)amount;
  return BurnResult{};
}

TransferResult User::transfer(const std::string& userId, const std::string& destPK, uint64_t amount) const {
  // [TODO-UTT] Implement User::transfer
  (void)userId;
  (void)destPK;
  (void)amount;
  return TransferResult{};
}

}  // namespace utt::client