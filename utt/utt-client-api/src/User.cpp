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
#include <config.hpp>
#include <budget.hpp>
#include <serialization.hpp>

namespace utt::client {

struct Impl {
  std::string sk_;                     // User's secret key
  std::string pk_;                     // User's public key
  libutt::api::types::CurvePoint s1_;  // User's secret part of the PRF key
  libutt::api::UTTParams params_;
  std::unique_ptr<libutt::api::Client> client_;  // User's credentials

  uint64_t lastExecutedTxNum_ = 0;
  std::vector<libutt::api::Coin> coins_;         // User's unspent UTT coins (tokens)
  std::optional<libutt::api::Coin> budgetCoin_;  // User's current UTT budget coin (token)
};

std::unique_ptr<User> User::createInitial(const std::string& userId,
                                          const PublicConfig& config,
                                          IUserPKInfrastructure& pki,
                                          IUserStorage& storage) {
  (void)pki;
  (void)storage;
  if (userId.empty()) throw std::runtime_error("User id cannot be empty!");
  if (config.empty()) throw std::runtime_error("UTT instance public config cannot be empty!");

  // [TODO-UTT] Maybe we do something with pki and storage here before we try to create the user.
  // - Ask pki to create a new public/private key pair?
  // - Ask storage to allocate X amount of storage?
  // - Generate s1

  // [TODO-UTT] To create a user we need to successfully generate and persist to storage the user's secret key
  // and PRF secret s1.
  auto userKeys = pki.generateKeys(userId);
  auto uttConfig = libutt::api::deserialize<libutt::api::PublicConfig>(config);

  auto user = std::make_unique<User>();

  // Create a client object with an RSA based PKI
  user->pImpl_->client_.reset(new libutt::api::Client(
      userId, uttConfig.getCommitVerificationKey(), uttConfig.getRegistrationVerificationKey(), userKeys.sk_));

  return user;
}

std::unique_ptr<User> User::createFromStorage(IUserStorage& storage) {
  (void)storage;
  // [TODO-UTT] Implement User::createFromStorage
  return nullptr;
}

User::User() : pImpl_{new Impl{}} {}
User::~User() = default;

UserRegistrationInput User::getRegistrationInput() const {
  if (!pImpl_->client_) return UserRegistrationInput{};  // Empty
  return libutt::api::serialize<libutt::api::Commitment>(pImpl_->client_->generateInputRCM());
}

bool User::updateRegistration(const std::string& pk, const RegistrationSig& rs, const S2& s2) {
  if (!pImpl_->client_) return false;
  if (!(pImpl_->pk_ == pk)) return false;  // Expect a matching public key

  // [TODO-UTT] What if we already updated a registration? How do we check it?
  pImpl_->client_->setRCMSig(pImpl_->params_, s2, rs);

  return true;
}

bool User::updatePrivacyBudget(const PrivacyBudget& budget, const PrivacyBudgetSig& sig) {
  if (!pImpl_->client_) return false;

  auto claimedCoins = pImpl_->client_->claimCoins(libutt::api::deserialize<libutt::api::operations::Budget>(budget),
                                                  pImpl_->params_,
                                                  std::vector<libutt::api::types::Signature>{sig});

  // Expect a single budget token to be claimed by the user
  if (claimedCoins.size() != 1) return false;

  pImpl_->budgetCoin_ = claimedCoins[0];

  return true;
}

uint64_t User::getBalance() const {
  uint64_t sum = 0;
  for (const auto& coin : pImpl_->coins_) {
    sum += coin.getVal();
  }
  return sum;
}

uint64_t User::getPrivacyBudget() const { return pImpl_->budgetCoin_ ? pImpl_->budgetCoin_->getVal() : 0; }

const std::string& User::getUserId() const {
  static const std::string s_empty;
  return pImpl_->client_ ? pImpl_->client_->getPid() : s_empty;
}

const std::string& User::getPK() const { return pImpl_->pk_; }

uint64_t User::getLastExecutedTxNum() const { return pImpl_->lastExecutedTxNum_; }

bool User::updateTransferTx(uint64_t txNum, const TransferTx& tx, const TxOutputSigs& sigs) {
  if (txNum != pImpl_->lastExecutedTxNum_ + 1) return false;
  pImpl_->lastExecutedTxNum_ = txNum;

  // [TODO-UTT] Claim output coins
  (void)tx;
  (void)sigs;

  return false;
}

bool User::updateMintTx(uint64_t txNum, const MintTx& tx, const TxOutputSigs& sigs) {
  if (txNum != pImpl_->lastExecutedTxNum_ + 1) return false;
  pImpl_->lastExecutedTxNum_ = txNum;

  // [TODO-UTT] Claim output coins
  (void)tx;
  (void)sigs;

  return false;
}

bool User::updateBurnTx(uint64_t txNum, const MintTx& tx) {
  if (txNum != pImpl_->lastExecutedTxNum_ + 1) return false;
  pImpl_->lastExecutedTxNum_ = txNum;

  // [TODO-UTT] Slash burned coins
  (void)tx;

  return false;
}

bool User::updateNoOp(uint64_t txNum) {
  if (txNum != pImpl_->lastExecutedTxNum_ + 1) return false;
  pImpl_->lastExecutedTxNum_ = txNum;
  return true;
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