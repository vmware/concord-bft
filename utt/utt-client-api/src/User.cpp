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
#include <utt/MintOp.h>
#include <utt/BurnOp.h>
#include <utt/DataUtils.hpp>

// libutt new interface
#include <UTTParams.hpp>
#include <coin.hpp>
#include <client.hpp>
#include <config.hpp>
#include <budget.hpp>
#include <mint.hpp>
#include <burn.hpp>
#include <transaction.hpp>
#include <serialization.hpp>

#include "utt-client-api/PickCoins.hpp"

namespace utt::client {

struct User::Impl {
  utt::Transaction createTx_Burn(const libutt::api::Coin& coin);

  utt::Transaction createTx_Self1t2(const libutt::api::Coin& coin, uint64_t amount);
  utt::Transaction createTx_Self2t1(const std::vector<libutt::api::Coin>& coins);
  utt::Transaction createTx_Self2t2(const std::vector<libutt::api::Coin>& coins, uint64_t amount);

  utt::Transaction createTx_1t1(const libutt::api::Coin& coin, const std::string& userId, const std::string& pk);
  utt::Transaction createTx_1t2(const libutt::api::Coin& coin,
                                uint64_t amount,
                                const std::string& userId,
                                const std::string& pk);
  utt::Transaction createTx_2t1(const std::vector<libutt::api::Coin>& coins,
                                const std::string& userId,
                                const std::string& pk);
  utt::Transaction createTx_2t2(const std::vector<libutt::api::Coin>& coins,
                                uint64_t amount,
                                const std::string& userId,
                                const std::string& pk);

  std::string sk_;                     // User's secret key
  std::string pk_;                     // User's public key
  libutt::api::types::CurvePoint s1_;  // User's secret part of the PRF key
  libutt::api::UTTParams params_;
  std::unique_ptr<libutt::api::Client> client_;  // User's credentials

  uint64_t lastExecutedTxNum_ = 0;
  std::vector<libutt::api::Coin> coins_;         // User's unspent UTT coins (tokens)
  std::optional<libutt::api::Coin> budgetCoin_;  // User's current UTT budget coin (token)
};

utt::Transaction User::Impl::createTx_Burn(const libutt::api::Coin& coin) {
  utt::Transaction tx;
  tx.type_ = utt::Transaction::Type::Burn;
  auto burn = libutt::api::operations::Burn(params_, *client_, coin);
  tx.data_ = libutt::api::serialize<libutt::api::operations::Burn>(burn);
  return tx;
}

utt::Transaction User::Impl::createTx_Self1t2(const libutt::api::Coin& coin, uint64_t amount) {
  std::vector<std::tuple<std::string, uint64_t>> recip;
  recip.emplace_back(client_->getPid(), amount);
  recip.emplace_back(client_->getPid(), coin.getVal() - amount);

  std::map<std::string, std::string> credentials;
  credentials.emplace(client_->getPid(), pk_);
  libutt::RSAEncryptor encryptor(credentials);

  auto uttTx = libutt::api::operations::Transaction(params_, *client_, {coin}, std::nullopt, recip, encryptor);

  utt::Transaction tx;
  tx.type_ = utt::Transaction::Type::Transfer;
  tx.data_ = libutt::api::serialize<libutt::api::operations::Transaction>(uttTx);
  return tx;
}

utt::Transaction User::Impl::createTx_Self2t1(const std::vector<libutt::api::Coin>& coins) {
  std::vector<std::tuple<std::string, uint64_t>> recip;
  recip.emplace_back(client_->getPid(), coins[0].getVal() + coins[1].getVal());

  std::map<std::string, std::string> credentials;
  credentials.emplace(client_->getPid(), pk_);
  libutt::RSAEncryptor encryptor(credentials);

  auto uttTx = libutt::api::operations::Transaction(params_, *client_, coins, std::nullopt, recip, encryptor);

  utt::Transaction tx;
  tx.type_ = utt::Transaction::Type::Transfer;
  tx.data_ = libutt::api::serialize<libutt::api::operations::Transaction>(uttTx);
  return tx;
}

utt::Transaction User::Impl::createTx_Self2t2(const std::vector<libutt::api::Coin>& coins, uint64_t amount) {
  std::vector<std::tuple<std::string, uint64_t>> recip;
  recip.emplace_back(client_->getPid(), amount);
  recip.emplace_back(client_->getPid(), (coins[0].getVal() + coins[1].getVal()) - amount);

  std::map<std::string, std::string> credentials;
  credentials.emplace(client_->getPid(), pk_);
  libutt::RSAEncryptor encryptor(credentials);

  auto uttTx = libutt::api::operations::Transaction(params_, *client_, coins, std::nullopt, recip, encryptor);

  utt::Transaction tx;
  tx.type_ = utt::Transaction::Type::Transfer;
  tx.data_ = libutt::api::serialize<libutt::api::operations::Transaction>(uttTx);
  return tx;
}

utt::Transaction User::Impl::createTx_1t1(const libutt::api::Coin& coin,
                                          const std::string& userId,
                                          const std::string& pk) {
  std::vector<std::tuple<std::string, uint64_t>> recip;
  recip.emplace_back(userId, coin.getVal());

  std::map<std::string, std::string> credentials;
  credentials.emplace(userId, pk);
  libutt::RSAEncryptor encryptor(credentials);

  auto uttTx = libutt::api::operations::Transaction(params_, *client_, {coin}, budgetCoin_, recip, encryptor);

  utt::Transaction tx;
  tx.type_ = utt::Transaction::Type::Transfer;
  tx.data_ = libutt::api::serialize<libutt::api::operations::Transaction>(uttTx);
  return tx;
}

utt::Transaction User::Impl::createTx_1t2(const libutt::api::Coin& coin,
                                          uint64_t amount,
                                          const std::string& userId,
                                          const std::string& pk) {
  std::vector<std::tuple<std::string, uint64_t>> recip;
  recip.emplace_back(userId, amount);
  recip.emplace_back(client_->getPid(), coin.getVal() - amount);

  std::map<std::string, std::string> credentials;
  credentials.emplace(userId, pk);
  credentials.emplace(client_->getPid(), pk_);
  libutt::RSAEncryptor encryptor(credentials);

  auto uttTx = libutt::api::operations::Transaction(params_, *client_, {coin}, budgetCoin_, recip, encryptor);

  utt::Transaction tx;
  tx.type_ = utt::Transaction::Type::Transfer;
  tx.data_ = libutt::api::serialize<libutt::api::operations::Transaction>(uttTx);
  return tx;
}

utt::Transaction User::Impl::createTx_2t1(const std::vector<libutt::api::Coin>& coins,
                                          const std::string& userId,
                                          const std::string& pk) {
  std::vector<std::tuple<std::string, uint64_t>> recip;
  recip.emplace_back(userId, coins[0].getVal() + coins[1].getVal());

  std::map<std::string, std::string> credentials;
  credentials.emplace(userId, pk);
  libutt::RSAEncryptor encryptor(credentials);

  auto uttTx = libutt::api::operations::Transaction(params_, *client_, coins, budgetCoin_, recip, encryptor);

  utt::Transaction tx;
  tx.type_ = utt::Transaction::Type::Transfer;
  tx.data_ = libutt::api::serialize<libutt::api::operations::Transaction>(uttTx);
  return tx;
}

utt::Transaction User::Impl::createTx_2t2(const std::vector<libutt::api::Coin>& coins,
                                          uint64_t amount,
                                          const std::string& userId,
                                          const std::string& pk) {
  std::vector<std::tuple<std::string, uint64_t>> recip;
  recip.emplace_back(userId, amount);
  recip.emplace_back(client_->getPid(), (coins[0].getVal() + coins[1].getVal()) - amount);

  std::map<std::string, std::string> credentials;
  credentials.emplace(userId, pk);
  credentials.emplace(client_->getPid(), pk_);
  libutt::RSAEncryptor encryptor(credentials);

  auto uttTx = libutt::api::operations::Transaction(params_, *client_, coins, budgetCoin_, recip, encryptor);

  utt::Transaction tx;
  tx.type_ = utt::Transaction::Type::Transfer;
  tx.data_ = libutt::api::serialize<libutt::api::operations::Transaction>(uttTx);
  return tx;
}

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
  user->pImpl_->params_ = uttConfig.getParams();
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
  if (rs.empty() || s2.empty()) return false;

  // [TODO-UTT] What if we already updated a registration? How do we check it?
  pImpl_->client_->setRCMSig(pImpl_->params_, s2, rs);

  // Un-blind the signature
  std::vector<libutt::api::types::CurvePoint> randomness = {libutt::Fr::zero().to_words(),
                                                            libutt::Fr::zero().to_words()};
  auto sig =
      libutt::api::Utils::unblindSignature(pImpl_->params_, libutt::api::Commitment::REGISTRATION, randomness, rs);
  if (sig.empty()) return false;

  pImpl_->client_->setRCMSig(pImpl_->params_, s2, sig);

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

bool User::updateTransferTx(uint64_t txNum, const Transaction& tx, const TxOutputSigs& sigs) {
  if (tx.type_ != Transaction::Type::Transfer) return false;
  if (txNum != pImpl_->lastExecutedTxNum_ + 1) return false;
  pImpl_->lastExecutedTxNum_ = txNum;

  // [TODO-UTT] Claim output coins
  (void)tx;
  (void)sigs;

  return false;
}

bool User::updateMintTx(uint64_t txNum, const Transaction& tx, const TxOutputSig& sig) {
  if (tx.type_ != Transaction::Type::Mint) return false;
  if (txNum != pImpl_->lastExecutedTxNum_ + 1) return false;
  pImpl_->lastExecutedTxNum_ = txNum;

  auto mint = libutt::api::deserialize<libutt::api::operations::Mint>(tx.data_);

  auto claimedCoins =
      pImpl_->client_->claimCoins(mint, pImpl_->params_, std::vector<libutt::api::types::Signature>{sig});

  // Expect a single token to be claimed by the user
  if (claimedCoins.size() != 1) return false;

  // [TODO-UTT] Requires atomic, durable write batch through IUserStorage
  pImpl_->lastExecutedTxNum_ = txNum;
  pImpl_->coins_.emplace_back(std::move(claimedCoins[0]));

  return true;
}

// [TODO-UTT] Do we actually need the whole BurnTx or we can simply use the nullifer to slash?
bool User::updateBurnTx(uint64_t txNum, const Transaction& tx) {
  if (tx.type_ != Transaction::Type::Burn) return false;
  if (txNum != pImpl_->lastExecutedTxNum_ + 1) return false;

  auto burn = libutt::api::deserialize<libutt::api::operations::Burn>(tx.data_);
  auto nullifier = burn.getNullifier();
  if (nullifier.empty()) return false;

  auto it = std::find_if(pImpl_->coins_.begin(), pImpl_->coins_.end(), [&nullifier](const libutt::api::Coin& coin) {
    return coin.getNullifier() == nullifier;
  });
  if (it == pImpl_->coins_.end()) return false;

  // [TODO-UTT] Requires atomic, durable write batch through IUserStorage
  pImpl_->lastExecutedTxNum_ = txNum;
  pImpl_->coins_.erase(it);

  return true;
}

bool User::updateNoOp(uint64_t txNum) {
  if (txNum != pImpl_->lastExecutedTxNum_ + 1) return false;
  pImpl_->lastExecutedTxNum_ = txNum;
  return true;
}

std::variant<utt::Transaction, Error> User::burn(uint64_t amount) const {
  try {
    if (!pImpl_->client_) throw std::runtime_error("User not initialized!");
    if (amount == 0) throw std::runtime_error("Burn amount must be positive!");

    const uint64_t balance = getBalance();
    if (balance < amount) throw std::runtime_error("User has insufficient balance!");

    auto pickedCoins = PickCoinsPreferExactMatch(pImpl_->coins_, amount);
    if (pickedCoins.empty()) throw std::runtime_error("Coin strategy didn't pick any coins!");

    if (pickedCoins.size() == 1) {
      const auto& coin = pImpl_->coins_.at(pickedCoins[0]);
      const uint64_t value = coin.getVal();
      if (value < amount) throw std::runtime_error("Coin strategy picked a single insufficient coin!");
      if (value == amount) return pImpl_->createTx_Burn(coin);
      return pImpl_->createTx_Self1t2(coin, amount);  // value > payment
    } else if (pickedCoins.size() == 2) {
      const std::vector<libutt::api::Coin> inputCoins = {pImpl_->coins_.at(pickedCoins[0]),
                                                         pImpl_->coins_.at(pickedCoins[1])};
      const uint64_t value = inputCoins[0].getVal() + inputCoins[1].getVal();
      if (value <= amount) return pImpl_->createTx_Self2t1(inputCoins);  // Coin merge
      return pImpl_->createTx_Self2t2(inputCoins, amount);               // value > payment
    } else {
      throw std::runtime_error("Coin strategy picked more than two coins!");
    }
  } catch (const std::runtime_error& e) {
    return std::string(e.what());
  }
}

std::variant<utt::Transaction, Error> User::transfer(const std::string& userId,
                                                     const std::string& destPK,
                                                     uint64_t amount) const {
  Transaction tx;
  tx.type_ = utt::Transaction::Type::Transfer;

  try {
    if (userId.empty() || destPK.empty() || amount == 0) throw std::runtime_error("Invalid arguments!");
    if (!pImpl_->client_) throw std::runtime_error("Uinitialized user!");

    const uint64_t balance = getBalance();
    if (balance < amount) throw std::runtime_error("User has insufficient balance!");
    const size_t budget = getPrivacyBudget();
    if (budget < amount) throw std::runtime_error("User has insufficient privacy budget!");

    auto pickedCoins = PickCoinsPreferExactMatch(pImpl_->coins_, amount);
    if (pickedCoins.empty()) throw std::runtime_error("Coin strategy didn't pick any coins!");

    if (pickedCoins.size() == 1) {
      const auto& coin = pImpl_->coins_.at(pickedCoins[0]);
      const auto value = coin.getVal();
      if (value < amount) throw std::runtime_error("Coin strategy picked a single insufficient coin!");
      if (value == amount) return pImpl_->createTx_1t1(coin, userId, destPK);
      return pImpl_->createTx_1t2(coin, amount, userId, destPK);  // value > amount
    } else if (pickedCoins.size() == 2) {
      std::vector<libutt::api::Coin> inputCoins{pImpl_->coins_.at(pickedCoins[0]), pImpl_->coins_.at(pickedCoins[1])};
      const auto value = inputCoins[0].getVal() + inputCoins[1].getVal();
      if (value < amount) return pImpl_->createTx_Self2t1(inputCoins);  // Coin merge
      if (value == amount) return pImpl_->createTx_2t1(inputCoins, userId, destPK);
      return pImpl_->createTx_2t2(inputCoins, amount, userId, destPK);  // value > amount
    } else {
      throw std::runtime_error("Coin strategy picked more than two coins!");
    }
  } catch (const std::runtime_error& e) {
    return std::string(e.what());
  }

  return tx;
}

}  // namespace utt::client