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

using namespace libutt;

#define logdbg_user logdbg << ((pImpl_->client_) ? pImpl_->client_->getPid() : "!!!uninitialized user!!!") << ' '

namespace {
std::string dbgPrintCoins(const std::vector<libutt::api::Coin>& coins) {
  if (coins.empty()) return "[empty]";
  std::stringstream ss;
  ss << '[';
  for (const auto& coin : coins) {
    ss << "type:" << (coin.getType() == libutt::api::Coin::Type::Budget ? "budget" : "normal");
    ss << "|val:" << coin.getVal();
    ss << "|exp:" << coin.getExpDate();
    ss << "|null:" << coin.getNullifier() << ", ";
  }
  ss << ']';
  return ss.str();
}

std::string dbgPrintRecipients(const std::vector<std::tuple<std::string, uint64_t>>& recips) {
  if (recips.empty()) return "[empty]";
  std::stringstream ss;
  ss << '[';
  for (const auto& recip : recips) {
    ss << std::get<0>(recip) << ": " << std::get<1>(recip) << ", ";
  }
  ss << ']';
  return ss.str();
}
}  // namespace

namespace utt::client {

struct User::Impl {
  utt::Transaction createTx_Burn(const libutt::api::Coin& coin);

  utt::Transaction createTx_Self1to2(const libutt::api::Coin& coin, uint64_t amount);
  utt::Transaction createTx_Self2to1(const std::vector<libutt::api::Coin>& coins);
  utt::Transaction createTx_Self2to2(const std::vector<libutt::api::Coin>& coins, uint64_t amount);

  utt::Transaction createTx_1to1(const libutt::api::Coin& coin, const std::string& userId, const std::string& pk);
  utt::Transaction createTx_1to2(const libutt::api::Coin& coin,
                                 uint64_t amount,
                                 const std::string& userId,
                                 const std::string& pk);
  utt::Transaction createTx_2to1(const std::vector<libutt::api::Coin>& coins,
                                 const std::string& userId,
                                 const std::string& pk);
  utt::Transaction createTx_2to2(const std::vector<libutt::api::Coin>& coins,
                                 uint64_t amount,
                                 const std::string& userId,
                                 const std::string& pk);

  libutt::RSAEncryptor createRsaEncryptorForTransferToOther(const std::string& otherUserId, const std::string& otherPk);

  std::string sk_;                     // User's secret key
  std::string pk_;                     // User's public key
  libutt::api::types::CurvePoint s1_;  // User's secret part of the PRF key
  libutt::api::UTTParams params_;
  std::unique_ptr<libutt::api::Client> client_;  // User's credentials
  std::unique_ptr<libutt::RSAEncryptor>
      selfTxEncryptor_;  // Encryptor with own's public key for self transactions (computed once for convenience)

  uint64_t lastExecutedTxNum_ = 0;
  std::vector<libutt::api::Coin> coins_;         // User's unspent UTT coins (tokens)
  std::optional<libutt::api::Coin> budgetCoin_;  // User's current UTT budget coin (token)
  std::set<std::string> budgetNullifiers_;
};

utt::Transaction User::Impl::createTx_Burn(const libutt::api::Coin& coin) {
  logdbg << client_->getPid() << " creates burn tx with input coins: " << dbgPrintCoins({coin}) << endl;

  utt::Transaction tx;
  tx.type_ = utt::Transaction::Type::Burn;
  auto burn = libutt::api::operations::Burn(params_, *client_, coin);
  tx.data_ = libutt::api::serialize<libutt::api::operations::Burn>(burn);
  return tx;
}

utt::Transaction User::Impl::createTx_Self1to2(const libutt::api::Coin& coin, uint64_t amount) {
  std::vector<std::tuple<std::string, uint64_t>> recip;
  recip.emplace_back(client_->getPid(), amount);
  recip.emplace_back(client_->getPid(), coin.getVal() - amount);

  logdbg << client_->getPid() << " creates self-split tx with input coins: " << dbgPrintCoins({coin})
         << " and recipients: " << dbgPrintRecipients(recip) << endl;

  auto uttTx = libutt::api::operations::Transaction(params_, *client_, {coin}, std::nullopt, recip, *selfTxEncryptor_);

  utt::Transaction tx;
  tx.type_ = utt::Transaction::Type::Transfer;
  tx.data_ = libutt::api::serialize<libutt::api::operations::Transaction>(uttTx);
  tx.numOutputs_ = uttTx.getNumOfOutputCoins();
  return tx;
}

utt::Transaction User::Impl::createTx_Self2to1(const std::vector<libutt::api::Coin>& coins) {
  std::vector<std::tuple<std::string, uint64_t>> recip;
  recip.emplace_back(client_->getPid(), coins[0].getVal() + coins[1].getVal());

  logdbg << client_->getPid() << " creates self-merge with input coins: " << dbgPrintCoins(coins)
         << " and recipients: " << dbgPrintRecipients(recip) << endl;

  auto uttTx = libutt::api::operations::Transaction(params_, *client_, coins, std::nullopt, recip, *selfTxEncryptor_);

  utt::Transaction tx;
  tx.type_ = utt::Transaction::Type::Transfer;
  tx.data_ = libutt::api::serialize<libutt::api::operations::Transaction>(uttTx);
  tx.numOutputs_ = uttTx.getNumOfOutputCoins();
  return tx;
}

utt::Transaction User::Impl::createTx_Self2to2(const std::vector<libutt::api::Coin>& coins, uint64_t amount) {
  std::vector<std::tuple<std::string, uint64_t>> recip;
  recip.emplace_back(client_->getPid(), amount);
  recip.emplace_back(client_->getPid(), (coins[0].getVal() + coins[1].getVal()) - amount);

  logdbg << client_->getPid() << " creates 2-to-2 self-split with input coins: " << dbgPrintCoins(coins)
         << " and recipients: " << dbgPrintRecipients(recip) << endl;

  auto uttTx = libutt::api::operations::Transaction(params_, *client_, coins, std::nullopt, recip, *selfTxEncryptor_);

  utt::Transaction tx;
  tx.type_ = utt::Transaction::Type::Transfer;
  tx.data_ = libutt::api::serialize<libutt::api::operations::Transaction>(uttTx);
  tx.numOutputs_ = uttTx.getNumOfOutputCoins();
  return tx;
}

utt::Transaction User::Impl::createTx_1to1(const libutt::api::Coin& coin,
                                           const std::string& userId,
                                           const std::string& pk) {
  std::vector<std::tuple<std::string, uint64_t>> recip;
  recip.emplace_back(userId, coin.getVal());

  logdbg << client_->getPid() << " creates 1-to-1 transfer with input coins: " << dbgPrintCoins({coin})
         << " and recipients: " << dbgPrintRecipients(recip) << endl;

  auto encryptor = createRsaEncryptorForTransferToOther(userId, pk);
  auto uttTx = libutt::api::operations::Transaction(
      params_, *client_, {coin}, params_.getBudgetPolicy() ? budgetCoin_ : std::nullopt, recip, encryptor);

  utt::Transaction tx;
  tx.type_ = utt::Transaction::Type::Transfer;
  tx.data_ = libutt::api::serialize<libutt::api::operations::Transaction>(uttTx);
  tx.numOutputs_ = uttTx.getNumOfOutputCoins();
  return tx;
}

utt::Transaction User::Impl::createTx_1to2(const libutt::api::Coin& coin,
                                           uint64_t amount,
                                           const std::string& userId,
                                           const std::string& pk) {
  std::vector<std::tuple<std::string, uint64_t>> recip;
  recip.emplace_back(userId, amount);
  recip.emplace_back(client_->getPid(), coin.getVal() - amount);

  logdbg << client_->getPid() << " creates 1-to-2 transfer with input coins: " << dbgPrintCoins({coin})
         << " and recipients: " << dbgPrintRecipients(recip) << endl;

  auto encryptor = createRsaEncryptorForTransferToOther(userId, pk);
  auto uttTx = libutt::api::operations::Transaction(
      params_, *client_, {coin}, params_.getBudgetPolicy() ? budgetCoin_ : std::nullopt, recip, encryptor);

  utt::Transaction tx;
  tx.type_ = utt::Transaction::Type::Transfer;
  tx.data_ = libutt::api::serialize<libutt::api::operations::Transaction>(uttTx);
  tx.numOutputs_ = uttTx.getNumOfOutputCoins();
  return tx;
}

utt::Transaction User::Impl::createTx_2to1(const std::vector<libutt::api::Coin>& coins,
                                           const std::string& userId,
                                           const std::string& pk) {
  std::vector<std::tuple<std::string, uint64_t>> recip;
  recip.emplace_back(userId, coins[0].getVal() + coins[1].getVal());

  logdbg << client_->getPid() << "creates 2-to-1 transfer with input coins: " << dbgPrintCoins(coins)
         << " and recipients: " << dbgPrintRecipients(recip) << endl;

  auto encryptor = createRsaEncryptorForTransferToOther(userId, pk);
  auto uttTx = libutt::api::operations::Transaction(
      params_, *client_, coins, params_.getBudgetPolicy() ? budgetCoin_ : std::nullopt, recip, encryptor);

  utt::Transaction tx;
  tx.type_ = utt::Transaction::Type::Transfer;
  tx.data_ = libutt::api::serialize<libutt::api::operations::Transaction>(uttTx);
  tx.numOutputs_ = uttTx.getNumOfOutputCoins();
  return tx;
}

utt::Transaction User::Impl::createTx_2to2(const std::vector<libutt::api::Coin>& coins,
                                           uint64_t amount,
                                           const std::string& userId,
                                           const std::string& pk) {
  std::vector<std::tuple<std::string, uint64_t>> recip;
  recip.emplace_back(userId, amount);
  recip.emplace_back(client_->getPid(), (coins[0].getVal() + coins[1].getVal()) - amount);

  logdbg << client_->getPid() << " creates 2-to-2 transfer with input coins: " << dbgPrintCoins(coins)
         << " and recipients: " << dbgPrintRecipients(recip) << endl;

  auto encryptor = createRsaEncryptorForTransferToOther(userId, pk);
  auto uttTx = libutt::api::operations::Transaction(
      params_, *client_, coins, params_.getBudgetPolicy() ? budgetCoin_ : std::nullopt, recip, encryptor);

  utt::Transaction tx;
  tx.type_ = utt::Transaction::Type::Transfer;
  tx.data_ = libutt::api::serialize<libutt::api::operations::Transaction>(uttTx);
  tx.numOutputs_ = uttTx.getNumOfOutputCoins();
  return tx;
}

libutt::RSAEncryptor User::Impl::createRsaEncryptorForTransferToOther(const std::string& otherUserId,
                                                                      const std::string& otherPk) {
  std::map<std::string, std::string> creds;
  creds.emplace(client_->getPid(), pk_);  // Own credentials used for budget coin and change output
  creds.emplace(otherUserId, otherPk);    // Other user credentials
  return libutt::RSAEncryptor{creds};
}

std::unique_ptr<User> User::createInitial(const std::string& userId,
                                          const PublicConfig& config,
                                          IUserPKInfrastructure& pki,
                                          IUserStorage& storage) {
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
  if (userKeys.sk_.empty()) throw std::runtime_error("User public key not generated!");
  if (userKeys.pk_.empty()) throw std::runtime_error("User private key not generated!");

  auto uttConfig = libutt::api::deserialize<libutt::api::PublicConfig>(config);

  auto user = std::make_unique<User>();
  user->pImpl_->pk_ = userKeys.pk_;
  user->pImpl_->params_ = uttConfig.getParams();
  // Create a client object with an RSA based PKI
  user->pImpl_->client_.reset(new libutt::api::Client(
      userId, uttConfig.getCommitVerificationKey(), uttConfig.getRegistrationVerificationKey(), userKeys.sk_));

  std::map<std::string, std::string> selfTxCredentials{{userId, userKeys.pk_}};
  user->pImpl_->selfTxEncryptor_ = std::make_unique<libutt::RSAEncryptor>(selfTxCredentials);

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

void User::updateRegistration(const std::string& pk, const RegistrationSig& rs, const S2& s2) {
  if (!pImpl_->client_) throw std::runtime_error("User not initialized!");
  if (rs.empty() || s2.empty() || pk.empty()) throw std::runtime_error("updateRegistration: invalid arguments!");
  if (!(pImpl_->pk_ == pk)) throw std::runtime_error("Public key mismatch!");

  // Un-blind the signature
  std::vector<libutt::api::types::CurvePoint> randomness = {libutt::Fr::zero().to_words(),
                                                            libutt::Fr::zero().to_words()};
  auto unblindedSig =
      libutt::api::Utils::unblindSignature(pImpl_->params_, libutt::api::Commitment::REGISTRATION, randomness, rs);
  if (unblindedSig.empty()) throw std::runtime_error("Failed to unblind reg signature!");

  // [TODO-UTT] What if we already updated a registration? How do we check it?
  pImpl_->client_->setRCMSig(pImpl_->params_, s2, unblindedSig);
}

void User::updatePrivacyBudget(const PrivacyBudget& budget, const PrivacyBudgetSig& sig) {
  if (!pImpl_->client_) throw std::runtime_error("User not initialized!");

  logdbg_user << "updating privacy budget" << endl;

  auto claimedCoins = pImpl_->client_->claimCoins(libutt::api::deserialize<libutt::api::operations::Budget>(budget),
                                                  pImpl_->params_,
                                                  std::vector<libutt::api::types::Signature>{sig});

  // Expect a single budget token to be claimed by the user
  if (claimedCoins.size() != 1) throw std::runtime_error("Expected single budget token!");
  if (!pImpl_->client_->validate(claimedCoins[0])) throw std::runtime_error("Invalid initial budget coin!");
  auto nullifer = claimedCoins[0].getNullifier();
  // std::cout << "Budget nullifer is, hash " << std::hash<std::string>{}(nullifer) << " raw " << nullifer << "\n";
  if (pImpl_->budgetNullifiers_.count(claimedCoins[0].getNullifier()) == 1) {
    // TODO debug log
    return;
  }

  // [TODO-UTT] Requires atomic, durable write through IUserStorage
  pImpl_->budgetCoin_ = claimedCoins[0];

  logdbg_user << "claimed budget token " << dbgPrintCoins({*pImpl_->budgetCoin_}) << endl;
  pImpl_->budgetNullifiers_.insert(claimedCoins[0].getNullifier());
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

void User::updateTransferTx(uint64_t txNum, const Transaction& tx, const TxOutputSigs& sigs) {
  if (!pImpl_->client_) throw std::runtime_error("User not initialized!");
  if (tx.type_ != Transaction::Type::Transfer) throw std::runtime_error("Transfer tx type mismatch!");
  if (txNum != pImpl_->lastExecutedTxNum_ + 1) throw std::runtime_error("Transfer tx number is not consecutive!");

  auto uttTx = libutt::api::deserialize<libutt::api::operations::Transaction>(tx.data_);

  logdbg_user << "executing transfer tx: " << txNum << endl;

  // [TODO-UTT] Requires atomic, durable write batch through IUserStorage
  pImpl_->lastExecutedTxNum_ = txNum;

  // [TODO-UTT] More consistency checks
  // If we slash coins we expect to also update our budget coin

  // Slash spent coins
  for (const auto& null : uttTx.getNullifiers()) {
    auto it = std::find_if(pImpl_->coins_.begin(), pImpl_->coins_.end(), [&null](const libutt::api::Coin& coin) {
      return coin.getNullifier() == null;
    });
    if (it != pImpl_->coins_.end()) {
      logdbg_user << "slashing spent coin " << dbgPrintCoins({*it}) << endl;
      pImpl_->coins_.erase(it);
    }
  }

  // Claim coins
  auto claimedCoins = pImpl_->client_->claimCoins(uttTx, pImpl_->params_, sigs);
  for (auto& coin : claimedCoins) {
    if (coin.getType() == libutt::api::Coin::Type::Normal) {
      if (!pImpl_->client_->validate(coin)) throw std::runtime_error("Invalid normal coin in transfer!");
      logdbg_user << "claimed normal coin: " << dbgPrintCoins({coin}) << endl;
      pImpl_->coins_.emplace_back(std::move(coin));
    } else if (coin.getType() == libutt::api::Coin::Type::Budget) {
      // Replace budget coin
      if (!pImpl_->client_->validate(coin)) throw std::runtime_error("Invalid budget coin in transfer!");
      if (coin.getVal() > 0) {
        logdbg_user << "claimed budget coin: " << dbgPrintCoins({coin}) << endl;
        pImpl_->budgetCoin_ = std::move(coin);
      } else {
        pImpl_->budgetCoin_ = std::nullopt;
      }
    }
  }
}

void User::updateMintTx(uint64_t txNum, const Transaction& tx, const TxOutputSig& sig) {
  if (!pImpl_->client_) throw std::runtime_error("User not initialized!");
  if (tx.type_ != Transaction::Type::Mint) throw std::runtime_error("Mint tx type mismatch!");
  if (txNum != pImpl_->lastExecutedTxNum_ + 1) throw std::runtime_error("Mint tx number is not consecutive!");

  auto mint = libutt::api::deserialize<libutt::api::operations::Mint>(tx.data_);

  logdbg_user << "executing mint tx: " << txNum << endl;

  if (mint.getRecipentID() != pImpl_->client_->getPid()) {
    logdbg_user << "ignores mint transaction for different user: " << mint.getRecipentID() << endl;
  } else {
    auto claimedCoins =
        pImpl_->client_->claimCoins(mint, pImpl_->params_, std::vector<libutt::api::types::Signature>{sig});

    // Expect a single token to be claimed by the user
    if (claimedCoins.size() != 1) throw std::runtime_error("Expected single coin in mint tx!");
    if (!pImpl_->client_->validate(claimedCoins[0])) throw std::runtime_error("Invalid minted coin!");

    pImpl_->coins_.emplace_back(std::move(claimedCoins[0]));
  }

  // [TODO-UTT] Requires atomic, durable write batch through IUserStorage
  pImpl_->lastExecutedTxNum_ = txNum;
}

// [TODO-UTT] Do we actually need the whole BurnTx or we can simply use the nullifier to slash?
void User::updateBurnTx(uint64_t txNum, const Transaction& tx) {
  if (!pImpl_->client_) throw std::runtime_error("User not initialized!");
  if (tx.type_ != Transaction::Type::Burn) throw std::runtime_error("Burn tx type mismatch!");
  if (txNum != pImpl_->lastExecutedTxNum_ + 1) throw std::runtime_error("Burn tx number is not consecutive!");

  auto burn = libutt::api::deserialize<libutt::api::operations::Burn>(tx.data_);

  logdbg_user << "executing burn tx: " << txNum << endl;

  if (burn.getOwnerPid() != pImpl_->client_->getPid()) {
    logdbg_user << "ignores burn tx for different user: " << burn.getOwnerPid() << endl;
  } else {
    auto nullifier = burn.getNullifier();
    if (nullifier.empty()) throw std::runtime_error("Burn tx has empty nullifier!");

    auto it = std::find_if(pImpl_->coins_.begin(), pImpl_->coins_.end(), [&nullifier](const libutt::api::Coin& coin) {
      return coin.getNullifier() == nullifier;
    });
    if (it == pImpl_->coins_.end()) throw std::runtime_error("Burned token missing in wallet!");
    pImpl_->coins_.erase(it);
  }

  // [TODO-UTT] Requires atomic, durable write batch through IUserStorage
  pImpl_->lastExecutedTxNum_ = txNum;
}

void User::updateNoOp(uint64_t txNum) {
  if (!pImpl_->client_) throw std::runtime_error("User not initialized!");
  if (txNum != pImpl_->lastExecutedTxNum_ + 1) throw std::runtime_error("Noop tx number is not consecutive!");

  logdbg_user << "executing noop tx: " << txNum << endl;

  pImpl_->lastExecutedTxNum_ = txNum;
}

utt::Transaction User::mint(uint64_t amount) const {
  std::stringstream ss;
  ss << Fr::random_element();
  auto randomHash = ss.str();

  logdbg_user << "creating a mint tx with random hash: " << randomHash << endl;

  auto mint = libutt::api::operations::Mint(randomHash, amount, pImpl_->client_->getPid());

  utt::Transaction tx;
  tx.type_ = utt::Transaction::Type::Mint;
  tx.data_ = libutt::api::serialize<libutt::api::operations::Mint>(mint);

  return tx;
}

void User::createPrivacyBudgetLocal(const utt::Configuration& config, uint64_t amount) {
  if (config.empty()) throw std::runtime_error("Privacy app config cannot be empty!");
  if (amount == 0) throw std::runtime_error("Positive privacy budget amount required!");

  logdbg_user << "creates " << amount << " privacy budget locally" << endl;

  auto uttConfig = libutt::api::deserialize<libutt::api::Configuration>(config);

  // Create coins signers from config
  auto commitVerificationKey = uttConfig.getPublicConfig().getCommitVerificationKey();
  auto registrationVerificationKey = uttConfig.getPublicConfig().getRegistrationVerificationKey();
  std::map<uint16_t, std::string> commitVerificationKeyShares;
  std::map<uint16_t, std::string> registrationVerificationKeyShares;
  std::vector<libutt::api::CoinsSigner> coinsSigners;

  for (uint16_t i = 0; i < uttConfig.getNumValidators(); ++i) {
    commitVerificationKeyShares.emplace(i, uttConfig.getCommitVerificationKeyShare(i));
    registrationVerificationKeyShares.emplace(i, uttConfig.getRegistrationVerificationKeyShare(i));
  }

  // Create coins signers
  for (uint16_t i = 0; i < uttConfig.getNumValidators(); ++i) {
    coinsSigners.emplace_back(i,
                              uttConfig.getCommitSecret(i),
                              commitVerificationKey,
                              commitVerificationKeyShares,
                              registrationVerificationKey);
  }

  // Create budget
  auto budget = libutt::api::operations::Budget(pImpl_->params_, *pImpl_->client_, amount, 0 /*expirationDate*/);

  // Sign budget
  const uint16_t n = uttConfig.getNumValidators();
  const uint16_t t = uttConfig.getThreshold();

  // We need only 'threshold' signers to sign
  std::map<uint32_t, std::vector<uint8_t>> shareSubset;
  for (size_t idx = 0; idx < t; ++idx) {
    shareSubset.emplace((uint32_t)idx, coinsSigners[idx].sign(budget).front());
  }

  auto sig = libutt::api::Utils::aggregateSigShares(n, shareSubset);

  // Claim and update
  auto claimedCoins =
      pImpl_->client_->claimCoins(budget, pImpl_->params_, std::vector<libutt::api::types::Signature>{sig});

  // Expect a single budget token to be claimed by the user
  if (claimedCoins.size() != 1) throw std::runtime_error("Expected single budget token!");
  if (!pImpl_->client_->validate(claimedCoins[0])) throw std::runtime_error("Invalid local budget coin!");

  // [TODO-UTT] Requires atomic, durable write through IUserStorage
  pImpl_->budgetCoin_ = claimedCoins[0];
}

BurnResult User::burn(uint64_t amount) const {
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
    if (value == amount) return {pImpl_->createTx_Burn(coin), true /*isFinal*/};
    return {pImpl_->createTx_Self1to2(coin, amount), false /*isFinal*/};  // value > payment
  } else if (pickedCoins.size() == 2) {
    const std::vector<libutt::api::Coin> inputCoins = {pImpl_->coins_.at(pickedCoins[0]),
                                                       pImpl_->coins_.at(pickedCoins[1])};
    const uint64_t value = inputCoins[0].getVal() + inputCoins[1].getVal();
    if (value <= amount) return {pImpl_->createTx_Self2to1(inputCoins), false /*isFinal*/};  // Coin merge
    return {pImpl_->createTx_Self2to2(inputCoins, amount), false /*isFinal*/};               // value > payment
  } else {
    throw std::runtime_error("Coin strategy picked more than two coins!");
  }
}

TransferResult User::transfer(const std::string& userId, const std::string& destPK, uint64_t amount) const {
  if (userId.empty() || destPK.empty() || amount == 0) throw std::runtime_error("Invalid arguments!");
  if (!pImpl_->client_) throw std::runtime_error("Uninitialized user!");

  const uint64_t balance = getBalance();
  if (balance < amount) throw std::runtime_error("User has insufficient balance!");
  if (pImpl_->params_.getBudgetPolicy()) {
    const size_t budget = getPrivacyBudget();
    if (budget < amount) throw std::runtime_error("User has insufficient privacy budget!");
  }
  auto pickedCoins = PickCoinsPreferExactMatch(pImpl_->coins_, amount);
  if (pickedCoins.empty()) throw std::runtime_error("Coin strategy didn't pick any coins!");

  if (pickedCoins.size() == 1) {
    const auto& coin = pImpl_->coins_.at(pickedCoins[0]);
    const auto value = coin.getVal();
    if (value < amount) throw std::runtime_error("Coin strategy picked a single insufficient coin!");
    if (value == amount) return {pImpl_->createTx_1to1(coin, userId, destPK), true /*isFinal*/};
    return {pImpl_->createTx_1to2(coin, amount, userId, destPK), true /*isFinal*/};  // value > amount
  } else if (pickedCoins.size() == 2) {
    std::vector<libutt::api::Coin> inputCoins{pImpl_->coins_.at(pickedCoins[0]), pImpl_->coins_.at(pickedCoins[1])};
    const auto value = inputCoins[0].getVal() + inputCoins[1].getVal();
    if (value < amount) return {pImpl_->createTx_Self2to1(inputCoins), false /*isFinal*/};  // Coin merge
    if (value == amount) return {pImpl_->createTx_2to1(inputCoins, userId, destPK), true /*isFinal*/};
    return {pImpl_->createTx_2to2(inputCoins, amount, userId, destPK), true /*isFinal*/};  // value > amount
  } else {
    throw std::runtime_error("Coin strategy picked more than two coins!");
  }
}

void User::debugOutput() const {
  std::cout << "------ USER DEBUG OUTPUT START -------------\n";
  if (!pImpl_->client_) {
    std::cout << "User's libutt::api::client object not initialized!\n";
  }
  std::cout << "lastExecutedTxNum:" << pImpl_->lastExecutedTxNum_ << '\n';
  std::cout << "coins: [\n";

  auto dbgOutputCoin = [](const libutt::api::Coin& coin) {
    std::cout << "type: " << (coin.getType() == libutt::api::Coin::Type::Budget ? "Budget" : "Normal") << ' ';
    std::cout << "value: " << coin.getVal() << ' ';
    std::cout << "expire: " << coin.getExpDate() << ' ';
    std::cout << "null: " << coin.getNullifier() << ' ';
    std::cout << "hasSig: " << coin.hasSig() << ' ';
    std::cout << '\n';
  };

  for (const auto& coin : pImpl_->coins_) {
    dbgOutputCoin(coin);
  }
  std::cout << "]\n";
  std::cout << "budget coin: [\n";
  if (pImpl_->budgetCoin_) dbgOutputCoin(*pImpl_->budgetCoin_);
  std::cout << "]\n";
  std::cout << "------ USER DEBUG OUTPUT END -------------\n";
}

}  // namespace utt::client