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

#include "Wallet.hpp"
#include <storage/FileBasedUserStorage.hpp>
#include <iostream>
#include <xutils/Log.h>
#include <utt-client-api/ClientApi.hpp>
#include <utils/crypto.hpp>
namespace utt::walletservice {
using namespace utt::client::utils::crypto;
Wallet::Wallet(std::string userId,
               const std::string& private_key,
               const std::string& public_key,
               const std::string& storage_path,
               const utt::PublicConfig& config)
    : userId_{std::move(userId)}, private_key_{private_key} {
  auto storage_ = std::make_shared<utt::client::FileBasedUserStorage>(storage_path);
  user_ = utt::client::createUser(userId_, config, private_key, public_key, storage_);
  if (!user_) throw std::runtime_error("Failed to create user!");
  registered_ = user_->hasRegistrationCommitment();
}

std::unique_ptr<Wallet> Wallet::recoverFromStorage(const std::string& storage_path) {
  auto storage_ = std::make_shared<utt::client::FileBasedUserStorage>(storage_path);
  if (storage_->isNewStorage()) return nullptr;
  std::unique_ptr<Wallet> wallet = std::unique_ptr<Wallet>(new Wallet());
  wallet->userId_ = storage_->getUserId();
  wallet->private_key_ = storage_->getKeyPair().first;
  wallet->user_ = utt::client::loadUserFromStorage(storage_);
  wallet->registered_ = wallet->user_->hasRegistrationCommitment();
  return wallet;
}

std::optional<Wallet::RegistrationInput> Wallet::generateRegistrationInput() {
  if (registered_) return std::nullopt;
  Wallet::RegistrationInput ret;
  ret.rcm1 = user_->getRegistrationInput();
  if (ret.rcm1.empty()) {
    std::cout << "failed to generate rcm1" << std::endl;
    return std::nullopt;
  }
  ret.rcm1_sig = signData(ret.rcm1, private_key_);
  return ret;
}

bool Wallet::updateRegistrationCommitment(const RegistrationSig& sig, const S2& s2) {
  if (registered_) return false;
  user_->updateRegistration(user_->getPK(), sig, s2);
  registered_ = true;
  return true;
}

const std::string& Wallet::getUserId() const { return userId_; }

bool Wallet::claimCoins(const utt::Transaction& tx, const std::vector<std::vector<uint8_t>>& sigs) {
  switch (tx.type_) {
    case utt::Transaction::Type::Mint: {
      user_->updateMintTx(tx, sigs[0]);
    } break;
    case utt::Transaction::Type::Transfer: {
      user_->updateTransferTx(tx, sigs);
    } break;
    case utt::Transaction::Type::Burn: {
      user_->updateBurnTx(tx);
    } break;
    case utt::Transaction::Type::Budget: {
      user_->updatePrivacyBudget(tx.data_, sigs[0]);
    } break;
    default:
      std::cout << "invalid tx type" << std::endl;
      return false;
  }
  return true;
}

utt::Transaction Wallet::generateMintTx(uint64_t amount) const {
  std::cout << "Processing mint request " << amount << "...\n";
  return user_->mint(amount);
}
utt::client::TxResult Wallet::generateTransferTx(uint64_t amount,
                                                 const std::string& recipient,
                                                 const std::string& recipient_public_key) const {
  std::cout << "Processing an anonymous transfer request of " << amount << " to " << recipient << "...\n";
  return user_->transfer(recipient, recipient_public_key, amount);
}
utt::client::TxResult Wallet::generateBurnTx(uint64_t amount) const {
  std::cout << "Processing a burn request of " << amount << "...\n";
  return user_->burn(amount);
}

uint64_t Wallet::getBalance() const { return user_->getBalance(); }
uint64_t Wallet::getBudget() const { return user_->getPrivacyBudget(); }

bool Wallet::isRegistered() const { return registered_; }

std::vector<utt::client::CoinDescriptor> Wallet::getCoinsDescriptors() const { return user_->getCoinsDescriptors(); }
}  // namespace utt::walletservice
