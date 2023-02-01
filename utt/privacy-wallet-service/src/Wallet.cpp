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
  storage_ = std::make_unique<utt::client::FileBasedUserStorage>(storage_path);
  user_ = utt::client::createUser(userId_, config, private_key, public_key, std::move(storage_));
  if (!user_) throw std::runtime_error("Failed to create user!");
  registered_ = user_->hasRegistrationCommitment();
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
}  // namespace utt::walletservice
