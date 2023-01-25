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
#include <utt-client-api/ClientApi.hpp>

Wallet::Wallet(std::string userId, utt::client::TestUserPKInfrastructure& pki, const utt::PublicConfig& config)
    : userId_{std::move(userId)}, pki_{pki} {
  storage_ = std::make_unique<utt::client::FileBasedUserStorage>("state/" + userId_);
  user_ = utt::client::createUser(userId_, config, pki_, std::move(storage_));
  if (!user_) throw std::runtime_error("Failed to create user!");
  registered_ = user_->hasRegistrationCommitment();
}
