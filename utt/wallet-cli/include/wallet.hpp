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

#pragma once

#include <string>
#include <memory>

#include <grpcpp/grpcpp.h>
#include "api.grpc.pb.h"  // Generated from utt/wallet/proto/api

#include <utt-client-api/ClientApi.hpp>

class Wallet {
 public:
  Wallet(std::string userId, utt::client::TestUserPKInfrastructure& pki);

  void showInfo() const;

  /// @brief Deploy a privacy application
  /// [TODO-UTT] Should be performed by an admin app
  void deployApp();

  /// @brief Request registration of the current user
  void registerUser();

  /// @brief Request the creation of a privacy budget. The amount of the budget is predetermined by the deployed app.
  /// This operation could be performed entirely by an administrator, but we add it in the wallet
  /// for demo purposes.
  void createPrivacyBudget();

  /// @brief Requests the minting of public funds to private funds.
  /// @param amount the amount of public funds
  void mint(uint64_t amount);

  /// @brief Transfers the desired amount anonymously to the recipient
  /// @param amount The amount to transfer
  /// @param recipient The user id of the recipient
  void transfer(uint64_t amount, const std::string& recipient);

  /// @brief Burns the desired amount of private funds and converts them to public funds.
  /// @param amount The amount of private funds to burn.
  void burn(uint64_t amount);

 private:
  void connect();

  /// @brief Sync up to the last known tx number. If the last known tx number is zero (or not provided) the
  /// last signed transaction number will be fetched from the system
  void syncState(uint64_t lastKnownTxNum = 0);

  // [TODO-UTT] This is a helper method to check if the user has been created successfully after generating
  // a privacy app config. Since we don't have access to the public config of an already deployed privacy app
  // we need to deploy it from the wallet first. This also means that currently we can test only with a single wallet
  // because a second wallet would also need to deploy an app to have access to the config. This needs to be
  // changed so we can deploy an app and then provide the public config to one or more wallets and create
  // the users as part of initialization.
  bool checkOperational() const;

  struct DummyUserStorage : public utt::client::IUserStorage {};

  using GrpcService = vmware::concord::utt::wallet::api::v1::WalletService::Stub;

  bool isOperational_ = false;
  DummyUserStorage storage_;
  std::string userId_;
  utt::client::TestUserPKInfrastructure& pki_;
  std::unique_ptr<utt::client::User> user_;
  std::unique_ptr<GrpcService> grpc_;
};