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
  using Connection = std::unique_ptr<vmware::concord::utt::wallet::api::v1::WalletService::Stub>;
  static Connection newConnection();

  /// [TODO-UTT] Should be performed by an admin app
  /// @brief Deploy a privacy application
  /// @return The public configuration of the deployed application
  static std::pair<utt::Configuration, utt::PublicConfig> deployApp(Connection& conn);

  /// [TODO-UTT] Create privacy budget locally because the system can't process budget requests yet.
  /// @brief Create a privacy budget locally for the user. This function is only for testing.
  /// @param config A Privacy app configuration
  /// @param amount The amount of privacy budget to create
  void createPrivacyBudgetLocal(const utt::Configuration& config, uint64_t amount);

  Wallet(std::string userId, utt::client::TestUserPKInfrastructure& pki, const utt::PublicConfig& config);

  void showInfo(Connection& conn);

  /// @brief Request registration of the current user
  void registerUser(Connection& conn);

  /// @brief Request the creation of a privacy budget. The amount of the budget is predetermined by the deployed app.
  /// This operation could be performed entirely by an administrator, but we add it in the wallet
  /// for demo purposes.
  void createPrivacyBudget(Connection& conn);

  /// @brief Requests the minting of public funds to private funds.
  /// @param amount the amount of public funds
  void mint(Connection& conn, uint64_t amount);

  /// @brief Transfers the desired amount anonymously to the recipient
  /// @param amount The amount to transfer
  /// @param recipient The user id of the recipient
  void transfer(Connection& conn, uint64_t amount, const std::string& recipient);

  /// @brief Burns the desired amount of private funds and converts them to public funds.
  /// @param amount The amount of private funds to burn.
  void burn(Connection& conn, uint64_t amount);

  void debugOutput() const;

 private:
  /// @brief Sync up to the last known tx number. If the last known tx number is zero (or not provided) the
  /// last signed transaction number will be fetched from the system
  void syncState(Connection& conn, uint64_t lastKnownTxNum = 0);

  struct DummyUserStorage : public utt::client::IUserStorage {};

  DummyUserStorage storage_;
  std::string userId_;
  utt::client::TestUserPKInfrastructure& pki_;
  std::unique_ptr<utt::client::User> user_;
  uint64_t publicBalance_ = 0;
};