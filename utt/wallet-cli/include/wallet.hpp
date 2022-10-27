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

namespace WalletApi = vmware::concord::utt::wallet::api::v1;

class Wallet {
 public:
  using Connection = std::unique_ptr<WalletApi::WalletService::Stub>;
  using Channel = std::unique_ptr<grpc::ClientReaderWriter<WalletApi::WalletRequest, WalletApi::WalletResponse>>;

  static Connection newConnection();

  /// @brief Get the configuration for a privacy application
  /// @return The full and public configurations of the deployed application
  /// [TODO-UTT] We only need the public config but the full config is returned
  /// because we needed for createPrivacyBudgetLocal which should also be removed
  /// in favor of a system-created budget tokens by the admin
  static std::pair<utt::Configuration, utt::PublicConfig> getConfigs(Channel& chan);

  bool isRegistered() const;

  /// [TODO-UTT] Create privacy budget locally because the system can't process budget requests yet.
  /// @brief Create a privacy budget locally for the user. This function is only for testing.
  /// @param config A Privacy app configuration
  /// @param amount The amount of privacy budget to create
  void createPrivacyBudgetLocal(const utt::Configuration& config, uint64_t amount);

  Wallet(std::string userId, utt::client::TestUserPKInfrastructure& pki, const utt::PublicConfig& config);

  void showInfo(Channel& chan);

  /// @brief Request registration of the current user
  void registerUser(Channel& chan);

  /// @brief Request the creation of a privacy budget. The amount of the budget is predetermined by the deployed app.
  /// This operation could be performed entirely by an administrator, but we add it in the wallet
  /// for demo purposes.
  void createPrivacyBudget(Channel& chan);

  /// @brief Requests the minting of public funds to private funds.
  /// @param amount the amount of public funds
  void mint(Channel& chan, uint64_t amount);

  /// @brief Transfers the desired amount anonymously to the recipient
  /// @param amount The amount to transfer
  /// @param recipient The user id of the recipient
  void transfer(Channel& chan, uint64_t amount, const std::string& recipient);

  /// @brief Burns the desired amount of private funds and converts them to public funds.
  /// @param amount The amount of private funds to burn.
  void burn(Channel& chan, uint64_t amount);

  void debugOutput() const;

 private:
  /// @brief Sync up to the last known tx number. If the last known tx number is zero (or not provided) the
  /// last signed transaction number will be fetched from the system
  void syncState(Channel& chan, uint64_t lastKnownTxNum = 0);

  struct DummyUserStorage : public utt::client::IUserStorage {};

  DummyUserStorage storage_;
  std::string userId_;
  utt::client::TestUserPKInfrastructure& pki_;
  std::unique_ptr<utt::client::User> user_;
  uint64_t publicBalance_ = 0;
  bool registered_ = false;
};