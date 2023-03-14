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
#include <tuple>

#include <grpcpp/grpcpp.h>
#include "wallet-api.grpc.pb.h"  // Generated from utt/wallet/proto/api

#include <utt-client-api/ClientApi.hpp>
#include <storage/FileBasedUserStorage.hpp>

namespace WalletApi = vmware::concord::utt::wallet::api::v1;

class Wallet {
  class CliCustomPersistentStorage : public utt::client::FileBasedUserStorage {
   public:
    CliCustomPersistentStorage(const std::string& path) : utt::client::FileBasedUserStorage{path} {}
    void setLastExecutedSn(uint64_t sn) {
      IStorage::tx_guard gurd(*this);
      state_["last_executed_sn"] = sn;
    }

    uint64_t getLastExecutedSn() {
      if (!state_.contains("last_executed_sn")) return 0;
      return state_["last_executed_sn"];
    }
  };

 public:
  using Connection = std::unique_ptr<WalletApi::WalletService::Stub>;
  using Channel = std::unique_ptr<grpc::ClientReaderWriter<WalletApi::WalletRequest, WalletApi::WalletResponse>>;

  static Connection newConnection();

  /// @brief Get the configuration for a privacy application
  /// @return The public configuration of the deployed application
  static utt::PublicConfig getPublicConfig(Channel& chan);

  bool isRegistered() const;

  Wallet(std::string userId,
         const std::string& private_key,
         const std::string& publick_key,
         const utt::PublicConfig& config);

  void showInfo(Channel& chan);

  /// @brief Get info about public and private balances and privacy budget
  /// @return Tuple representing user balances: (public balance, private balance, privacy budget)
  std::tuple<uint64_t, uint64_t, uint64_t> getBalanceInfo(Channel& chan);

  /// @brief Get id of wallet user
  /// @return User id
  std::string getUserId();

  /// @brief Request registration of the current user
  void registerUser(Channel& chan);

  /// @brief Requests the minting of public funds to private funds.
  /// @param amount the amount of public funds
  void mint(Channel& chan, uint64_t amount);

  /// @brief Transfers the desired amount anonymously to the recipient
  /// @param amount The amount to transfer
  /// @param recipient The user id of the recipient
  void transfer(Channel& chan, uint64_t amount, const std::string& recipient, const std::string& recipient_public_key);

  /// @brief  Transfer public (ERC20) funds from wallet to address.
  /// @param amount The amount of public funds to transfer
  /// @param recipient The user id of the recipient
  void publicTransfer(Channel& chan, uint64_t amount, const std::string& recipient);

  /// @brief Burns the desired amount of private funds and converts them to public funds.
  /// @param amount The amount of private funds to burn.
  void burn(Channel& chan, uint64_t amount);

  void debugOutput() const;

 private:
  /// @brief Sync up to the last known tx number. If the last known tx number is zero (or not provided) the
  /// last signed transaction number will be fetched from the system
  void syncState(Channel& chan, uint64_t lastKnownTxNum = 0);
  void updateBudget(Channel& chan);

  /// @brief Updates the ERC20 balance of the wallet.
  void updatePublicBalance(Channel& chan);

  std::string userId_;
  std::unique_ptr<utt::client::User> user_;
  uint64_t publicBalance_ = 0;
  bool registered_ = false;
  std::string public_key_;
  std::string private_key_;
  std::shared_ptr<CliCustomPersistentStorage> cli_storage_;
};