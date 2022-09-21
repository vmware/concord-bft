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

#include <vector>
#include <string>
#include <variant>
#include <optional>
#include <memory>

#include <utt-common-api/CommonApi.hpp>

namespace utt::client {

struct IUserPKInfrastructure;
struct IUserStorage;

enum class Error {};
struct TransferTx {};
struct MintTx {};
struct BurnTx {};

using Tx = std::variant<TransferTx, BurnTx, MintTx>;
using BurnResult = std::variant<TransferTx, BurnTx, Error>;
using TransferResult = std::variant<TransferTx, Error>;

class User {
 public:
  User();  // Default empty user object
  ~User();

  /**
   * @brief Create the user registration commitment "rcm1".
   *
   * @return rcm1 as a byte array
   */
  std::vector<uint8_t> getRegistrationInput() const;

  /**
   * @brief Use a registration with the following credentials:
   *
   * @param pk The user's public key associated with the registration
   * @param rs The signature on user's registration commitment
   * @param s2 The system-side part of the user's PRF key
   *
   * @return true if the use of the registration is accepted
   */
  bool useRegistration(const std::string& pk, const std::vector<uint8_t>& rs, const std::vector<uint8_t>& s2);

  /**
   * @brief Use a previously created budget coin for anonymous transactions
   *
   * @param budgetCoin The budget coin
   *
   * @return true if the use of the budget coin is accepted
   */
  bool useBudgetCoin(const std::vector<uint8_t>& budgetCoin);

  // [TODO-UTT] Decide if we need a separate interface for setting the budget coin and sig.
  /**
   * @brief Use a signature for a previously created budget coin
   *
   * @param budgetCoin The budget coin
   *
   * @return true if the use of the budget coin signature is accepted
   */
  bool useBudgetCoinSig(const std::vector<uint8_t>& sig);

  /**
   * @brief Get the total value of unspent UTT tokens
   */
  uint64_t getBalance() const;

  /**
   * @brief Get the value of the currently available budget coin
   */
  uint64_t getPrivacyBudget() const;

  /**
   * @brief Get the user's id
   */
  const std::string& getUserId() const;

  /**
   * @brief Get the user's public key
   */
  const std::string& getPK() const;

  /**
   * @brief Get the last executed transaction number
   */
  uint64_t getLastExecutedTxNum() const;

  /**
   * @brief Update the state of the client by applying a new transaction
   *
   * @param txNum The transaction number
   * @param tx The transaction object
   * @param sigs A vector of signatures for each output of the transaction
   *
   * @return true if the transaction was applied to the state
   */
  bool update(uint64_t txNum, const Tx& tx, const std::vector<std::vector<uint8_t>>& sigs);

  /**
   * @brief The client can ignore irrelevant transactions by recording the transaction number as a no-op.
   *
   */
  void update(uint64_t txNum);

  /**
   * @brief Ask to burn some amount of tokens. This function needs to be called repeatedly until the final burn
   * transaction is produced.
   *
   * @param amount The amount of tokens to burn.
   *
   * @return Returns a valid UTT transaction to burn or merge/split UTT tokens or an error.
   *
   */
  BurnResult burn(uint64_t amount) const;

  /**
   * @brief Ask to transfer some amount of tokens. This function needs to be called repeatedly until the final transfer
   * transaction is produced.
   *
   * @param amount The amount of tokens to transfer.
   *
   * @return Returns a valid UTT transaction to transfer or merge/split UTT tokens or an error.
   *
   */
  TransferResult transfer(const std::string& userId, const std::string& destPK, uint64_t amount) const;

 private:
  // Users can be created only by the top-level ClientApi functions
  friend std::unique_ptr<User> createUser(const std::string& userId,
                                          const PublicParams& params,
                                          IUserPKInfrastructure& pki,
                                          IUserStorage& storage);

  friend std::unique_ptr<User> loadUserFromStorage(IUserStorage& storage);

  static std::unique_ptr<User> createInitial(const std::string& userId,
                                             const PublicParams& params,
                                             IUserPKInfrastructure& pki,
                                             IUserStorage& storage);

  static std::unique_ptr<User> createFromStorage(IUserStorage& storage);

  std::unique_ptr<struct Impl> pImpl_;
};

}  // namespace utt::client
