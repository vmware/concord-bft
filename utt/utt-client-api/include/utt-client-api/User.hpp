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

// Provides the means to store the user's data
struct IUserStorage {};

// Provides the means to generate a public/private key pair when creating a user
struct IUserPKInfrastructure {
  struct KeyPair {
    KeyPair(std::string sk, std::string pk) : sk_{std::move(sk)}, pk_{std::move(pk)} {}
    std::string sk_;
    std::string pk_;
  };

  /// @brief Generate a private/public key pair for a user
  /// @param userId The user's id (used optionally)
  /// @return The generate key pair
  virtual KeyPair generateKeys(const std::string& userId) = 0;

  virtual ~IUserPKInfrastructure() = default;
};

class User {
 public:
  User();  // Default empty user object
  ~User();

  /// @brief Creates an input registration commitment. Multiple calls generate the same object.
  /// @return The user's registration input object
  UserRegistrationInput getRegistrationInput() const;

  /// @brief Updates the registration for the user using data computed by the system
  /// @param pk The system sent public key for the registration (must be equal to the user's public key)
  /// @param rs A signature on the user's registration
  /// @param s2 A system generated part of the user's nullifier secret key
  /// @return True if the registration is accepted by the user
  bool updateRegistration(const std::string& pk, const utt::RegistrationSig& rs, const utt::S2& s2);

  /// @brief Updates the privacy budget of the user
  /// @param token The budget token object
  /// @param sig The budget token signature
  /// @return True if the budget token is accepted
  bool updatePrivacyBudget(const utt::PrivacyBudget& budget, const utt::PrivacyBudgetSig& sig);

  /**
   * @brief Get the total value of unspent UTT tokens
   */
  uint64_t getBalance() const;

  /**
   * @brief Get the value of the currently available privacy budget
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

  /// @brief Update the user's state with the effects of a transfer transaction
  /// @param txNum The transaction number
  /// @param tx A transfer transaction
  /// @param sigs The signatures on the transaction outputs
  /// @return
  bool updateTransferTx(uint64_t txNum, const utt::Transaction& tx, const utt::TxOutputSigs& sigs);

  /// @brief Update the user's state with the effects of a mint transaction
  /// @param txNum The transaction number
  /// @param tx A mint transaction
  /// @param sig The signature on the transaction output (we assume a mint tx has a single output)
  /// @return
  bool updateMintTx(uint64_t txNum, const utt::Transaction& tx, const utt::TxOutputSig& sig);

  /// @brief Update the user's state with the effects of a burn transaction
  /// @param txNum The transaction number
  /// @param tx A burn transaction
  /// @return
  bool updateBurnTx(uint64_t txNum, const utt::Transaction& tx);

  /// @brief The user records the tx as a no-op and skips it
  /// @param txNum
  bool updateNoOp(uint64_t txNum);

  /**
   * @brief Ask to burn some amount of tokens. This function needs to be called repeatedly until the final burn
   * transaction is produced.
   *
   * @param amount The amount of tokens to burn.
   *
   * @return Returns a valid UTT transaction to burn or merge/split UTT tokens or an error.
   *
   */
  utt::Transaction burn(uint64_t amount) const;

  /**
   * @brief Ask to transfer some amount of tokens. This function needs to be called repeatedly until the final transfer
   * transaction is produced.
   *
   * @param amount The amount of tokens to transfer.
   *
   * @return Returns a valid UTT transaction to transfer or merge/split UTT tokens or an error.
   *
   */
  utt::Transaction transfer(const std::string& userId, const std::string& destPK, uint64_t amount) const;

 private:
  // Users can be created only by the top-level ClientApi functions
  friend std::unique_ptr<User> createUser(const std::string& userId,
                                          const utt::PublicConfig& config,
                                          IUserPKInfrastructure& pki,
                                          IUserStorage& storage);

  friend std::unique_ptr<User> loadUserFromStorage(IUserStorage& storage);

  static std::unique_ptr<User> createInitial(const std::string& userId,
                                             const utt::PublicConfig& config,
                                             IUserPKInfrastructure& pki,
                                             IUserStorage& storage);

  static std::unique_ptr<User> createFromStorage(IUserStorage& storage);

  struct Impl;
  std::unique_ptr<Impl> pImpl_;
};

}  // namespace utt::client
