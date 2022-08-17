// UTT
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
#include "client.hpp"
#include "coin.hpp"
#include "globalParams.hpp"
#include "coinsSigner.hpp"
#include "client.hpp"
#include <memory>
#include <optional>
#include <vector>
namespace libutt {
class Tx;
class IEncryptor;
}  // namespace libutt

namespace libutt::api::operations {
class Transaction {
  /**
   * @brief A transaction represents a transfer of UTT coin(s) from a sender accound c1 to other accoun(s) [c2, ...,
   * cn]. Notice, that a self transaction is also valid
   *
   */
 public:
  /**
   * @brief Construct a new Transaction object
   *
   * @param p The shared global UTT parametrs
   * @param client The client that creates the transaction
   * @param input_coins The transaction's input UTT coins
   * @param budget_coin An optionsl budget coin (self transaction doesn't require a budget coin)
   * @param recipients A vector of receipents, each repcient is represented as <string id,numeric id>
   * @param encryptor An obeject that responsible for encrypting some of the transaction data for the given recipients
   * (see libutt/include/DataUtils.h)
   */
  Transaction(const GlobalParams& p,
              const Client& client,
              const std::vector<Coin>& input_coins,
              const std::optional<Coin>& budget_coin,
              const std::vector<std::tuple<std::string, uint64_t>>& recipients,
              const IEncryptor& encryptor);
  /**
   * @brief Get the transaction's nullifiers
   *
   * @return std::vector<std::string>
   */
  std::vector<std::string> getNullifiers() const;

  /**
   * @brief Get the transaction's input coins
   *
   * @return const std::vector<Coin>&
   */
  const std::vector<Coin>& getInputCoins() const;

  /**
   * @brief Get the transaction's budget coin
   *
   * @return std::optional<Coin>
   */
  std::optional<Coin> getBudgetCoin() const;

 private:
  friend class libutt::api::CoinsSigner;
  friend class libutt::api::Client;
  std::shared_ptr<libutt::Tx> tx_;
  std::vector<Coin> input_coins_;
  std::optional<Coin> budget_coin_;
};
}  // namespace libutt::api::operations