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
#include "coin.hpp"
#include "client.hpp"
#include "globalParams.hpp"
#include "committer.hpp"
#include <memory>
namespace libutt {
class BurnOp;
}
namespace libutt::api::operations {
/**
 * @brief This object represent a Burn operation.
 *
 */
class Burn {
 public:
  /**
   * @brief Construct a new Burn object
   *
   * @param p The shared global UTT parametrs
   * @param cid The client object, notice that the Burn request can be created only by the owner of the coin.
   * @param coin_to_burn The coin we want to burn
   */
  Burn(const GlobalParams& p, const Client& cid, const Coin& coin_to_burn);

  /**
   * @brief Get the Nullifier object, to be used by the bank
   *
   * @return std::string
   */
  std::string getNullifier() const;

  /**
   * @brief Get the Coin object we want to burn.
   *
   * @return const Coin&
   */
  const Coin& getCoin() const;

 public:
  friend class libutt::api::CoinsSigner;
  friend class libutt::api::Client;
  std::unique_ptr<libutt::BurnOp> burn_{nullptr};
  const Coin& c_;
};
}  // namespace libutt::api::operations