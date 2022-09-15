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
#include "UTTParams.hpp"
#include "coinsSigner.hpp"
#include <memory>
namespace libutt {
class BurnOp;
}

namespace libutt::api::operations {
class Burn;
}

std::ostream& operator<<(std::ostream& out, const libutt::api::operations::Burn& burn);
std::istream& operator>>(std::istream& in, libutt::api::operations::Burn& burn);
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
   * @param p The shared global UTT parameters
   * @param cid The client object, notice that the Burn request can be created only by the owner of the coin.
   * @param coin_to_burn The coin we want to burn
   */
  Burn(const UTTParams& p, const Client& cid, const Coin& coin_to_burn);
  Burn();
  Burn(const Burn& other);
  Burn& operator=(const Burn& other);
  Burn(Burn&& other);
  Burn& operator=(Burn&& other);
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
  friend std::ostream& ::operator<<(std::ostream& out, const libutt::api::operations::Burn& burn);
  friend std::istream& ::operator>>(std::istream& in, libutt::api::operations::Burn& burn);
  std::unique_ptr<libutt::BurnOp> burn_{nullptr};
  Coin c_;
};
}  // namespace libutt::api::operations