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
#include "types.hpp"
#include <string>
namespace libutt::api::operations {
/**
 * @brief
 * The Budget is used for creating an initial budget coin. This can ber viewed as a mint operation for budget coin.
 **/
class Budget {
 public:
  /**
   * @brief Construct a new Budget object by the client (including a nullfier)
   *
   * @param p The shared global UTT parametrs
   * @param cid The client who creates the budget coin
   * @param val The budget value
   * @param exp_date Expiration date in the format of YYYY-mm-dd HH:MM::SS, for example, 2100-12-10 09:12:34
   */
  Budget(const GlobalParams& p, const libutt::api::Client& cid, uint64_t val, const std::string& exp_date);

  /**
   * @brief Construct a new Budget object without the client secret data. For building a budget coin by the bank
   *
   * @param p The shared global UTT parametrs
   * @param pidHash The public client ID hash
   * @param val The budget value
   * @param exp_date Expiration date in the format of YYYY-mm-dd HH:MM::SS, for example, 2100-12-10 09:12:34
   */
  Budget(const GlobalParams& p, const types::CurvePoint& pidHash, uint64_t val, const std::string& exp_date);

  /**
   * @brief Get the Coin object
   *
   * @return libutt::api::Coin& The budget coin notice that having the budget coin doesn't necessarly mean that it also
   * has the signature.
   */
  libutt::api::Coin& getCoin();
  const libutt::api::Coin& getCoin() const;

  /**
   * @brief Get the Hash of new budget operation. This hash serves as the basis of the coin serial number.
   *
   * @return std::string The hash in HEX format
   */
  std::string getHashHex() const;

 private:
  libutt::api::Coin coin_;
};
}  // namespace libutt::api::operations
