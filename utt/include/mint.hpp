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
#include <string>
#include <memory>
#include <vector>
#include "coin.hpp"
#include "globalParams.hpp"
namespace libutt {
class MintOp;
}
namespace libutt::api {
class CoinsSigner;
class Client;
}  // namespace libutt::api
namespace libutt::api::operations {

class Mint {
  /**
   * @brief The mint operation takes public tokens and convert them to a single UTT coin
   *
   */
 public:
  /**
   * @brief Construct a new Mint object
   *
   * @param uniqueHash The mint transaction hash
   * @param value The required value
   * @param recipPID The recipent id
   */
  Mint(const std::string& uniqueHash, size_t value, const std::string& recipPID);
  std::string getHash() const;
  uint64_t getVal() const;
  std::string getRecipentID() const;

 private:
  friend class libutt::api::CoinsSigner;
  friend class libutt::api::Client;
  std::unique_ptr<libutt::MintOp> op_;
};
}  // namespace libutt::api::operations