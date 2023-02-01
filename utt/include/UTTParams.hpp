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
namespace libutt::api {
class UTTParams;
class Coin;
class Utils;
class Commitment;
class CoinsSigner;
class Configuration;

namespace operations {
class Transaction;
class Burn;
}  // namespace operations
}  // namespace libutt::api
std::ostream& operator<<(std::ostream& out, const libutt::api::UTTParams& params);
std::istream& operator>>(std::istream& in, libutt::api::UTTParams& params);
bool operator==(const libutt::api::UTTParams& params1, const libutt::api::UTTParams& params2);

namespace libutt::api {
class UTTParams {
  /**
   * @brief Represents a shared known UTT instance params. These parameters include commitment keys, nullifier
   * parameters and more (see uttlib/Params.h)
   *
   */

 public:
  struct BaseLibsInitData {
    bool libff_inhibit_profiling_info{true};
    bool libff_inhibit_profiling_counters{true};
    std::pair<unsigned char*, int> entropy_source{nullptr, false};
  };
  UTTParams() = default;
  /**
   * @brief Initialize the global parameters object with a default object.
   *
   */
  static UTTParams create(void* initData);
  static void initLibs(const BaseLibsInitData& initData);

  /**
   * @brief Get the libutt::Params object
   *
   * @return const libutt::Params&
   */
  UTTParams(const UTTParams& other);
  UTTParams& operator=(const UTTParams& other);
  ~UTTParams();
  UTTParams(UTTParams&&);
  UTTParams& operator=(UTTParams&&);

 private:
  friend std::ostream& ::operator<<(std::ostream& out, const libutt::api::UTTParams& params);
  friend std::istream& ::operator>>(std::istream& in, libutt::api::UTTParams& params);
  friend bool ::operator==(const libutt::api::UTTParams& params1, const libutt::api::UTTParams& params2);
  friend class Coin;
  friend class Utils;
  friend class Commitment;
  friend class CoinsSigner;
  friend class Configuration;
  friend class operations::Transaction;
  friend class operations::Burn;
  struct Impl;
  Impl* pImpl_;

 public:
  // For testing only
  const Impl* getImpl() const;
};
}  // namespace libutt::api
