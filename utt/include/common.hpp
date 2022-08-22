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

#include "commitment.hpp"
#include "globalParams.hpp"
#include "types.hpp"
#include <memory>
#include <vector>
#include <map>

namespace libutt {
class CommKey;
}
namespace libutt::api {
class Utils {
  /**
   * @brief Implements some common operations
   *
   */
 public:
  /**
   * @brief Aggreate the shared signatures for a given signature
   *
   * @param n The total number of coinsSigner replicas in the system
   * @param rsigs A map of <signer ID, Signature> of a thershold collected signatures
   * @return types::Signature The combined signature
   */
  static types::Signature aggregateSigShares(uint32_t n, const std::map<uint32_t, types::Signature>& rsigs);

  /**
   * @brief ublined a signature randomness
   *
   * @param p The shared global UTT parametrs
   * @param t The signature type (one of REGISTRATION or COIN)
   * @param randomness A vector of randoms known only by the client
   * @param sig The signature to unblind
   * @return types::Signature
   */
  static types::Signature unblindSignature(const GlobalParams& p,
                                           Commitment::Type t,
                                           const std::vector<types::CurvePoint>& randomness,
                                           const types::Signature& sig);
  /**
   * @brief Convert a string format date to uint.
   *
   * @param exp_date An input string represents a UTC date in the format of %Y-%m-%d %H:%M:%S. For example, 2022-08-17
   * 11:28:32
   * @return uint64_t The encoded date as uint
   */
  static uint64_t getExpirationDateAsUint(const std::string& exp_date);

  /**
   * @brief The reverese of getExpirationDateAsUint method i.e converts a uint date to a string format
   *
   * @param exp_date
   * @return std::string
   */
  static std::string getStrExpirationDateFromUint(uint64_t exp_date);
};
}  // namespace libutt::api