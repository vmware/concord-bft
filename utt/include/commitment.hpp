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
#include "UTTParams.hpp"
#include "types.hpp"
#include <vector>
#include <cstdint>
#include <optional>

namespace libutt {
class Comm;
class CommKey;
}  // namespace libutt
namespace libutt::api {
class Commitment;
}
libutt::api::Commitment operator+(libutt::api::Commitment lhs, const libutt::api::Commitment& rhs);
namespace libutt::api {
class Registrator;
class Client;
namespace operations {
class Burn;
class Transaction;
}  // namespace operations
class Commitment {
  /**
   * @brief Commitment is a cryptographic object that allows one to commit a value without exposing the value itself.
   * The commitment then can be manipulated with ZK techniques
   *
   */
 public:
  enum Type { REGISTRATION = 0, COIN };
  /**
   * @brief Get the Commitment Key object for the given commitment type
   *
   * @param p The shared global UTT parameters
   * @param t The commitment type
   * @return const libutt::CommKey&
   */
  static const libutt::CommKey& getCommitmentKey(const UTTParams& p, Type t);

  /**
   * @brief Construct a new Commitment object
   *
   * @param p The shared global UTT parameters
   * @param t The commitment type
   * @param messages A vector if messages we want to commit on (given as CurvePoints)
   * @param withG2 Indicates if we want to have the commitment in the G2 group. In the regular case this should be
   * always true
   */
  Commitment(const UTTParams& p, Type t, const std::vector<types::CurvePoint>& messages, bool withG2);
  Commitment(const Commitment& comm);
  Commitment();
  Commitment& operator=(const Commitment&);

  /**
   * @brief A + operator for multiplexing two commitments. Given two commitments c1 and c2 and their signature s1, s2.
   * then c3 = c1 + c2 and s3 = s1 + s2
   *
   * @return Commitment&
   */
  Commitment& operator+=(const Commitment&);

  /**
   * @brief Re-randomize the commitment (possibly based on a given base_randomness)
   *
   * @param p The shared global UTT parameters
   * @param t The commitment type
   * @param base_randomness An optional based randomness
   * @return types::CurvePoint
   */
  types::CurvePoint rerandomize(const UTTParams& p, Type t, std::optional<types::CurvePoint> base_randomness);

 private:
  friend class Registrator;
  friend class Client;
  friend class operations::Burn;
  friend class operations::Transaction;
  std::unique_ptr<libutt::Comm> comm_;
};
}  // namespace libutt::api