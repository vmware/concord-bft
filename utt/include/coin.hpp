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
#include "client.hpp"
#include "commitment.hpp"
#include "types.hpp"
#include <memory>
#include <optional>
#include <iostream>

namespace libutt {
class Coin;
namespace api {
class Coin;
}
}  // namespace libutt

std::ostream& operator<<(std::ostream& out, const libutt::api::Coin& coin);
std::istream& operator>>(std::istream& in, libutt::api::Coin& coin);
namespace libutt::api {
class Client;
namespace operations {
class Burn;
class Mint;
class Transaction;
class Budget;
}  // namespace operations
class Coin {
  /**
   * @brief Represent a UTT coin. A coin can be either a normal value coin or a budget coin. A budget coin is a coin
   * that limits the amount of anonymous transferred tokens.
   *
   */
 public:
  enum Type : uint16_t { Normal = 0x0, Budget };
  /**
   * @brief Constructs a new Coin object. This constructor also construct the coin's nullifier. Hence it can be invoked
   * only by the client.
   *
   * @param p The shared global UTT parameters
   * @param prf The secret client's PRF key
   * @param serial_number The coin's serial number (determined by the bank). The serial number is not a secret.
   * @param val The coin value
   * @param client_id_hash The client ID hash (as a CurvePoint)
   * @param t The coin's type (wither Normal or Budget)
   * @param expiration_date Expiration date given as CurvePoint, in case of a Normal coin, the expiration_date is
   * ignored
   */
  Coin(const UTTParams& p,
       const types::CurvePoint& prf,
       const types::CurvePoint& serial_number,
       const types::CurvePoint& val,
       const types::CurvePoint& client_id_hash,
       Type t,
       const types::CurvePoint& expiration_date);
  /**
   * @brief Construct a new Coin object without the nullifier (can be constructed not only by the client)
   *
   * @param p The shared global UTT parameters
   * @param serial_number The coin's serial number (determined by the bank). The serial number is not a secret.
   * @param val The coin value
   * @param client_id_hash The client ID hash (as a CurvePoint)
   * @param t The coin's type (either Normal or Budget)
   * @param expiration_date Expiration date given as CurvePoint, in case of a Normal coin, the expiration_date is
   * ignored
   */
  Coin(const UTTParams& p,
       const types::CurvePoint& serial_number,
       const types::CurvePoint& val,
       const types::CurvePoint& client_id_hash,
       Type t,
       const types::CurvePoint& expiration_date,
       bool finalize = true);
  Coin();
  Coin(const Coin& c);
  Coin& operator=(const Coin& c);
  Coin(Coin&& c) = default;
  Coin& operator=(Coin&& c) = default;

  /**
   * @brief Get the coin's Nullifier as a string
   *
   * @return std::string
   */
  std::string getNullifier() const;

  /**
   * @brief Create a Nullifier for this coin. Only the client can invoke this method
   *
   * @param p The shared global UTT parameters
   * @param prf The secret client's PRF key
   */
  void createNullifier(const UTTParams& p, const types::CurvePoint& prf);
  void finalize(const UTTParams& p, const types::CurvePoint& prf);

  /**
   * @brief Check if this coin has a signature associated with it
   *
   * @return true if there is a signature
   * @return false if not
   */
  bool hasSig() const;

  /**
   * @brief Set the coin'd signature. The signature is computed by the bank.
   *
   * @param sig
   */
  void setSig(const types::Signature& sig);

  /**
   * @brief Get the coin's type
   *
   * @return Type
   */
  Type getType() const;

  /**
   * @brief Get the coin's signature
   *
   * @return types::Signature
   */
  types::Signature getSig() const;

  /**
   * @brief Re-randomize the coin's signature with the coin's randomness
   *
   * @param base_randomness An optional base randomness to be used for the re-randomization (not be used in the regular
   * case)
   */
  void rerandomize(std::optional<types::CurvePoint> base_randomness);

  /**
   * @brief Get the coin's value
   *
   * @return uint64_t
   */
  uint64_t getVal() const;

  /**
   * @brief Get the client's id hash
   *
   * @return types::CurvePoint
   */
  types::CurvePoint getPidHash() const;

  /**
   * @brief Get the coin's serial number
   *
   * @return types::CurvePoint
   */
  types::CurvePoint getSN() const;

  /**
   * @brief Get the coin's expiration date as a CurvePoint
   *
   * @return types::CurvePoint
   */
  types::CurvePoint getExpDateAsCurvePoint() const;

  uint64_t getExpDate() const;

 private:
  friend class Client;
  friend class operations::Burn;
  friend class operations::Transaction;
  friend class operations::Budget;
  friend std::ostream& ::operator<<(std::ostream&, const libutt::api::Coin&);
  friend std::istream& ::operator>>(std::istream&, libutt::api::Coin&);
  std::unique_ptr<libutt::Coin> coin_;
  bool has_sig_{false};

  Type type_;
};
}  // namespace libutt::api