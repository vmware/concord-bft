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
#include "types.hpp"
#include "UTTParams.hpp"
#include <string>
#include <memory>
#include <vector>
namespace libutt {
class RandSigShareSK;
class RegAuthPK;
class RandSigPK;
}  // namespace libutt
namespace libutt::api {
class CoinsSigner {
  /**
   * @brief The CoinsSigner is the component that is responsible for signing and validating the UTT coin's signatures
   *
   */
 public:
  /**
   * @brief Construct a new Coins Signer object
   *
   * @param id The signer id
   * @param signer_secret_key The signer's secret key serialized as a string
   * @param bank_public_key The bank's public key serialized as string
   * @param registration_public_key The registration service's public key serialized as string
   */
  CoinsSigner(const std::string& id,
              const std::string& signer_secret_key,
              const std::string& bank_public_key,
              const std::string& registration_public_key);

  /**
   * @brief Signing a UTT transaction
   *
   * @tparam T One of <operations::Mint, operations::Budget, operations::Transaction>
   * @param data
   * @return std::vector<types::Signature> A transaction may yield more than one signature (one per coin)
   */
  template <typename T>
  std::vector<types::Signature> sign(T& data) const;

  /**
   * @brief Get the Singer's ID
   *
   * @return const std::string&
   */
  const std::string& getId() const;

  /**
   * @brief Validating UTT transactions. Validating here is only about UTT validation - mostly verifying cryptographic
   * statements
   *
   * @tparam T One of <operations::Burn, operations::Transaction>
   * @param p The shared global UTT parameters
   * @return true if the transcation is valid
   * @return false if not
   */
  template <typename T>
  bool validate(const UTTParams& p, const T&) const;

 private:
  std::string bid_;
  std::unique_ptr<libutt::RandSigShareSK> bsk_;
  std::unique_ptr<libutt::RandSigPK> bvk_;
  std::unique_ptr<libutt::RegAuthPK> rvk_;
};
}  // namespace libutt::api