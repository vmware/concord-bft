// UTT Client API
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
#include <vector>

namespace utt::client {

/// @brief Interface that provides the means to generate a public/private key pair given a userId
struct IUserPKInfrastructure {
  struct KeyPair {
    KeyPair() = default;
    KeyPair(std::string sk, std::string pk) : sk_{std::move(sk)}, pk_{std::move(pk)} {}
    std::string sk_;
    std::string pk_;
  };

  /// @brief Generate a private/public key pair for a user
  /// @param userId The user's id (used optionally)
  /// @return The generate key pair
  virtual KeyPair generateKeys(const std::string& userId) = 0;

  virtual ~IUserPKInfrastructure() = default;
};

// A simple implementation of IUserPKInfrastructure with several pre-generated
// user ids and their RSA private/public keys.
struct TestUserPKInfrastructure : public IUserPKInfrastructure {
  /// @brief Get the list of valid user-ids for which we have pre-generated keys
  std::vector<std::string> getUserIds() const;

  /// @brief Convenience method to get the pre-generated public key of a user
  /// @param userId A userId from the list returned by getUserIds()
  /// @return The user's public key
  const std::string& getPublicKey(const std::string& userId) const;

  /// @brief Returns pre-generated RSA keys
  /// @param userId A userId from the list returned by getUserIds()
  /// @return the
  KeyPair generateKeys(const std::string& userId) override;
};

}  // namespace utt::client