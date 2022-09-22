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

#include <memory>

namespace libutt::api {
class PublicConfig;
class Configuration;
}  // namespace libutt::api

std::ostream& operator<<(std::ostream& out, const libutt::api::PublicConfig& config);
std::istream& operator>>(std::istream& in, libutt::api::PublicConfig& config);

std::ostream& operator<<(std::ostream& out, const libutt::api::Configuration& config);
std::istream& operator>>(std::istream& in, libutt::api::Configuration& config);

namespace libutt::api {
/// @brief The set of public parameters visible to all users of the system
class PublicConfig {
 public:
  PublicConfig();
  ~PublicConfig();

  PublicConfig(PublicConfig&& o);
  PublicConfig& operator=(PublicConfig&& o);

  bool operator==(const PublicConfig& o);
  bool operator!=(const PublicConfig& o);

  std::string getCommitVerificationKey() const;
  std::string getRegistrationVerificationKey() const;

  // [TODO-UTT] More getters if needed

 private:
  friend class Configuration;
  friend std::ostream& ::operator<<(std::ostream& out, const libutt::api::PublicConfig& config);
  friend std::istream& ::operator>>(std::istream& in, libutt::api::PublicConfig& config);

  struct Impl;
  std::unique_ptr<Impl> pImpl_;
};

/// @brief The complete configuration of a UTT Instance, including the PublicConfig
class Configuration {
 public:
  Configuration();
  /// @brief Constructs a UTT instance configuration
  /// @param n The number of participants for multiparty signature computation
  /// @param t The number of participant shares required to reconstruct a signature
  Configuration(size_t n, size_t t);
  ~Configuration();

  Configuration(Configuration&& o);
  Configuration& operator=(Configuration&& o);

  bool operator==(const Configuration& o);
  bool operator!=(const Configuration& o);

  bool isValid() const;

  size_t getNumParticipants() const;
  size_t getThreshold() const;
  const PublicConfig& getPublicConfig() const;

  std::string getCommitSecret(size_t idx) const;
  std::string getRegistrationSecret(size_t idx) const;

  // [TODO-UTT] More getters if needed

 private:
  friend std::ostream& ::operator<<(std::ostream& out, const libutt::api::Configuration& config);
  friend std::istream& ::operator>>(std::istream& in, libutt::api::Configuration& config);
  struct Impl;
  std::unique_ptr<Impl> pImpl_;
};
}  // namespace libutt::api