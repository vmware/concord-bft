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
class Configuration;
}  // namespace libutt::api

std::ostream& operator<<(std::ostream& out, const libutt::api::Configuration& config);
std::istream& operator>>(std::istream& in, libutt::api::Configuration& config);

namespace libutt::api {
class Configuration {
 public:
  Configuration();
  Configuration(size_t n, size_t t);
  ~Configuration();

  Configuration(Configuration&& o);
  Configuration& operator=(Configuration&& o);

  bool operator==(const Configuration& o);
  bool operator!=(const Configuration& o);

  // [TODO-UTT] Public getters for different "generic typed" values of the config

 private:
  friend std::ostream& ::operator<<(std::ostream& out, const libutt::api::Configuration& config);
  friend std::istream& ::operator>>(std::istream& in, libutt::api::Configuration& config);
  struct Impl;
  std::unique_ptr<Impl> pImpl_;
};
}  // namespace libutt::api