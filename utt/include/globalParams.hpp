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
namespace libutt {
class Params;
}
namespace libutt::api {
class GlobalParams {
  /**
   * @brief Represents a shared known global UTT params. These parameters includes commitment keys, nullifier parametrs
   * and more (see uttlib/Params.h)
   *
   */
  static bool initialized;

 public:
  GlobalParams() = default;
  /**
   * @brief Initialize the global parameters object with a default object.
   *
   */
  void init();

  /**
   * @brief Get the libutt::Params object
   *
   * @return const libutt::Params&
   */
  const libutt::Params& getParams() const;
  libutt::Params& getParams();
  GlobalParams(const GlobalParams& other);
  GlobalParams& operator=(const GlobalParams& other);

 private:
  std::unique_ptr<libutt::Params> params;
};
}  // namespace libutt::api
