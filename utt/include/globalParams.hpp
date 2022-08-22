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
   * @brief Represents a shared known global UTT params. These parametesr include commitment keys, nullifier parameters
   * and more (see uttlib/Params.h)
   *
   */

 public:
  struct BaseLibsInitData {
    std::string ntl_finite_field{"21888242871839275222246405745257275088548364400416034343698204186575808495617"};
    bool libff_inhibit_profiling_info{true};
    bool libff_inhibit_profiling_counters{true};
    std::pair<unsigned char*, int> entropy_source{nullptr, false};
  };
  GlobalParams() = default;
  /**
   * @brief Initialize the global parameters object with a default object.
   *
   */
  static GlobalParams create(void* initData);
  static void initLibs(const BaseLibsInitData& initData);

  /**
   * @brief Get the libutt::Params object
   *
   * @return const libutt::Params&
   */
  const libutt::Params& getParams() const;
  GlobalParams(const GlobalParams& other);
  GlobalParams& operator=(const GlobalParams& other);

 private:
  std::unique_ptr<libutt::Params> params;
};
}  // namespace libutt::api
