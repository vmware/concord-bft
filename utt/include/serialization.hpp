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

#include <iostream>
#include <vector>
#include <sstream>
namespace libutt::api {

template <typename T>
std::vector<uint8_t> serialize(const T& data) {
  std::stringstream ss;
  ss << data;
  const auto& str_data = ss.str();
  return std::vector<uint8_t>(str_data.begin(), str_data.end());
}
template <typename T>
T deserialize(const std::vector<uint8_t>& data) {
  std::stringstream ss;
  std::string str_data(data.begin(), data.end());
  ss.str(str_data);
  T ret;
  ss >> ret;
  return ret;
}
};  // namespace libutt::api