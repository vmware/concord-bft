// UTT Pick Tokens
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

#include <vector>
#include <unordered_map>
#include <cstdint>
#include <string>
namespace libutt::api {
class Coin;
}  // namespace libutt::api

namespace utt::client {

/// @brief A strategy to pick coins to satisfy a target amount to be transferred or burned
/// @param coins The available coins to pick from
/// @param amount The target amount
/// @return Up to two coins that satisfy the amount (preferring exact matches) or need to be merged
std::vector<std::string> PickCoinsPreferExactMatch(const std::unordered_map<std::string, libutt::api::Coin>& coins,
                                                   uint64_t targetAmount);

}  // namespace utt::client
