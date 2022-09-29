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

#include "utt-client-api/PickCoins.hpp"

#include <algorithm>

// new libutt api
#include <coin.hpp>

namespace utt::client {

std::vector<size_t> PickCoinsPreferExactMatch(const std::vector<libutt::api::Coin>& coins, uint64_t targetAmount) {
  // Precondition: 0 < targetAmount <= balance
  if (targetAmount == 0) throw std::runtime_error("Target amount cannot be zero!");
  uint64_t sum = 0;
  for (const auto& coin : coins) {
    sum += coin.getVal();
  }
  if (targetAmount > sum)
    throw std::runtime_error("Expected targetAmount to be less than or equal to the value of the coins!");

  // Prefer exactly matching coins (using sorted coins)
  //
  // (1) look for a single coin where value >= k, an exact coin will be preferred
  // (2) look for two coins with total value >= k, an exact sum will be preferred
  // (3) no two coins sum up to k, do a merge on the largest two coins

  // Example 1 (1 coin match):
  // Target Amount: 5
  // Wallet: [2, 3, 4, 4, 7, 8]
  //
  // (1) find lower bound (LB) of 5: [2, 3, 4, 4, 7, 8] --> found a coin >= 5
  //                                              ^
  // (2) Use a single coin of 7
  // Note: If we prefer to use two exact coins (if possible) we can go to example 2
  // by considering the subrange [2, 3, 4, 4] and skip step (1) of example 2.

  // Example 2 (2 coin matches):
  // Target Amount: 5
  // Wallet: [2, 3, 4, 4]
  //
  // (1) find lower bound (LB) of 5: [2, 3, 4, 4, end] -- we don't have a single coin candidate
  //                                               ^
  // (2) search for pairs >= 5
  // [2, 3, 4, 4]
  //  l        h      l+h = 6 found a match -- save and continue iterating
  //  l     h         l+h = 6 found a match -- ignore, already has a match
  //  l  h            l+h = 5 found exact match -- save and break (we prefer the exact match)
  //  Termination: l == h
  //
  // (3) Use exact match
  // Note: We use exact match over an inexact match and if neither exists we merge the
  // top two coins and try again.

  using CoinRef = std::pair<size_t, size_t>;  // [coinValue, coinIdx]

  std::vector<CoinRef> aux;

  for (size_t i = 0; i < coins.size(); ++i) {
    aux.emplace_back(coins[i].getVal(), i);
  }

  auto cmpCoinValue = [](const CoinRef& lhs, const CoinRef& rhs) { return lhs.first < rhs.first; };
  std::sort(aux.begin(), aux.end(), cmpCoinValue);

  auto lb = std::lower_bound(aux.begin(), aux.end(), CoinRef{targetAmount, -1}, cmpCoinValue);
  if (lb != aux.end()) {
    // We can pay with one coin (>= targetAmount)
    return std::vector<size_t>{lb->second};
  } else {  // Try to pay with two coins
    // We know that our balance is enough and no coin is >= payment (because lower_bound == end)
    // Then we may have two coins that satisfy the payment
    // If we don't have two coins we must merge
    size_t low = 0;
    size_t high = aux.size() - 1;
    std::optional<std::pair<size_t, size_t>> match;
    std::optional<std::pair<size_t, size_t>> exactMatch;

    while (low < high) {
      const auto sum = aux[low].first + aux[high].first;
      if (sum == targetAmount) {
        exactMatch = std::make_pair(aux[low].second, aux[high].second);
        break;  // exact found
      } else if (sum > targetAmount) {
        // found a pair (but we want to look for an exact match)
        if (!match) match = std::make_pair(aux[low].second, aux[high].second);
        --high;
      } else {
        ++low;
      }
    }

    // Prefer an exact match over an inexact
    if (exactMatch) {
      return std::vector<size_t>{exactMatch->first, exactMatch->second};
    } else if (match) {
      return std::vector<size_t>{match->first, match->second};
    }
  }

  // At this point no one or two coins are sufficient for the target amount
  // so we merge the top two coins
  const auto lastIdx = aux.size() - 1;
  return std::vector<size_t>{aux[lastIdx - 1].second, aux[lastIdx].second};
}

}  // namespace utt::client