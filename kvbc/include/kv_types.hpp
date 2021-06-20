// Copyright (c) 2018-2019 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the
// "License").  You may not use this product except in compliance with the
// Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.
//

#pragma once

#include <string_view>
#include <unordered_map>
#include <map>
#include <set>
#include <vector>
#include <functional>
#include <cstdint>

#include "sliver.hpp"

namespace concord::kvbc {

typedef concordUtils::Sliver Key;
typedef concordUtils::Sliver Value;
typedef concordUtils::Sliver RawBlock;
typedef std::pair<Key, Value> KeyValuePair;
typedef std::unordered_map<Key, Value> SetOfKeyValuePairs;
typedef std::map<Key, Value> OrderedSetOfKeyValuePairs;
typedef std::vector<Key> KeysVector;
typedef std::set<Key> OrderedKeysSet;
typedef KeysVector ValuesVector;
typedef std::uint64_t BlockId;

template <typename ContainerIn>
OrderedSetOfKeyValuePairs order(const ContainerIn& unordered) {
  OrderedSetOfKeyValuePairs out;
  for (auto&& kv : unordered) out.insert(kv);
  return out;
}

}  // namespace concord::kvbc

namespace std {

template <>
struct hash<concord::kvbc::KeyValuePair> {
  std::size_t operator()(const concord::kvbc::KeyValuePair& kv) const noexcept {
    return std::hash<std::string_view>{}(std::string_view(kv.first.data(), kv.first.length()));
  }
};

}  // namespace std
