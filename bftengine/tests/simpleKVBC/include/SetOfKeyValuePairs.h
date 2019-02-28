// Concord
//
// Copyright (c) 2018 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#pragma once

#include <unordered_set>

#include "PrimitiveTypes.h"

namespace SimpleKVBC {
namespace HelpFuncs {

struct HashKeyValuePair {
  size_t operator()(const SimpleKVBC::KeyValuePair& t) const {
    size_t keyHash = simpleHash(t.first.data, t.first.size);
    return keyHash;
  }

 protected:
  // TODO(GG): TBD - change hash function
  // (see also http://www.cse.yorku.ca/~oz/hash.html)
  static size_t simpleHash(const char* data, const size_t len) {
    size_t hash = 5381;
    size_t t;
    for (size_t i = 0; i < len; i++) {
      t = data[i];
      hash = ((hash << 5) + hash) + t;
    }
    return hash;
  }
};

struct EqualKeyValuePair {
  bool operator()(const KeyValuePair& lhs, const KeyValuePair& rhs) const {
    const bool sameKey = lhs.first == rhs.first;
    return sameKey;
  }
};
}  // namespace HelpFuncs

// SetOfKeyValuePairs
// In this set each key is unique (see HelpFuncs::EqualKeyValuePair)
class SetOfKeyValuePairs
    : public std::unordered_set<KeyValuePair,
                                HelpFuncs::HashKeyValuePair,
                                HelpFuncs::EqualKeyValuePair> {};
}  // namespace SimpleKVBC
