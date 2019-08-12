// Copyright 2018 VMware, all rights reserved
//
// Hash functions for our Sliver and KeyValuePair types.

#ifndef CONCORD_BFT_UTIL_HASH_DEFS_H_
#define CONCORD_BFT_UTIL_HASH_DEFS_H_

#include <stdlib.h>
#include "sliver.hpp"
#include "kv_types.hpp"

namespace concordUtils {

// TODO(GG): do we want this hash function ? See also
// http://www.cse.yorku.ca/~oz/hash.html
inline size_t simpleHash(const uint8_t *data, const size_t len) {
  size_t hash = 5381;
  size_t t;

  for (size_t i = 0; i < len; i++) {
    t = data[i];
    hash = ((hash << 5) + hash) + t;
  }

  return hash;
}

} // namespace concordUtils

namespace std {
template <>
struct hash<concordUtils::Sliver> {
  typedef concordUtils::Sliver argument_type;
  typedef std::size_t result_type;

  result_type operator()(const concordUtils::Sliver &t) const {
    return concordUtils::simpleHash(t.data(), t.length());
  }
};

template <>
struct hash<concordUtils::KeyValuePair> {
  typedef concordUtils::KeyValuePair argument_type;
  typedef std::size_t result_type;

  result_type operator()(const concordUtils::KeyValuePair &t) const {
    size_t keyHash = concordUtils::simpleHash(t.first.data(), t.first.length());
    return keyHash;
  }
};
}  // namespace std

#endif  // CONCORD_BFT_UTIL_HASH_DEFS_H_
