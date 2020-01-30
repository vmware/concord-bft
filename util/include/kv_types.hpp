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

#ifndef CONCORD_BFT_UTIL_KV_TYPES_H_
#define CONCORD_BFT_UTIL_KV_TYPES_H_

#include <unordered_map>
#include <vector>
#include "sliver.hpp"

namespace concordUtils {

typedef Sliver Key;
typedef Sliver Value;
typedef std::pair<Key, Value> KeyValuePair;
typedef std::unordered_map<Key, Value> SetOfKeyValuePairs;
typedef std::vector<Key> KeysVector;
typedef KeysVector ValuesVector;
typedef uint64_t BlockId;

}  // namespace concordUtils

// Provide hashing for slivers without requiring the user to incldue the header
// and get confused from a weird template error.
//
// Note that this must come after the typedefs above.
#include "hash_defs.h"

#endif  // CONCORD_BFT_UTIL_KV_TYPES_H_
