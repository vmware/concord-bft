// Concord
//
// Copyright (c) 2019-2020 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the sub-component's license, as noted in the
// LICENSE file.

// All freestanding functions must be in a source file

#include "sparse_merkle/base_types.h"

namespace concord::kvbc::sparse_merkle {

std::ostream& operator<<(std::ostream& os, const Version& version) {
  os << version.value();
  return os;
}

std::ostream& operator<<(std::ostream& os, const Nibble& nibble) {
  os << nibble.hexChar();
  return os;
}

std::ostream& operator<<(std::ostream& os, const Hash& hash) {
  for (size_t i = 0; i < Hash::MAX_NIBBLES; i++) {
    os << hash.getNibble(i).hexChar();
  }
  return os;
}

std::ostream& operator<<(std::ostream& os, const NibblePath& path) {
  for (size_t i = 0; i < path.length(); i++) {
    os << path.get(i).hexChar();
  }
  return os;
}

}  // namespace concord::kvbc::sparse_merkle
