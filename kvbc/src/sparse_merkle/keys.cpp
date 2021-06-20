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

#include "sparse_merkle/keys.h"

namespace concord::kvbc::sparse_merkle {

std::ostream& operator<<(std::ostream& os, const LeafKey& key) {
  os << key.hash() << "-" << key.version();
  return os;
}

}  // namespace concord::kvbc::sparse_merkle
