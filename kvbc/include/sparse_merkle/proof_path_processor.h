// Concord
//
// Copyright (c) 2023 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the sub-component's license, as noted in the
// LICENSE file.

#pragma once

#include <vector>
#include <memory>
#include <string>

#include "sparse_merkle/base_types.h"
#include "sparse_merkle/db_reader.h"

namespace concord {
namespace kvbc {
namespace sparse_merkle {
namespace proof_path_processor {

// Verify that a given list of hashes (proofPath) will
// reach the given Merkle root (rootHash) for the
// key/value that we need to check.
bool verifyProofPath(concordUtils::Sliver key,
                     concordUtils::Sliver value,
                     const std::vector<Hash>& proofPath,
                     const Hash& rootHash);

// Extract the elements needed to reach the Merkle root starting from a given key/value,
// effectively building a path in the Merkle tree.
// For example, in the diagram below we will return the vector {Element 0, Element 1, Element 2}.
// The ComputedHash {0-2} can be generated by the verifying entity,
// therefore are not present in the returned value.
//
//       ---------root-------
//       |                  |
// Element 0         ComputedHash 2
//                   /            \
//             Element 1      ComputedHash 1
//                            /            \
//                      Element 2      ComputedHash 0
//                                          |
//                                        Value
std::vector<Hash> getProofPath(concordUtils::Sliver key,
                               std::shared_ptr<IDBReader> db,
                               const std::string& custom_prefix = "");

}  // namespace proof_path_processor
}  // namespace sparse_merkle
}  // namespace kvbc
}  // namespace concord
