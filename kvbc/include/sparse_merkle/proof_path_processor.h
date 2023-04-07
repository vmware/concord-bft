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

bool verifyProofPath(concordUtils::Sliver key,
                     concordUtils::Sliver value,
                     const std::vector<Hash>& proofPath,
                     const Hash& rootHash);
std::vector<Hash> getProofPath(concordUtils::Sliver key,
                               std::shared_ptr<IDBReader> db,
                               const std::string& custom_prefix = "");

}  // namespace proof_path_processor
}  // namespace sparse_merkle
}  // namespace kvbc
}  // namespace concord
