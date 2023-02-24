// Concord
//
// Copyright (c) 2022 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the
// "License").  You may not use this product except in compliance with the
// Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#pragma once

#include "rocksdb/native_client.h"
#include <memory>
#include <unordered_map>
#include "categorization/updates.h"
#include "v4blockchain/detail/categories.h"
#include <rocksdb/compaction_filter.h>
#include "rocksdb/snapshot.h"
#include "merkle_builder/IMerkleBuilder.h"

namespace concord::kvbc::v4blockchain::detail {

using namespace concord::kvbc::sparse_merkle;

class MerkleManagementLayer {
 public:
  MerkleManagementLayer();

  IMerkleBuilder* getBuilder() { return merkleBuilder_.get(); }

 private:
  std::unique_ptr<IMerkleBuilder> merkleBuilder_;
};

}  // namespace concord::kvbc::v4blockchain::detail
