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

#include "v4blockchain/detail/merkle_management_layer.h"
#include "v4blockchain/detail/column_families.h"
#include "log/logger.hpp"
#include "v4blockchain/detail/blockchain.h"
#include "rocksdb/details.h"
#include "merkle_builder/EmptyMerkleBuilder.h"
#include "merkle_builder/SyncMerkleBuilder.h"

using namespace concord::kvbc;
namespace concord::kvbc::v4blockchain::detail {

static const int NUM_MAINTAINED_TREE_VERSIONS = 10;

MerkleManagementLayer::MerkleManagementLayer() {
  // TODO: instantiate based on configuration.
  merkleBuilder_ = std::make_unique<EmptyMerkleBuilder>();
  // TODO: get configuration parameters from config manager
  merkleBuilder_->Init(NUM_MAINTAINED_TREE_VERSIONS);
}

}  // namespace concord::kvbc::v4blockchain::detail
