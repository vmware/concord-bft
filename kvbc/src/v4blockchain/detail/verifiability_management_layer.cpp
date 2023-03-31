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

#include "v4blockchain/detail/verifiability_management_layer.h"
#include "v4blockchain/detail/column_families.h"
#include "log/logger.hpp"
#include "v4blockchain/detail/blockchain.h"
#include "rocksdb/details.h"
#include "proof_processor/EmptyProofProcessor.h"
#include "merkle_processor/SyncMerkleProcessor.h"

using namespace concord::kvbc;
namespace concord::kvbc::v4blockchain::detail {

static const int NUM_MAINTAINED_TREE_VERSIONS = 10;

VerifiabilityManagementLayer::VerifiabilityManagementLayer() {
  // TODO: instantiate based on configuration.
  proofProcessor_ = std::make_unique<EmptyProofProcessor>();
  // TODO: get configuration parameters from config manager
  proofProcessor_->Init(NUM_MAINTAINED_TREE_VERSIONS);
}

}  // namespace concord::kvbc::v4blockchain::detail
