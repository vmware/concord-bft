// Concord
//
// Copyright (c) 2022-2023 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include "blockchain_misc.hpp"
#include "storage/merkle_tree_key_manipulator.h"

namespace concord::kvbc::bcutil {

static const auto kPublicStateHashKey = concord::storage::v2MerkleTree::detail::serialize(
    concord::storage::v2MerkleTree::detail::EBFTSubtype::PublicStateHashAtDbCheckpoint);

std::string BlockChainUtils::publicStateHashKey() { return kPublicStateHashKey; }

}  // end of namespace concord::kvbc::bcutil
