// Concord
//
// Copyright (c) 2020 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the
// "License").  You may not use this product except in compliance with the
// Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include "storage/merkle_tree_key_manipulator.h"

#include "endianness.hpp"
#include "storage/db_types.h"

#include <string>

namespace {

using namespace ::concord::storage::v2MerkleTree::detail;
using concordUtils::toBigEndianStringBuffer;

}  // namespace

namespace concord::storage::v2MerkleTree {

using ::concordUtils::Sliver;

Sliver MetadataKeyManipulator::generateMetadataKey(ObjectId objectId) const {
  return serialize(EBFTSubtype::Metadata) + toBigEndianStringBuffer(objectId);
}

Sliver STKeyManipulator::generateStateTransferKey(ObjectId objectId) const {
  return serialize(EBFTSubtype::ST) + toBigEndianStringBuffer(objectId);
}

Sliver STKeyManipulator::generateSTPendingPageKey(uint32_t pageid) const {
  return serialize(EBFTSubtype::STPendingPage) + toBigEndianStringBuffer(pageid);
}

Sliver STKeyManipulator::generateSTCheckpointDescriptorKey(uint64_t chkpt) const {
  return serialize(EBFTSubtype::STCheckpointDescriptor) + toBigEndianStringBuffer(chkpt);
}

Sliver STKeyManipulator::generateSTReservedPageDynamicKey(uint32_t pageId, uint64_t chkpt) const {
  return serialize(EBFTSubtype::STReservedPageDynamic) + toBigEndianStringBuffer(pageId) +
         toBigEndianStringBuffer(chkpt);
}
Sliver STKeyManipulator::getReservedPageKeyPrefix() const {
  static Sliver s(serialize(EBFTSubtype::STReservedPageDynamic));
  return s;
}

}  // namespace concord::storage::v2MerkleTree
