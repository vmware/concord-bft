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

#pragma once

#include "key_manipulator_interface.h"
#include "string.hpp"

#include <string>

namespace concord::storage::v2MerkleTree {

class MetadataKeyManipulator : public IMetadataKeyManipulator {
 public:
  concordUtils::Sliver generateMetadataKey(ObjectId objectId) const override;
};

class STKeyManipulator : public ISTKeyManipulator {
 public:
  concordUtils::Sliver generateStateTransferKey(ObjectId objectId) const override;
  concordUtils::Sliver generateSTPendingPageKey(uint32_t pageid) const override;
  concordUtils::Sliver generateSTCheckpointDescriptorKey(uint64_t chkpt) const override;
  concordUtils::Sliver generateSTReservedPageDynamicKey(uint32_t pageId, uint64_t chkpt) const override;
  concordUtils::Sliver getReservedPageKeyPrefix() const override;
};

namespace detail {
inline std::string serialize(detail::EBFTSubtype type) {
  return std::string{concord::util::toChar(detail::EDBKeyType::BFT), concord::util::toChar(type)};
}
}  // namespace detail

}  // namespace concord::storage::v2MerkleTree
