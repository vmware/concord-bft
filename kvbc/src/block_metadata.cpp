// Copyright 2019 VMware, all rights reserved
//
// Wrapper used by concord::consensus::ConcordCommandsHandler to store BFT
// metadata (sequence number).

#include "block_metadata.hpp"

#include "assertUtils.hpp"
#include "endianness.hpp"

namespace concord {
namespace kvbc {

std::string BlockMetadata::serialize(uint64_t bft_sequence_num) const {
  return concordUtils::toBigEndianStringBuffer(bft_sequence_num);
}

uint64_t BlockMetadata::getLastBlockSequenceNum() const {
  const auto value = storage_.getLatest(concord::kvbc::categorization::kConcordInternalCategoryId,
                                        IBlockMetadata::kBlockMetadataKeyStr);
  auto sequenceNum = uint64_t{0};
  if (value) {
    const auto& data = std::get<categorization::VersionedValue>(*value).data;
    ConcordAssertEQ(data.size(), sizeof(uint64_t));
    sequenceNum = concordUtils::fromBigEndianBuffer<uint64_t>(data.data());
  } else {
    LOG_WARN(logger_, "Unable to get last block sequence number");
  }

  LOG_INFO(logger_, "last block sequenceNum = " << sequenceNum);
  return sequenceNum;
}

}  // namespace kvbc
}  // namespace concord
