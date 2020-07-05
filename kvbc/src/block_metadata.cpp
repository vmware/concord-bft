// Copyright 2019 VMware, all rights reserved
//
// Wrapper used by concord::consensus::ConcordCommandsHandler to store BFT
// metadata (sequence number).

#include "block_metadata.hpp"

using concordUtils::Status;

namespace concord {
namespace kvbc {

Sliver BlockMetadata::serialize(uint64_t bft_sequence_num) const {
  return Sliver::copy((char*)&bft_sequence_num, sizeof(uint64_t));
}

uint64_t BlockMetadata::getLastBlockSequenceNum(const Sliver& key) const {
  Sliver outValue;
  Status status = storage_.get(key, outValue);
  uint64_t sequenceNum = 0;
  if (status.isOK() && outValue.length() > 0) {
    sequenceNum = *(uint64_t*)outValue.data();
  } else {
    LOG_WARN(
        logger_,
        "Unable to get block or has zero-length; status = " << status << ", outValue.length = " << outValue.length());
  }
  LOG_INFO(logger_, "key = " << key << ", sequenceNum = " << sequenceNum);
  return sequenceNum;
}

}  // namespace kvbc
}  // namespace concord
