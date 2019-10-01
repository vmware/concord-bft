// Copyright 2019 VMware, all rights reserved
//
// Wrapper used by concord::consensus::ConcordCommandsHandler to store BFT
// metadata (sequence number).

#include "block_metadata.hpp"
#include "blockchain/db_types.h"

using concordUtils::Sliver;
using concordUtils::Status;

namespace SimpleKVBC {

Sliver BlockMetadata::Serialize(uint64_t bft_sequence_num) {
  return Sliver::copy((uint8_t*)&bft_sequence_num, sizeof(uint64_t));
}

uint64_t BlockMetadata::Get(Sliver& key) {
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

}  // namespace SimpleKVBC
