// Copyright 2019 VMware, all rights reserved
//
// Wrapper code to store BFT metadata (sequence number).

#pragma once

#include <exception>
#include <string>

#include "sliver.hpp"
#include "Logger.hpp"
#include "blockchain/db_interfaces.h"

namespace SimpleKVBC {

const uint8_t kBlockMetadataKey = 0x21;

class BlockMetadata {
 private:
  concordlogger::Logger logger_;
  const concord::storage::blockchain::ILocalKeyValueStorageReadOnly &storage_;
  const concordUtils::Sliver key_;

 public:
  BlockMetadata(const concord::storage::blockchain::ILocalKeyValueStorageReadOnly &storage)
      : logger_(concordlogger::Log::getLogger("skvbc.MetadataStorage")),
        storage_(storage),
        key_(new uint8_t[1]{kBlockMetadataKey}, 1) {}

  concordUtils::Sliver Key() const { return key_; }

  uint64_t Get(concordUtils::Sliver &key);

  concordUtils::Sliver Serialize(uint64_t bft_sequence_num);
};

}  // namespace SimpleKVBC
