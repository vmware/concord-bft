// Copyright 2019 VMware, all rights reserved
//
// Wrapper code to store BFT metadata (sequence number).

#pragma once

#include <exception>
#include <string>

#include "sliver.hpp"
#include "Logger.hpp"
#include "db_interfaces.h"

namespace concord {
namespace kvbc {

using concordUtils::Sliver;
/**
 * Interface defining the way block is serialized
 */
class IBlockMetadata {
 public:
  IBlockMetadata(const ILocalKeyValueStorageReadOnly& storage)
      : logger_(logging::getLogger("block-metadata")), storage_(storage), key_(new char[1]{kBlockMetadataKey}, 1) {}

  virtual ~IBlockMetadata() = default;

  Sliver getKey() const { return key_; }

  virtual uint64_t getLastBlockSequenceNum(const Sliver& key) const = 0;

  virtual Sliver serialize(uint64_t sequence_num) const = 0;

  static const char kBlockMetadataKey = 0x21;

 protected:
  logging::Logger logger_;
  const ILocalKeyValueStorageReadOnly& storage_;
  const concordUtils::Sliver key_;
};

/**
 * Default block serialization
 */
class BlockMetadata : public IBlockMetadata {
 public:
  BlockMetadata(const ILocalKeyValueStorageReadOnly& storage) : IBlockMetadata(storage) {
    logger_ = logging::getLogger("skvbc.MetadataStorage");
  }
  virtual uint64_t getLastBlockSequenceNum(const Sliver& key) const override;
  virtual Sliver serialize(uint64_t sequence_num) const override;
};

}  // namespace kvbc
}  // namespace concord
