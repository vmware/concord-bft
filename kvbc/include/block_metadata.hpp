// Copyright 2019 VMware, all rights reserved
//
// Wrapper code to store BFT metadata (sequence number).

#pragma once

#include <exception>
#include <string>

#include "Logger.hpp"
#include "db_interfaces.h"
#include "categorization/db_categories.h"

namespace concord {
namespace kvbc {

/**
 * Interface defining the way block is serialized
 */
class IBlockMetadata {
 public:
  static const auto kBlockMetadataKey = 0x21;
  static inline const std::string kBlockMetadataKeyStr = std::string(1, kBlockMetadataKey);

 public:
  IBlockMetadata(const IReader& storage) : logger_(logging::getLogger("block-metadata")), storage_(storage) {}

  virtual ~IBlockMetadata() = default;

  virtual uint64_t getLastBlockSequenceNum() const = 0;

  virtual std::string serialize(uint64_t sequence_num) const = 0;

 protected:
  logging::Logger logger_;
  const IReader& storage_;
};

/**
 * Default block serialization
 */
class BlockMetadata : public IBlockMetadata {
 public:
  BlockMetadata(const IReader& storage) : IBlockMetadata(storage) {
    logger_ = logging::getLogger("skvbc.MetadataStorage");
  }
  virtual uint64_t getLastBlockSequenceNum() const override;
  virtual std::string serialize(uint64_t sequence_num) const override;
};

}  // namespace kvbc
}  // namespace concord
