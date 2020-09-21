// Copyright 2018 VMware, all rights reserved
//
// Contains functionality for working with composite database keys and
// using them to perform basic blockchain operations.

#pragma once

#include "key_manipulator_interface.h"

#include "db_interface.h"
#include "storage/db_types.h"
#include "Logger.hpp"

namespace concord::storage::v1DirectKeyValue {

class DBKeyGeneratorBase {
 protected:
  static bool copyToAndAdvance(char* buf, size_t* offset, size_t _maxOffset, const char* _src, const size_t& _srcSize);
  static logging::Logger& logger() {
    static logging::Logger logger_ = logging::getLogger("concord.storage.blockchain.DBKeyManipulator");
    return logger_;
  }
};

class MetadataKeyManipulator : public DBKeyGeneratorBase, public IMetadataKeyManipulator {
 public:
  concordUtils::Sliver generateMetadataKey(ObjectId objectId) const override;
};

class STKeyManipulator : public DBKeyGeneratorBase, public ISTKeyManipulator {
 public:
  concordUtils::Sliver generateStateTransferKey(ObjectId objectId) const override;
  concordUtils::Sliver generateSTPendingPageKey(uint32_t pageid) const override;
  concordUtils::Sliver generateSTCheckpointDescriptorKey(uint64_t chkpt) const override;
  concordUtils::Sliver generateSTReservedPageDynamicKey(uint32_t pageid, uint64_t chkpt) const override;
  concordUtils::Sliver getReservedPageKeyPrefix() const override;

  static uint64_t extractCheckPointFromKey(const char* _key_data, size_t _key_length);
  static std::pair<uint32_t, uint64_t> extractPageIdAndCheckpointFromKey(const char* _key_data, size_t _key_length);

 private:
  static Sliver generateReservedPageKey(detail::EDBKeyType, uint32_t pageid, uint64_t chkpt);
};

}  // namespace concord::storage::v1DirectKeyValue
