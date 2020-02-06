// Copyright 2018 VMware, all rights reserved
//
// Contains functionality for working with composite database keys and
// using them to perform basic blockchain operations.

#pragma once

#include "db_interface.h"
#include "storage/db_types.h"
#include "Logger.hpp"

namespace concord::storage {

inline namespace v1DirectKeyValue {
class DBKeyGeneratorBase {
 protected:
  static bool copyToAndAdvance(char* buf, size_t* offset, size_t _maxOffset, const char* _src, const size_t& _srcSize);
  static concordlogger::Logger& logger() {
    static concordlogger::Logger logger_ = concordlogger::Log::getLogger("concord.storage.blockchain.DBKeyManipulator");
    return logger_;
  }
};

class MetadataKeyManipulator : public DBKeyGeneratorBase {
 public:
  static concordUtils::Sliver generateMetadataKey(ObjectId objectId);
};

class STKeyManipulator : public DBKeyGeneratorBase {
 public:
  static concordUtils::Sliver generateStateTransferKey(ObjectId objectId);
  static concordUtils::Sliver generateSTPendingPageKey(uint32_t pageid);
  static concordUtils::Sliver generateSTCheckpointDescriptorKey(uint64_t chkpt);
  static concordUtils::Sliver generateSTReservedPageStaticKey(uint32_t pageid, uint64_t chkpt);
  static concordUtils::Sliver generateSTReservedPageDynamicKey(uint32_t pageid, uint64_t chkpt);
  static uint64_t extractCheckPointFromKey(const char* _key_data, size_t _key_length);
  static std::pair<uint32_t, uint64_t> extractPageIdAndCheckpointFromKey(const char* _key_data, size_t _key_length);

 private:
  static Sliver generateReservedPageKey(detail::EDBKeyType, uint32_t pageid, uint64_t chkpt);
};

}  // namespace v1DirectKeyValue
}  // namespace concord::storage
