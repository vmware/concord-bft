// Concord
//
// Copyright (c) 2019 VMware, Inc. All Rights Reserved.
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

#include <mutex>
#include <functional>
#include <map>
#include "Logger.hpp"
#include "bftengine/MetadataStorage.hpp"
#include "storage/db_interface.h"
#include "sliver.hpp"

namespace concord {
namespace storage {

typedef std::vector<uint32_t> ObjectIdsVector;
typedef std::map<uint32_t, size_t> ObjectIdToSizeMap;

class DBMetadataStorage : public bftEngine::MetadataStorage {
 public:
  explicit DBMetadataStorage(IDBClient *dbClient, std::function<concordUtils::Sliver(uint32_t)> genMetadataKey)
      : logger_(concordlogger::Log::getLogger("com.concord.vmware.metadatastorage")),
        dbClient_(dbClient), genMetadataKey_(std::move(genMetadataKey)) {
    objectIdToSizeMap_[objectsNumParameterId_] = sizeof(objectsNum_);
  }

  bool initMaxSizeOfObjects(ObjectDesc *metadataObjectsArray, uint32_t metadataObjectsArrayLength) override;
  void read(uint32_t objectId, uint32_t bufferSize, char *outBufferForObject,
            uint32_t &outActualObjectSize) override;
  void atomicWrite(uint32_t objectId, char *data, uint32_t dataLength) override;
  void beginAtomicWriteOnlyBatch() override;
  void writeInBatch(uint32_t objectId, char *data, uint32_t dataLength) override;
  void commitAtomicWriteOnlyBatch() override;
  concordUtils::Status multiDel(const ObjectIdsVector &objectIds);
  bool isNewStorage() override;

 private:
  void verifyOperation(uint32_t objectId, uint32_t dataLen, const char *buffer, bool writeOperation) const;

 private:
  const char *WRONG_FLOW = "beginAtomicWriteOnlyBatch should be launched first";
  const char *WRONG_PARAMETER = "Wrong parameter value specified";

  const uint8_t objectsNumParameterId_ = 1;

  concordlogger::Logger logger_;
  IDBClient *dbClient_ = nullptr;
  SetOfKeyValuePairs *batch_ = nullptr;
  std::mutex ioMutex_;
  ObjectIdToSizeMap objectIdToSizeMap_;
  uint32_t objectsNum_ = 0;

  // A function that creates a metadata key given an object ID
  std::function<Sliver(uint32_t)> genMetadataKey_;
};

}
}
