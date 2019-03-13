// Concord
//
// Copyright (c) 2018 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#ifndef FILE_STORAGE_HPP
#define FILE_STORAGE_HPP

#include <mutex>

#include "Logging.hpp"
#include "MetadataStorage.hpp"
#include "MetadataStorageTypes.hpp"

namespace bftEngine {

class ObjectsMetadataHandler;

class FileStorage : public MetadataStorage {
 public:
  FileStorage(concordlogger::Logger &logger, std::string fileName);

  virtual ~FileStorage();

  void initMaxSizeOfObjects(ObjectDesc *metadataObjectsArray,
                            uint16_t metadataObjectsArrayLength) override;

  void read(uint16_t objectId, uint32_t bufferSize,
            char *outBufferForObject,
            uint32_t &outActualObjectSize) override;

  void atomicWrite(uint16_t objectId, char *data, uint32_t dataLength) override;

  void beginAtomicWriteOnlyTransaction() override;

  void writeInTransaction(uint16_t objectId, char *data,
                          uint32_t dataLength) override;

  void commitAtomicWriteOnlyTransaction() override;

 private:
  void read(void *dataPtr, size_t offset, size_t itemSize,
            size_t count, const char *errorMsg);
  void write(void *dataPtr, size_t offset, size_t itemSize,
             size_t count, const char *errorMsg, bool toFlush = true);
  void handleObjectWrite(uint16_t objectId, void *dataPtr, uint32_t objectSize,
                         bool toFlush = true);
  void handleObjectRead(uint16_t objectId, char *outBufferForObject,
                        uint32_t &outActualObjectSize);
  void loadFileMetadata();
  void writeFileMetadata();
  void writeFileObjectsMetadata();
  void updateFileObjectMetadata(MetadataObjectInfo &objectInfo);
  void verifyFileMetadataSetup() const;
  void verifyOperation(uint16_t objectId, uint32_t dataLen, char *buffer) const;

 private:
  const char *WRONG_NUM_OF_OBJ_READ =
      "Failed to read a number of objects or it is 0";
  const char *WRONG_NUM_OF_OBJ_WRITE =
      "Failed to write a number of objects";
  const char *WRONG_OBJ_INFO_WRITE =
      "Failed to write objects info";
  const char *METADATA_IS_NOT_SET_PROPERLY =
      "File metadata is not set up properly";
  const char *WRONG_PARAMETER =
      "Wrong parameter value specified";
  const char *FAILED_TO_WRITE_OBJECT =
      "Failed to write an object to the file";
  const char *FAILED_TO_READ_OBJECT =
      "Failed to read an object from the file";
  const char *WRONG_OBJ_INFO_READ =
      "Failed to read objects info or it is different from expected";
  const char *WRONG_FLOW =
      "beginAtomicWriteOnlyTransaction should be launched first";

  concordlogger::Logger &logger_;
  const std::string fileName_;
  FILE *dataStream_ = nullptr;
  ObjectsMetadataHandler *objectsMetadata_ = nullptr;
  ObjectIdToRequestMap *transaction_ = nullptr;
  std::mutex ioMutex_;
};

}

#endif

