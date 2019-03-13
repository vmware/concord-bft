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

#include "MetadataStorage.hpp"

#include <string>

#include "Logging.hpp"

namespace bftEngine {

  class FileStorage : public MetadataStorage
  {
    public:
      FileStorage(concordlogger::Logger& logger, std::string fileName);

      virtual ~FileStorage();

      void initMaxSizeOfObjects(ObjectDesc* metadataObjectsArray,
                                uint16_t metadataObjectsArrayLength) override;

      void read(uint16_t objectId, uint32_t bufferSize,
                char* outBufferForObject,
                uint32_t &outActualObjectSize) override;

      void atomicWrite(uint16_t objectId, char* data,
                       uint32_t dataLength) override;

      void beginAtomicWriteOnlyTransaction() override;

      void writeInTransaction(uint16_t objectId, char* data,
                              uint32_t dataLength) override;

      void commitAtomicWriteOnlyTransaction() override;

    private:
      bool writeToFile(const void* dataPtr, size_t dataSize);

    private:
      concordlogger::Logger& logger_;
      const std::string      fileName_;
      FILE*                  dataStream_ = nullptr;
      uint16_t               objectsNumber_ = 0;
      ObjectDesc*            objectsArray_ = nullptr;
  };

}

#endif

