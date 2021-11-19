// Concord
//
// Copyright (c) 2018 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the
// "License").  You may not use this product except in compliance with the
// Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to
// the terms and conditions of the subcomponent's license, as noted in the
// LICENSE file.

#pragma once

#include <stdint.h>

namespace bftEngine {

// MetadataStorage class functions could throw runtime_error exceptions.
class MetadataStorage {
 public:
  struct ObjectDesc {
    uint32_t id;
    uint32_t maxSize;
  };

  virtual ~MetadataStorage() = default;

  // Initialize the storage before the first time used. In case storage is already initialized, do nothing.
  // Return 'true' in case DB was virgin (not initialized) before a call to this function
  // (the IDs and their maximal size are known in advance).
  virtual bool initMaxSizeOfObjects(ObjectDesc *metadataObjectsArray, uint32_t metadataObjectsArrayLength) = 0;

  // Return 'true' in case DB is virgin (not initialized).
  virtual bool isNewStorage() = 0;

  // Read object from storage (only used to restart/recovery)
  virtual void read(uint32_t objectId,
                    uint32_t bufferSize,
                    char *outBufferForObject,
                    uint32_t &outActualObjectSize) = 0;

  // Atomically write an object to storage
  virtual void atomicWrite(uint32_t objectId, const char *data, uint32_t dataLength) = 0;

  // Atomic write-only transactions
  virtual void beginAtomicWriteOnlyBatch() = 0;
  virtual void writeInBatch(uint32_t objectId, const char *data, uint32_t dataLength) = 0;
  virtual void commitAtomicWriteOnlyBatch() = 0;

  // In some cases, we would like to load a new metadata after a crash (for example, on reconfiguration actions).
  virtual void eraseData() = 0;

  //
  virtual void atomicWriteArbitraryObject(const std::string &key, const char *data, uint32_t dataLength) = 0;
};

}  // namespace bftEngine
