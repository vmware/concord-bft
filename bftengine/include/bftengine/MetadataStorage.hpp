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
    uint16_t id;
    uint32_t maxSize;
  };

  // Used to initialize the storage the first time this storage is used
  // (the IDs and their maximal size are known in advance)
  virtual void initMaxSizeOfObjects(ObjectDesc *metadataObjectsArray,
                                    uint16_t metadataObjectsArrayLength) = 0;

  // Read object from storage (only used to restart/recovery)
  virtual void read(uint16_t objectId,
                    uint32_t bufferSize,
                    char *outBufferForObject,
                    uint32_t &outActualObjectSize) = 0;

  // Atomically write an object to storage
  virtual void atomicWrite(uint16_t objectId,
                           char *data,
                           uint32_t dataLength) = 0;

  // Atomic write-only transactions
  virtual void beginAtomicWriteOnlyTransaction() = 0;
  virtual void writeInTransaction(uint16_t objectId,
                                  char *data,
                                  uint32_t dataLength) = 0;
  virtual void commitAtomicWriteOnlyTransaction() = 0;
};

}
