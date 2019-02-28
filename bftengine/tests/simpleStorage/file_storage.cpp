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

#include "file_storage.hpp"

#include <stdio.h>
#include <exception>
#include <unistd.h>

using namespace concordlogger;

namespace bftEngine {

  FileStorage::FileStorage(Logger& logger, std::string fileName):
    logger_(logger), fileName_(fileName) {

    dataStream_ = fopen(fileName.c_str(), "wxb+");
    if (!dataStream_) { // The file already exists
      dataStream_ = fopen(fileName.c_str(), "rb+");
    }
    if (!dataStream_) {
      std::ostringstream err;
      err << "Failed to open file " << fileName_ << ", errno is " << errno;
      LOG_FATAL(logger_, err.str());
      throw(err.str());
    }
  }

  FileStorage::~FileStorage() {
    if (dataStream_) {
      fclose(dataStream_);
    }
  }

  bool FileStorage::writeToFile(const void* dataPtr, size_t dataSize) {
    fwrite (dataPtr, dataSize, 1, dataStream_);
    fflush(dataStream_);
    return true;
  }

  void FileStorage::initMaxSizeOfObjects(ObjectDesc* metadataObjectsArray,
                                         uint16_t metadataObjectsArrayLength) {
    objectsArray_  = metadataObjectsArray;
    objectsNumber_ = metadataObjectsArrayLength;
    uint32_t maxFileSize = 0;
    for (uint32_t i = 0; i <objectsNumber_; i++) {
      maxFileSize += metadataObjectsArray[i].maxSize;
    }
    LOG_INFO(logger_, "Calculated maximum file size is " << maxFileSize);
    ftruncate(fileno(dataStream_), maxFileSize);
  }

  void FileStorage::read(uint16_t objectId, uint32_t bufferSize,
                         char* outBufferForObject,
                         uint32_t &outActualObjectSize) {
    // Verify that an object size is less than max
  }

  void FileStorage::atomicWrite(uint16_t objectId, char* data,
                                uint32_t dataLength) {

  }

  void FileStorage::beginAtomicWriteOnlyTransaction() {

  }

  void FileStorage::writeInTransaction(uint16_t objectId, char* data,
                                       uint32_t dataLength) {

  }

  void FileStorage::commitAtomicWriteOnlyTransaction() {

  }

}

