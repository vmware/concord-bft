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

#include "FileStorage.hpp"
#include "ObjectsMetadataHandler.hpp"

#include <cstring>
#include <exception>
#include <unistd.h>

using namespace std;
using namespace concordlogger;

namespace bftEngine {

FileStorage::FileStorage(Logger &logger, string fileName) :
    logger_(logger), fileName_(fileName) {

  dataStream_ = fopen(fileName.c_str(), "wxb+");
  if (!dataStream_) { // The file already exists
    dataStream_ = fopen(fileName.c_str(), "rb+");
    if (!dataStream_) {
      ostringstream err;
      err << "Failed to open file " << fileName_ << ", errno is " << errno;
      LOG_FATAL(logger_, err.str());
      throw runtime_error(err.str());
    }
    LOG_INFO(logger_, "FileStorage::ctor File "
        << fileName_ << " successfully opened");
    loadFileMetadata();
  }
}

FileStorage::~FileStorage() {
  if (dataStream_) {
    fclose(dataStream_);
  }
  delete objectsMetadata_;
}

void FileStorage::loadFileMetadata() {
  uint16_t objectsNum = 0;
  const size_t sizeOfObjectsNum = sizeof(objectsNum);
  read(&objectsNum, 0, sizeOfObjectsNum, 1, WRONG_NUM_OF_OBJ_READ);
  LOG_INFO(logger_, "FileStorage::readFileMetadata objectsNum=" << objectsNum);
  if (objectsNum) {
    objectsMetadata_ = new ObjectsMetadataHandler(objectsNum);
    MetadataObjectInfo objectInfo;
    size_t readOffset = sizeOfObjectsNum;
    for (auto i = 0; i < objectsNum; i++) {
      read(&objectInfo, readOffset, sizeof(objectInfo), 1, WRONG_OBJ_INFO_READ);
      readOffset += sizeof(objectInfo);
      objectsMetadata_->setObjectInfo(objectInfo);
    }
    LOG_INFO(logger_, "" << *objectsMetadata_);
  } else LOG_INFO(logger_, "FileStorage::readFileMetadata Metadata is empty");
}

void FileStorage::updateFileObjectMetadata(MetadataObjectInfo &objectInfo) {
  write(&objectInfo, objectInfo.metadataOffset, sizeof(objectInfo), 1,
        WRONG_OBJ_INFO_WRITE);
}

void FileStorage::writeFileObjectsMetadata() {
  const uint16_t objectsNum = objectsMetadata_->getObjectsNum();
  size_t writeOffset = sizeof(objectsNum);
  for (auto it : objectsMetadata_->getObjectsMap()) {
    MetadataObjectInfo &objectInfo = it.second;
    write(&objectInfo, writeOffset, sizeof(objectInfo), 1,
          WRONG_OBJ_INFO_WRITE);
    writeOffset += sizeof(objectInfo);
  }
}

void FileStorage::writeFileMetadata() {
  uint16_t objectsNum = objectsMetadata_->getObjectsNum();
  write(&objectsNum, 0, sizeof(objectsNum), 1, WRONG_NUM_OF_OBJ_WRITE);
  writeFileObjectsMetadata();
}

void FileStorage::read(void *dataPtr, size_t offset, size_t itemSize,
                       size_t count, const char *errorMsg) {
  fseek(dataStream_, offset, SEEK_SET);
  if (fread(dataPtr, itemSize, count, dataStream_) != count) {
    LOG_FATAL(logger_, "FileStorage::read " << errorMsg);
    throw runtime_error(errorMsg);
  }
}

void FileStorage::write(void *dataPtr, size_t offset, size_t itemSize,
                        size_t count, const char *errorMsg, bool toFlush) {
  fseek(dataStream_, offset, SEEK_SET);
  if (fwrite(dataPtr, itemSize, count, dataStream_) != count) {
    LOG_FATAL(logger_, "FileStorage::write " << errorMsg);
    throw runtime_error(errorMsg);
  }
  if (toFlush)
    fflush(dataStream_);
}

void FileStorage::initMaxSizeOfObjects(ObjectDesc *metadataObjectsArray,
                                       uint16_t metadataObjectsArrayLength) {
  if (objectsMetadata_) {
    LOG_WARN(logger_, "FileStorage::initMaxSizeOfObjects Storage file already"
                      " initialized; ignoring");
    return;
  }
  objectsMetadata_ = new ObjectsMetadataHandler(metadataObjectsArrayLength);
  const uint64_t fileMetadataSize = objectsMetadata_->getObjectsMetadataSize();
  LOG_INFO(logger_, "FileStorage::initMaxSizeOfObjects objectsNum="
      << objectsMetadata_->getObjectsNum());
  uint64_t objMetaOffset = sizeof(objectsMetadata_->getObjectsNum());
  uint64_t objOffset = fileMetadataSize;
  for (auto i = 0; i < metadataObjectsArrayLength; i++) {
    MetadataObjectInfo objectInfo(metadataObjectsArray[i].id, objMetaOffset,
                                  objOffset, metadataObjectsArray[i].maxSize);
    objMetaOffset += sizeof(objectInfo);
    objOffset += metadataObjectsArray[i].maxSize;
    objectsMetadata_->setObjectInfo(objectInfo);
  }
  const uint64_t maxObjectsSize = objOffset - fileMetadataSize;
  const size_t maxFileSize = fileMetadataSize + maxObjectsSize;
  LOG_INFO(logger_,
           "FileStorage::initMaxSizeOfObjects Maximum size of objects is "
               << maxObjectsSize << ", file size is " << maxFileSize);
  ftruncate(fileno(dataStream_), maxFileSize);
  writeFileMetadata();
  LOG_INFO(logger_, "" << *objectsMetadata_);
}

void FileStorage::verifyFileMetadataSetup() const {
  if (!objectsMetadata_ || !(objectsMetadata_->getObjectsNum())) {
    LOG_FATAL(logger_, "FileStorage::verifyFileMetadataSetup "
        << METADATA_IS_NOT_SET_PROPERLY);
    throw runtime_error(METADATA_IS_NOT_SET_PROPERLY);
  }
}

void FileStorage::verifyOperation(uint16_t objectId, uint32_t dataLen,
                                  char *buffer) const {
  verifyFileMetadataSetup();
  MetadataObjectInfo *objectInfo = objectsMetadata_->getObjectInfo(objectId);
  if (!objectInfo || objectId > objectsMetadata_->getObjectsNum() - 1 ||
      !dataLen || dataLen > objectInfo->maxSize || !buffer) {
    LOG_FATAL(logger_, "FileStorage::verifyOperation " << WRONG_PARAMETER);
    throw runtime_error(WRONG_PARAMETER);
  }
}

void FileStorage::handleObjectWrite(uint16_t objectId, void *dataPtr,
                                    uint32_t objectSize, bool toFlush) {
  MetadataObjectInfo *objectInfo = objectsMetadata_->getObjectInfo(objectId);
  if (objectInfo) {
    write(dataPtr, objectInfo->offset, objectSize, 1,
          FAILED_TO_WRITE_OBJECT, toFlush);
    LOG_INFO(logger_, "FileStorage::handleObjectWrite "
        << objectInfo->toString());
    // Update the file metadata with a real object size
    objectInfo->realSize = objectSize;
    objectsMetadata_->setObjectInfo(*objectInfo);
    updateFileObjectMetadata(*objectInfo);
  }
}

void FileStorage::handleObjectRead(uint16_t objectId, char *outBufferForObject,
                                   uint32_t &outActualObjectSize) {
  MetadataObjectInfo *objectInfo = objectsMetadata_->getObjectInfo(objectId);
  if (objectInfo) {
    read(outBufferForObject, objectInfo->offset, objectInfo->realSize, 1,
         FAILED_TO_READ_OBJECT);
    outActualObjectSize = objectInfo->realSize;
    LOG_INFO(logger_, "FileStorage::handleObjectRead "
        << objectInfo->toString());
  }
}

void FileStorage::read(uint16_t objectId, uint32_t bufferSize,
                       char *outBufferForObject,
                       uint32_t &outActualObjectSize) {
  LOG_INFO(logger_, "FileStorage::read objectId="
      << objectId << ", bufferSize=" << bufferSize);
  lock_guard<mutex> lock(ioMutex_);
  verifyOperation(objectId, bufferSize, outBufferForObject);
  handleObjectRead(objectId, outBufferForObject, outActualObjectSize);
}

void FileStorage::atomicWrite(uint16_t objectId, char *data,
                              uint32_t dataLength) {
  LOG_INFO(logger_, "FileStorage::atomicWrite objectId="
      << objectId << ", dataLength=" << dataLength);
  lock_guard<mutex> lock(ioMutex_);
  verifyOperation(objectId, dataLength, data);
  handleObjectWrite(objectId, data, dataLength);
}

void FileStorage::beginAtomicWriteOnlyTransaction() {
  LOG_INFO(logger_, "FileStorage::beginAtomicWriteOnlyTransaction");
  lock_guard<mutex> lock(ioMutex_);
  verifyFileMetadataSetup();
  if (transaction_) {
    LOG_INFO(logger_, "FileStorage::beginAtomicWriteOnlyTransaction "
                      "Transaction has been opened before; ignoring.");
    return;
  }
  transaction_ = new ObjectIdToRequestMap;
}

void FileStorage::writeInTransaction(uint16_t objectId, char *data,
                                     uint32_t dataLength) {
  LOG_INFO(logger_, "FileStorage::writeInTransaction objectId="
      << objectId << ", dataLength=" << dataLength);
  lock_guard<mutex> lock(ioMutex_);
  verifyOperation(objectId, dataLength, data);
  if (!transaction_) {
    LOG_ERROR(logger_, "FileStorage::writeInTransaction " << WRONG_FLOW);
    throw runtime_error(WRONG_FLOW);
  }
  char *buf = new char[dataLength];
  memcpy(buf, data, dataLength);
  transaction_->insert(pair<uint16_t, RequestInfo>
                           (objectId, RequestInfo(buf, dataLength)));
}

void FileStorage::commitAtomicWriteOnlyTransaction() {
  LOG_INFO(logger_, "FileStorage::commitAtomicWriteOnlyTransaction");
  lock_guard<mutex> lock(ioMutex_);
  verifyFileMetadataSetup();
  if (!transaction_) {
    LOG_ERROR(logger_, "FileStorage::commitAtomicWriteOnlyTransaction "
        << WRONG_FLOW);
    throw runtime_error(WRONG_FLOW);
  }
  for (auto it : *transaction_) {
    handleObjectWrite(it.first, it.second.data, it.second.dataLen, false);
    delete[] it.second.data;
  }
  fflush(dataStream_);
  delete transaction_;
}

}

