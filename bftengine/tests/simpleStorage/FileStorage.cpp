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

#include "errnoString.hpp"
#include "assertUtils.hpp"

#include <cstring>
#include <exception>
#include <unistd.h>

using namespace std;
using namespace logging;

namespace bftEngine {

FileStorage::FileStorage(Logger &logger, const string &fileName) : logger_(logger), fileName_(fileName) {
  dataStream_ = fopen(fileName.c_str(), "w+x");
  if (!dataStream_) {  // The file already exists
    LOG_INFO(logger_, "FileStorage::ctor File " << fileName_ << " exists");
    dataStream_ = fopen(fileName.c_str(), "r+");
    if (!dataStream_) {
      ostringstream err;
      err << "Failed to open file " << fileName_ << ", errno is " << errno;
      LOG_FATAL(logger_, err.str());
      throw runtime_error(err.str());
    }
    LOG_INFO(logger_, "FileStorage::ctor File " << fileName_ << " successfully opened");
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
  uint32_t objectsNum = 0;
  const size_t sizeOfObjectsNum = sizeof(objectsNum);
  read(&objectsNum, 0, sizeOfObjectsNum, 1, WRONG_NUM_OF_OBJ_READ);
  LOG_DEBUG(logger_, "FileStorage::readFileMetadata objectsNum=" << objectsNum);
  if (objectsNum) {
    objectsMetadata_ = new ObjectsMetadataHandler(objectsNum);
    MetadataObjectInfo objectInfo;
    size_t readOffset = sizeOfObjectsNum;
    for (uint32_t i = initializedObjectId_ + 1; i < objectsNum; i++) {
      read(&objectInfo, readOffset, sizeof(objectInfo), 1, WRONG_OBJ_INFO_READ);
      readOffset += sizeof(objectInfo);
      objectsMetadata_->setObjectInfo(objectInfo);
    }
    LOG_DEBUG(logger_, "" << *objectsMetadata_);
  } else
    LOG_DEBUG(logger_, "FileStorage::readFileMetadata Metadata is empty");
}

void FileStorage::updateFileObjectMetadata(MetadataObjectInfo &objectInfo) {
  write(&objectInfo, objectInfo.metadataOffset, sizeof(objectInfo), 1, WRONG_OBJ_INFO_WRITE);
}

void FileStorage::writeFileObjectsMetadata() {
  const uint32_t objectsNum = objectsMetadata_->getObjectsNum();
  size_t writeOffset = sizeof(objectsNum);
  for (auto it : objectsMetadata_->getObjectsMap()) {
    MetadataObjectInfo &objectInfo = it.second;
    write(&objectInfo, writeOffset, sizeof(objectInfo), 1, WRONG_OBJ_INFO_WRITE);
    writeOffset += sizeof(objectInfo);
  }
}

void FileStorage::writeFileMetadata() {
  uint32_t objectsNum = objectsMetadata_->getObjectsNum();
  write(&objectsNum, 0, sizeof(objectsNum), 1, WRONG_NUM_OF_OBJ_WRITE);
  writeFileObjectsMetadata();
}

void FileStorage::read(void *dataPtr, size_t offset, size_t itemSize, size_t count, const char *errorMsg) {
  if (fseek(dataStream_, offset, SEEK_SET) != 0) {
    throw runtime_error("FileStorage::read " + concordUtils::errnoString(errno));
  }
  size_t read_ = fread(dataPtr, itemSize, count, dataStream_);
  int err = ferror(dataStream_);
  if (err) throw runtime_error("FileStorage::read " + concordUtils::errnoString(errno));
  if (feof(dataStream_)) throw runtime_error("FileStorage::read EOF");
  if (read_ != count) throw runtime_error("FileStorage::read " + std::string(errorMsg));
}

void FileStorage::write(
    const void *dataPtr, size_t offset, size_t itemSize, size_t count, const char *errorMsg, bool toFlush) {
  if (fseek(dataStream_, offset, SEEK_SET) != 0) {
    throw runtime_error("FileStorage::write " + concordUtils::errnoString(errno));
  }
  if (fwrite(dataPtr, itemSize, count, dataStream_) != count)
    throw runtime_error("FileStorage::write " + std::string(errorMsg));
  if (toFlush) fflush(dataStream_);
}

bool FileStorage::isNewStorage() { return (objectsMetadata_ == nullptr); }

bool FileStorage::initMaxSizeOfObjects(const std::map<uint32_t, ObjectDesc> &metadataObjectsArray,
                                       uint32_t metadataObjectsArrayLength) {
  if (!isNewStorage()) {
    LOG_WARN(logger_, "FileStorage::initMaxSizeOfObjects Storage file already initialized; ignoring");
    return false;
  }
  objectsMetadata_ = new ObjectsMetadataHandler(metadataObjectsArrayLength);
  const uint64_t fileMetadataSize = objectsMetadata_->getObjectsMetadataSize();
  LOG_INFO(logger_, "FileStorage::initMaxSizeOfObjects objectsNum=" << objectsMetadata_->getObjectsNum());
  uint64_t objMetaOffset = sizeof(objectsMetadata_->getObjectsNum());
  uint64_t objOffset = fileMetadataSize;
  // Metadata object with id=0 is used to indicate storage initialization state (not used by FileStorage).
  for (uint32_t i = initializedObjectId_ + 1; i < metadataObjectsArrayLength; i++) {
    MetadataObjectInfo objectInfo(
        metadataObjectsArray.at(i).id, objMetaOffset, objOffset, metadataObjectsArray.at(i).maxSize);
    objMetaOffset += sizeof(objectInfo);
    objOffset += metadataObjectsArray.at(i).maxSize;
    objectsMetadata_->setObjectInfo(objectInfo);
  }
  const uint64_t maxObjectsSize = objOffset - fileMetadataSize;
  const size_t maxFileSize = fileMetadataSize + maxObjectsSize;
  LOG_INFO(logger_,
           "FileStorage::initMaxSizeOfObjects Maximum size of objects is " << maxObjectsSize << ", file size is "
                                                                           << maxFileSize);
  if (ftruncate(fileno(dataStream_), maxFileSize)) {
    LOG_ERROR(logger_, "Failed to truncate file: " << fileName_);
    ConcordAssert(false);
  }
  writeFileMetadata();
  LOG_DEBUG(logger_, "" << *objectsMetadata_);
  return true;
}

void FileStorage::verifyFileMetadataSetup() const {
  if (!objectsMetadata_ || !(objectsMetadata_->getObjectsNum())) {
    LOG_FATAL(logger_, "FileStorage::verifyFileMetadataSetup " << METADATA_IS_NOT_SET_PROPERLY);
    throw runtime_error(METADATA_IS_NOT_SET_PROPERLY);
  }
}

void FileStorage::verifyOperation(uint32_t objectId, uint32_t dataLen, const char *buffer) const {
  verifyFileMetadataSetup();
  MetadataObjectInfo *objectInfo = objectsMetadata_->getObjectInfo(objectId);
  if (objectInfo && objectId <= (objectsMetadata_->getObjectsNum() - 1) && dataLen &&
      (dataLen <= objectInfo->maxSize && buffer))
    return;
  if (!objectInfo) {
    LOG_FATAL(logger_, "FileStorage::verifyOperation objectInfo is NULL for objectId=" << objectId);
  } else if (objectId > objectsMetadata_->getObjectsNum() - 1) {
    LOG_FATAL(logger_,
              "FileStorage::verifyOperation objectId=" << objectId << " is too big; maximum value is "
                                                       << objectsMetadata_->getObjectsNum() - 2);
  } else if (!dataLen) {
    LOG_FATAL(logger_, "FileStorage::verifyOperation dataLen is 0 for objectId=" << objectId);
  } else if (dataLen > objectInfo->maxSize) {
    LOG_FATAL(logger_,
              "FileStorage::verifyOperation dataLen is too big for objectId=" << objectId << "; maximum value is "
                                                                              << objectInfo->maxSize);
  } else if (!buffer) {
    LOG_FATAL(logger_, "FileStorage::verifyOperation buffer is NULL for objectId=" << objectId);
  }
  throw runtime_error(WRONG_PARAMETER);
}

void FileStorage::handleObjectWrite(uint32_t objectId, const void *dataPtr, uint32_t objectSize, bool toFlush) {
  MetadataObjectInfo *objectInfo = objectsMetadata_->getObjectInfo(objectId);
  if (objectInfo) {
    write(dataPtr, objectInfo->offset, objectSize, 1, FAILED_TO_WRITE_OBJECT, toFlush);
    LOG_DEBUG(logger_, "FileStorage::handleObjectWrite " << objectInfo->toString());
    // Update the file metadata with a real object size
    objectInfo->realSize = objectSize;
    objectsMetadata_->setObjectInfo(*objectInfo);
    updateFileObjectMetadata(*objectInfo);
  }
}

void FileStorage::handleObjectRead(uint32_t objectId, char *outBufferForObject, uint32_t &outActualObjectSize) {
  MetadataObjectInfo *objectInfo = objectsMetadata_->getObjectInfo(objectId);
  if (objectInfo) {
    if (objectInfo->realSize == 0) return;  // This means that this object has never been wrote to the file.
    read(outBufferForObject, objectInfo->offset, objectInfo->realSize, 1, FAILED_TO_READ_OBJECT);
    outActualObjectSize = objectInfo->realSize;
    LOG_DEBUG(logger_, "FileStorage::handleObjectRead " << objectInfo->toString());
  }
}

void FileStorage::read(uint32_t objectId,
                       uint32_t bufferSize,
                       char *outBufferForObject,
                       uint32_t &outActualObjectSize) {
  LOG_DEBUG(logger_, "FileStorage::read objectId=" << objectId << ", bufferSize=" << bufferSize);
  lock_guard<mutex> lock(ioMutex_);
  verifyOperation(objectId, bufferSize, outBufferForObject);
  handleObjectRead(objectId, outBufferForObject, outActualObjectSize);
}

void FileStorage::atomicWrite(uint32_t objectId, const char *data, uint32_t dataLength) {
  LOG_DEBUG(logger_, "FileStorage::atomicWrite objectId=" << objectId << ", dataLength=" << dataLength);
  lock_guard<mutex> lock(ioMutex_);
  verifyOperation(objectId, dataLength, data);
  handleObjectWrite(objectId, data, dataLength);
}

void FileStorage::beginAtomicWriteOnlyBatch() {
  LOG_DEBUG(logger_, "FileStorage::beginAtomicWriteOnlyBatch");
  lock_guard<mutex> lock(ioMutex_);
  verifyFileMetadataSetup();
  if (transaction_) {
    LOG_DEBUG(logger_, "FileStorage::beginAtomicWriteOnlyBatch Transaction has been opened before; ignoring.");
    return;
  }
  transaction_ = new ObjectIdToRequestMap;
}

void FileStorage::writeInBatch(uint32_t objectId, const char *data, uint32_t dataLength) {
  LOG_DEBUG(logger_, "FileStorage::writeInBatch objectId=" << objectId << ", dataLength=" << dataLength);
  lock_guard<mutex> lock(ioMutex_);
  verifyOperation(objectId, dataLength, data);
  if (!transaction_) {
    LOG_ERROR(logger_, "FileStorage::writeInBatch " << WRONG_FLOW);
    throw runtime_error(WRONG_FLOW);
  }

  auto it = transaction_->find(objectId);
  if (it != transaction_->end()) transaction_->erase(it);
  transaction_->insert(pair<uint32_t, RequestInfo>(objectId, RequestInfo(data, dataLength)));
}

void FileStorage::commitAtomicWriteOnlyBatch() {
  LOG_DEBUG(logger_, "FileStorage::commitAtomicWriteOnlyBatch");
  lock_guard<mutex> lock(ioMutex_);
  verifyFileMetadataSetup();
  if (!transaction_) {
    LOG_ERROR(logger_, "FileStorage::commitAtomicWriteOnlyBatch " << WRONG_FLOW);
    throw runtime_error(WRONG_FLOW);
  }
  for (const auto &it : *transaction_) {
    handleObjectWrite(it.first, it.second.data.get(), it.second.dataLen, false);
  }
  fflush(dataStream_);
  delete transaction_;
  transaction_ = nullptr;
}
void FileStorage::eraseData() {
  try {
    cleanStorage();
  } catch (std::exception &e) {
    LOG_FATAL(logger_, e.what());
    std::terminate();
  }
}
void FileStorage::cleanStorage() {
  // To clean the storage such that the replica will come back with a new metadata storage, we just need to set the
  // number of stored objects to 0. Note that as this method is called from the destructor, we don't need to catch the
  // mutex.
  uint32_t objectsNum = 0;
  write(&objectsNum, 0, sizeof(objectsNum), 1, WRONG_NUM_OF_OBJ_WRITE);
  LOG_INFO(logger_,
           "set the number of metadata storage to 0. This was done in order to load a fresh metadata on the next "
           "replica startup");
}

}  // namespace bftEngine
