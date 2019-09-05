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

#include "storage/db_metadata_storage.h"

#include <cstring>
#include <exception>
#include "hash_defs.h"

using namespace std;

using concordUtils::Status;

namespace concord {
namespace storage {

void DBMetadataStorage::verifyOperation(uint32_t objectId, uint32_t dataLen, const char *buffer,
                                        bool writeOperation) const {
  auto elem = objectIdToSizeMap_.find(objectId);
  bool found = (elem != objectIdToSizeMap_.end());
  if (!dataLen || !buffer || !found || !objectId) {
    throw runtime_error(WRONG_PARAMETER);
  }
  if (writeOperation && (dataLen > elem->second)) {
    ostringstream error;
    error << "Metadata object objectId " << objectId << " size is too big: given "
          << dataLen << ", allowed " << elem->second << endl;
    throw runtime_error(error.str());
  }
}

bool DBMetadataStorage::isNewStorage() {
  uint32_t outActualObjectSize;
  read(objectsNumParameterId_, sizeof(objectsNum_), (char *) &objectsNum_, outActualObjectSize);
  return (outActualObjectSize == 0);
}

bool DBMetadataStorage::initMaxSizeOfObjects(ObjectDesc *metadataObjectsArray, uint32_t metadataObjectsArrayLength) {
  for (uint32_t i = objectsNumParameterId_ + 1; i < metadataObjectsArrayLength; ++i) {
    objectIdToSizeMap_[i] = metadataObjectsArray[i].maxSize;
    LOG_DEBUG(logger_, "initMaxSizeOfObjects i=" << i << " object data: id=" << metadataObjectsArray[i].id
                                                 << ", maxSize=" << metadataObjectsArray[i].maxSize);
  }
  // Metadata object with id=1 is used to indicate storage initialization state
  // (number of specified metadata objects).
  bool isNew = isNewStorage();
  if (isNew) {
    objectsNum_ = metadataObjectsArrayLength;
    atomicWrite(objectsNumParameterId_, (char *) &objectsNum_, sizeof(objectsNum_));
  }
  LOG_DEBUG(logger_, "initMaxSizeOfObjects objectsNum_=" << objectsNum_);
  return isNew;
}

void DBMetadataStorage::read(uint32_t objectId, uint32_t bufferSize, char *outBufferForObject,
                             uint32_t &outActualObjectSize) {
  verifyOperation(objectId, bufferSize, outBufferForObject, false);
  lock_guard<mutex> lock(ioMutex_);
  Status status = dbClient_->get(genMetadataKey_(objectId), outBufferForObject, bufferSize, outActualObjectSize);
  if (status.isNotFound()) {
    memset(outBufferForObject, 0, bufferSize);
    outActualObjectSize = 0;
    return;
  }
  if (!status.isOK()) {
    throw runtime_error("DBClient get operation failed");
  }
}

void DBMetadataStorage::atomicWrite(uint32_t objectId, char *data, uint32_t dataLength) {
  verifyOperation(objectId, dataLength, data, true);
  auto *dataCopy = new uint8_t[dataLength];
  memcpy(dataCopy, data, dataLength);
  lock_guard<mutex> lock(ioMutex_);
  Status status = dbClient_->put(genMetadataKey_(objectId), Sliver(dataCopy, dataLength));
  if (!status.isOK()) {
    throw runtime_error("DBClient put operation failed");
  }
}

void DBMetadataStorage::beginAtomicWriteOnlyBatch() {
  LOG_DEBUG(logger_, "Begin atomic transaction");
  lock_guard<mutex> lock(ioMutex_);
  if (batch_) {
    LOG_INFO(logger_, "Transaction has been opened before; ignoring");
    return;
  }
  batch_ = new SetOfKeyValuePairs;
}

void DBMetadataStorage::writeInBatch(uint32_t objectId, char *data, uint32_t dataLength) {
  LOG_DEBUG(logger_, "objectId=" << objectId << ", dataLength=" << dataLength);
  verifyOperation(objectId, dataLength, data, true);
  auto *dataCopy = new uint8_t[dataLength];
  memcpy(dataCopy, data, dataLength);
  lock_guard<mutex> lock(ioMutex_);
  if (!batch_) {
    LOG_ERROR(logger_, WRONG_FLOW);
    throw runtime_error(WRONG_FLOW);
  }
  batch_->insert(KeyValuePair(genMetadataKey_(objectId), Sliver(dataCopy, dataLength)));
}

void DBMetadataStorage::commitAtomicWriteOnlyBatch() {
  LOG_DEBUG(logger_, "Commit atomic transaction");
  lock_guard<mutex> lock(ioMutex_);
  if (!batch_) {
    LOG_ERROR(logger_, WRONG_FLOW);
    throw runtime_error(WRONG_FLOW);
  }
  Status status = dbClient_->multiPut(*batch_);
  if (!status.isOK()) {
    throw runtime_error("DBClient multiPut operation failed");
  }
  delete batch_;
  batch_ = nullptr;
}

Status DBMetadataStorage::multiDel(const ObjectIdsVector &objectIds) {
  size_t objectsNumber = objectIds.size();
  assert(objectsNum_ >= objectsNumber);
  LOG_DEBUG(logger_, "Going to perform multiple delete");
  KeysVector keysVec;
  for (size_t objectId = 0; objectId < objectsNumber; objectId++) {
    auto key = genMetadataKey_(objectId);
    keysVec.push_back(key);
    LOG_INFO(logger_, "Deleted object id=" << objectId << ", key=" << key);
  }
  return dbClient_->multiDel(keysVec);
}

}
}
