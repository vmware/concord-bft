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

#include "DbMetadataStorage.hpp"
#include "assertUtils.hpp"
#include <cstring>
#include <exception>

using namespace std;

using concordUtils::Status;

namespace concord {
namespace storage {

void DBMetadataStorage::verifyOperation(uint32_t objectId,
                                        uint32_t dataLen,
                                        const char *buffer,
                                        bool writeOperation) const {
  auto elem = objectIdToSizeMap_.find(objectId);
  bool found = (elem != objectIdToSizeMap_.end());
  if (!dataLen || !buffer || !found || !objectId) {
    throw runtime_error(WRONG_PARAMETER);
  }
  if (writeOperation && (dataLen > elem->second)) {
    ostringstream error;
    error << "Metadata object objectId " << objectId << " size is too big: given " << dataLen << ", allowed "
          << elem->second << endl;
    throw runtime_error(error.str());
  }
}

bool DBMetadataStorage::isNewStorage() {
  uint32_t outActualObjectSize;
  read(objectsNumParameterId_, sizeof(objectsNum_), (char *)&objectsNum_, outActualObjectSize);
  return (outActualObjectSize == 0);
}

bool DBMetadataStorage::initMaxSizeOfObjects(const std::map<uint32_t, ObjectDesc> &metadataObjectsArray,
                                             uint32_t metadataObjectsArrayLength) {
  for (uint32_t i = objectsNumParameterId_ + 1; i < metadataObjectsArrayLength; ++i) {
    const auto objectData = metadataObjectsArray.at(i);
    objectIdToSizeMap_[i] = objectData.maxSize;
    LOG_TRACE(
        logger_,
        "initMaxSizeOfObjects i=" << i << " object data: id=" << objectData.id << ", maxSize=" << objectData.maxSize);
  }
  // Metadata object with id=1 is used to indicate storage initialization state
  // (number of specified metadata objects).
  bool isNew = isNewStorage();
  if (isNew) {
    objectsNum_ = metadataObjectsArrayLength;
    atomicWrite(objectsNumParameterId_, (char *)&objectsNum_, sizeof(objectsNum_));
  }
  LOG_TRACE(logger_, "initMaxSizeOfObjects objectsNum_=" << objectsNum_);
  return isNew;
}

void DBMetadataStorage::read(uint32_t objectId,
                             uint32_t bufferSize,
                             char *outBufferForObject,
                             uint32_t &outActualObjectSize) {
  verifyOperation(objectId, bufferSize, outBufferForObject, false);
  lock_guard<mutex> lock(ioMutex_);
  Status status = dbClient_->get(
      metadataKeyManipulator_->generateMetadataKey(objectId), outBufferForObject, bufferSize, outActualObjectSize);
  if (status.isNotFound()) {
    memset(outBufferForObject, 0, bufferSize);
    outActualObjectSize = 0;
    return;
  }
  if (!status.isOK()) {
    throw runtime_error("DBClient get operation failed");
  }
}

void DBMetadataStorage::atomicWrite(uint32_t objectId, const char *data, uint32_t dataLength) {
  verifyOperation(objectId, dataLength, data, true);
  Sliver copy = Sliver::copy(data, dataLength);
  lock_guard<mutex> lock(ioMutex_);
  Status status = dbClient_->put(metadataKeyManipulator_->generateMetadataKey(objectId), copy);
  if (!status.isOK()) {
    throw runtime_error("DBClient put operation failed");
  }
}

void DBMetadataStorageUnbounded::atomicWriteArbitraryObject(const std::string &key,
                                                            const char *data,
                                                            uint32_t dataLength) {
  Sliver k = Sliver::copy(key.data(), key.length());
  Sliver v = Sliver::copy(data, dataLength);
  lock_guard<mutex> lock(ioMutex_);
  LOG_DEBUG(logger_, "key: " << key);
  Status status = dbClient_->put(metadataKeyManipulator_->generateMetadataKey(k), v);
  if (!status.isOK()) {
    throw runtime_error("DBClient put operation failed");
  }
}

void DBMetadataStorage::atomicWriteArbitraryObject(const std::string &key, const char *data, uint32_t dataLength) {
  LOG_ERROR(GL, "shouldn't have been called. key: " << key);
  throw runtime_error("DBMetadataStorage::atomicWriteArbitraryObject() shouldn't have been called.");
}

void DBMetadataStorage::beginAtomicWriteOnlyBatch() {
  lock_guard<mutex> lock(ioMutex_);
  LOG_DEBUG(logger_, "Begin atomic transaction");
  if (batch_) {
    LOG_INFO(logger_, "Transaction has been opened before; ignoring");
    return;
  }
  batch_ = new SetOfKeyValuePairs;
}

void DBMetadataStorage::writeInBatch(uint32_t objectId, const char *data, uint32_t dataLength) {
  LOG_TRACE(logger_, "writeInBatch: objectId=" << objectId << ", dataLength=" << dataLength);
  verifyOperation(objectId, dataLength, data, true);
  Sliver copy = Sliver::copy(data, dataLength);
  lock_guard<mutex> lock(ioMutex_);
  if (!batch_) {
    LOG_FATAL(logger_, WRONG_FLOW);
    throw runtime_error(WRONG_FLOW);
  }
  // Delete an older parameter with the same key (if exists) before inserting a new one.
  auto elem = batch_->find(metadataKeyManipulator_->generateMetadataKey(objectId));
  if (elem != batch_->end()) batch_->erase(elem);
  batch_->insert(KeyValuePair(metadataKeyManipulator_->generateMetadataKey(objectId), copy));
}

void DBMetadataStorage::commitAtomicWriteOnlyBatch(bool sync) {
  lock_guard<mutex> lock(ioMutex_);
  LOG_DEBUG(logger_, "Begin Commit atomic transaction");
  if (!batch_) {
    LOG_FATAL(logger_, WRONG_FLOW);
    throw runtime_error(WRONG_FLOW);
  }
  Status status = dbClient_->multiPut(*batch_, sync);
  LOG_DEBUG(logger_, "End Commit atomic transaction");
  if (!status.isOK()) {
    LOG_FATAL(logger_, "DBClient multiPut operation failed");
    throw runtime_error("DBClient multiPut operation failed");
  }
  delete batch_;
  batch_ = nullptr;
}

Status DBMetadataStorage::multiDel(const ObjectIdsVector &objectIds) {
  size_t objectsNumber = objectIds.size();
  ConcordAssertGE(objectsNum_, objectsNumber);
  LOG_TRACE(logger_, "Going to perform multiple delete");
  KeysVector keysVec;
  for (size_t objectId = 0; objectId < objectsNumber; objectId++) {
    auto key = metadataKeyManipulator_->generateMetadataKey(objectId);
    keysVec.push_back(key);
    LOG_INFO(logger_, "Deleted object id=" << objectId << ", key=" << key);
  }
  return dbClient_->multiDel(keysVec);
}
void DBMetadataStorage::eraseData() { cleanDB(); }
void DBMetadataStorage::cleanDB() {
  try {
    ObjectIdsVector objectIds = {objectsNumParameterId_};
    for (const auto &id : objectIdToSizeMap_) {
      // If the object id is higher than the objectsNum_ it means that we try to remove metadata (and hence the size of
      // objectsNum_ is still the old one)
      if (id.first >= objectsNum_) {
        LOG_WARN(
            logger_,
            "during cleaning the metadata, an invalid object was detected, this is probably due to reconfiguration");
        continue;
      }
      objectIds.push_back(id.first);
    }
    multiDel(objectIds);
  } catch (std::exception &e) {
    LOG_FATAL(logger_, e.what());
    std::terminate();
  }
}

}  // namespace storage
}  // namespace concord
