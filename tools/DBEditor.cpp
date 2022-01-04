// Concord
//
// Copyright (c) 2019 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.
// This module provides an ability for offline DB modifications for a specific
// replica.

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fstream>
#include <sstream>
#include <string>
#include <iostream>

#define USE_ROCKSDB 1

#include "assertUtils.hpp"
#include "Logger.hpp"
#include "rocksdb/key_comparator.h"
#include "rocksdb/client.h"
#include "bftengine/DbMetadataStorage.hpp"
#include "direct_kv_block.h"
#include "direct_kv_db_adapter.h"
#include "storage/direct_kv_key_manipulator.h"

using namespace bftEngine;
using namespace std;

using concord::storage::rocksdb::Client;
using concord::storage::rocksdb::KeyComparator;
using concord::storage::DBMetadataStorage;
using namespace concord::storage;
using namespace concord::storage::v1DirectKeyValue;
using namespace concord::kvbc;
using namespace concord::kvbc::v1DirectKeyValue;

stringstream dbPath;
Client *dbClient = nullptr;
const uint32_t MAX_OBJECT_ID = 10;
const uint32_t MAX_OBJECT_SIZE = 200;
uint32_t numOfObjectsToAdd = MAX_OBJECT_ID;
const uint32_t firstObjId = 2;
DBMetadataStorage *metadataStorage = nullptr;
ObjectIdsVector objectIdsVector;

enum DB_OPERATION {
  NO_OPERATION,
  GET_LAST_BLOCK_SEQ_NBR,
  ADD_STATE_METADATA_OBJECTS,
  DELETE_LAST_STATE_METADATA,
  DUMP_ALL_VALUES
};

DB_OPERATION dbOperation = NO_OPERATION;
logging::Logger logger = logging::getLogger("skvbtest.db_editor");

void printUsageAndExit(char **argv) {
  LOG_ERROR(logger,
            "Wrong usage! \nRequired parameters: " << argv[0] << " -p FULL_PATH_TO_DB_DIR \n"
                                                   << "One of those parameter parameters should also be "
                                                      "specified: -s / -g / -d / -a");
  exit(-1);
}

void setupDBEditorParams(int argc, char **argv) {
  string valStr;
  char argTempBuffer[PATH_MAX + 10];
  int o = 0;
  uint32_t tempNum = 0;
  while ((o = getopt(argc, argv, "gdsa:p:")) != EOF) {
    switch (o) {
      case 'p':
        strncpy(argTempBuffer, optarg, sizeof(argTempBuffer) - 1);
        argTempBuffer[sizeof(argTempBuffer) - 1] = 0;
        dbPath << argTempBuffer;
        break;
      case 'g':
        dbOperation = GET_LAST_BLOCK_SEQ_NBR;
        break;
      case 'd':
        dbOperation = DELETE_LAST_STATE_METADATA;
        break;
      case 's':
        dbOperation = DUMP_ALL_VALUES;
        break;
      case 'a':
        strncpy(argTempBuffer, optarg, sizeof(argTempBuffer) - 1);
        argTempBuffer[sizeof(argTempBuffer) - 1] = 0;
        valStr = argTempBuffer;
        tempNum = stoi(valStr);
        if (tempNum >= 0 && tempNum < MAX_OBJECT_ID) numOfObjectsToAdd = tempNum;
        dbOperation = ADD_STATE_METADATA_OBJECTS;
        break;
      default:
        printUsageAndExit(argv);
    }
  }
}

void setupMetadataStorage() {
  std::map<uint32_t, MetadataStorage::ObjectDesc> objectsDesc;
  MetadataStorage::ObjectDesc objectDesc = {0, MAX_OBJECT_SIZE};
  for (uint32_t i = firstObjId; i < MAX_OBJECT_ID; i++) {
    objectDesc.id = i;
    objectsDesc[i] = objectDesc;
    objectIdsVector.push_back(i);
  }
  metadataStorage =
      new DBMetadataStorage(dbClient, std::make_unique<concord::storage::v1DirectKeyValue::MetadataKeyManipulator>());
  metadataStorage->initMaxSizeOfObjects(objectsDesc, MAX_OBJECT_ID);
}

void dumpObjectBuf(uint32_t objectId, char *objectData, size_t objectLen) {
  LOG_INFO(logger, "*** State metadata for object id " << objectId << " :");
  printf("0x");
  for (size_t i = 0; i < objectLen; ++i) printf("%.2x", objectData[i]);
  printf("\n");
}

bool addStateMetadataObjects() {
  char objectData[MAX_OBJECT_SIZE];
  memset(objectData, 0, MAX_OBJECT_SIZE);
  LOG_INFO(logger, "*** Going to add " << numOfObjectsToAdd << " objects");
  for (uint32_t objectId = firstObjId; objectId < numOfObjectsToAdd; objectId++) {
    memcpy(objectData, &objectId, sizeof(objectId));
    metadataStorage->atomicWrite(objectId, objectData, MAX_OBJECT_SIZE);
    dumpObjectBuf(objectId, objectData, MAX_OBJECT_SIZE);
  }
  return true;
}

bool getLastStateMetadata() {
  uint32_t outActualObjectSize = 0;
  char outBufferForObject[MAX_OBJECT_SIZE];
  bool found = false;
  for (uint32_t i = firstObjId; i < MAX_OBJECT_ID; i++) {
    metadataStorage->read(i, MAX_OBJECT_SIZE, outBufferForObject, outActualObjectSize);
    if (outActualObjectSize) {
      found = true;
      dumpObjectBuf(i, outBufferForObject, MAX_OBJECT_SIZE);
    }
  }
  if (!found) LOG_ERROR(logger, "No State Metadata objects found");
  return found;
}

bool deleteLastStateMetadata() {
  concordUtils::Status status = metadataStorage->multiDel(objectIdsVector);
  if (!status.isOK()) {
    LOG_ERROR(logger, "Failed to delete metadata keys");
    return false;
  }
  return true;
}

void verifyInputParams(char **argv) {
  if (dbPath.str().empty() || (dbOperation == NO_OPERATION)) printUsageAndExit(argv);

  ifstream ifile(dbPath.str());
  if (ifile.good()) {
    return;
  }

  LOG_ERROR(logger, "Specified DB directory " << dbPath.str() << " does not exist.");
  exit(-1);
}

void parseAndPrint(const ::rocksdb::Slice &key, const ::rocksdb::Slice &val) {
  const auto aType = DBKeyManipulator::extractTypeFromKey(key.data());

  switch (aType) {
    case detail::EDBKeyType::E_DB_KEY_TYPE_BFT_ST_KEY:
    case detail::EDBKeyType::E_DB_KEY_TYPE_BFT_ST_PENDING_PAGE_KEY:
    case detail::EDBKeyType::E_DB_KEY_TYPE_BFT_METADATA_KEY: {
      //      // Compare object IDs.
      //      ObjectId aObjId = KeyManipulator::extractObjectIdFromKey(_a_data, _a_length);
      //      ObjectId bObjId = KeyManipulator::extractObjectIdFromKey(_b_data, _b_length);
      //      return (aObjId > bObjId) ? 1 : (bObjId > aObjId) ? -1 : 0;
      break;
    }
    case detail::EDBKeyType::E_DB_KEY_TYPE_BFT_ST_CHECKPOINT_DESCRIPTOR_KEY: {
      //      uint64_t aChkpt, bChkpt;
      //      aChkpt = extractCheckPointFromKey(_a_data, _a_length);
      //      bChkpt = extractCheckPointFromKey(_b_data, _b_length);
      //      return (aChkpt > bChkpt) ? 1 : (bChkpt > aChkpt) ? -1 : 0;
      break;
    }
    case detail::EDBKeyType::E_DB_KEY_TYPE_BFT_ST_RESERVED_PAGE_DYNAMIC_KEY: {
      //      // Pages are sorted in ascending order, checkpoints in descending order
      //       uint32_t aPageId, bPageId;
      //       uint64_t aChkpt, bChkpt;
      //       std::tie(aPageId, aChkpt) = extractPageIdAndCheckpointFromKey(_a_data, _a_length);
      //       std::tie(bPageId, bChkpt) = extractPageIdAndCheckpointFromKey(_b_data, _b_length);
      //       if (aPageId != bPageId) return (aPageId > bPageId)? 1: (bPageId > aPageId)? -1:0;
      //       return (aChkpt < bChkpt)? 1 : (aChkpt > bChkpt) ? -1 : 0;
      break;
    }
    case detail::EDBKeyType::E_DB_KEY_TYPE_KEY: {
      //      int keyComp = KeyManipulator::compareKeyPartOfComposedKey(_a_data, _a_length, _b_data, _b_length);
      //      if (keyComp != 0) return keyComp;
      //      // Extract the block ids to compare so that endianness of environment does not matter.
      //      BlockId aId = KeyManipulator::extractBlockIdFromKey(_a_data, _a_length);
      //      BlockId bId = KeyManipulator::extractBlockIdFromKey(_b_data, _b_length);
      //      // Block ids are sorted in reverse order when part of a composed key (key + blockId)
      //      return (bId > aId) ? 1 : (aId > bId) ? -1 : 0;
      break;
    }
    case detail::EDBKeyType::E_DB_KEY_TYPE_BLOCK: {
      //      // Extract the block ids to compare so that endianness of environment does not matter.
      BlockId aId = DBKeyManipulator::extractBlockIdFromKey(key.data(), key.size());
      std::cout << "Block ID: " << aId << std::endl;
      const auto numOfElements = ((block::detail::Header *)val.data())->numberOfElements;
      auto *entries = (block::detail::Entry *)(val.data() + sizeof(block::detail::Header));
      for (size_t i = 0u; i < numOfElements; i++) {
        std::string kv_key = std::string(val.ToString(), entries[i].keyOffset, entries[i].keySize);
        for (size_t i = 0; i < kv_key.size(); ++i) printf("%.2x", kv_key[i]);
        printf("\n");
      }
      break;
    }
    default:
      LOG_ERROR(logger, "invalid key type: " << (char)aType);
      ConcordAssert(false);

  }  // switch
}

void dumpAllValues() {
  ::rocksdb::Iterator *it = dbClient->getNewRocksDbIterator();
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    //    printf("Key = 0x");
    //    for (size_t i = 0; i < it->key().size(); ++i) printf("%.2x", it->key()[i]);
    //    printf("\nValue = 0x");
    //    for (size_t i = 0; i < it->value().size(); ++i) printf("%.2x", it->value()[i]);
    //    printf("\n");
    parseAndPrint(it->key(), it->value());
  }
  ConcordAssert(it->status().ok());
  delete it;
}

int main(int argc, char **argv) {
  try {
    setupDBEditorParams(argc, argv);
    verifyInputParams(argv);

    dbClient = new Client(dbPath.str(), std::make_unique<KeyComparator>(new DBKeyComparator{}));
    dbClient->init(dbOperation == DUMP_ALL_VALUES);
    if (dbOperation != DUMP_ALL_VALUES) setupMetadataStorage();
    bool res = false;
    switch (dbOperation) {
      case GET_LAST_BLOCK_SEQ_NBR:
        res = getLastStateMetadata();
        break;
      case DELETE_LAST_STATE_METADATA:
        res = deleteLastStateMetadata();
        break;
      case ADD_STATE_METADATA_OBJECTS:
        res = addStateMetadataObjects();
        break;
      case DUMP_ALL_VALUES:
        dumpAllValues();
        break;
      default:;
    }

    string result = res ? "success" : "fail";
    LOG_INFO(logger, "*** Operation completed with result: " << result);
    return res;
  } catch (std::exception &e) {
    std::cerr << "exception: " << e.what() << std::endl;
    return 1;
  }
}
