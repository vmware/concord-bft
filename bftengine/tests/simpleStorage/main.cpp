// Concord
//
// Copyright (c) 2019 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of sub-components with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the sub-component's license, as noted in the LICENSE
// file.

#include "FileStorage.hpp"
#include "assertUtils.hpp"

#include <string.h>
#include <cassert>

using namespace std;
using namespace bftEngine;

struct Object {
  Object() : elem1(0), elem2(0), elem3(0), elem4(0) {}
  Object(unsigned int e1, unsigned long e2, unsigned short e3, unsigned int e4)
      : elem1(e1), elem2(e2), elem3(e3), elem4(e4) {}

  unsigned int elem1;
  unsigned long elem2;
  unsigned short elem3;
  unsigned int elem4;
};

const unsigned int ELEM1 = 0xaaaaaaaa;
const unsigned long ELEM2 = 0xbbbbbbbbbbbbbbb;
const unsigned short ELEM3 = 0xccc;
const unsigned int ELEM4 = 0xdddddddd;

void verifyData(FileStorage &fileStorage, uint32_t objId) {
  Object objIn;
  char *objInBuf = new char[sizeof(objIn)];
  uint32_t actualObjectSize = 0;
  fileStorage.read(objId, sizeof(objIn), objInBuf, actualObjectSize);
  memcpy(&objIn, objInBuf, sizeof(objIn));
  printf(
      "*** Read from the storage: objIn.elem1=0x%xd, objIn.elem2=%ld,"
      " objIn.elem3=0x%xd, objIn.elem4=0x%xd\n",
      objIn.elem1,
      objIn.elem2,
      objIn.elem3,
      objIn.elem4);
  ConcordAssert(objIn.elem1 == ELEM1);
  ConcordAssert(objIn.elem2 == ELEM2);
  ConcordAssert(objIn.elem3 == ELEM3);
  ConcordAssert(objIn.elem4 == ELEM4);
}

// The tests verify that all MetadataStorage interfaces work correctly using file-based storage.
int main(int argc, char **argv) {
  try {
    auto logger = logging::getLogger("simpleStorage.test");
    string dbFile = "fileStorageTest.txt";
    remove(dbFile.c_str());
    auto *fileStorage = new FileStorage(logger, dbFile);
    const uint32_t objNum = 6;
    FileStorage::ObjectDesc objects[objNum];
    // Metadata object with id=1 is used to indicate storage initialization state
    for (uint32_t i = 2; i < objNum; i++) {
      objects[i].id = i;
      objects[i].maxSize = (uint32_t)i * 2 + 30;
    }
    fileStorage->initMaxSizeOfObjects(objects, objNum);
    Object objOut = {ELEM1, ELEM2, ELEM3, ELEM4};
    const uint32_t objId1 = 2;
    char *objOutBuf = new char[sizeof(objOut)];
    memcpy(objOutBuf, &objOut, sizeof(objOut));
    fileStorage->atomicWrite(objId1, objOutBuf, sizeof(objOut));
    verifyData(*fileStorage, objId1);

    const uint32_t objId2 = 3;
    const uint32_t objId3 = 4;
    const uint32_t objId4 = 5;
    fileStorage->beginAtomicWriteOnlyBatch();
    fileStorage->writeInBatch(objId2, objOutBuf, sizeof(objOut));
    fileStorage->writeInBatch(objId3, objOutBuf, sizeof(objOut));
    fileStorage->writeInBatch(objId4, objOutBuf, sizeof(objOut));
    fileStorage->commitAtomicWriteOnlyBatch();
    for (int i = 0; i < 2; i++) {
      verifyData(*fileStorage, objId2);
      verifyData(*fileStorage, objId3);
      verifyData(*fileStorage, objId4);
      delete fileStorage;
      // Verify that data persists after a restart.
      if (i == 0) {
        fileStorage = new FileStorage(logger, dbFile);
        fileStorage->initMaxSizeOfObjects(objects, objNum);
      }
    }
  } catch (exception &e) {
    return -1;
  }

  return 0;
}
