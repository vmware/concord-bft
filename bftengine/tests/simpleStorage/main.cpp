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
#include "../../src/bftengine/assertUtils.hpp"

#include <string.h>
#include <cassert>

using namespace bftEngine;

struct Object {
  Object() : elem1(0), elem2(0), elem3(0), elem4(0) {}
  Object(int e1, long e2, short e3, int e4) :
      elem1(e1), elem2(e2), elem3(e3), elem4(e4) {}

  int elem1;
  long elem2;
  short elem3;
  int elem4;
};

const int ELEM1 = 0xaaaaaaaa;
const long ELEM2 = 0xbbbbbbbbbbbbbbb;
const short ELEM3 = 0xccc;
const int ELEM4 = 0xdddddddd;

void verifyData(FileStorage &fileStorage, uint16_t objId) {
  Object objIn;
  char *objInBuf = new char[sizeof(objIn)];
  uint32_t actualObjectSize = 0;
  fileStorage.read(objId, sizeof(objIn), objInBuf, actualObjectSize);
  memcpy(&objIn, objInBuf, sizeof(objIn));
  printf("*** Read from the storage: objIn.elem1=0x%xd, objIn.elem2=%ld,"
         " objIn.elem3=0x%xd, objIn.elem4=0x%xd\n", objIn.elem1, objIn.elem2,
         objIn.elem3, objIn.elem4);
  assert(objIn.elem1 == ELEM1);
  assert(objIn.elem2 == ELEM2);
  assert(objIn.elem3 == ELEM3);
  assert(objIn.elem4 == ELEM4);
}

// The tests verify that all MetadataStorage interfaces work correctly using
// file-based storage.
int main(int argc, char **argv) {

  try {
    auto logger = concordlogger::Logger::getLogger("simpleStorage.test");
    FileStorage fileStorage(logger, "test.txt");
    const uint32_t objNum = 6;
    FileStorage::ObjectDesc objects[objNum];
    for (uint16_t i = 0; i < objNum; i++) {
      objects[i].id = i;
      objects[i].maxSize = (uint32_t) i * 2 + 30;
    }
    fileStorage.initMaxSizeOfObjects(objects, objNum);
    Object objOut = {ELEM1, ELEM2, ELEM3, ELEM4};
    const uint16_t objId1 = 1;
    char *objOutBuf = new char[sizeof(objOut)];
    memcpy(objOutBuf, &objOut, sizeof(objOut));
    fileStorage.atomicWrite(objId1, objOutBuf, sizeof(objOut));
    verifyData(fileStorage, objId1);

    const uint16_t objId2 = 2;
    const uint16_t objId3 = 3;
    const uint16_t objId4 = 4;
    fileStorage.beginAtomicWriteOnlyTransaction();
    fileStorage.writeInTransaction(objId2, objOutBuf, sizeof(objOut));
    fileStorage.writeInTransaction(objId3, objOutBuf, sizeof(objOut));
    fileStorage.writeInTransaction(objId4, objOutBuf, sizeof(objOut));
    fileStorage.commitAtomicWriteOnlyTransaction();
    verifyData(fileStorage, objId2);
    verifyData(fileStorage, objId3);
    verifyData(fileStorage, objId4);
  } catch (std::exception &e) {
    return -1;
  }

  return 0;
}

