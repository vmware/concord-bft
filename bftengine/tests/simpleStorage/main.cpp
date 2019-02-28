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

using namespace bftEngine;

int main(int argc, char **argv) {

  auto logger = concordlogger::Logger::getLogger("simpleStorage.test");
  try {
    FileStorage fileStorage(logger, "test.txt");
    FileStorage::ObjectDesc objects[5];
    for (uint16_t i = 0; i < 5; i++) {
      objects[i].id = i;
      objects[i].maxSize = i + 48;
    }
    fileStorage.initMaxSizeOfObjects(objects, 5);
  } catch (exception& e) {
    return -1;
  }

  return 0;
}

