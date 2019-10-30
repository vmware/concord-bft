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

#include "ObjectsMetadataHandler.hpp"

#include <stdio.h>
#include <cstring>

using namespace std;

namespace bftEngine {

ostream &operator<<(ostream &stream, const ObjectsMetadataHandler &objectsMetadataHandler) {
  stream << "ObjectsMetadataHandler: objectsNumber=" << objectsMetadataHandler.getObjectsNum() << '\n';
  for (auto it : objectsMetadataHandler.getObjectsMap()) {
    stream << it.second.toString() << '\n';
  }
  return stream;
}

size_t ObjectsMetadataHandler::getObjectsMetadataSize() {
  return (sizeof(objectsNum_) + sizeof(MetadataObjectInfo) * objectsNum_);
}

void ObjectsMetadataHandler::setObjectInfo(const MetadataObjectInfo &objectInfo) {
  objectsMap_.insert(pair<uint32_t, MetadataObjectInfo>(objectInfo.id, objectInfo));
}

MetadataObjectInfo *ObjectsMetadataHandler::getObjectInfo(uint32_t objectId) {
  auto it = objectsMap_.find(objectId);
  if (it != objectsMap_.end()) {
    return &(it->second);
  }
  return nullptr;
}

}  // namespace bftEngine
