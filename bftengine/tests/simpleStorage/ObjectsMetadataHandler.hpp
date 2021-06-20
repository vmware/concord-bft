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

#pragma once

#include "MetadataStorageTypes.hpp"

namespace bftEngine {

typedef std::map<uint32_t, MetadataObjectInfo> MetadataObjectIdToInfoMap;

// This class represents a memory copy of objects metadata information stored
// in the beginning of the DB file.
class ObjectsMetadataHandler {
 public:
  explicit ObjectsMetadataHandler(uint32_t objectsNum) : objectsNum_(objectsNum) {}

  size_t getObjectsMetadataSize();
  void setObjectInfo(const MetadataObjectInfo &objectInfo);
  MetadataObjectInfo *getObjectInfo(uint32_t objectId);
  uint32_t getObjectsNum() const { return objectsNum_; }
  MetadataObjectIdToInfoMap getObjectsMap() const { return objectsMap_; }
  friend std::ostream &operator<<(std::ostream &stream, const ObjectsMetadataHandler &fileMetadata);

 private:
  uint32_t objectsNum_;
  MetadataObjectIdToInfoMap objectsMap_;
};

}  // namespace bftEngine
