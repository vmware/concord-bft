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

#include <sstream>
#include <memory>
#include <map>
#include <cstring>

namespace bftEngine {

// Single metadata object structure stored in the beginning of the DB file
// for each object.
struct MetadataObjectInfo {
  explicit MetadataObjectInfo() : id(0), metadataOffset(0), offset(0), realSize(0), maxSize(0) {}
  MetadataObjectInfo(uint16_t objId, uint64_t objMetadataOffset, uint64_t objOffset, uint32_t maxObjSize)
      : id(objId), metadataOffset(objMetadataOffset), offset(objOffset), realSize(0), maxSize(maxObjSize) {}

  std::string toString() {
    std::ostringstream stream;
    stream << "MetadataObjectInfo: id=" << id << ", metadataOffset=" << metadataOffset << ", offset=" << offset
           << ", realSize=" << realSize << ", maxSize=" << maxSize << " ";
    return stream.str();
  }

  uint16_t id;
  uint64_t metadataOffset;
  uint64_t offset;
  uint32_t realSize;
  uint32_t maxSize;
};

// Object data to be stored for every write operation in transaction.
struct RequestInfo {
  RequestInfo(const char *buf, uint32_t len) {
    dataLen = len;
    data.reset(new char[dataLen]);
    std::memcpy(data.get(), buf, dataLen);
  }
  std::unique_ptr<char[], std::default_delete<char[]>> data;
  uint32_t dataLen;
};

typedef std::map<uint16_t, RequestInfo> ObjectIdToRequestMap;

}  // namespace bftEngine
