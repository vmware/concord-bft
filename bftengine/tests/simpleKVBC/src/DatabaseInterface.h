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

/** @file DatabaseInterface.h
 *  @brief Header file containing the IDBClient and IDBClientIterator class
 * definitions.
 *
 */

#pragma once
#include "Status.h"
#include "Slice.h"

#define OUT

using std::pair;

namespace SimpleKVBC {
typedef pair<Slice, Slice> KeyValuePair;

class IDBClient {
 public:
  typedef bool (*KeyComparator)(const Slice&, const Slice&);

  virtual Status init() = 0;
  virtual Status close() = 0;
  virtual Status get(Slice _key, OUT Slice& _outValue) const = 0;
  virtual Status hasKey(Slice key) const = 0;
  virtual Status put(Slice _key, Slice _value) = 0;
  virtual Status del(Slice _key) = 0;
  virtual Status freeValue(Slice& _value) = 0;

  // TODO(GG): add multi-get , multi-put, and multi-del

  class IDBClientIterator {
   public:
    virtual KeyValuePair first() = 0;
    virtual KeyValuePair seekAtLeast(
        Slice _searchKey) = 0;  ///< Returns next keys if not found for this key
    virtual KeyValuePair next() = 0;
    virtual KeyValuePair getCurrent() = 0;
    virtual bool isEnd() = 0;
    virtual Status getStatus() = 0;  ///< Status of last operation
  };

  virtual IDBClientIterator* getIterator() const = 0;
  virtual Status freeIterator(IDBClientIterator* _iter) const = 0;
};
}  // namespace SimpleKVBC
