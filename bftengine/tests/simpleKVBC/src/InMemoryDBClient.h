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

/** @file InMemoryDBClient.h
 *  @brief Header file containing the InMemoryDBClient and
 * InMemoryDBClientIterator class definitions.
 *
 *  Objects of InMemoryDBClientIterator contain an iterator for the in memory
 * object store (implemented as a map) along with a pointer to the map.
 *
 *  Objects of InMemoryDBClient are implementations of an in memory database
 * (implemented as a map). The map contains key value pairs of the type
 * KeyValuePair. Keys and values are of type Slice.
 *
 */

#pragma once
#include "DatabaseInterface.h"
#include <map>

namespace SimpleKVBC {
class InMemoryDBClient;

typedef std::map<Slice, Slice, IDBClient::KeyComparator> TKVStore;

class InMemoryDBClientIterator : public IDBClient::IDBClientIterator {
  friend class InMemoryDBClient;

 public:
  InMemoryDBClientIterator(InMemoryDBClient* _parentClient)
      : m_parentClient(_parentClient) {}
  virtual ~InMemoryDBClientIterator() {}

  // Inherited via IDBClientIterator
  virtual KeyValuePair first() override;
  virtual KeyValuePair seekAtLeast(Slice _searchKey) override;
  virtual KeyValuePair next() override;
  virtual KeyValuePair getCurrent() override;
  virtual bool isEnd() override;
  virtual Status getStatus() override;

 private:
  // rocksdb::Iterator* m_iter;
  InMemoryDBClient* m_parentClient;    ///< Pointer to the InMemoryDBClient.
  TKVStore::const_iterator m_current;  ///< Current iterator inside the map.
};

class InMemoryDBClient : public IDBClient {
 public:
  InMemoryDBClient();

  virtual Status init() override;
  virtual Status get(Slice _key, OUT Slice& _outValue) const override;
  virtual Status hasKey(Slice key) const override;
  virtual IDBClientIterator* getIterator() const override;
  virtual Status freeIterator(IDBClientIterator* _iter) const override;
  virtual Status put(Slice _key, Slice _value) override;
  virtual Status close() override { return Status::OK(); };
  virtual Status del(Slice _key) override;
  virtual Status freeValue(Slice& _value) override;

  TKVStore& getMap() { return map; }
  void setComparator(KeyComparator comp) { map = TKVStore(comp); }

 private:
  TKVStore map;  ///< map that stores the in memory database.
};
}  // namespace SimpleKVBC
