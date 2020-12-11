// Concord
//
// Copyright (c) 2020 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the
// "License").  You may not use this product except in compliance with the
// Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#pragma once

#ifdef USE_ROCKSDB

#include <rocksdb/write_batch.h>

#include <memory>
#include <string>

namespace concord::storage::rocksdb {

class NativeClient;

// For use with NativeClient. See native_client.h .
class NativeWriteBatch {
 public:
  NativeWriteBatch(const std::shared_ptr<const NativeClient> &) noexcept;

  template <typename KeySpan, typename ValueSpan>
  void put(const std::string &cFamily, const KeySpan &key, const ValueSpan &value);
  template <typename KeySpan, typename ValueSpan>
  void put(const KeySpan &key, const ValueSpan &value);

  // Deleting a key that doesn't exist is not an error.
  template <typename KeySpan>
  void del(const std::string &cFamily, const KeySpan &key);
  template <typename KeySpan>
  void del(const KeySpan &key);

  // Remove the DB entries in the range [beginKey, endKey).
  template <typename BeginSpan, typename EndSpan>
  void delRange(const std::string &cFamily, const BeginSpan &beginKey, const EndSpan &endKey);
  template <typename BeginSpan, typename EndSpan>
  void delRange(const BeginSpan &beginKey, const EndSpan &endKey);

 private:
  std::shared_ptr<const NativeClient> client_;
  ::rocksdb::WriteBatch batch_;
  friend class NativeClient;
};

// Put a container of key-value pair spans.
template <typename Container>
void putInBatch(NativeWriteBatch &batch, const std::string &cFamily, const Container &cont);
template <typename Container>
void putInBatch(NativeWriteBatch &batch, const Container &cont);

// Delete a container of key spans.
template <typename Container>
void delInBatch(NativeWriteBatch &batch, const std::string &cFamily, const Container &cont);
template <typename Container>
void delInBatch(NativeWriteBatch &batch, const Container &cont);

}  // namespace concord::storage::rocksdb

#endif  // USE_ROCKSDB
