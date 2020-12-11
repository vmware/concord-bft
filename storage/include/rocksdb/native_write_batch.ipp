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

namespace concord::storage::rocksdb {

inline NativeWriteBatch::NativeWriteBatch(const std::shared_ptr<const NativeClient> &client) noexcept
    : client_{client} {}

template <typename KeySpan, typename ValueSpan>
void NativeWriteBatch::put(const std::string &cFamily, const KeySpan &key, const ValueSpan &value) {
  auto s = batch_.Put(client_->columnFamilyHandle(cFamily), detail::toSlice(key), detail::toSlice(value));
  detail::throwOnError("batch put failed"sv, std::move(s));
}

template <typename KeySpan, typename ValueSpan>
void NativeWriteBatch::put(const KeySpan &key, const ValueSpan &value) {
  put(NativeClient::defaultColumnFamily(), key, value);
}

template <typename KeySpan>
void NativeWriteBatch::del(const std::string &cFamily, const KeySpan &key) {
  auto s = batch_.Delete(client_->columnFamilyHandle(cFamily), detail::toSlice(key));
  detail::throwOnError("batch del failed"sv, std::move(s));
}

template <typename KeySpan>
void NativeWriteBatch::del(const KeySpan &key) {
  del(NativeClient::defaultColumnFamily(), key);
}

template <typename BeginSpan, typename EndSpan>
void NativeWriteBatch::delRange(const std::string &cFamily, const BeginSpan &beginKey, const EndSpan &endKey) {
  auto s = batch_.DeleteRange(client_->columnFamilyHandle(cFamily), detail::toSlice(beginKey), detail::toSlice(endKey));
  detail::throwOnError("delRange failed", std::move(s));
}

template <typename BeginSpan, typename EndSpan>
void NativeWriteBatch::delRange(const BeginSpan &beginKey, const EndSpan &endKey) {
  delRange(client_->defaultColumnFamily(), beginKey, endKey);
}

inline std::size_t NativeWriteBatch::size() const { return batch_.GetDataSize(); }

template <typename Container>
void putInBatch(NativeWriteBatch &batch, const std::string &cFamily, const Container &cont) {
  for (const auto &[key, value] : cont) {
    batch.put(cFamily, key, value);
  }
}

template <typename Container>
void putInBatch(NativeWriteBatch &batch, const Container &cont) {
  putInBatch(batch, NativeClient::defaultColumnFamily(), cont);
}

template <typename Container>
void delInBatch(NativeWriteBatch &batch, const std::string &cFamily, const Container &cont) {
  for (const auto &key : cont) {
    batch.del(cFamily, key);
  }
}

template <typename Container>
void delInBatch(NativeWriteBatch &batch, const Container &cont) {
  delInBatch(batch, NativeClient::defaultColumnFamily(), cont);
}

}  // namespace concord::storage::rocksdb

#endif  // USE_ROCKSDB
