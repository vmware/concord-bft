// Concord
//
// Copyright (c) 2021 VMware, Inc. All Rights Reserved.
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

#include <list>
#include <optional>
#include <unordered_map>

#include "util/assertUtils.hpp"

namespace concord::util {

template <typename Key, typename Value>
class LruCache {
 public:
  LruCache(size_t capacity) : capacity_(capacity) { map_.reserve(capacity); }

  void put(const Key& k, const Value& v) { putImp(k, v); }
  void put(const Key& k, Value&& v) { putImp(k, std::move(v)); }
  void put(Key&& k, Value&& v) { putImp(std::move(k), std::move(v)); }
  void put(Key&& k, const Value& v) { putImp(std::move(k), v); }

  std::optional<Value> get(const Key& key) {
    auto iter = map_.find(key);
    if (iter == map_.end()) {
      stats_.misses++;
      return std::nullopt;
    } else {
      stats_.hits++;
      moveToFront(iter->second.second);
      return iter->second.first;
    }
  }

  size_t size() const {
    ConcordAssertEQ(map_.size(), keys_.size());
    return keys_.size();
  }

  size_t capacity() const { return capacity_; }

  struct Stats {
    size_t hits = 0;
    size_t misses = 0;
    size_t puts = 0;
  };

  Stats getStats() const { return stats_; }

  void clear() {
    keys_.clear();
    map_.clear();
    stats_ = Stats{};
  }

 protected:
  using MapVal = std::pair<Value, typename std::list<Key>::iterator>;

  const size_t capacity_;
  // Access list from most recently used at the front, to least recently used at the back
  std::list<Key> keys_;
  std::unordered_map<Key, MapVal> map_;
  Stats stats_;

  void moveToFront(typename std::list<Key>::iterator it) { keys_.splice(keys_.begin(), keys_, it); }

  template <typename K, typename V>
  void putImp(K&& key, V&& value) {
    stats_.puts++;
    auto iter = map_.find(key);
    if (iter != map_.end()) {
      moveToFront(iter->second.second);
      iter->second.first = std::forward<V>(value);
    } else {
      if (keys_.size() == capacity_) {
        beforeErase();
        map_.erase(keys_.back());
        keys_.pop_back();
      }
      keys_.push_front(key);
      map_.insert({std::forward<K>(key), {std::forward<V>(value), keys_.begin()}});
    }
  }

  // A call back method, if a derived class needs to do something before it erases the element.
  virtual void beforeErase() {}
};

}  // namespace concord::util
