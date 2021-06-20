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

#include "assertUtils.hpp"

namespace concord::util {

template <typename Key, typename Value>
class LruCache {
 public:
  LruCache(size_t capacity) : capacity_(capacity) { map_.reserve(capacity); }

  void put(const Key& key, const Value& value) {
    stats_.puts++;
    auto iter = map_.find(key);
    if (iter != map_.end()) {
      moveToFront(iter->second.second);
      iter->second.first = value;
    } else {
      if (keys_.size() == capacity_) {
        map_.erase(keys_.back());
        keys_.pop_back();
      }
      keys_.push_front(key);
      map_.insert({key, {value, keys_.begin()}});
    }
  }

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

 private:
  void moveToFront(typename std::list<Key>::iterator it) { keys_.splice(keys_.begin(), keys_, it); }

  const size_t capacity_;

  using MapVal = std::pair<Value, typename std::list<Key>::iterator>;

  // Access list from most recently used at the front, to least recently used at the back
  std::list<Key> keys_;
  std::unordered_map<Key, MapVal> map_;

  Stats stats_;
};

}  // namespace concord::util
