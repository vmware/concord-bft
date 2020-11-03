// Concord
//
// Copyright (c) 2020 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the sub-component's license, as noted in the
// LICENSE file.

#pragma once

#include <cstddef>
#include <cstdint>
#include <exception>
#include <iterator>
#include <limits>
#include <type_traits>
#include <unordered_map>
#include <utility>

#include <boost/iterator/iterator_categories.hpp>
#include <boost/iterator/iterator_facade.hpp>

#include "kv_types.hpp"

namespace concord {
namespace kvbc {

namespace detail {

template <typename BlockInfo>
using BlockInfoCache = std::unordered_map<concord::kvbc::BlockId, BlockInfo>;

using BlockIdDifferenceType = std::make_signed<concord::kvbc::BlockId>::type;

}  // namespace detail

// The maximum possible number of blocks this view is able to hold.
inline constexpr auto MAX_BLOCKCHAIN_VIEW_SIZE = std::numeric_limits<detail::BlockIdDifferenceType>::max();

// Represents lack of block info initialization data.
struct NoBlockInfoInit {};

// A random access blockchain iterator that supports caching block info.
template <typename BlockInfo, typename BlockInfoInit, bool loadDataOnAccess>
class BlockchainIterator : public boost::iterator_facade<BlockchainIterator<BlockInfo, BlockInfoInit, loadDataOnAccess>,
                                                         const BlockInfo,  // The value type this iterator points to.
                                                         boost::random_access_traversal_tag,
                                                         const BlockInfo&,  // Reference to the value type.
                                                         detail::BlockIdDifferenceType> {
 private:
  friend class boost::iterator_core_access;
  template <typename, typename, bool>
  friend class BlockchainView;

  using Cache = detail::BlockInfoCache<BlockInfo>;

  BlockchainIterator(concord::kvbc::BlockId id,
                     concord::kvbc::BlockId genesisId,
                     concord::kvbc::BlockId endId,
                     BlockInfoInit blockInfoInit,
                     Cache& cache)
      : blockId_{id}, genesisId_{genesisId}, endId_{endId}, blockInfoInit_{blockInfoInit}, cache_{&cache} {
    cacheBlock(blockId_);
  }

  void loadAndCacheInfo(BlockInfo& blockInfo) const {
    const auto id = blockInfo.id();
    blockInfo.loadIndices();
    if constexpr (loadDataOnAccess) {
      blockInfo.loadData();
    }
    cache_->emplace(id, std::move(blockInfo));
  }

  void cacheBlock(concord::kvbc::BlockId id) const {
    if (isOutsideRange(id)) {
      return;
    }

    // already cached, don't do any work
    if (auto it = cache_->find(id); it != std::cend(*cache_)) {
      return;
    }

    // load data from storage and, if successful, add to cache
    if constexpr (std::is_same_v<BlockInfoInit, NoBlockInfoInit>) {
      auto blockInfo = BlockInfo{id};
      loadAndCacheInfo(blockInfo);
    } else {
      auto blockInfo = BlockInfo{id, blockInfoInit_};
      loadAndCacheInfo(blockInfo);
    }
  }

  bool isOutsideRange(concord::kvbc::BlockId id) const noexcept { return id < genesisId_ || id >= endId_; }

  // The following methods are required by boost::iterator_facade as defined by:
  // https://www.boost.org/doc/libs/1_64_0/libs/iterator/doc/iterator_facade.html#iterator-facade-requirements
  void increment() {
    cacheBlock(blockId_ + 1);
    ++blockId_;
  }

  void decrement() {
    cacheBlock(blockId_ - 1);
    --blockId_;
  }

  bool equal(const BlockchainIterator& other) const noexcept { return blockId_ == other.blockId_; }

  // Since we always insert into the cache when constructing or incrementing/decrementing BlockchainIterator objects, we
  // expect find() will not return cend(). Dereferencing BlockchainIterator::cend() is defined as undefined behaviour in
  // itself. Additionally, we don't expect boost calling dereference() itself.
  const BlockInfo& dereference() const { return cache_->find(blockId_)->second; }

  void advance(detail::BlockIdDifferenceType n) {
    cacheBlock(blockId_ + n);
    blockId_ += n;
  }

  detail::BlockIdDifferenceType distance_to(const BlockchainIterator& other) const noexcept {
    return other.blockId_ - blockId_;
  }

  concord::kvbc::BlockId blockId_{0};
  concord::kvbc::BlockId genesisId_{0};
  concord::kvbc::BlockId endId_{0};
  BlockInfoInit blockInfoInit_;
  Cache* cache_{nullptr};
};

// A block info object that serves as a base class for user-provided block info types.
// See BlockchainView for requirements on block info types.
// See TimestampedBlockInfo in blockchain_view_test.cpp for an example.
class BaseBlockInfo {
 public:
  BaseBlockInfo() = default;
  BaseBlockInfo(concord::kvbc::BlockId id) : id_{id} {}

  concord::kvbc::BlockId id() const { return id_; }
  void loadIndices() {}

 private:
  template <typename, typename, bool>
  friend class BlockchainIterator;

  concord::kvbc::BlockId id_{0};
};

// Any block info object is equal to another one (from the same blockchain) if their IDs match.
inline bool operator==(const BaseBlockInfo& lhs, const BaseBlockInfo& rhs) { return lhs.id() == rhs.id(); }

// BlockchainView describes an object that refers to a read-only blockchain. It tries to mimic the interfaces of
// a const std::vector and of a std::string_view . It assumes block IDs are always increasing and there are no
// gaps between them.
//
// BlockchainView supports a cache of BlockInfo objects that is populated with values only when a block is being
// loaded from storage. BlockInfo objects store two types of data:
//  * indices - needed for indexing/ordering of blocks. For example, if the user wants to order blocks on some block
//   field, he/she can store it in BlockInfo and use it in comparator functions. Indices are always loaded on access.
//  * data - any data fetched for a block from storage (block key-values, for example). Users can select whether to
//    automatically load it on access (via the loadDataOnAccess parameter) or they can load it at their own discretion.
//
// Users are required to pass a BlockInfo type that represents the block information as fetched from storage. The
// BlockInfoInit type specifies an optional initialization passed to the BlockInfo constructor. Requirements on
// BlockInfo are:
//  * BlockInfo(concord::kvbc::BlockId) or BlockInfo(concord::kvbc::BlockId, BlockInfoInit) - a constructor that takes
//  the block ID if BlockInfoInit == NoBlockInfoInit or a constructor that takes both an ID and an initialization value
//  if BlockInfoInit != NoBlockInfoInit
//  * [ingored return] loadIndices() - a method that is called when accessing the block through iterators and is
//  expected to populate any indices needed by the user. Indices will be cached inside the BlockchainView.
//  * [ignored return] loadData() - an optional method that can be either called by users themselves on demand or by
//  iterators when loadDataOnAccess = true (mandatory in this case). This method should load block data from storage.
//  Data will be cached inside the BlockchainView.
//
// Incrementing or attempting to access end(), cend(), rend() and crend() results in undefined behavior.
// Decrementing or attempting to access begin(), cbegin(), rbegin() and crbegin() results in undefined behavior.
//
// Non-standard members are named in a camelCase convention. Standard member types and methods are named based on:
// https://en.cppreference.com/w/cpp/named_req/Container
template <typename BlockInfo, typename BlockInfoInit = NoBlockInfoInit, bool loadDataOnAccess = false>
class BlockchainView {
 private:
  using CacheType = detail::BlockInfoCache<BlockInfo>;

 public:
  using value_type = BlockInfo;
  using size_type = concord::kvbc::BlockId;
  using difference_type = detail::BlockIdDifferenceType;
  using const_reference = const BlockInfo&;
  using reference = const_reference;
  using const_iterator = BlockchainIterator<BlockInfo, BlockInfoInit, loadDataOnAccess>;
  using iterator = const_iterator;
  using const_reverse_iterator = std::reverse_iterator<const_iterator>;
  using reverse_iterator = const_reverse_iterator;

  // Constructs an empty view.
  BlockchainView() = default;

  // Constructs a view with count elements and the first element pointing to the block at genesisId. Additionally, an
  // optional initialization value can be provided.
  //
  // Note: genesisId + count must be <= MAX_BLOCKCHAIN_VIEW_SIZE to properly support end() and cend() iterators.
  //
  // If count > MAX_BLOCKCHAIN_VIEW_SIZE, an exception is thrown.
  // If genesisId + count would overflow the concord::kvbc::BlockId type, behavior is undefined.
  // If genesisId + count > MAX_BLOCKCHAIN_VIEW_SIZE, an exception is thrown. Constant complexity.
  BlockchainView(concord::kvbc::BlockId genesisId, size_type count, BlockInfoInit blockInfoInit = BlockInfoInit{})
      : genesisId_{genesisId}, endId_{genesisId + count}, size_{count}, blockInfoInit_{blockInfoInit} {
    if (count > MAX_BLOCKCHAIN_VIEW_SIZE) {
      throw std::length_error{"BlockchainView: exceeded MAX_BLOCKCHAIN_VIEW_SIZE"};
    } else if (genesisId + count > MAX_BLOCKCHAIN_VIEW_SIZE) {
      throw std::invalid_argument{"BlockchainView: exceeded allowed range"};
    }
  }

  // Return iterators to the first block. If the view is empty, iterators will be equal to end() and cend() .
  // Attempting to access on an empty view results in undefined behavior.
  // Decrementing results in undefined behavior.
  const_iterator begin() const { return const_iterator{genesisId_, genesisId_, endId_, blockInfoInit_, cache_}; }
  const_iterator cbegin() const { return begin(); }

  // Return iterators to the block following the last block.
  // Incrementing or attempting to access results in undefined behavior.
  const_iterator end() const { return const_iterator{endId_, genesisId_, endId_, blockInfoInit_, cache_}; }
  const_iterator cend() const { return end(); }

  // Return reverse iterators to the first block of the reversed view.
  // If the view is empty, the returned iterator is equal to rend() and crend() .
  // Attempting to access on an empty view results in undefined behavior.
  // Decrementing results in undefined behavior.
  const_reverse_iterator rbegin() const { return std::make_reverse_iterator(end()); }
  const_reverse_iterator crbegin() const { return rbegin(); }

  // Return reverse iterators to the block following the last block of the reversed view.
  // Incrementing or attempting to access results in undefined behavior.
  const_reverse_iterator rend() const { return std::make_reverse_iterator(begin()); }
  const_reverse_iterator crend() const { return rend(); }

  // Returns a const reference to the first block. Calling on an empty view results in undefined behavior.
  const_reference front() const { return *begin(); }

  // Returns a const reference to the last block. Calling on an empty view results in undefined behavior.
  const_reference back() const {
    auto it = cend();
    --it;
    return *it;
  }

  // Returns the block count in the view.
  size_type size() const noexcept { return size_; }

  // Returns the maximum possible number of blocks this view is able to hold.
  // Returned value is MAX_BLOCKCHAIN_VIEW_SIZE .
  size_type max_size() const noexcept { return MAX_BLOCKCHAIN_VIEW_SIZE; }

  // Checks if the container has no elements.
  bool empty() const noexcept { return size_ == 0; }

  // Returns a const reference to the block at location pos. Calling with an invalid pos results in undefined behavior.
  const_reference operator[](size_type pos) const {
    auto it = const_iterator{genesisId_ + pos, genesisId_, endId_, blockInfoInit_, cache_};
    return *it;
  }

  // Returns an iterator to the block with the passed ID. No bounds checking. Calling with an ID that is outside the
  // range results in undefined behavior. Constant complexity.
  const_iterator get(concord::kvbc::BlockId id) const {
    return const_iterator{id, genesisId_, endId_, blockInfoInit_, cache_};
  }

  // Finds a block with the passed ID. Returns a const iterator to the block with the passed ID. If no such block is
  // found, cend() is returned. Constant complexity.
  const_iterator find(concord::kvbc::BlockId id) const {
    if (empty() || id < genesisId_ || id > endId_ - 1) {
      return cend();
    }
    return get(id);
  }

  // Returns the current cache size.
  size_type cacheSize() const noexcept(noexcept(CacheType{}.size())) { return cache_.size(); }

  // Clears the cache of this view. Invalidates all iterators.
  void clearCache() noexcept(noexcept(CacheType{}.clear())) { cache_.clear(); }

 private:
  friend iterator;

  concord::kvbc::BlockId genesisId_{0};
  concord::kvbc::BlockId endId_{0};
  size_type size_{0};
  BlockInfoInit blockInfoInit_;
  mutable CacheType cache_;
};

}  // namespace kvbc
}  // namespace concord
