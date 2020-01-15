// Concord
//
// Copyright (c) 2019-2020 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the sub-component's license, as noted in the
// LICENSE file.

#pragma once

#include <cstdint>
#include <vector>

#include "assertUtils.hpp"
#include "sha3_256.h"

namespace concord {
namespace storage {
namespace sparse_merkle {

constexpr size_t BYTE_SIZE_IN_BITS = 8;
static_assert(BYTE_SIZE_IN_BITS == CHAR_BIT);

// A type safe version wrapper
class Version {
 public:
  Version() {}
  Version(uint64_t val) : value_(val) {}
  bool operator==(const Version& other) const { return value_ == other.value_; }
  bool operator!=(const Version& other) const { return value_ != other.value_; }
  bool operator<(const Version& other) const { return value_ < other.value_; }
  Version operator+(const Version& other) const { return Version(value_ + other.value_); }
  Version operator+(const int other) const {
    Assert(other > 0);
    return Version(value_ + other);
  }
  uint64_t value() const { return value_; }
  std::string toString() const { return std::to_string(value_); };

 private:
  uint64_t value_ = 0;
};

// A nibble is 4 bits, stored in the lower bits of a byte.
class Nibble {
 public:
  static constexpr size_t SIZE_IN_BITS = 4;

  Nibble(uint8_t byte) {
    Assert((byte & 0xF0) == 0);
    data_ = byte;
  }

  // Get the bit of the Nibble starting from LSB.
  // Bits 0-3 are available.
  bool getBit(size_t bit) const {
    Assert(bit < SIZE_IN_BITS);
    return (data_ >> bit) & 1;
  }

  bool operator==(const Nibble& other) const { return data_ == other.data_; }
  bool operator<(const Nibble& other) const { return data_ < other.data_; }

  // Return the underlying representation
  uint8_t data() const { return data_; }

  // Return the nibble as its lowercase hex character.
  char hexChar() const {
    if (data_ >= 0 && data_ <= 9) {
      return '0' + data_;
    }
    return 'a' + (data_ - 10);
  }

 private:
  // Only the lower 4 bits are used
  uint8_t data_;
};

template <typename T>
Nibble getNibble(const size_t n, const T& buf) {
  size_t index = n / 2;
  uint8_t byte = buf[index];
  if (n % 2 == 0) {
    // Even value: Get the first nibble of the byte
    byte = byte >> Nibble::SIZE_IN_BITS;
  } else {
    // Odd value: Get the second nibble of the byte
    byte = byte & 0x0F;
  }
  return Nibble(byte);
}

// A Hash is a wrapper around a byte buffer containing a hash
class Hash {
 public:
  // Change the following 2 constants if using a different hash algorithm
  static constexpr size_t SIZE_IN_BITS = 256;
  static constexpr const char* const HASH_ALGORITHM = "SHA3-256";

  static constexpr size_t SIZE_IN_BYTES = SIZE_IN_BITS / BYTE_SIZE_IN_BITS;
  static constexpr size_t MAX_NIBBLES = SIZE_IN_BYTES * 2;

  // This is the hash of the empty string ''. It's used as a placeholder value
  // for merkle tree hash calculations.
  //
  // Change this if the hash algorithm changes
  static constexpr std::array<uint8_t, SIZE_IN_BYTES> EMPTY_BUF = {
      0xa7, 0xff, 0xc6, 0xf8, 0xbf, 0x1e, 0xd7, 0x66, 0x51, 0xc1, 0x47, 0x56, 0xa0, 0x61, 0xd6, 0x62,
      0xf5, 0x80, 0xff, 0x4d, 0xe4, 0x3b, 0x49, 0xfa, 0x82, 0xd8, 0x0a, 0x4b, 0x80, 0xf8, 0x43, 0x4a};

  Hash() {}

  Hash(std::array<uint8_t, SIZE_IN_BYTES> buf) : buf_(buf) {}

  bool operator==(const Hash& other) const { return buf_ == other.buf_; }
  bool operator!=(const Hash& other) const { return buf_ != other.buf_; }
  bool operator<(const Hash& other) const { return buf_ < other.buf_; }

  const uint8_t* data() const { return buf_.data(); }
  size_t size() const { return buf_.size(); }

  // Return buf_ as lowercase hex string
  std::string toString() const {
    std::string output;
    for (size_t i = 0; i < MAX_NIBBLES; i++) {
      output.push_back(getNibble(i).hexChar());
    }
    return output;
  }

  Nibble getNibble(const size_t n) const {
    Assert(!buf_.empty());
    Assert(n < MAX_NIBBLES);
    return ::concord::storage::sparse_merkle::getNibble(n, buf_);
  }

  // Count the number of contiguous bits in common these hashes have from the
  // start (MSB) until a bit mismatch.
  size_t prefix_bits_in_common(const Hash& other) const {
    size_t count = 0;
    for (size_t i = 0; i < SIZE_IN_BYTES; i++) {
      if ((buf_[i] ^ other.buf_[i]) == 0) {
        count += BYTE_SIZE_IN_BITS;
      } else {
        // Check bit by bit
        for (size_t j = 7; j >= 0; j--) {
          if (((buf_[i] >> j) & 1) == ((other.buf_[i] >> j) & 1)) {
            ++count;
          } else {
            return count;
          }
        }
      }
    }
    return count;
  };

  // Count the number of contiguous bits in common (starting from MSB) common
  // these hashes have from depth*4 until a bit mismatch.
  //
  // TODO: This can be optimized to only start searching from depth
  size_t prefix_bits_in_common(const Hash& other, size_t depth) const {
    Assert(depth < Hash::MAX_NIBBLES);
    auto total = prefix_bits_in_common(other);
    auto bits_to_depth = depth * Nibble::SIZE_IN_BITS;
    if (total > bits_to_depth) {
      return total - bits_to_depth;
    }
    return 0;
  };

 private:
  std::array<uint8_t, SIZE_IN_BYTES> buf_;
};

// A Hasher computes hashes
class Hasher {
 public:
  Hasher() {}

  // Hash a buffer
  Hash hash(const void* buf, size_t size) { return Hash(sha3_256.digest(buf, size)); }

  // Compute the parent hash by concatenating the left and right hashes and
  // hashing.
  Hash parent(const Hash& left, const Hash& right) {
    sha3_256.init();
    sha3_256.update(left.data(), left.size());
    sha3_256.update(right.data(), right.size());
    return sha3_256.finish();
  }

 private:
  util::SHA3_256 sha3_256;
};

static const Hash PLACEHOLDER_HASH = Hash(Hash::EMPTY_BUF);

// This is a path used to find internal nodes in a merkle tree.
//
// To find the location of where to put a leaf in a tree, the hash of the key is
// walked nibble by nibble until an empty node is found.
class NibblePath {
 public:
  NibblePath() : num_nibbles_(0) {}

  bool operator==(const NibblePath& other) const { return num_nibbles_ == other.num_nibbles_ && path_ == other.path_; }
  bool operator<(const NibblePath& other) const {
    size_t count = std::min(num_nibbles_, other.num_nibbles_);
    for (size_t i = 0; i < count; i++) {
      if (get(i) < other.get(i)) {
        return true;
      } else if (other.get(i) < get(i)) {
        return false;
      }
    }
    // Both match up to count nibbles. Is this one shorter ?
    if (num_nibbles_ < other.num_nibbles_) {
      return true;
    }
    return false;
  }

  // Return the length of the path_ in nibbles
  size_t length() const { return num_nibbles_; }

  // Return whether the path is empty
  size_t empty() const { return num_nibbles_ == 0; }

  // Append a nibble to the path_
  void append(Nibble nibble) {
    // We don't want a NibblePath to ever be longer than a Hash
    Assert(num_nibbles_ < Hash::MAX_NIBBLES - 1);
    if (num_nibbles_ % 2 == 0) {
      path_.push_back(nibble.data() << Nibble::SIZE_IN_BITS);
    } else {
      path_.back() |= nibble.data();
    }
    num_nibbles_ += 1;
  }

  // Pop the last nibble off the path
  Nibble popBack() {
    Assert(!empty());
    uint8_t data = 0;
    if (num_nibbles_ % 2 == 0) {
      // We have a complete byte at the end of path_. Remove the lower nibble.
      data = path_.back() & 0x0F;
      // Clear the lower nibble.
      path_.back() &= 0xF0;
    } else {
      // We have an incomplete byte at the end of path_. Remove the upper nibble.
      data = path_.back() >> Nibble::SIZE_IN_BITS;
      // Remove the last byte containing a single nibble.
      path_.pop_back();
    }
    num_nibbles_ -= 1;
    return Nibble(data);
  }

  // Get the nth nibble.
  Nibble get(size_t n) const {
    Assert(n < num_nibbles_);
    return ::concord::storage::sparse_merkle::getNibble(n, path_);
  }

  // Return path_ as lowercase hex string
  std::string toString() const {
    std::string output;
    for (size_t i = 0; i < num_nibbles_; i++) {
      output.push_back(get(i).hexChar());
    }
    return output;
  }

 private:
  size_t num_nibbles_;
  std::vector<uint8_t> path_;
};

}  // namespace sparse_merkle
}  // namespace storage
}  // namespace concord
