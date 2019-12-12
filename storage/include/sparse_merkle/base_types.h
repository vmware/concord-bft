// Concord
//
// Copyright (c) 2019 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the sub-component's license, as noted in the
// LICENSE file.

#include <cstdint>
#include <vector>

#include "assertUtils.hpp"

namespace concord {
namespace storage {
namespace sparse_merkle {

// A nibble is 4 bits, stored in the lower bits of a byte.
class Nibble {
 public:
  Nibble(uint8_t byte) {
    Assert((byte & 0xF0) == 0);
    data_ = byte;
  }

  bool get_bit(const size_t bit) const {
    Assert(bit < 4);
    return (data_ >> bit) & 1;
  }

  bool operator==(const Nibble& other) const { return data_ == other.data_; }

  // Promote the underlying uint8_t to a size_t so that it can be used as an
  // index into arrays and vectors.
  size_t to_index() const { return data_; }

  // Return the underlying representation
  uint8_t data() const { return data_; }

 private:
  // Only the lower 4 bits are used
  uint8_t data_;
};

template <typename T>
Nibble get_nibble(const size_t n, const T& buf) {
  size_t index = n / 2;
  uint8_t byte = buf[index];
  if (n % 2 == 0) {
    // Even value: Get the first nibble of the byte
    byte = byte >> 4;
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

  static constexpr size_t SIZE_IN_BYTES = SIZE_IN_BITS / 8;
  static constexpr size_t MAX_NIBBLES = SIZE_IN_BITS / 16;

  // This is the hash of the empty string ''. It's used as a placeholder value
  // for merkle tree hash calculations.
  //
  // Change this if the hash algorithm changes
  static constexpr std::array<uint8_t, SIZE_IN_BYTES> EMPTY = {
      0xa7, 0xff, 0xc6, 0xf8, 0xbf, 0x1e, 0xd7, 0x66, 0x51, 0xc1, 0x47, 0x56, 0xa0, 0x61, 0xd6, 0x62,
      0xf5, 0x80, 0xff, 0x4d, 0xe4, 0x3b, 0x49, 0xfa, 0x82, 0xd8, 0x0a, 0x4b, 0x80, 0xf8, 0x43, 0x4a};

  Nibble get_nibble(const size_t n) const {
    Assert(n < MAX_NIBBLES);
    return ::concord::storage::sparse_merkle::get_nibble(n, buf);
  }

 private:
  std::array<uint8_t, SIZE_IN_BYTES> buf;
};

// This is a path used to find internal nodes in a merkle tree.
//
// To find the location of where to put a leaf in a tree, the hash of the key is
// walked nibble by nibble until an empty node is found.
class NibblePath {
 public:
  NibblePath() : num_nibbles_(0) {}

  // Return the length of the path_ in nibbles
  size_t length() const { return num_nibbles_; }

  // Append a nibble to the path_
  void append(Nibble nibble) {
    // We don't want a NibblePath to ever be longer than a Hash
    Assert(num_nibbles_ < Hash::MAX_NIBBLES - 1);
    if (num_nibbles_ % 2 == 0) {
      path_.push_back(nibble.data() << 4);
    } else {
      path_.back() |= nibble.data();
    }
    num_nibbles_ += 1;
  }

  // Get the nth nibble.
  Nibble get(size_t n) const {
    Assert(n < num_nibbles_);
    return ::concord::storage::sparse_merkle::get_nibble(n, path_);
  }

 private:
  size_t num_nibbles_;
  std::vector<uint8_t> path_;
};

}  // namespace sparse_merkle
}  // namespace storage
}  // namespace concord
