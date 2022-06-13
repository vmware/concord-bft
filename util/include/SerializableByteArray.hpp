// Concord
//
// Copyright (c) 2018-2022 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the
// LICENSE file.
//
#pragma once
#include <array>
#include <boost/algorithm/hex.hpp>
#include "assertUtils.hpp"
#include "Serializable.h"

template <size_t ByteCount>
class SerializableByteArray : concord::serialize::IStringSerializable {
 public:
  static constexpr const size_t ByteSize = ByteCount;
  using ByteArray = std::array<uint8_t, ByteSize>;

  SerializableByteArray(const ByteArray& bytes) : bytes_(bytes) {}
  virtual ~SerializableByteArray() = default;

  const ByteArray& getBytes() const { return bytes_; }

  std::string toHexString() const {
    std::string ret;
    boost::algorithm::hex(bytes_.begin(), bytes_.end(), std::back_inserter(ret));
    ConcordAssertEQ(ret.size(), ByteSize * 2);
    return ret;
  }

  std::string toString() const override { return toHexString(); }

 private:
  ByteArray bytes_;
};

template <typename ByteArrayClass>
static ByteArrayClass fromHexString(const std::string& hexString) {
  std::string keyBytes = boost::algorithm::unhex(hexString);
  ConcordAssertEQ(keyBytes.size(), ByteArrayClass::ByteSize);
  typename ByteArrayClass::ByteArray resultBytes;
  std::memcpy(resultBytes.data(), keyBytes.data(), keyBytes.size());
  return ByteArrayClass{resultBytes};
}