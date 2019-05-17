// Concord
//
// Copyright (c) 2018 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the
// LICENSE file.

#pragma once

#include <string>
#include <map>
#include <fstream>
#include <memory>

/**
 * This class defines common functionality used for classes
 * serialization/deserialization. This provides an ability to save/retrieve
 * classes as a raw byte arrays to/from the local disk (DB).
 * Format is as follows:
 * - Class name length followed by a class name as a string.
 * - Numeric class serialization/deserialization version.
 * - Class related data members.
 */

class Serializable;

typedef std::unique_ptr<char> UniquePtrToChar;
typedef std::unique_ptr<unsigned char> UniquePtrToUChar;
typedef std::unique_ptr<Serializable> UniquePtrToClass;

class MemoryBasedBuf : public std::basic_streambuf<char> {
 public:
  MemoryBasedBuf(const UniquePtrToChar &buf, size_t size) {
    setg(buf.get(), buf.get(), buf.get() + size);
  }
};

// This class allows usage of a regular buffer as a stream.
class MemoryBasedStream : public std::istream {
 public:
  MemoryBasedStream(const UniquePtrToChar &buf, size_t size) :
      std::istream(&buffer_), buffer_(buf, size) {
    rdbuf(&buffer_);
  }

 private:
  MemoryBasedBuf buffer_;
};

class Serializable {
  typedef std::map<std::string, UniquePtrToClass> ClassNameToObjectMap;

 public:
  virtual ~Serializable() = default;

 public:
  static void retrieveSerializedBuffer(const std::string &className,
                                       UniquePtrToChar &outBuf,
                                       int64_t &outBufSize);
  static void serializeClassName(const std::string &name,
                                 std::ostream &outStream);
  static void verifyClassName(const std::string &expectedClassName,
                              std::istream &inStream);
  static void verifyClassVersion(uint32_t expectedVersion,
                                 std::istream &inStream);
  static UniquePtrToClass deserialize(const UniquePtrToChar &inBuf,
                                     int64_t inBufSize);

 protected:
  virtual UniquePtrToClass create(std::istream &inStream) = 0;

 private:
  static UniquePtrToChar deserializeClassName(std::istream &inStream);

 protected:
  static ClassNameToObjectMap classNameToObjectMap_;
};
