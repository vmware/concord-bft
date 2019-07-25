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

#pragma once

#include <string>
#include <unordered_map>
#include <fstream>
#include <memory>

namespace concordSerializable {

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

typedef std::unique_ptr<char[], std::default_delete<char[]>> UniquePtrToChar;
typedef std::unique_ptr<unsigned char[], std::default_delete<unsigned char[]>> UniquePtrToUChar;
typedef std::shared_ptr<Serializable> SharedPtrToClass;

class MemoryBasedBuf : public std::basic_streambuf<char> {
 public:
  MemoryBasedBuf(const UniquePtrToChar &buf, size_t size) {
    setg(buf.get(), buf.get(), buf.get() + size);
  }
};

// This class allows usage of a regular buffer as a stream.
class MemoryBasedStream : public std::istream {
 public:
  MemoryBasedStream(const UniquePtrToChar &buf, size_t size) : std::istream(&buffer_), buffer_(buf, size) {
    rdbuf(&buffer_);
  }

 private:
  MemoryBasedBuf buffer_;
};

typedef std::unordered_map<std::string, SharedPtrToClass> ClassNameToObjectMap;

class Serializable {
 public:
  virtual ~Serializable() = default;
  virtual void serialize(UniquePtrToChar &outBuf, int64_t &outBufSize) const;
  virtual void serialize(std::ostream &outStream) const;
  virtual std::string getName() const = 0;
  virtual std::string getVersion() const = 0;

  static void verifyClassName(const std::string &expectedClassName, std::istream &inStream);
  static void verifyClassVersion(const std::string &expectedVersion, std::istream &inStream);
  static SharedPtrToClass deserialize(const UniquePtrToChar &inBuf, int64_t inBufSize);
  static SharedPtrToClass deserialize(std::istream &inStream);

 protected:
  void serializeClassName(std::ostream &outStream) const;
  void serializeClassVersion(std::ostream &outStream) const;
  virtual void serializeDataMembers(std::ostream &outStream) const = 0;
  virtual SharedPtrToClass create(std::istream &inStream) = 0;
  static void retrieveSerializedBuffer(const std::string &className, UniquePtrToChar &outBuf, int64_t &outBufSize);

 private:
  static void serializeString(const std::string &str, std::ostream &outStream);
  static UniquePtrToChar deserializeClassName(std::istream &inStream);
  static UniquePtrToChar deserializeClassVersion(std::istream &inStream);
  static UniquePtrToChar deserializeString(std::istream &inStream);
};

class SerializableObjectsDB {
 public:
  static void registerObject(const std::string &className, const SharedPtrToClass &objectPtr);
  friend class Serializable;

 private:
  static ClassNameToObjectMap classNameToObjectMap_;
};

}