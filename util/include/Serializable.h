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
#include "Logger.hpp"

namespace serialize {

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
typedef std::shared_ptr<Serializable> SerializablePtr;



//TODO [TK] remove
class MemoryBasedBuf : public std::basic_streambuf<char> {
 public:
  MemoryBasedBuf(const UniquePtrToChar &buf, size_t size) {
    setg(buf.get(), buf.get(), buf.get() + size);
  }
};

// This class allows usage of a regular buffer as a stream.
// TODO [TK] remove
class MemoryBasedStream : public std::istream {
 public:
  MemoryBasedStream(const UniquePtrToChar &buf, size_t size) : std::istream(&buffer_), buffer_(buf, size) {
    rdbuf(&buffer_);
  }

 private:
  MemoryBasedBuf buffer_;
};

class Serializable {
public:

  Serializable();
  virtual ~Serializable() = default;

  virtual void        serialize  (std::ostream&) const final;

  virtual std::string getName() const = 0;
  virtual std::string getVersion() const = 0;

  static void verifyClassName(const std::string &expectedClassName, std::istream &inStream);
  static void verifyClassVersion(const std::string &expectedVersion, std::istream &inStream);
  static SerializablePtr deserialize(const UniquePtrToChar &inBuf, int64_t inBufSize); //__attribute__ ((deprecated)) TODO [TK] remove;
  static SerializablePtr deserialize(std::istream &inStream);

protected:
  virtual void serializeClassName    (std::ostream&) const final;
  virtual void serializeClassVersion (std::ostream&) const final;
  virtual void serializeDataMembers  (std::ostream&) const = 0;
  virtual void deserializeDataMembers(std::istream&)       = 0;
  virtual SerializablePtr create(std::istream&) = 0;


  void serializeString(const std::string &str, std::ostream &outStream) const;
  template<typename INT>
  void serializeInt(const INT& num, std::ostream& outStream) const
  {
    outStream.write((char*)&num, sizeof(INT));
  }
  static std::string deserializeClassName(std::istream &inStream);
  static std::string deserializeClassVersion(std::istream &inStream);
  static std::string deserializeString(std::istream &inStream);
  template<typename INT>
  static INT deserializeInt(std::istream& inStream)
  {
    INT res = 0;
    inStream.read((char*)&res, sizeof(INT));
    return res;
  }

  static void registerObject(const std::string& className, const SerializablePtr& objectPtr)
  {
      objectFactory_[className] = objectPtr;
  }
  typedef std::unordered_map<std::string, SerializablePtr> ClassToObjectMap;
  static std::unordered_map<std::string, SerializablePtr> objectFactory_;
  concordlogger::Logger log_srlz_;

};



}
