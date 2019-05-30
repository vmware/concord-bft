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

#include <sstream>
#include "Serializable.h"

using namespace std;

ClassNameToObjectMap Serializable::classNameToObjectMap_;

/************** Serialization **************/

void Serializable::serialize(ostream &outStream) const {
  serializeClassName(outStream);
  serializeClassVersion(outStream);
  serializeDataMembers(outStream);
}

void Serializable::serialize(UniquePtrToChar &outBuf,
                             int64_t &outBufSize) const {
  ofstream outStream(getName().c_str(), ofstream::binary | ofstream::trunc);
  serialize(outStream);
  outStream.close();
  retrieveSerializedBuffer(getName(), outBuf, outBufSize);
}

void Serializable::serializeClassName(std::ostream &outStream) const {
  serializeString(getName(), outStream);
}

void Serializable::serializeClassVersion(std::ostream &outStream) const {
  serializeString(getVersion(), outStream);
}

void Serializable::serializeString(const string &str, ostream &outStream) {
  auto sizeofString = (int64_t) str.size();
  outStream.write((char *) &sizeofString, sizeof(sizeofString));
  outStream.write(str.c_str(), sizeofString);
}

void Serializable::retrieveSerializedBuffer(
    const string &className, UniquePtrToChar &outBuf, int64_t &outBufSize) {
  ifstream infile(className.c_str(), ofstream::binary);
  infile.seekg(0, ios::end);
  outBufSize = infile.tellg();
  infile.seekg(0, ios::beg);
  UniquePtrToChar newOne(new char[outBufSize]);
  outBuf.swap(newOne);
  infile.read(outBuf.get(), outBufSize);
  infile.close();
}

/************** Deserialization **************/

UniquePtrToChar Serializable::deserializeString(istream &inStream) {
  int64_t sizeofString = 0;
  inStream.read((char *) &sizeofString, sizeof(sizeofString));
  UniquePtrToChar str(new char[sizeofString + 1]);
  str.get()[sizeofString] = '\0';
  inStream.read(str.get(), sizeofString);
  return str;
}

UniquePtrToChar Serializable::deserializeClassName(std::istream &inStream) {
  return deserializeString(inStream);
}

UniquePtrToChar Serializable::deserializeClassVersion(std::istream &inStream) {
  return deserializeString(inStream);
}

UniquePtrToClass Serializable::deserialize(istream &inStream) {
  // Deserialize first the class name.
  UniquePtrToChar className = deserializeString(inStream);
  auto it = classNameToObjectMap_.find(className.get());
  if (it != classNameToObjectMap_.end()) {
    // Create corresponding class instance
    return it->second->create(inStream);
  }
  ostringstream error;
  error << "Deserialization failed: unknown class name: " << className.get();
  throw runtime_error(error.str());
}

UniquePtrToClass Serializable::deserialize(const UniquePtrToChar &inBuf,
                                           int64_t inBufSize) {
  MemoryBasedStream inStream(inBuf, (uint64_t) inBufSize);
  return deserialize(inStream);
}

void Serializable::verifyClassName(const string &expectedClassName,
                                   istream &inStream) {
  UniquePtrToChar className = deserializeString(inStream);
  if (className.get() != expectedClassName) {
    ostringstream error;
    error << "Unsupported class name: " << className.get()
          << ", expected class name: " << expectedClassName;
    throw runtime_error(error.str());
  }
}

void Serializable::verifyClassVersion(const std::string &expectedVersion,
                                      istream &inStream) {
  UniquePtrToChar version = deserializeClassVersion(inStream);
  if (version.get() != expectedVersion) {
    ostringstream error;
    error << "Unsupported class version: " << version.get()
          << ", expected version: " << expectedVersion;
    throw runtime_error(error.str());
  }
}

