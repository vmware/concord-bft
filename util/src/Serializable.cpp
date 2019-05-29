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

Serializable::ClassNameToObjectMap Serializable::classNameToObjectMap_;

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

/************** Serialization **************/

void Serializable::serializeClassName(ostream &outStream) const {
  auto sizeofClassName = (int64_t) getName().size();
  outStream.write((char *) &sizeofClassName, sizeof(sizeofClassName));
  outStream.write(getName().c_str(), sizeofClassName);
}

void Serializable::serializeClassVersion(ostream &outStream) const {
  const uint32_t version = getVersion();
  outStream.write((char *) &version, sizeof(version));
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

UniquePtrToChar Serializable::deserializeClassName(istream &inStream) {
  int64_t sizeofClassName = 0;
  inStream.read((char *) &sizeofClassName, sizeof(sizeofClassName));
  UniquePtrToChar className(new char[sizeofClassName + 1]);
  className.get()[sizeofClassName] = '\0';
  inStream.read(className.get(), sizeofClassName);
  return className;
}

UniquePtrToClass Serializable::deserialize(istream &inStream) {
  // Deserialize first the class name.
  UniquePtrToChar className = deserializeClassName(inStream);
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
  UniquePtrToChar className = deserializeClassName(inStream);
  if (className.get() != expectedClassName) {
    ostringstream error;
    error << "Unsupported class name: " << className.get()
          << ", expected class name: " << expectedClassName;
    throw runtime_error(error.str());
  }
}

void Serializable::verifyClassVersion(uint32_t expectedVersion,
                                      istream &inStream) {
  uint32_t version = 0;
  inStream.read((char *) &version, sizeof(version));

  if (version != expectedVersion) {
    ostringstream error;
    error << "Unsupported class version: " << version
          << ", expected version: " << expectedVersion;
    throw runtime_error(error.str());
  }
}

