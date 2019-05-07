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

#include <sstream>
#include "threshsign/Serializable.h"

using namespace std;

Serializable::ClassNameToObjectMap Serializable::classNameToObjectMap_;

/************** Serialization **************/

void Serializable::serializeClassName(const string& name,
                                           ostream &outStream) {
  auto sizeofClassName = (int64_t)name.size();
  outStream.write((char *)&sizeofClassName, sizeof(sizeofClassName));
  outStream.write(name.c_str(), sizeofClassName);
}

void Serializable::retrieveSerializedBuffer(
    const string &className, char *&outBuf, int64_t &outBufSize) {
  ifstream infile(className.c_str(), ofstream::binary);
  infile.seekg (0, ios::end);
  outBufSize = infile.tellg();
  infile.seekg (0, ios::beg);
  outBuf = new char[outBufSize];
  infile.read(outBuf, outBufSize);
  infile.close();
}

/************** Deserialization **************/

Serializable *Serializable::deserialize(const char *inBuf, int64_t inBufSize) {
  MemoryBasedStream inStream((char *)inBuf, (uint64_t)inBufSize);

  // Deserialize first the class name.
  int64_t sizeofClassName = 0;
  inStream.read((char *)&sizeofClassName, sizeof(sizeofClassName));
  char *className = new char[sizeofClassName];
  inStream.read(className, sizeofClassName);

  auto it = classNameToObjectMap_.find(className);
  if (it != classNameToObjectMap_.end()) {
    // Create corresponding class instance
    return it->second->create(inStream);
  }
  ostringstream error;
  error << "Deserialization failed: unknown class name: " << className;
  throw runtime_error(error.str());
}

void Serializable::verifyClassName(const string &expectedClassName,
                                        istream &inStream) {
  int64_t sizeofClassName = 0;
  inStream.read((char *)&sizeofClassName, sizeof(sizeofClassName));
  char *className = new char[sizeofClassName + 1];
  inStream.read(className, sizeofClassName);
  className[sizeofClassName] = '\0';

  if (className != expectedClassName) {
    ostringstream error;
    error << "Unsupported class name: " << className
          << ", expected class name: " << expectedClassName;
    throw runtime_error(error.str());
  }
}

void Serializable::verifyClassVersion(uint32_t expectedVersion,
                                           istream &inStream) {
  uint32_t version = 0;
  inStream.read((char *)&version, sizeof(version));

  if (version != expectedVersion) {
    ostringstream error;
    error << "Unsupported class version: " << version
          << ", expected version: " << expectedVersion;
    throw runtime_error(error.str());
  }
}

