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

namespace concord {
namespace serialize {

Serializable::ClassToObjectMap Serializable::objectFactory_;

Serializable::Serializable() : loggerSerializable_(concordlogger::Log::getLogger("serializable")) {}

/************** Serialization **************/

void Serializable::serialize(ostream &outStream) const {
  serializeClassName(outStream);
  serializeClassVersion(outStream);
  serializeDataMembers(outStream);
}

void Serializable::serializeClassName(ostream &outStream) const {
  serializeString(getName(), outStream);
  LOG_TRACE(loggerSerializable_, "<<< class name: " << getName());
}

void Serializable::serializeClassVersion(ostream &outStream) const {
  serializeString(getVersion(), outStream);
  LOG_TRACE(loggerSerializable_, "<<< class version: " << getVersion());
}

void Serializable::serializeString(const string &str, ostream &outStream) const {
  serializeInt(str.size(), outStream);
  std::string::size_type sizeofString = str.size();
  outStream.write(str.data(), sizeofString);
  LOG_TRACE(loggerSerializable_, "<<< string size: " << sizeofString);
}

/************** Deserialization **************/

std::string Serializable::deserializeString(istream &inStream) {
  auto sizeofString = deserializeInt<std::string::size_type>(inStream);
  LOG_TRACE(concordlogger::Log::getLogger("serializable"), ">>> string size: " << sizeofString);
  char *str = new char[sizeofString];
  inStream.read(str, sizeofString);
  std::string result(str, sizeofString);
  delete[] str;
  return result;
}

std::string Serializable::deserializeClassName(istream &inStream) {
  std::string res = deserializeString(inStream);
  LOG_TRACE(concordlogger::Log::getLogger("serializable"), ">>> class name: " << res);
  return res;
}

std::string Serializable::deserializeClassVersion(istream &inStream) {
  std::string res = deserializeString(inStream);
  LOG_TRACE(concordlogger::Log::getLogger("serializable"), ">>> class version: " << res);
  return res;
}

SerializablePtr Serializable::deserialize(istream &inStream) {
  // Deserialize first the class name.
  std::string className = deserializeClassName(inStream);
  auto it = objectFactory_.find(className);
  if (it == objectFactory_.end())
    throw runtime_error("Deserialization failed: unknown class name: " + className);

  SerializablePtr obj = it->second->create(inStream);
  obj->deserializeDataMembers(inStream);
  return obj;
}

SerializablePtr Serializable::deserialize(const UniquePtrToChar &inBuf, int64_t inBufSize) {
  MemoryBasedStream inStream(inBuf, (uint64_t) inBufSize);
  return deserialize(inStream);
}

void Serializable::verifyClassVersion(const string &expectedVersion, istream &inStream) {
  std::string version = deserializeClassVersion(inStream);
  if (version != expectedVersion)
    throw runtime_error(
        "Unsupported class version: " + version + std::string(", expected version: ") + expectedVersion);
}

}
}
