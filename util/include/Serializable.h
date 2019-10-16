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

#include <functional>
#include "type_traits.h"
#include "demangle.hpp"
#include "Logger.hpp"

namespace concord {
namespace serialize {

/** *******************************************************************************************************************
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

class Serializable {
public:
  Serializable() = default;
  virtual ~Serializable() = default;
  /**
   * serialization API
   */
  template<typename T>
  static void serialize(std::ostream& outStream, const T& t) { serialize_impl(outStream, t, int{}); }
  template<typename T>
  /**
   * deserialization API
   */
  static void deserialize(std::istream& inStream, T& t) { deserialize_impl(inStream, t, int{}); }
  /**
   * convenience serialization function
   */
  virtual void serialize(std::ostream& outStream) const final{ serialize(outStream, *this); }

protected:
  /**
   * the class version
   */
  virtual const std::string getVersion()  const = 0;
  /**
   * each class knows how to serialize its data members
   */
  virtual void serializeDataMembers  (std::ostream&) const = 0;
  /**
   * each class knows how to deserialize its data members
   */
  virtual void deserializeDataMembers(std::istream&)       = 0;

  virtual const std::string getName() const final { return demangler::demangle(typeid(*this)); }

protected:
  /** *****************************************************************************************************************
   *  std::container
   *  - container size
   *  - every element of the container (recursively)
   */
  template<typename T, typename std::enable_if<is_std_container<T>::value, T>::type* = nullptr>
  static void serialize_impl(std::ostream& outStream, const T& container, int) {
   typename T::size_type size = container.size();
   LOG_TRACE(logger(), " size: " << size);
   serialize(outStream, size);
   for (auto& it : container)
     serialize(outStream, it);
  }
  /**
   *  std::vector
   *  we always deserialize containers to vector first and then copy to the designated container if needed
   */
  template<typename T, typename std::enable_if<is_vector<std::vector<T>>::value, T>::type* = nullptr>
  static void deserialize_impl(std::istream& inStream, std::vector<T>& vec, int) {
    LOG_TRACE(logger(), "");
    typename std::vector<T>::size_type size = 0;
    deserialize(inStream, size);
    for(typename std::vector<T>::size_type i = 0; i < size; ++i) {
     T t;
     deserialize(inStream, t);
     vec.push_back(t);
    }
  }
  /**
   *  std::set
   *  if implementing std::set<ClassDerivedFromSerializable> should implement
   *  bool operator < (const ClassDerivedFromSerializable& other) const;
   *  in order to use std::set default std::less comparator
   *  rather than providing a custom comparator
   */
  template<typename T, typename std::enable_if<is_set<std::set<T>>::value, T>::type* = nullptr>
  static void deserialize_impl(std::istream& inStream, std::set<T>& set, int) {
    LOG_TRACE(logger(), "");
    std::vector<T> vec;
    deserialize_impl(inStream, vec, int{});
    std::copy(vec.begin(), vec.end(), std::inserter(set, set.begin()));
  }
  /** *****************************************************************************************************************
   *  Serializable
   *  - name
   *  - version
   *  - data members (recursively)
   */
  template<typename T, typename std::enable_if<std::is_convertible<T*, Serializable*>::value>::type* = nullptr>
  static void serialize_impl(std::ostream& outStream, const T& t, int) {
    const Serializable& s = static_cast<const Serializable&>(t);
    serialize(outStream, s.getName());
    serialize(outStream, s.getVersion());
    s.serializeDataMembers(outStream);
  }
  template<typename T, typename std::enable_if<std::is_convertible<T, Serializable*>::value>::type* = nullptr>
  static void serialize_impl(std::ostream& outStream, const T t, int) {
    const Serializable& s = static_cast<const Serializable&>(*t);
    serialize_impl(outStream, s, int{});
  }
  template<typename T, typename std::enable_if<std::is_convertible<T, Serializable*>::value>::type* = nullptr>
  static void deserialize_impl(std::istream& inStream, T& t, int) {
    std::string className;
    deserialize(inStream, className);
    LOG_TRACE(logger(), className.c_str());
    auto it = registry().find(className);
    if (it == registry().end())
      throw std::runtime_error("Deserialization failed: unknown class name: " + className);
    Serializable* s = it->second();
    std::string version;
    deserialize(inStream, version);
    if (version != s->getVersion())
      throw std::runtime_error("Deserialization failed: wrong version: " +  version +
                               std::string(", expected version: ") + s->getVersion());
    s->deserializeDataMembers(inStream);

    t = dynamic_cast<T>(s);// shouldn't throw because T is convertible to Serializable*
  }
  template<typename T, typename std::enable_if<std::is_convertible<T*, Serializable*>::value>::type* = nullptr>
  static void deserialize_impl(std::istream& inStream, T& t, int) {
    T* s = nullptr;
    deserialize_impl(inStream, s, int{});
    t = T(*s);
  }
  /** *****************************************************************************************************************
   *  std::string
   *  - string length
   *  - char array
   */
  template<typename T, typename std::enable_if<std::is_same<T, std::string>::value>::type* = nullptr>
  static void serialize_impl(std::ostream& outStream, const T& str, int) {
    LOG_TRACE(logger(), str);
    const std::size_t sz = str.size();
    serialize_impl(outStream, sz, int{});
    std::string::size_type size = str.size();
    serialize(outStream, str.data(), size);
  }
  template<typename T, typename std::enable_if<std::is_same<T, std::string>::value>::type* = nullptr>
  static void deserialize_impl(std::istream& inStream, T& t, int) {
    std::size_t sz;
    deserialize_impl(inStream, sz, int{});
    char* str =  new char[sz];
    deserialize(inStream, str, sz);
    t.assign(str, sz);
    LOG_TRACE(logger(), t);
    delete [] str;
  }
  /** *****************************************************************************************************************
   * integral types
   */
  template<typename T, typename std::enable_if<std::is_integral<T>::value>::type* = nullptr>
  static void serialize_impl(std::ostream& outStream, const T& t, int) {
    LOG_TRACE(logger(), t);
    outStream.write((char*)&t, sizeof(T));
  }
  template<typename T, typename std::enable_if<std::is_integral<T>::value>::type* = nullptr>
  static void deserialize_impl(std::istream& inStream, T& t, int) {
    inStream.read((char*)&t, sizeof(T));
    LOG_TRACE(logger(), t);
  }
public:
  /** ****************************************************************************************************************/
  static void serialize(std::ostream& outStream, const char* p, const std::size_t& size) {
    outStream.write(p, static_cast<std::streamsize>(size));
  }
  static void deserialize(std::istream& inStream, char* p, const std::size_t& size) {
    inStream.read(p, static_cast<std::streamsize>(size));
  }
protected:
  static concordlogger::Logger& logger(){
    static concordlogger::Logger logger_ = concordlogger::Log::getLogger("serializable");
    return logger_;
  }

  typedef std::unordered_map<std::string, std::function<Serializable*()>> Registry;

  static Registry& registry(){
    static Registry registry_;
    return registry_;
  }
};

/** *******************************************************************************************************************
 *  Class for automatic Serializable object registration
 */
template<typename T>
class SerializableFactory: public virtual Serializable {
public:
  SerializableFactory(){(void)registered_;}
  static bool registerT() {
    registry()[demangler::demangle<T>()] = []() -> Serializable* { return new T;};
    return true;
  }
  static bool registered_;
};

template <typename T>
bool SerializableFactory<T>::registered_ = SerializableFactory<T>::registerT();

}
}
