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
#include <iterator>
#include <functional>

#include "type_traits.h"
#include "demangle.hpp"
#include "Logger.hpp"

namespace concord {
namespace serialize {

class IStringSerializable {
 public:
  virtual std::string toString() const = 0;
  virtual ~IStringSerializable() = default;
  friend std::ostream& operator<<(std::ostream& out, const IStringSerializable& serializable) {
    return out << serializable.toString();
  }
};

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

// Overload ostream (<<) for std::set
template <class T>
std::ostream& operator<<(std::ostream& stream, const std::set<T>& values) {
  stream << "[ ";
  std::copy(std::begin(values), std::end(values), std::ostream_iterator<T>(stream, " "));
  stream << ']';
  return stream;
}

class Serializable {
 public:
  Serializable() = default;
  virtual ~Serializable() = default;
  /**
   * serialization API
   */
  template <typename T>
  static void serialize(std::ostream& outStream, const T& t) {
    serialize_impl(outStream, t, int{});
  }
  /**
   * deserialization API
   */
  template <typename T>
  static void deserialize(std::istream& inStream, T& t) {
    deserialize_impl(inStream, t, int{});
  }
  /**
   * convenience serialization function
   */
  virtual void serialize(std::ostream& outStream) const final { serialize(outStream, *this); }

 protected:
  /**
   * the class version
   */
  virtual const std::string getVersion() const { return "1"; };
  /**
   * each class knows how to serialize its data members
   */
  virtual void serializeDataMembers(std::ostream&) const = 0;
  /**
   * each class knows how to deserialize its data members
   */
  virtual void deserializeDataMembers(std::istream&) = 0;

  virtual const std::string getName() const final { return demangler::demangle(typeid(*this)); }

 protected:
  /** *****************************************************************************************************************
   *  std::container
   *  - container size
   *  - every element of the container (recursively)
   */
  template <typename T, typename std::enable_if_t<is_std_container_v<T>, T>* = nullptr>
  static void serialize_impl(std::ostream& outStream, const T& container, int) {
    typename T::size_type size = container.size();
    LOG_TRACE(logger(), " size: " << size);
    serialize(outStream, size);
    for (auto& it : container) serialize(outStream, it);
  }
  /**
   *  std::vector
   *  we always deserialize containers to vector first and then copy to the designated container if needed
   */
  template <typename T, typename std::enable_if_t<is_vector_v<std::vector<T>>, T>* = nullptr>
  static void deserialize_impl(std::istream& inStream, std::vector<T>& vec, int) {
    LOG_TRACE(logger(), "");
    typename std::vector<T>::size_type size = 0;
    deserialize(inStream, size);
    for (typename std::vector<T>::size_type i = 0; i < size; ++i) {
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
  template <typename T, typename std::enable_if_t<is_set_v<std::set<T>>, T>* = nullptr>
  static void deserialize_impl(std::istream& inStream, std::set<T>& set, int) {
    LOG_TRACE(logger(), "");
    std::vector<T> vec;
    deserialize_impl(inStream, vec, int{});
    std::copy(vec.begin(), vec.end(), std::inserter(set, set.begin()));
  }

  template <typename T, typename std::enable_if_t<is_deque_v<std::deque<T>>, T>* = nullptr>
  static void deserialize_impl(std::istream& inStream, std::deque<T>& deque, int) {
    LOG_TRACE(logger(), "");
    std::vector<T> vec;
    deserialize_impl(inStream, vec, int{});
    std::copy(vec.begin(), vec.end(), std::back_inserter(deque));
  }

  template <typename T, typename std::enable_if_t<is_map_v<T>, T>* = nullptr>
  static void deserialize_impl(std::istream& inStream, T& map_, int) {
    LOG_TRACE(logger(), "");
    std::vector<std::pair<typename T::key_type, typename T::mapped_type>> vec;
    deserialize_impl(inStream, vec, int{});
    std::copy(vec.begin(), vec.end(), std::inserter(map_, map_.begin()));
  }

  /** *****************************************************************************************************************
   *  Serializable
   *  - name
   *  - version
   *  - data members (recursively)
   */
  template <typename T, typename std::enable_if_t<std::is_convertible_v<T*, Serializable*>>* = nullptr>
  static void serialize_impl(std::ostream& outStream, const T& t, int) {
    const Serializable& s = static_cast<const Serializable&>(t);
    serialize(outStream, s.getName());
    serialize(outStream, s.getVersion());
    s.serializeDataMembers(outStream);
  }
  template <typename T, typename std::enable_if_t<std::is_convertible_v<T, Serializable*>>* = nullptr>
  static void serialize_impl(std::ostream& outStream, const T t, int) {
    const Serializable& s = static_cast<const Serializable&>(*t);
    serialize_impl(outStream, s, int{});
  }
  template <typename T, typename std::enable_if_t<std::is_convertible_v<T, Serializable*>>* = nullptr>
  static void deserialize_impl(std::istream& inStream, T& t, int) {
    std::string className;
    deserialize(inStream, className);
    LOG_TRACE(logger(), className.c_str());
    auto it = registry().find(className);
    if (it == registry().end()) throw std::runtime_error("Deserialization failed: unknown class name: " + className);
    Serializable* s = it->second();
    std::string version;
    deserialize(inStream, version);
    if (version != s->getVersion())
      throw std::runtime_error("Deserialization failed: wrong version: " + version +
                               std::string(", expected version: ") + s->getVersion());
    s->deserializeDataMembers(inStream);

    t = dynamic_cast<T>(s);  // shouldn't throw because T is convertible to Serializable*
  }
  template <typename T, typename std::enable_if_t<std::is_convertible_v<T*, Serializable*>>* = nullptr>
  static void deserialize_impl(std::istream& inStream, T& t, int) {
    T* s = nullptr;
    deserialize_impl(inStream, s, int{});
    t = T(*s);
    delete s;
  }
  /** *****************************************************************************************************************
   *  std::string
   *  - string length
   *  - char array
   */
  template <typename T, typename std::enable_if_t<std::is_same_v<T, std::string>>* = nullptr>
  static void serialize_impl(std::ostream& outStream, const T& str, int) {
    LOG_TRACE(logger(), str);
    const std::size_t sz = str.size();
    serialize_impl(outStream, sz, int{});
    std::string::size_type size = str.size();
    serialize(outStream, str.data(), size);
  }
  template <typename T, typename std::enable_if_t<std::is_same_v<typename std::remove_cv_t<T>, std::string>>* = nullptr>

  static void deserialize_impl(std::istream& inStream, T& t, int) {
    std::size_t sz;
    deserialize_impl(inStream, sz, int{});
    char* str = new char[sz];
    deserialize(inStream, str, sz);
    const_cast<std::string&>(t).assign(str, sz);
    LOG_TRACE(logger(), t);
    delete[] str;
  }

  /** *****************************************************************************************************************
   * duration types
   */
  template <typename T, typename std::enable_if_t<is_duration_v<T>>* = nullptr>
  static void serialize_impl(std::ostream& outStream, const T& duration, int) {
    auto raw_value = duration.count();
    LOG_TRACE(logger(), raw_value);
    outStream.write((char*)&raw_value, sizeof(raw_value));
  }

  template <typename T, typename std::enable_if_t<is_duration_v<T>>* = nullptr>
  static void deserialize_impl(std::istream& inStream, T& t, int) {
    typename T::rep raw_value = 0;
    inStream.read((char*)&raw_value, sizeof(raw_value));
    t = T(raw_value);
    LOG_TRACE(logger(), raw_value);
  }

  /** *****************************************************************************************************************
   * integral types
   */
  template <typename T, typename std::enable_if_t<std::is_integral_v<T>>* = nullptr>
  static void serialize_impl(std::ostream& outStream, const T& t, int) {
    LOG_TRACE(logger(), t);
    outStream.write((char*)&t, sizeof(T));
  }
  template <typename T, typename std::enable_if_t<std::is_integral_v<T>>* = nullptr>
  static void deserialize_impl(std::istream& inStream, T& t, int) {
    inStream.read((char*)&t, sizeof(T));
    LOG_TRACE(logger(), t);
  }

  /** *****************************************************************************************************************
   * integral enum
   */
  template <typename T, typename std::enable_if_t<std::is_enum_v<T>>* = nullptr>
  static void serialize_impl(std::ostream& outStream, const T& t, int) {
    static_assert(std::is_integral_v<std::underlying_type_t<T>>);
    LOG_TRACE(logger(), static_cast<std::underlying_type_t<T>>(t));
    outStream.write((char*)&t, sizeof(T));
  }
  template <typename T, typename std::enable_if_t<std::is_enum_v<T>>* = nullptr>
  static void deserialize_impl(std::istream& inStream, T& t, int) {
    static_assert(std::is_integral_v<std::underlying_type_t<T>>);
    inStream.read((char*)&t, sizeof(T));
    LOG_TRACE(logger(), static_cast<std::underlying_type_t<T>>(t));
  }

  /** *****************************************************************************************************************
   * std::pair
   */
  template <typename T, typename std::enable_if_t<is_pair_v<T>>* = nullptr>
  static void serialize_impl(std::ostream& outStream, const T& t, int) {
    serialize(outStream, t.first);
    serialize(outStream, t.second);
  }
  template <typename T, typename std::enable_if_t<is_pair_v<T>>* = nullptr>
  static void deserialize_impl(std::istream& inStream, T& t, int) {
    deserialize(inStream, t.first);
    deserialize(inStream, t.second);
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
  static logging::Logger& logger() {
    static logging::Logger logger_ = logging::getLogger("serializable");
    return logger_;
  }

  typedef std::unordered_map<std::string, std::function<Serializable*()>> Registry;

  static Registry& registry() {
    static Registry registry_;
    return registry_;
  }
};

/** *******************************************************************************************************************
 *  Class for automatic Serializable object registration
 */
template <typename T>
class SerializableFactory : public virtual Serializable {
 public:
  SerializableFactory() { (void)registered_; }
  static bool registerT() {
    registry()[demangler::demangle<T>()] = []() -> Serializable* { return new T; };
    return true;
  }
  static bool registered_;
};

template <typename T>
bool SerializableFactory<T>::registered_ = SerializableFactory<T>::registerT();

}  // namespace serialize
}  // namespace concord
