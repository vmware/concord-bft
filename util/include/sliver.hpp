// Copyright 2018 VMware, all rights reserved

/**
 * Sliver -- Zero-copy management of bytes.
 *
 * A Sliver provides an immutable view into an allocated region of memory. Views of
 * sub-regions, or "sub-slivers" do not copy data, but instead reference the
 * memory of the "base" sliver.
 *
 * The memory is managed through a std::shared_ptr. If the `Sliver(char* data,
 * size_t length)` constructor is called, that sliver wraps the data pointer in
 * a shared pointer. If the `Sliver(const std::sring&&) constructor is called,
 * the string is wrapped in a shared pointer. Sub-slivers reference this same
 * shared pointer, such that the memory is kept around as long as the base
 * sliver or any sub-sliver needs it, and cleaned up once the base sliver and
 * all sub-slivers have finished using it.
 *
 * Intentionally copyable (via default copy constructor and assignment
 * operator). Copying the shared_ptr increases its reference count by one, so
 * that it is not released until both copies go out of scope.
 *
 * Intentionally movable (via default move constructor and assignment
 * operator). Moving the shared_ptr avoids modifying its reference count, which
 * requires an atomic operation that might be considered expensive.
 */

#pragma once

#include <ios>
#include <variant>
#include <memory>
#include <string_view>
#include <functional>
#include <utility>

namespace concordUtils {

class Sliver {
 public:
  Sliver();
  Sliver(const char* data, const size_t length);
  Sliver(std::string&& s);
  Sliver(const Sliver& base, const size_t offset, const size_t length);
  static Sliver copy(const char* data, const size_t length);

  char operator[](const size_t offset) const;

  Sliver subsliver(const size_t offset, const size_t length) const;
  Sliver clone() const;

  bool empty() const;
  size_t length() const;
  size_t size() const;
  const char* data() const;
  std::string_view string_view() const;

  std::ostream& operator<<(std::ostream& s) const;
  bool operator==(const Sliver& other) const;
  bool operator!=(const Sliver& other) const;
  int compare(const Sliver& other) const;

  std::string toString() const { return std::string(data(), length()); }

 private:
  // A wrapper around a std::string. We need to be able to allocate the wrapper
  // so that we have a pointer that can be stored in a shared_ptr. We don't want
  // allocate a copy of a string we already have.
  struct StringBuf {
    StringBuf(std::string&& p) : s{std::move(p)} {}
    std::string s;
  };

  std::variant<std::shared_ptr<StringBuf>, std::shared_ptr<const char[]>> data_;

  size_t offset_;
  size_t length_;

  // Delete new and delete, to force the Sliver to be allocated on the stack, so
  // that it is cleaned up properly via RAII scoping.
  static void* operator new(size_t) = delete;
  static void* operator new[](size_t) = delete;
  static void operator delete(void*) = delete;
  static void operator delete[](void*) = delete;
};

std::ostream& operator<<(std::ostream& s, const Sliver& sliver);

inline bool operator<(const Sliver& lhs, const Sliver& rhs) { return (lhs.compare(rhs) < 0); }

}  // namespace concordUtils

namespace std {
template <>
struct hash<concordUtils::Sliver> {
  std::size_t operator()(concordUtils::Sliver const& s) const noexcept {
    return std::hash<std::string_view>{}(std::string_view(s.data(), s.length()));
  }
};
}  // namespace std
