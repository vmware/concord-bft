// Copyright 2018 VMware, all rights reserved

/**
 * Sliver -- Zero-copy management of bytes.
 *
 * See sliver.hpp for design details.
 */

#include "sliver.hpp"

#include <algorithm>
#include <cstring>
#include <ios>
#include <memory>

#include "hex_tools.h"
#include "assertUtils.hpp"

using namespace std;

namespace concordUtils {

/**
 * Create an empty sliver.
 *
 * Remember the data inside slivers is immutable.
 */
Sliver::Sliver() : data_(shared_ptr<const char[]>()), offset_(0), length_(0) {}

/**
 * Create a new sliver that will own the memory pointed to by `data`, which is
 * `length` bytes in size.
 *
 * * Important: the `data` buffer should have been allocated with `new`, and not
 * `malloc`, because the shared pointer will use `delete` and not `free`.
 */
Sliver::Sliver(const char* data, const size_t length)
    : data_(std::shared_ptr<const char[]>(data)), offset_(0), length_(length) {
  // Data must be non-null.
  ConcordAssert(data != nullptr);
}

/**
 * Create a sub-sliver that references a region of a base sliver.
 */
Sliver::Sliver(const Sliver& base, const size_t offset, const size_t length)
    : data_(base.data_),
      // This sliver starts offset bytes from the offset of its base.
      offset_(base.offset_ + offset),
      length_(length) {
  // This sliver must start no later than the end of the base sliver.
  ConcordAssert(offset <= base.length_);
  // This sliver must end no later than the end of the base sliver.
  ConcordAssert(length <= base.length_ - offset);
}

/**
 * Create a Sliver by moving a string into it.
 */
Sliver::Sliver(string&& s) : offset_(0), length_(s.length()) { data_ = std::make_shared<StringBuf>(std::move(s)); }

/**
 * Shorthand for the copy constructor.
 * */
Sliver Sliver::clone() const { return subsliver(0, length_); }

/**
 * Create a sliver from a copy of the memory pointed to by `data`, which is
 * `length` bytes in size.
 */
Sliver Sliver::copy(const char* data, const size_t length) {
  auto* copy = new char[length];
  memcpy(copy, data, length);
  return Sliver(copy, length);
}

/**
 * Get the byte at `offset` in this sliver.
 */
char Sliver::operator[](const size_t offset) const {
  const size_t total_offset = offset_ + offset;
  // This offset must be within this sliver.
  ConcordAssert(offset < length_);

  // The data for the requested offset is that many bytes after the offset from
  // the base sliver.
  if (std::holds_alternative<shared_ptr<StringBuf>>(data_)) {
    return std::get<shared_ptr<StringBuf>>(data_)->s.data()[total_offset];
  } else {
    return std::get<shared_ptr<const char[]>>(data_).get()[total_offset];
  }
}

/**
 * Get a direct pointer to the data for this sliver. Remember that the Sliver
 * (or its base) still owns the data, so ensure that the lifetime of this Sliver
 * (or its base) is at least as long as the lifetime of the returned pointer.
 */
const char* Sliver::data() const {
  if (std::holds_alternative<shared_ptr<StringBuf>>(data_)) {
    return std::get<shared_ptr<StringBuf>>(data_)->s.data() + offset_;
  } else {
    return std::get<shared_ptr<const char[]>>(data_).get() + offset_;
  }
}

/**
 * Create a subsliver. Syntactic sugar for cases where a function call is more
 * natural than using the sub-sliver constructor directly.
 */
Sliver Sliver::subsliver(const size_t offset, const size_t length) const { return Sliver(*this, offset, length); }

bool Sliver::empty() const { return (length_ == 0); }

size_t Sliver::length() const { return length_; }

size_t Sliver::size() const { return length(); }

std::string_view Sliver::string_view() const { return std::string_view(data(), length_); }

std::ostream& Sliver::operator<<(std::ostream& s) const { return hexPrint(s, data(), length()); }

std::ostream& operator<<(std::ostream& s, const Sliver& sliver) { return sliver.operator<<(s); }

/**
 * Slivers are == if their lengths are the same, and each byte of their data is
 * the same.
 */
bool Sliver::operator==(const Sliver& other) const {
  // This could be just "compare(other) == 0", but the short-circuit of checking
  // lengths first can save us many cycles in some cases.
  return length() == other.length() && memcmp(data(), other.data(), length()) == 0;
}

bool Sliver::operator!=(const Sliver& other) const { return !(*this == other); }

/**
 * a.compare(b) is:
 *  - 0 if lengths are the same, and bytes are the same
 *  - -1 if bytes are the same, but a is shorter
 *  - 1 if bytes are the same, but a is longer
 */
int Sliver::compare(const Sliver& other) const {
  int comp = memcmp(data(), other.data(), std::min(length(), other.length()));
  if (comp == 0) {
    if (length() < other.length()) {
      comp = -1;
    } else if (length() > other.length()) {
      comp = 1;
    }
  }
  return comp;
}

}  // namespace concordUtils
