/*
 * AutoBuf.h
 *
 *  Created on: September 14th, 2017
 *      Author: Alin Tomescu <alinush@mit.edu>
 */

#pragma once

#include <cstring>
#include <algorithm>

#include <xutils/Log.h>
//#include <xutils/Utils.h>

template <class T>
class AutoBuf {
 private:
  size_t len;
  T* buf;

 public:
  AutoBuf() : len(0), buf(nullptr) {}

  AutoBuf(size_t l) : len(l), buf(new T[len]) {}

  AutoBuf(long l) : len(static_cast<size_t>(l)), buf(new T[len]) {}

  AutoBuf(int l) : len(static_cast<size_t>(l)), buf(new T[len]) {}

  AutoBuf(const AutoBuf<T>& ab) : AutoBuf(ab.len) { std::copy(ab.buf, ab.buf + ab.len, buf); }

  AutoBuf(AutoBuf&& other) : len(other.len), buf(other.buf) {
    // logdbg << "Move constructor called: this = " << this << ", other = " << &other << std::endl;

    other.buf = nullptr;
    other.len = 0;
  }

  // Move assignment
  AutoBuf& operator=(AutoBuf&& other) noexcept {
    // logdbg << "Move assignment called: this = " << this << ", other = " << &other << std::endl;

    if (this == &other) return *this;

    delete[] buf;

    buf = other.buf;
    len = other.len;

    other.buf = nullptr;
    other.len = 0;

    return *this;
  }

  ~AutoBuf() { delete[] buf; }

 public:
  operator T*() { return buf; }
  operator const T*() const { return buf; }

  bool operator==(const AutoBuf& o) const {
    // logdbg << len << " bytes: " << Utils::bin2hex(buf, len) << std::endl;
    // logdbg << o.len << " bytes: " << Utils::bin2hex(o.buf, o.len) << std::endl;

    if (len != o.len) return false;

    return memcmp(buf, o.buf, o.len) == 0;
  }

  bool operator!=(const AutoBuf& o) const { return !operator==(o); }

 public:
  T* getBuf() { return buf; }
  const T* getBuf() const { return buf; }

  size_t size() const { return len; }

  void zeroize() { memset(buf, 0, len); }

  void shrink(size_t newlen) {
    if (newlen > len) {
      throw std::runtime_error("AutoBuf::shrink() expects a length smaller than the current one");
    }

    len = newlen;
  }
};

typedef AutoBuf<char> AutoCharBuf;
typedef AutoBuf<unsigned char> AutoByteBuf;
