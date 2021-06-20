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

#pragma once

#include <algorithm>

template <class T>
class AutoBuf {
 private:
  T* buf;
  int len;

 public:
  AutoBuf(int len) : buf(new T[static_cast<size_t>(len)]), len(len) {}

  AutoBuf(const AutoBuf<T>& ab) : AutoBuf(ab.len) { std::copy(ab.buf, ab.buf + ab.len, buf); }

  ~AutoBuf() { delete[] buf; }

 public:
  operator T*() { return buf; }
  operator const T*() const { return buf; }

 public:
  T* getBuf() { return buf; }
  const T* getBuf() const { return buf; }

  int size() const { return len; }
};

typedef AutoBuf<char> AutoCharBuf;
typedef AutoBuf<unsigned char> AutoByteBuf;
