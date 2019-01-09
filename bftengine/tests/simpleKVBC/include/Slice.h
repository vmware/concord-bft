// Concord
//
// Copyright (c) 2018 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#pragma once

#include <string.h>
#include <algorithm>

namespace SimpleKVBC {

struct Slice
{
  Slice() : data(nullptr), size(0) { }
  Slice(const char* p, size_t l) : data(p), size(l) { }
  Slice(const char* str) : Slice(str, strlen(str)) {}

  void clear() { data = nullptr; size = 0; }

  int compare(const Slice& other) const
  {
      const size_t minLen = std::min(size, other.size);
      if(minLen > 0)
      {
        int c = memcmp(data, other.data, minLen);
        if (c != 0) return c;
      }
      if (size < other.size) return (-1);
      else if (size > other.size) return (1);
      else return 0;
  }

  const char* data;
  size_t size;
};


inline bool operator==(const Slice& a, const Slice& b)
{
  return (
          (a.size == b.size) &&
          ((a.size==0) || (memcmp(a.data, b.data, a.size) == 0))
          );
}

inline bool operator!=(const Slice& a, const Slice& b)
{
  return !(a == b);
}

bool copyToAndAdvance(char* _buf, size_t* _offset, size_t _maxOffset, char* _src, size_t _srcSize);

}