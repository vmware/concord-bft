// Concord
//
// Copyright (c) 2022 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").  You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include "DigestImpl.hpp"
#include "hex_tools.h"

namespace concord::util::digest {

template <class T>
DigestHolder<T>::DigestHolder() {
  d.fill(0);
};

template <class T>
DigestHolder<T>::DigestHolder(unsigned char initVal) {
  d.fill(initVal);
}

template <class T>
DigestHolder<T>::DigestHolder(const char* other) {
  memcpy(d.data(), other, DIGEST_SIZE);
}

template <class T>
DigestHolder<T>::DigestHolder(char* buf, size_t len) {}

template <class T>
char* DigestHolder<T>::content() const {
  return (char*)d.begin();
}

template <class T>
void DigestHolder<T>::makeZero() {
  d.fill(0);
}

template <class T>
std::string DigestHolder<T>::toString() const {
  return concordUtils::bufferToHex(d, DIGEST_SIZE, false);
}

template <class T>
void DigestHolder<T>::print() {
  printf("digest=[%s]", toString().c_str());
}

template <class T>
const char* const DigestHolder<T>::get() const {
  return (const char*)d.data();
}

template <class T>
char* DigestHolder<T>::getForUpdate() {
  return (char*)d.data();
}

template <class T>
bool DigestHolder<T>::isZero() const {
  for (int i = 0; i < DIGEST_SIZE; ++i) {
    if (d[i] != 0) return false;
  }
  return true;
}

template <class T>
int DigestHolder<T>::hash() const {
  uint64_t* p = (uint64_t*)d.begin();
  int h = (int)p[0];
  return h;
}

template <class T>
bool DigestHolder<T>::operator==(const DigestHolder& other) const {
  int r = memcmp(d, other.d, DIGEST_SIZE);
  return (r == 0);
}

template <class T>
bool DigestHolder<T>::operator!=(const DigestHolder& other) const {
  int r = memcmp(d, other.d, DIGEST_SIZE);
  return (r != 0);
}

template <class T>
DigestHolder<T>& DigestHolder<T>::operator=(const DigestHolder<T>& other) {
  if (this == &other) {
    return *this;
  }
  memcpy(d, other.d, DIGEST_SIZE);
  return *this;
}

template <class T>
void DigestHolder<T>::digestOfDigest(const DigestHolder& inDigest, DigestHolder& outDigest) {
  DigestUtil::compute(inDigest.d, DIGEST_SIZE, outDigest.d, DIGEST_SIZE);
}
}  // namespace concord::util::digest