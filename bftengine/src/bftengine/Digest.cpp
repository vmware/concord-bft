// Concord
//
// Copyright (c) 2018 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").  You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include "Digest.hpp"

#include <string.h>
#include <stdio.h>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wpedantic"
#include <cryptopp/dll.h>
#include <cryptopp/pem.h>
#pragma GCC diagnostic pop

#include "DigestType.h"
#include "assertUtils.hpp"
#include <cryptopp/cryptlib.h>
#include <cryptopp/ida.h>
#include <cryptopp/eccrypto.h>

using namespace CryptoPP;
using namespace std;

#if defined MD5_DIGEST
#include <cryptopp/md5.h>
#define DigestType Weak1::MD5
#elif defined SHA256_DIGEST
#define DigestType SHA256
#elif defined SHA512_DIGEST
#define DigestType SHA512
#endif

namespace bftEngine {
namespace impl {

size_t DigestUtil::digestLength() { return DigestType::DIGESTSIZE; }

bool DigestUtil::compute(const char* input,
                         size_t inputLength,
                         char* outBufferForDigest,
                         size_t lengthOfBufferForDigest) {
  DigestType dig;

  size_t size = dig.DigestSize();

  if (lengthOfBufferForDigest < size) return false;

  SecByteBlock digest(size);

  dig.Update((CryptoPP::byte*)input, inputLength);
  dig.Final(digest);
  const CryptoPP::byte* h = digest;
  memcpy(outBufferForDigest, h, size);

  return true;
}

DigestUtil::Context::Context() {
  DigestType* p = new DigestType();
  internalState = p;
}

void DigestUtil::Context::update(const char* data, size_t len) {
  ConcordAssert(internalState != NULL);
  DigestType* p = (DigestType*)internalState;
  p->Update((CryptoPP::byte*)data, len);
}

void DigestUtil::Context::writeDigest(char* outDigest) {
  ConcordAssert(internalState != NULL);
  DigestType* p = (DigestType*)internalState;
  SecByteBlock digest(digestLength());
  p->Final(digest);
  const CryptoPP::byte* h = digest;
  memcpy(outDigest, h, digestLength());

  delete p;
  internalState = NULL;
}

DigestUtil::Context::~Context() {
  if (internalState != NULL) {
    DigestType* p = (DigestType*)internalState;
    delete p;
    internalState = NULL;
  }
}

Digest::Digest(char* buf, size_t len) { DigestUtil::compute(buf, len, (char*)d, sizeof(Digest)); }

std::string Digest::toString() const {
  char c[DIGEST_SIZE * 2];
  char t[3];
  for (size_t i = 0; i < DIGEST_SIZE; i++) {
    // TODO(DD): Is it by design?
    // NOLINTNEXTLINE(bugprone-signed-char-misuse)
    unsigned int b = (unsigned int)d[i];
    snprintf(t, 3, "%02X", b);
    c[i * 2] = t[0];
    c[i * 2 + 1] = t[1];
  }

  std::string ret(c, DIGEST_SIZE * 2);

  return ret;
}

void Digest::print() { printf("digest=[%s]", toString().c_str()); }

void Digest::digestOfDigest(const Digest& inDigest, Digest& outDigest) {
  DigestUtil::compute(inDigest.d, sizeof(Digest), outDigest.d, sizeof(Digest));
}

}  // namespace impl
}  // namespace bftEngine
