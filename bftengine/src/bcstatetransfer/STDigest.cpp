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

#include "STDigest.hpp"

#include <cryptopp/dll.h>

#include <cassert>
#include <string>

namespace bftEngine {
namespace SimpleBlockchainStateTransfer {
namespace impl {

std::string STDigest::toString() const {
  char c[BLOCK_DIGEST_SIZE * 2];
  char t[3];
  static_assert(sizeof(t) == 3, "");
  for (size_t i = 0; i < BLOCK_DIGEST_SIZE; i++) {
    unsigned int b = (unsigned int)content[i];
    snprintf(t, sizeof(t), "%02X", b);
    c[i * 2] = t[0];
    c[i * 2 + 1] = t[1];
  }

  std::string ret(c, BLOCK_DIGEST_SIZE * 2);

  return ret;
}

DigestContext::DigestContext() {
  CryptoPP::SHA256* p = new CryptoPP::SHA256();
  internalState = p;
}

void DigestContext::update(const char* data, size_t len) {
  assert(internalState != NULL);
  CryptoPP::SHA256* p = (CryptoPP::SHA256*)internalState;
  p->Update(reinterpret_cast<byte*>(const_cast<char*>(data)), len);
}

void DigestContext::writeDigest(char* outDigest) {
  assert(internalState != NULL);
  CryptoPP::SHA256* p = (CryptoPP::SHA256*)internalState;
  CryptoPP::SecByteBlock digest(CryptoPP::SHA256::DIGESTSIZE);
  p->Final(digest);
  const byte* h = digest;
  memcpy(outDigest, h, CryptoPP::SHA256::DIGESTSIZE);

  delete p;
  internalState = NULL;
}

DigestContext::~DigestContext() {
  if (internalState != NULL) {
    CryptoPP::SHA256* p = (CryptoPP::SHA256*)internalState;
    delete p;
    internalState = NULL;
  }
}

}  // namespace impl
}  // namespace SimpleBlockchainStateTransfer
}  // namespace bftEngine
