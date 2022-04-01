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

#include <string.h>
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wpedantic"
#include <cryptopp/dll.h>
#include <cryptopp/pem.h>
#pragma GCC diagnostic pop

#include "assertUtils.hpp"
#include <cryptopp/cryptlib.h>
#include <cryptopp/ida.h>
#include <cryptopp/eccrypto.h>

#include "cryptopp_digest_creator.hpp"
#include "digest.hpp"

namespace concord::util::digest {

using CryptoPP::SecByteBlock;

CryptoppDigestCreator::CryptoppDigestCreator() {
  DigestType* p = new DigestType();
  internalState_ = p;
}

size_t CryptoppDigestCreator::digestLength() { return DigestType::DIGESTSIZE; }

bool CryptoppDigestCreator::compute(const char* input,
                                    size_t inputLength,
                                    char* outBufferForDigest,
                                    size_t lengthOfBufferForDigest) {
  DigestType dig;

  const size_t size = dig.DigestSize();

  if (lengthOfBufferForDigest < size) {
    return false;
  }

  SecByteBlock digest(size);

  dig.Update((CryptoPP::byte*)input, inputLength);
  dig.Final(digest);

  const CryptoPP::byte* h = digest;
  memcpy(outBufferForDigest, h, size);

  return true;
}

void CryptoppDigestCreator::update(const char* data, size_t len) {
  ConcordAssert(nullptr != internalState_);

  DigestType* p = (DigestType*)internalState_;
  p->Update((CryptoPP::byte*)data, len);
}

void CryptoppDigestCreator::writeDigest(char* outDigest) {
  ConcordAssert(nullptr != internalState_);

  DigestType* p = (DigestType*)internalState_;

  DigestGenerator digestGenerator;
  SecByteBlock digest(digestGenerator.digestLength());
  p->Final(digest);
  const CryptoPP::byte* h = digest;
  memcpy(outDigest, h, digestGenerator.digestLength());

  delete p;
  internalState_ = nullptr;
}

CryptoppDigestCreator::~CryptoppDigestCreator() {
  if (nullptr != internalState_) {
    DigestType* p = (DigestType*)internalState_;
    delete p;
    internalState_ = nullptr;
  }
}
}  // namespace concord::util::digest
