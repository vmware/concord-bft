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

#pragma once

#include <memory.h>
#include <stdint.h>
#include <string>
#include <array>
#include "DigestType.hpp"
#include "DigestImpl.ipp"

namespace concord::util::digest {

using BlockDigest = std::array<std::uint8_t, DIGEST_SIZE>;

#define USE_CRYPTOPP
#if defined USE_CRYPTOPP
using Digest = DigestHolder<CryptoppDigestCreator>;
#elif defined USE_OPENSSL_SHA256
using Digest = DigestHolder<OpenSSLDigestCreator<EVP_sha256> >;
#elif defined USE_OPENSSL_SHA3_256
using Digest = DigestHolder<OpenSSLDigestCreator<EVP_sha3_256> >;
#endif

static_assert(DIGEST_SIZE >= sizeof(uint64_t), "Digest size should be >= sizeof(uint64_t)");
static_assert(sizeof(Digest) == DIGEST_SIZE, "sizeof(Digest) != DIGEST_SIZE");

inline std::ostream& operator<<(std::ostream& os, const Digest& digest) {
  os << digest.toString();
  return os;
}

}  // namespace concord::util::digest