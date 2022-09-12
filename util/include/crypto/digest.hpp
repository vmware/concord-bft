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

#include "digest_holder.hpp"

#if defined USE_CRYPTOPP_SHA_256
#include "cryptopp/digest_creator.hpp"
#elif defined USE_OPENSSL_SHA_256
#include "crypto/openssl/digest_creator.hpp"
#endif

namespace concord::crypto {

#if defined USE_CRYPTOPP_SHA_256
using Digest = DigestHolder<concord::crypto::cryptopp::CryptoppDigestCreator>;
using DigestGenerator = concord::crypto::cryptopp::CryptoppDigestCreator;
#elif defined USE_OPENSSL_SHA_256
using Digest = DigestHolder<concord::crypto::openssl::OpenSSLDigestCreator<concord::crypto::openssl::SHA2_256> >;
using DigestGenerator = concord::crypto::openssl::OpenSSLDigestCreator<concord::crypto::openssl::SHA2_256>;
#endif

static_assert(DIGEST_SIZE >= sizeof(uint64_t), "Digest size should be >= sizeof(uint64_t)");
static_assert(sizeof(Digest) == DIGEST_SIZE, "sizeof(Digest) != DIGEST_SIZE");

inline std::ostream& operator<<(std::ostream& os, const Digest& digest) {
  os << digest.toString();
  return os;
}
}  // namespace concord::crypto
