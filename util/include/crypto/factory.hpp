// Concord
//
// Copyright (c) 2022 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the sub-component's license, as noted in the LICENSE
// file.

#pragma once

#include "crypto/cryptopp/signers.hpp"
#include "crypto/cryptopp/verifiers.hpp"
#include "crypto/openssl/EdDSASigner.hpp"
#include "crypto/openssl/EdDSAVerifier.hpp"
#include "Logger.hpp"

namespace concord::crypto {

enum class SIGN_VERIFY_ALGO : uint8_t { ECDSA, RSA, EDDSA };
enum class Provider : uint16_t { OpenSSL, CryptoPP };

constexpr static const Provider DefaultProvider = Provider::OpenSSL;

/**
 * This class hides the implementation details from users of cryptographic algorithms
 * and allows the addition and removal of libraries in a single place
 */
class Factory {
 public:
  static std::unique_ptr<ISigner> getSigner(
      const std::string& signingKey,
      SIGN_VERIFY_ALGO signingAlgo,
      concord::crypto::KeyFormat fmt = concord::crypto::KeyFormat::HexaDecimalStrippedFormat,
      Provider provider = DefaultProvider);

  static std::unique_ptr<IVerifier> getVerifier(
      const std::string& verificationKey,
      SIGN_VERIFY_ALGO verifierAlgo,
      concord::crypto::KeyFormat fmt = concord::crypto::KeyFormat::HexaDecimalStrippedFormat,
      Provider provider = DefaultProvider);

  static std::pair<std::string, std::string> generateKeys(SIGN_VERIFY_ALGO verifierAlgo,
                                                          Provider provider = DefaultProvider);
};
}  // namespace concord::crypto
