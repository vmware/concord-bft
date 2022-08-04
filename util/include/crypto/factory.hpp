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

class Factory {
 public:
  static std::unique_ptr<ISigner> getSigner(
      const std::string& signingKey,
      SIGN_VERIFY_ALGO signingAlgo,
      concord::crypto::KeyFormat fmt = concord::crypto::KeyFormat::HexaDecimalStrippedFormat) {
    switch (signingAlgo) {
      case SIGN_VERIFY_ALGO::ECDSA: {
        return std::unique_ptr<concord::crypto::ISigner>(new concord::crypto::cryptopp::ECDSASigner(signingKey, fmt));
      }
      case SIGN_VERIFY_ALGO::RSA: {
        return std::unique_ptr<concord::crypto::ISigner>(new concord::crypto::cryptopp::RSASigner(signingKey, fmt));
      }
      case SIGN_VERIFY_ALGO::EDDSA: {
        using MainReplicaSigner =
            concord::crypto::openssl::EdDSASigner<concord::util::crypto::openssl::EdDSAPrivateKey>;
        const auto signingKeyObject =
            concord::util::crypto::openssl::deserializeKey<concord::util::crypto::openssl::EdDSAPrivateKey>(signingKey,
                                                                                                            fmt);
        return std::unique_ptr<MainReplicaSigner>(new MainReplicaSigner(signingKeyObject.getBytes()));
      }
      default:
        LOG_ERROR(EDDSA_SIG_LOG, "Invalid signing algorithm.");
        return {};
    }
  }

  static std::unique_ptr<IVerifier> getVerifier(
      const std::string& verificationKey,
      SIGN_VERIFY_ALGO verifierAlgo,
      concord::crypto::KeyFormat fmt = concord::crypto::KeyFormat::HexaDecimalStrippedFormat) {
    switch (verifierAlgo) {
      case SIGN_VERIFY_ALGO::ECDSA: {
        return std::unique_ptr<concord::crypto::IVerifier>(
            new concord::crypto::cryptopp::ECDSAVerifier(verificationKey, fmt));
      }
      case SIGN_VERIFY_ALGO::RSA: {
        return std::unique_ptr<concord::crypto::IVerifier>(
            new concord::crypto::cryptopp::RSAVerifier(verificationKey, fmt));
      }
      case SIGN_VERIFY_ALGO::EDDSA: {
        using MainReplicaVerifier =
            concord::crypto::openssl::EdDSAVerifier<concord::util::crypto::openssl::EdDSAPublicKey>;
        const auto verifyingKeyObject =
            concord::util::crypto::openssl::deserializeKey<concord::util::crypto::openssl::EdDSAPublicKey>(
                verificationKey, fmt);
        return std::unique_ptr<MainReplicaVerifier>(new MainReplicaVerifier(verifyingKeyObject.getBytes()));
      }
      default:
        LOG_ERROR(EDDSA_SIG_LOG, "Invalid verifying algorithm.");
        return {};
    }
  }
};
}  // namespace concord::crypto
