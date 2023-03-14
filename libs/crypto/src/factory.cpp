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

#include "crypto/factory.hpp"
#ifdef USE_OPENSSL
#include "crypto/openssl/EdDSASigner.hpp"
#include "crypto/openssl/EdDSAVerifier.hpp"
#endif

namespace concord::crypto {

using AlgProvider = std::pair<SignatureAlgorithm, Provider>;

class AlgProviderHash {
 public:
  size_t operator()(const AlgProvider& params) const {
    size_t result = static_cast<uint16_t>(std::get<0>(params));
    result <<= (sizeof(uint16_t) * 8);
    result |= static_cast<uint16_t>(std::get<1>(params));
    return result;
  }
};

std::unique_ptr<ISigner> Factory::getSigner(const std::string& signingKey,
                                            SignatureAlgorithm signingAlgo,
                                            concord::crypto::KeyFormat fmt,
                                            Provider provider) {
  using ImplInitializer = std::function<std::unique_ptr<ISigner>()>;

  const std::unordered_map<AlgProvider, ImplInitializer, AlgProviderHash> providerAlgorithmToSigner = {
#ifdef USE_OPENSSL
      {{SignatureAlgorithm::EdDSA, Provider::OpenSSL},
       [&]() {
         const auto signingKeyObject = openssl::deserializeKey<openssl::EdDSAPrivateKey>(signingKey, fmt);
         return std::make_unique<openssl::EdDSASigner<openssl::EdDSAPrivateKey>>(signingKeyObject.getBytes());
       }}
#endif
  };

  return providerAlgorithmToSigner.at({signingAlgo, provider})();
}

std::unique_ptr<IVerifier> Factory::getVerifier(const std::string& verificationKey,
                                                SignatureAlgorithm verifierAlgo,
                                                concord::crypto::KeyFormat fmt,
                                                Provider provider) {
  using ImplInitializer = std::function<std::unique_ptr<IVerifier>()>;
  const std::unordered_map<AlgProvider, ImplInitializer, AlgProviderHash> providerAlgorithmToVerifier = {
#ifdef USE_OPENSSL
      {{SignatureAlgorithm::EdDSA, Provider::OpenSSL},
       [&]() {
         const auto verifyingKeyObject = openssl::deserializeKey<openssl::EdDSAPublicKey>(verificationKey, fmt);
         return std::make_unique<openssl::EdDSAVerifier<openssl::EdDSAPublicKey>>(verifyingKeyObject.getBytes());
       }}
#endif
  };

  return providerAlgorithmToVerifier.at({verifierAlgo, provider})();
}

}  // namespace concord::crypto
