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

namespace concord::crypto {

using AlgProvider = std::pair<SIGN_VERIFY_ALGO, Provider>;

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
                                            SIGN_VERIFY_ALGO signingAlgo,
                                            concord::crypto::KeyFormat fmt,
                                            Provider provider) {
  using ImplInitializer = std::function<std::unique_ptr<ISigner>()>;
  namespace cryptopp = concord::crypto::cryptopp;
  namespace openssl = concord::crypto::openssl;

  const std::unordered_map<AlgProvider, ImplInitializer, AlgProviderHash> providerAlgorithmToSigner = {
      {{SIGN_VERIFY_ALGO::ECDSA, Provider::CryptoPP},
       [&]() { return std::make_unique<cryptopp::ECDSASigner>(signingKey, fmt); }},
      {{SIGN_VERIFY_ALGO::RSA, Provider::CryptoPP},
       [&]() { return std::make_unique<cryptopp::RSASigner>(signingKey, fmt); }},
      {{SIGN_VERIFY_ALGO::EDDSA, Provider::OpenSSL}, [&]() {
         const auto signingKeyObject = openssl::deserializeKey<openssl::EdDSAPrivateKey>(signingKey, fmt);
         return std::make_unique<openssl::EdDSASigner<openssl::EdDSAPrivateKey>>(signingKeyObject.getBytes());
       }}};

  return providerAlgorithmToSigner.at({signingAlgo, provider})();
}

std::unique_ptr<IVerifier> Factory::getVerifier(const std::string& verificationKey,
                                                SIGN_VERIFY_ALGO verifierAlgo,
                                                concord::crypto::KeyFormat fmt,
                                                Provider provider) {
  using ImplInitializer = std::function<std::unique_ptr<IVerifier>()>;
  namespace cryptopp = concord::crypto::cryptopp;
  namespace openssl = concord::crypto::openssl;

  const std::unordered_map<AlgProvider, ImplInitializer, AlgProviderHash> providerAlgorithmToVerifier = {
      {{SIGN_VERIFY_ALGO::ECDSA, Provider::CryptoPP},
       [&]() { return std::make_unique<cryptopp::ECDSAVerifier>(verificationKey, fmt); }},
      {{SIGN_VERIFY_ALGO::RSA, Provider::CryptoPP},
       [&]() { return std::make_unique<cryptopp::RSAVerifier>(verificationKey, fmt); }},
      {{SIGN_VERIFY_ALGO::EDDSA, Provider::OpenSSL}, [&]() {
         const auto signingKeyObject = openssl::deserializeKey<openssl::EdDSAPublicKey>(verificationKey, fmt);
         return std::make_unique<openssl::EdDSAVerifier<openssl::EdDSAPublicKey>>(signingKeyObject.getBytes());
       }}};

  return providerAlgorithmToVerifier.at({verifierAlgo, provider})();
}

}  // namespace concord::crypto
