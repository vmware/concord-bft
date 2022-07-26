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
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.
//

#include "gtest/gtest.h"
#include "Logger.hpp"
#include "sign_verify_utils.hpp"
#include "crypto/eddsa/EdDSA.hpp"
#include "crypto/eddsa/EdDSASigner.hpp"
#include "crypto/eddsa/EdDSAVerifier.hpp"

namespace {
using concord::util::crypto::KeyFormat;
using concord::crypto::openssl::OpenSSLCryptoImpl;

using TestTxnSigner = concord::crypto::openssl::EdDSASigner<EdDSAPrivateKey>;
using TestTxnVerifier = concord::crypto::openssl::EdDSAVerifier<EdDSAPublicKey>;

TEST(openssl_utils, check_eddsa_keys_hex_format_length) {
  const auto hexKeys = OpenSSLCryptoImpl::instance().generateEdDSAKeyPair(KeyFormat::HexaDecimalStrippedFormat);
  ASSERT_EQ(hexKeys.first.size(), EdDSAPrivateKeyByteSize * 2);
  ASSERT_EQ(hexKeys.second.size(), EdDSAPrivateKeyByteSize * 2);
}

TEST(openssl_utils, generate_eddsa_keys_hex_format) {
  ASSERT_NO_THROW(OpenSSLCryptoImpl::instance().generateEdDSAKeyPair());
  const auto hexKeys1 = OpenSSLCryptoImpl::instance().generateEdDSAKeyPair();
  LOG_INFO(GL, hexKeys1.first << " | " << hexKeys1.second);

  ASSERT_NO_THROW(OpenSSLCryptoImpl::instance().generateEdDSAKeyPair(KeyFormat::HexaDecimalStrippedFormat));
  const auto hexKeys2 = OpenSSLCryptoImpl::instance().generateEdDSAKeyPair(KeyFormat::HexaDecimalStrippedFormat);
  LOG_INFO(GL, hexKeys2.first << " | " << hexKeys2.second);
}

TEST(openssl_utils, generate_eddsa_keys_pem_format) {
  ASSERT_NO_THROW(OpenSSLCryptoImpl::instance().generateEdDSAKeyPair());
  ASSERT_NO_THROW(OpenSSLCryptoImpl::instance().generateEdDSAKeyPair(KeyFormat::PemFormat));
  const auto pemKeys = OpenSSLCryptoImpl::instance().generateEdDSAKeyPair(KeyFormat::PemFormat);
  LOG_INFO(GL, pemKeys.first << " | " << pemKeys.second);
}

TEST(openssl_utils, test_eddsa_keys_hex_ok) {
  auto hexKeys = OpenSSLCryptoImpl::instance().generateEdDSAKeyPair();

  const auto signingKey = deserializeKey<EdDSAPrivateKey>(hexKeys.first);
  const auto verificationKey = deserializeKey<EdDSAPublicKey>(hexKeys.second);

  TestTxnSigner signer(signingKey.getBytes());
  TestTxnVerifier verifier(verificationKey.getBytes());

  const std::string data = "Hello VMworld";
  auto sig = signer.sign(data);
  ASSERT_TRUE(verifier.verify(data, sig));
}

TEST(openssl_utils, test_eddsa_keys_hex_nok) {
  const auto hexKeys = OpenSSLCryptoImpl::instance().generateEdDSAKeyPair();

  const auto signingKey = deserializeKey<EdDSAPrivateKey>(hexKeys.first);
  const auto verificationKey = deserializeKey<EdDSAPublicKey>(hexKeys.second);

  TestTxnSigner signer(signingKey.getBytes());
  TestTxnVerifier verifier(verificationKey.getBytes());

  const std::string data = "Hello VMworld";
  auto sig = signer.sign(data);

  // Corrupt data.
  ++sig[0];

  ASSERT_FALSE(verifier.verify(data, sig));
}

TEST(openssl_utils, test_eddsa_keys_pem_ok) {
  const auto pemKeys = OpenSSLCryptoImpl::instance().generateEdDSAKeyPair(KeyFormat::PemFormat);

  const auto signingKey = deserializeKey<EdDSAPrivateKey>(pemKeys.first, KeyFormat::PemFormat);
  const auto verificationKey = deserializeKey<EdDSAPublicKey>(pemKeys.second, KeyFormat::PemFormat);

  TestTxnSigner signer(signingKey.getBytes());
  TestTxnVerifier verifier(verificationKey.getBytes());

  const std::string data = "Hello VMworld";
  auto sig = signer.sign(data);
  ASSERT_TRUE(verifier.verify(data, sig));
}

TEST(openssl_utils, test_eddsa_keys_pem_nok) {
  const auto pemKeys = OpenSSLCryptoImpl::instance().generateEdDSAKeyPair(KeyFormat::PemFormat);

  const auto signingKey = deserializeKey<EdDSAPrivateKey>(pemKeys.first, KeyFormat::PemFormat);
  const auto verificationKey = deserializeKey<EdDSAPublicKey>(pemKeys.second, KeyFormat::PemFormat);

  TestTxnSigner signer(signingKey.getBytes());
  TestTxnVerifier verifier(verificationKey.getBytes());

  const std::string data = "Hello VMworld";
  auto sig = signer.sign(data);

  // Corrupt data.
  ++sig[0];

  ASSERT_FALSE(verifier.verify(data, sig));
}

TEST(openssl_utils, test_eddsa_keys_combined_a_ok) {
  const auto hexKeys = OpenSSLCryptoImpl::instance().generateEdDSAKeyPair();
  const auto pemKeys = OpenSSLCryptoImpl::instance().EdDSAHexToPem(hexKeys);

  const auto signingKey = deserializeKey<EdDSAPrivateKey>(hexKeys.first);
  const auto verificationKey = deserializeKey<EdDSAPublicKey>(pemKeys.second, KeyFormat::PemFormat);

  TestTxnSigner signer(signingKey.getBytes());
  TestTxnVerifier verifier(verificationKey.getBytes());

  const std::string data = "Hello VMworld";
  auto sig = signer.sign(data);
  ASSERT_TRUE(verifier.verify(data, sig));
}

TEST(openssl_utils, test_eddsa_keys_combined_a_nok) {
  const auto hexKeys = OpenSSLCryptoImpl::instance().generateEdDSAKeyPair();
  const auto pemKeys = OpenSSLCryptoImpl::instance().EdDSAHexToPem(hexKeys);

  const auto signingKey = deserializeKey<EdDSAPrivateKey>(hexKeys.first);
  const auto verificationKey = deserializeKey<EdDSAPublicKey>(pemKeys.second, KeyFormat::PemFormat);

  TestTxnSigner signer(signingKey.getBytes());
  TestTxnVerifier verifier(verificationKey.getBytes());

  const std::string data = "Hello VMworld";
  auto sig = signer.sign(data);

  // Corrupt data.
  ++sig[0];

  ASSERT_FALSE(verifier.verify(data, sig));
}

TEST(openssl_utils, test_eddsa_keys_combined_b_ok) {
  const auto hexKeys = OpenSSLCryptoImpl::instance().generateEdDSAKeyPair();
  const auto pemKeys = OpenSSLCryptoImpl::instance().EdDSAHexToPem(hexKeys);

  const auto signingKey = deserializeKey<EdDSAPrivateKey>(pemKeys.first, KeyFormat::PemFormat);
  const auto verificationKey = deserializeKey<EdDSAPublicKey>(hexKeys.second);

  TestTxnSigner signer(signingKey.getBytes());
  TestTxnVerifier verifier(verificationKey.getBytes());

  const std::string data = "Hello VMworld";
  auto sig = signer.sign(data);
  ASSERT_TRUE(verifier.verify(data, sig));
}

TEST(openssl_utils, test_eddsa_keys_combined_b_nok) {
  const auto hexKeys = OpenSSLCryptoImpl::instance().generateEdDSAKeyPair();
  const auto pemKeys = OpenSSLCryptoImpl::instance().EdDSAHexToPem(hexKeys);

  const auto signingKey = deserializeKey<EdDSAPrivateKey>(pemKeys.first, KeyFormat::PemFormat);
  const auto verificationKey = deserializeKey<EdDSAPublicKey>(hexKeys.second);

  TestTxnSigner signer(signingKey.getBytes());
  TestTxnVerifier verifier(verificationKey.getBytes());

  std::string data = "Hello VMworld";
  auto sig = signer.sign(data);

  // Corrupt data.
  ++sig[0];

  ASSERT_FALSE(verifier.verify(data, sig));
}
}  // namespace

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
