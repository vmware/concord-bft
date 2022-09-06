// Concord
//
// Copyright (c) 2020 VMware, Inc. All Rights Reserved.
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
#include "crypto/crypto.hpp"
#include "crypto/cryptopp/signers.hpp"
#include "crypto/cryptopp/verifiers.hpp"
#include "crypto/cryptopp/keygen.hpp"
#include "Logger.hpp"

namespace {
using namespace concord::crypto::cryptopp;
using concord::crypto::KeyFormat;
using concord::crypto::cryptopp::RSA_SIGNATURE_LENGTH;
using concord::crypto::generateRsaKeyPair;
using concord::crypto::RsaHexToPem;
using concord::crypto::generateECDSAKeyPair;
using concord::crypto::ECDSAHexToPem;

std::string getSig(const std::string& data, concord::crypto::ISigner& signer) {
  std::string buf(signer.signatureLength() + 1, '\x00');
  signer.sign(data, reinterpret_cast<concord::Byte*>(buf.data()));
  return buf;
}

TEST(cryptopp_utils, generate_rsa_keys_hex_format) {
  ASSERT_NO_THROW(generateRsaKeyPair());
  auto keys = generateRsaKeyPair();
  LOG_INFO(GL, keys.first << " | " << keys.second);
}

TEST(cryptopp_utils, generate_rsa_keys_pem_format) {
  ASSERT_NO_THROW(generateRsaKeyPair(KeyFormat::PemFormat));
  auto keys = generateRsaKeyPair(KeyFormat::PemFormat);
  LOG_INFO(GL, keys.first << " | " << keys.second);
}

TEST(cryptopp_utils, generate_ECDSA_keys_pem_format) {
  ASSERT_NO_THROW(generateECDSAKeyPair(KeyFormat::PemFormat));
  auto keys = generateECDSAKeyPair(KeyFormat::PemFormat);
  LOG_INFO(GL, keys.first << " | " << keys.second);
}

TEST(cryptopp_utils, generate_ECDSA_keys_hex_format) {
  ASSERT_NO_THROW(generateECDSAKeyPair(KeyFormat::HexaDecimalStrippedFormat));
  auto keys = generateECDSAKeyPair(KeyFormat::HexaDecimalStrippedFormat);
  LOG_INFO(GL, keys.first << " | " << keys.second);
}

TEST(cryptopp_utils, test_rsa_keys_hex) {
  auto keys = generateRsaKeyPair();
  RSASigner signer(keys.first, KeyFormat::HexaDecimalStrippedFormat);
  RSAVerifier verifier(keys.second, KeyFormat::HexaDecimalStrippedFormat);
  std::string data = "Hello world";
  auto sig = getSig(data, signer);
  ASSERT_TRUE(verifier.verify(data, sig));
}

TEST(cryptopp_utils, test_rsa_keys_pem) {
  auto keys = generateRsaKeyPair(KeyFormat::PemFormat);
  RSASigner signer(keys.first, KeyFormat::PemFormat);
  RSAVerifier verifier(keys.second, KeyFormat::PemFormat);
  std::string data = "Hello world";
  auto sig = getSig(data, signer);
  ASSERT_TRUE(verifier.verify(data, sig));
}

TEST(cryptopp_utils, test_rsa_keys_combined_a) {
  auto keys = generateRsaKeyPair();
  auto pemKeys = RsaHexToPem(keys);
  RSASigner signer(keys.first, KeyFormat::HexaDecimalStrippedFormat);
  RSAVerifier verifier(pemKeys.second, KeyFormat::PemFormat);
  std::string data = "Hello world";
  auto sig = getSig(data, signer);
  ASSERT_TRUE(verifier.verify(data, sig));
}

TEST(cryptopp_utils, test_rsa_keys_combined_b) {
  auto keys = generateRsaKeyPair();
  auto pemKeys = RsaHexToPem(keys);
  RSASigner signer(pemKeys.first, KeyFormat::PemFormat);
  RSAVerifier verifier(keys.second, KeyFormat::HexaDecimalStrippedFormat);
  std::string data = "Hello world";
  auto sig = getSig(data, signer);
  ASSERT_TRUE(verifier.verify(data, sig));
}

TEST(cryptopp_utils, test_ecdsa_keys_hex) {
  auto keys = generateECDSAKeyPair(KeyFormat::HexaDecimalStrippedFormat);
  ECDSASigner signer(keys.first, KeyFormat::HexaDecimalStrippedFormat);
  ECDSAVerifier verifier(keys.second, KeyFormat::HexaDecimalStrippedFormat);
  std::string data = "Hello world";
  auto sig = getSig(data, signer);
  ASSERT_TRUE(verifier.verify(data, sig));
}

TEST(cryptopp_utils, test_ecdsa_keys_pem) {
  auto keys = generateECDSAKeyPair(KeyFormat::PemFormat);
  ECDSASigner signer(keys.first, KeyFormat::PemFormat);
  ECDSAVerifier verifier(keys.second, KeyFormat::PemFormat);
  std::string data = "Hello world";
  auto sig = getSig(data, signer);
  ASSERT_TRUE(verifier.verify(data, sig));
}

TEST(cryptopp_utils, test_ecdsa_keys_pem_combined_a) {
  auto keys = generateECDSAKeyPair(KeyFormat::HexaDecimalStrippedFormat);
  auto pemKeys = ECDSAHexToPem(keys);
  ECDSASigner signer(keys.first, KeyFormat::HexaDecimalStrippedFormat);
  ECDSAVerifier verifier(pemKeys.second, KeyFormat::PemFormat);
  std::string data = "Hello world";
  auto sig = getSig(data, signer);
  ASSERT_TRUE(verifier.verify(data, sig));
}

TEST(cryptopp_utils, test_ecdsa_keys_pem_combined_b) {
  auto keys = generateECDSAKeyPair(KeyFormat::HexaDecimalStrippedFormat);
  auto pemKeys = ECDSAHexToPem(keys);
  ECDSASigner signer(pemKeys.first, KeyFormat::PemFormat);
  ECDSAVerifier verifier(keys.second, KeyFormat::HexaDecimalStrippedFormat);
  std::string data = "Hello world";
  auto sig = getSig(data, signer);
  ASSERT_TRUE(verifier.verify(data, sig));
}
}  // namespace

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
