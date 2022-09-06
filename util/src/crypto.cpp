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

#include "crypto/crypto.hpp"
#include "Logger.hpp"
#include "assertUtils.hpp"
#include "hex_tools.h"
#include "string.hpp"
#include "types.hpp"
#include "crypto/openssl/EdDSA.hpp"
#include "util/filesystem.hpp"
#include <utility>

namespace concord::crypto {
using std::array;
using std::pair;
using std::string;
using concord::Byte;
using concord::crypto::KeyFormat;
using concord::crypto::CurveType;
using concord::util::openssl_utils::UniquePKEY;
using concord::util::openssl_utils::UniqueOpenSSLPKEYContext;
using concord::util::openssl_utils::UniqueOpenSSLBIO;
using concord::util::openssl_utils::OPENSSL_SUCCESS;
using concord::crypto::openssl::EdDSASignatureByteSize;
using concord::crypto::openssl::EdDSAPrivateKeyByteSize;
using concord::crypto::openssl::EdDSAPublicKeyByteSize;

pair<string, string> generateEdDSAKeyPair(const KeyFormat fmt) {
  UniquePKEY edPkey;
  UniqueOpenSSLPKEYContext edPkeyCtx(EVP_PKEY_CTX_new_id(NID_ED25519, nullptr));

  ConcordAssertNE(edPkeyCtx, nullptr);
  ConcordAssertEQ(OPENSSL_SUCCESS, EVP_PKEY_keygen_init(edPkeyCtx.get()));
  EVP_PKEY* keygenRet = nullptr;
  ConcordAssertEQ(OPENSSL_SUCCESS, EVP_PKEY_keygen(edPkeyCtx.get(), &keygenRet));
  edPkey.reset(keygenRet);

  array<Byte, EdDSAPrivateKeyByteSize> privKey;
  array<Byte, EdDSAPublicKeyByteSize> pubKey;
  size_t keyLen{EdDSAPrivateKeyByteSize};

  ConcordAssertEQ(OPENSSL_SUCCESS, EVP_PKEY_get_raw_private_key(edPkey.get(), privKey.data(), &keyLen));
  ConcordAssertEQ(keyLen, EdDSAPrivateKeyByteSize);
  keyLen = EdDSAPublicKeyByteSize;
  ConcordAssertEQ(OPENSSL_SUCCESS, EVP_PKEY_get_raw_public_key(edPkey.get(), pubKey.data(), &keyLen));
  ConcordAssertEQ(keyLen, EdDSAPublicKeyByteSize);

  pair<string, string> keyPair(
      boost::algorithm::hex(string(reinterpret_cast<char*>(privKey.data()), EdDSAPrivateKeyByteSize)),
      boost::algorithm::hex(string(reinterpret_cast<char*>(pubKey.data()), EdDSAPublicKeyByteSize)));

  if (KeyFormat::PemFormat == fmt) {
    keyPair = EdDSAHexToPem(keyPair);
  }
  return keyPair;
}

pair<string, string> EdDSAHexToPem(const std::pair<std::string, std::string>& hex_key_pair) {
  string privPemString;
  string pubPemString;

  if (!hex_key_pair.first.empty()) {  // Proceed with private key pem file generation.
    const auto privKey = boost::algorithm::unhex(hex_key_pair.first);

    UniquePKEY ed_privKey(EVP_PKEY_new_raw_private_key(
        NID_ED25519, nullptr, reinterpret_cast<const unsigned char*>(privKey.data()), privKey.size()));
    ConcordAssertNE(nullptr, ed_privKey);

    UniqueOpenSSLBIO bio(BIO_new(BIO_s_mem()));
    ConcordAssertNE(nullptr, bio);

    ConcordAssertEQ(OPENSSL_SUCCESS,
                    PEM_write_bio_PrivateKey(bio.get(), ed_privKey.get(), nullptr, nullptr, 0, nullptr, nullptr));

    const auto lenToRead = BIO_pending(bio.get());
    std::vector<uint8_t> output(lenToRead);
    ConcordAssertGT(BIO_read(bio.get(), output.data(), lenToRead), 0);
    privPemString = string(output.begin(), output.end());
  }

  if (!hex_key_pair.second.empty()) {  // Proceed with public key pem file generation.
    const auto pubKey = boost::algorithm::unhex(hex_key_pair.second);

    UniquePKEY ed_pubKey(EVP_PKEY_new_raw_public_key(
        NID_ED25519, nullptr, reinterpret_cast<const unsigned char*>(pubKey.data()), pubKey.size()));
    ConcordAssertNE(nullptr, ed_pubKey);

    UniqueOpenSSLBIO bio(BIO_new(BIO_s_mem()));
    ConcordAssertNE(nullptr, bio);

    ConcordAssertEQ(OPENSSL_SUCCESS, PEM_write_bio_PUBKEY(bio.get(), ed_pubKey.get()));

    const auto lenToRead = BIO_pending(bio.get());
    std::vector<uint8_t> output(lenToRead);
    ConcordAssertGT(BIO_read(bio.get(), output.data(), lenToRead), 0);
    pubPemString = string(output.begin(), output.end());
  }
  return make_pair(privPemString, pubPemString);
}

KeyFormat getFormat(const std::string& key) {
  return (key.find("BEGIN") != std::string::npos) ? KeyFormat::PemFormat : KeyFormat::HexaDecimalStrippedFormat;
}

bool isValidKey(const std::string& keyName, const std::string& key, size_t expectedSize) {
  auto isValidHex = util::isValidHexString(key);
  if ((expectedSize == 0 or (key.length() == expectedSize)) and isValidHex) {
    return true;
  }
  throw std::runtime_error("Invalid " + keyName + " key (" + key + ") of size " + std::to_string(expectedSize) +
                           " bytes.");
}
}  // namespace concord::crypto
