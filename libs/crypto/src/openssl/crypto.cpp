// Concord
//
// Copyright (c) 2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the
// LICENSE file.

#include "crypto/openssl/crypto.hpp"
#include "crypto/openssl/EdDSA.hpp"
#include "types.hpp"
#include "assertUtils.hpp"
#include "hex_tools.h"

#include <openssl/ec.h>
#include <openssl/pem.h>
#include <stdio.h>
#include <cassert>

namespace concord::crypto::openssl {

using std::pair;
using std::string;
using std::array;

using concord::Byte;
using concord::crypto::KeyFormat;
using concord::crypto::openssl::UniquePKEY;
using concord::crypto::openssl::UniqueECKEY;
using concord::crypto::openssl::UniquePKEYContext;
using concord::crypto::openssl::UniqueBIO;
using concord::crypto::openssl::OPENSSL_SUCCESS;
using concord::crypto::Ed25519PrivateKeyByteSize;
using concord::crypto::Ed25519PublicKeyByteSize;

void OpenSSLAssert(bool expr, const std::string& msg) {
  if (!expr) {
    throw OpenSSLError(msg);
  }
}

const string kLegalHexadecimalDigits = "0123456789ABCDEFabcdef";

string computeSHA256Hash(const string& data) { return computeSHA256Hash(data.data(), data.length()); }

string computeSHA256Hash(const char* data, size_t length) {
  UniqueContext digest_context(EVP_MD_CTX_new());
  OpenSSLAssert(digest_context.get() != nullptr, "failed to allocate a message digest context object.");
  OpenSSLAssert(EVP_DigestInit_ex(digest_context.get(), EVP_sha256(), nullptr),
                "failed to initialize a message digest context object for SHA-256.");
  OpenSSLAssert(EVP_DigestUpdate(digest_context.get(), data, length), "failed to hash data with SHA-256.");
  ConcordAssert(kExpectedSHA256HashLengthInBytes == EVP_MD_size(EVP_sha256()));

  string hash(kExpectedSHA256HashLengthInBytes, (char)0);
  unsigned int hash_bytes_written;
  OpenSSLAssert(
      EVP_DigestFinal_ex(digest_context.get(), reinterpret_cast<unsigned char*>(hash.data()), &hash_bytes_written),
      "failed to retrieve the resulting hash from a message digest context object for an SHA-256 hash operation.");
  OpenSSLAssert(
      hash_bytes_written <= kExpectedSHA256HashLengthInBytes,
      "reported retrieving a hash value longer than that expected for an SHA-256 hash from a SHA-256 hash operation.");

  return hash;
}

// Unique_ptr custom deleter. OPENSSL_FREE macro should be wrapped because of CRYPTO_free's signature inside
void _openssl_free(void* ptr) { OPENSSL_free(ptr); }

pair<string, string> generateEdDSAKeyPair(const KeyFormat fmt) {
  UniquePKEY edPkey;
  UniquePKEYContext edPkeyCtx(EVP_PKEY_CTX_new_id(NID_ED25519, nullptr));

  ConcordAssertNE(edPkeyCtx, nullptr);
  ConcordAssertEQ(OPENSSL_SUCCESS, EVP_PKEY_keygen_init(edPkeyCtx.get()));
  EVP_PKEY* keygenRet = nullptr;
  ConcordAssertEQ(OPENSSL_SUCCESS, EVP_PKEY_keygen(edPkeyCtx.get(), &keygenRet));
  edPkey.reset(keygenRet);

  array<Byte, Ed25519PrivateKeyByteSize> privKey;
  array<Byte, Ed25519PublicKeyByteSize> pubKey;
  size_t keyLen{Ed25519PrivateKeyByteSize};

  ConcordAssertEQ(OPENSSL_SUCCESS, EVP_PKEY_get_raw_private_key(edPkey.get(), privKey.data(), &keyLen));
  ConcordAssertEQ(keyLen, Ed25519PrivateKeyByteSize);
  keyLen = Ed25519PublicKeyByteSize;
  ConcordAssertEQ(OPENSSL_SUCCESS, EVP_PKEY_get_raw_public_key(edPkey.get(), pubKey.data(), &keyLen));
  ConcordAssertEQ(keyLen, Ed25519PublicKeyByteSize);

  pair<string, string> keyPair(
      boost::algorithm::hex(string(reinterpret_cast<char*>(privKey.data()), Ed25519PrivateKeyByteSize)),
      boost::algorithm::hex(string(reinterpret_cast<char*>(pubKey.data()), Ed25519PublicKeyByteSize)));

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

    UniqueBIO bio(BIO_new(BIO_s_mem()));
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

    UniqueBIO bio(BIO_new(BIO_s_mem()));
    ConcordAssertNE(nullptr, bio);

    ConcordAssertEQ(OPENSSL_SUCCESS, PEM_write_bio_PUBKEY(bio.get(), ed_pubKey.get()));

    const auto lenToRead = BIO_pending(bio.get());
    std::vector<uint8_t> output(lenToRead);
    ConcordAssertGT(BIO_read(bio.get(), output.data(), lenToRead), 0);
    pubPemString = string(output.begin(), output.end());
  }
  return make_pair(privPemString, pubPemString);
}

}  // namespace concord::crypto::openssl
