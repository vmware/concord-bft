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

#include "crypto.hpp"
#include "Logger.hpp"
#include "assertUtils.hpp"
#include "hex_tools.h"
#include "openssl_crypto.hpp"
#include "crypto/openssl/EdDSA.hpp"
#include "util/filesystem.hpp"

#include <regex>
#include <utility>

namespace concord::crypto {
using std::array;
using std::pair;
using std::string;
using concord::crypto::KeyFormat;
using concord::crypto::CurveType;
using concord::util::openssl_utils::UniquePKEY;
using concord::util::openssl_utils::UniqueOpenSSLPKEYContext;
using concord::util::openssl_utils::UniqueOpenSSLBIO;
using concord::util::openssl_utils::OPENSSL_SUCCESS;
using concord::util::crypto::openssl::EdDSASignatureByteSize;
using namespace CryptoPP;

pair<string, string> generateEdDSAKeyPair(const KeyFormat fmt) {
  UniquePKEY edPkey;
  UniqueOpenSSLPKEYContext edPkeyCtx(EVP_PKEY_CTX_new_id(NID_ED25519, nullptr));

  ConcordAssertNE(edPkeyCtx, nullptr);

  ConcordAssertEQ(OPENSSL_SUCCESS, EVP_PKEY_keygen_init(edPkeyCtx.get()));
  ConcordAssertEQ(
      OPENSSL_SUCCESS,
      EVP_PKEY_keygen(edPkeyCtx.get(), reinterpret_cast<EVP_PKEY**>(&edPkey)));  // Generate EdDSA key 'edPkey'.

  array<unsigned char, EdDSASignatureByteSize> privKey;
  array<unsigned char, EdDSASignatureByteSize> pubKey;
  size_t privlen{EdDSASignatureByteSize};
  size_t publen{EdDSASignatureByteSize};

  ConcordAssertEQ(OPENSSL_SUCCESS, EVP_PKEY_get_raw_private_key(edPkey.get(), privKey.data(), &privlen));
  ConcordAssertEQ(OPENSSL_SUCCESS, EVP_PKEY_get_raw_public_key(edPkey.get(), pubKey.data(), &publen));

  pair<string, string> keyPair(boost::algorithm::hex(string(reinterpret_cast<char*>(privKey.data()), privlen)),
                               boost::algorithm::hex(string(reinterpret_cast<char*>(pubKey.data()), publen)));

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

pair<string, string> RsaHexToPem(const pair<string, string>& key_pair) {
  pair<string, string> out;
  if (!key_pair.first.empty()) {
    StringSource priv_str(key_pair.first, true, new HexDecoder());
    CryptoPP::RSA::PrivateKey priv;
    priv.Load(priv_str);
    StringSink priv_string_sink(out.first);
    PEM_Save(priv_string_sink, priv);
    priv_string_sink.MessageEnd();
  }

  if (!key_pair.second.empty()) {
    StringSource pub_str(key_pair.second, true, new HexDecoder());
    CryptoPP::RSA::PublicKey pub;
    pub.Load(pub_str);
    StringSink pub_string_sink(out.second);
    PEM_Save(pub_string_sink, pub);
    pub_string_sink.MessageEnd();
  }
  return out;
}

pair<string, string> ECDSAHexToPem(const pair<string, string>& key_pair) {
  pair<string, string> out;
  if (!key_pair.first.empty()) {
    StringSource priv_str(key_pair.first, true, new HexDecoder());
    ECDSA<ECP, CryptoPP::SHA256>::PrivateKey priv;
    priv.Load(priv_str);
    StringSink priv_string_sink(out.first);
    PEM_Save(priv_string_sink, priv);
  }
  if (!key_pair.second.empty()) {
    StringSource pub_str(key_pair.second, true, new HexDecoder());
    ECDSA<ECP, CryptoPP::SHA256>::PublicKey pub;
    pub.Load(pub_str);
    StringSink pub_string_sink(out.second);
    PEM_Save(pub_string_sink, pub);
  }
  return out;
}

pair<string, string> generateRsaKeyPair(const uint32_t sig_length, const KeyFormat fmt) {
  AutoSeededRandomPool rng;
  pair<string, string> keyPair;

  RSAES<OAEP<CryptoPP::SHA256>>::Decryptor priv(rng, sig_length);
  RSAES<OAEP<CryptoPP::SHA256>>::Encryptor pub(priv);
  HexEncoder privEncoder(new StringSink(keyPair.first));
  priv.AccessMaterial().Save(privEncoder);
  privEncoder.MessageEnd();

  HexEncoder pubEncoder(new StringSink(keyPair.second));
  pub.AccessMaterial().Save(pubEncoder);
  pubEncoder.MessageEnd();
  if (fmt == KeyFormat::PemFormat) {
    keyPair = RsaHexToPem(keyPair);
  }
  return keyPair;
}

pair<string, string> generateECDSAKeyPair(const KeyFormat fmt, CurveType curve_types) {
  AutoSeededRandomPool prng;
  ECDSA<ECP, CryptoPP::SHA256>::PrivateKey privateKey;
  ECDSA<ECP, CryptoPP::SHA256>::PublicKey publicKey;

  privateKey.Initialize(prng, curve_types == CurveType::secp256k1 ? ASN1::secp256k1() : ASN1::secp384r1());
  privateKey.MakePublicKey(publicKey);
  pair<string, string> keyPair;
  HexEncoder privEncoder(new StringSink(keyPair.first));
  privateKey.Save(privEncoder);
  HexEncoder pubEncoder(new StringSink(keyPair.second));
  publicKey.Save(pubEncoder);
  if (fmt == KeyFormat::PemFormat) {
    keyPair = ECDSAHexToPem(keyPair);
  }
  return keyPair;
}

KeyFormat getFormat(const std::string& key) {
  return (key.find("BEGIN") != std::string::npos) ? KeyFormat::PemFormat : KeyFormat::HexaDecimalStrippedFormat;
}
}  // namespace concord::crypto
