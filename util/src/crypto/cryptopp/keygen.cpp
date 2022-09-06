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
#include "crypto/cryptopp/keygen.hpp"

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wpedantic"
#include <cryptopp/dll.h>
#include <cryptopp/pem.h>
#include <cryptopp/rsa.h>
#include <cryptopp/cryptlib.h>
#include <cryptopp/oids.h>
#pragma GCC diagnostic pop

namespace concord::crypto {
using CryptoPP::StringSource;
using CryptoPP::StringSink;
using CryptoPP::HexDecoder;
using CryptoPP::AutoSeededRandomPool;
using CryptoPP::HexEncoder;
using CryptoPP::ECP;
using CryptoPP::ECDSA;
using CryptoPP::SHA256;
using CryptoPP::OAEP;
using CryptoPP::RSAES;
using std::pair;
using std::string;

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

pair<string, string> generateRsaKeyPair(const KeyFormat fmt) {
  AutoSeededRandomPool rng;
  pair<string, string> keyPair;

  constexpr const size_t privateKeyLength = 2048;
  RSAES<OAEP<CryptoPP::SHA256>>::Decryptor priv(rng, privateKeyLength);
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

  privateKey.Initialize(
      prng, curve_types == CurveType::secp256k1 ? CryptoPP::ASN1::secp256k1() : CryptoPP::ASN1::secp384r1());
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

}  // namespace concord::crypto
