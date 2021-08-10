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
// terms and conditions of the sub-component's license, as noted in the LICENSE
// file.
#include "crypto_utils.hpp"

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wpedantic"
#include <cryptopp/dll.h>
#include <cryptopp/pem.h>
#include <cryptopp/rsa.h>
#pragma GCC diagnostic pop

namespace concord::util {
class Crypto::Impl {
 public:
  std::pair<std::string, std::string> hexToPem(const std::pair<std::string, std::string>& key_pair) {
    std::pair<std::string, std::string> out;

    CryptoPP::HexDecoder priv_hd;
    CryptoPP::StringSource priv_str(key_pair.first, true, new CryptoPP::HexDecoder());
    CryptoPP::RSA::PrivateKey priv;
    priv.Load(priv_str);
    CryptoPP::StringSink priv_string_sink(out.first);
    CryptoPP::PEM_Save(priv_string_sink, priv);
    priv_string_sink.MessageEnd();

    CryptoPP::HexDecoder pub_hd;
    CryptoPP::StringSource pub_str(key_pair.second, true, new CryptoPP::HexDecoder());
    CryptoPP::RSA::PublicKey pub;
    pub.Load(pub_str);
    CryptoPP::StringSink pub_string_sink(out.second);
    CryptoPP::PEM_Save(pub_string_sink, pub);
    pub_string_sink.MessageEnd();
    return out;
  }

  std::pair<std::string, std::string> generateRsaKeyPairs(uint32_t sig_length, KeyFormat fmt) {
    CryptoPP::AutoSeededRandomPool rng;
    std::pair<std::string, std::string> keyPair;

    CryptoPP::RSAES<CryptoPP::OAEP<CryptoPP::SHA256>>::Decryptor priv(rng, sig_length);
    CryptoPP::RSAES<CryptoPP::OAEP<CryptoPP::SHA256>>::Encryptor pub(priv);
    CryptoPP::HexEncoder privEncoder(new CryptoPP::StringSink(keyPair.first));
    priv.AccessMaterial().Save(privEncoder);
    privEncoder.MessageEnd();

    CryptoPP::HexEncoder pubEncoder(new CryptoPP::StringSink(keyPair.second));
    pub.AccessMaterial().Save(pubEncoder);
    pubEncoder.MessageEnd();
    if (fmt == Crypto::KeyFormat::PemFormat) {
      keyPair = hexToPem(keyPair);
    }
    return keyPair;
  }
  ~Impl() = default;
};

std::pair<std::string, std::string> Crypto::generateRsaKeyPair(const uint32_t sig_length, const KeyFormat fmt) const {
  return impl_->generateRsaKeyPairs(sig_length, fmt);
}

std::pair<std::string, std::string> Crypto::hexToPem(const std::pair<std::string, std::string>& key_pair) const {
  return impl_->hexToPem(key_pair);
}

Crypto::Crypto() : impl_{new Impl()} {}
Crypto::~Crypto() = default;
}  // namespace concord::util
