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

#include "crypto/cryptopp/verifiers.hpp"
#include "types.hpp"

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wpedantic"
#include <cryptopp/dll.h>
#include <cryptopp/pem.h>
#include <cryptopp/rsa.h>
#include <cryptopp/cryptlib.h>
#include <cryptopp/oids.h>
#pragma GCC diagnostic pop

namespace concord::crypto::cryptopp {

using namespace CryptoPP;
using concord::crypto::KeyFormat;
using concord::Byte;

class ECDSAVerifier::Impl {
  std::unique_ptr<ECDSA<ECP, CryptoPP::SHA256>::Verifier> verifier_;

 public:
  Impl(ECDSA<ECP, CryptoPP::SHA256>::PublicKey &publicKey) {
    verifier_ = std::make_unique<ECDSA<ECP, CryptoPP::SHA256>::Verifier>(std::move(publicKey));
  }

  bool verify(const Byte *msg, size_t msgLen, const Byte *sig, size_t sigLen) const {
    return verifier_->VerifyMessage((const CryptoPP::byte *)&msg[0], msgLen, (const CryptoPP::byte *)sig, sigLen);
  }

  uint32_t signatureLength() const { return verifier_->SignatureLength(); }
};

bool ECDSAVerifier::verifyBuffer(const Byte *msg, size_t msgLen, const Byte *sig, size_t sigLen) const {
  return impl_->verify(msg, msgLen, sig, sigLen);
}

ECDSAVerifier::ECDSAVerifier(const std::string &str_pub_key, KeyFormat fmt) : key_str_{str_pub_key} {
  ECDSA<ECP, CryptoPP::SHA256>::PublicKey publicKey;
  if (fmt == KeyFormat::PemFormat) {
    StringSource s(str_pub_key, true);
    PEM_Load(s, publicKey);
  } else {
    StringSource s(str_pub_key, true, new HexDecoder());
    publicKey.Load(s);
  }
  impl_.reset(new Impl(publicKey));
}

uint32_t ECDSAVerifier::signatureLength() const { return impl_->signatureLength(); }

ECDSAVerifier::~ECDSAVerifier() = default;

class RSAVerifier::Impl {
 public:
  Impl(CryptoPP::RSA::PublicKey &public_key) {
    verifier_ = std::make_unique<RSASS<PKCS1v15, CryptoPP::SHA256>::Verifier>(std::move(public_key));
  }
  bool verify(const Byte *msg, size_t msgLen, const Byte *sig, size_t sigLen) const {
    return verifier_->VerifyMessage((const CryptoPP::byte *)msg, msgLen, (const CryptoPP::byte *)sig, sigLen);
  }

  uint32_t signatureLength() const { return verifier_->SignatureLength(); }

 private:
  std::unique_ptr<RSASS<PKCS1v15, CryptoPP::SHA256>::Verifier> verifier_;
};

RSAVerifier::RSAVerifier(const std::string &str_pub_key, KeyFormat fmt) : key_str_{str_pub_key} {
  CryptoPP::RSA::PublicKey public_key;
  if (fmt == KeyFormat::PemFormat) {
    StringSource s(str_pub_key, true);
    PEM_Load(s, public_key);
  } else {
    StringSource s(str_pub_key, true, new HexDecoder());
    public_key.Load(s);
  }
  impl_.reset(new RSAVerifier::Impl(public_key));
}

bool RSAVerifier::verifyBuffer(const Byte *msg, size_t msgLen, const Byte *sig, size_t sigLen) const {
  return impl_->verify(msg, msgLen, sig, sigLen);
}

uint32_t RSAVerifier::signatureLength() const { return impl_->signatureLength(); }

RSAVerifier::~RSAVerifier() = default;

}  // namespace concord::crypto::cryptopp
