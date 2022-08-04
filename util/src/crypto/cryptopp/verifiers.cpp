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

class ECDSAVerifier::Impl {
  std::unique_ptr<ECDSA<ECP, CryptoPP::SHA256>::Verifier> verifier_;

 public:
  Impl(ECDSA<ECP, CryptoPP::SHA256>::PublicKey& publicKey) {
    verifier_ = std::make_unique<ECDSA<ECP, CryptoPP::SHA256>::Verifier>(std::move(publicKey));
  }

  bool verify(const std::string& data_to_verify, const std::string& signature) const {
    return verifier_->VerifyMessage((const CryptoPP::byte*)&data_to_verify[0],
                                    data_to_verify.size(),
                                    (const CryptoPP::byte*)&signature[0],
                                    signature.size());
  }

  uint32_t signatureLength() const { return verifier_->SignatureLength(); }
};

bool ECDSAVerifier::verify(const std::string& data, const std::string& sig) const { return impl_->verify(data, sig); }

ECDSAVerifier::ECDSAVerifier(const std::string& str_pub_key, KeyFormat fmt) : key_str_{str_pub_key} {
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
  Impl(CryptoPP::RSA::PublicKey& public_key) {
    verifier_ = std::make_unique<RSASS<PKCS1v15, CryptoPP::SHA256>::Verifier>(std::move(public_key));
  }
  bool verify(const std::string& data_to_verify, const std::string& signature) const {
    return verifier_->VerifyMessage((const CryptoPP::byte*)&data_to_verify[0],
                                    data_to_verify.size(),
                                    (const CryptoPP::byte*)&signature[0],
                                    signature.size());
  }

  uint32_t signatureLength() const { return verifier_->SignatureLength(); }

 private:
  std::unique_ptr<RSASS<PKCS1v15, CryptoPP::SHA256>::Verifier> verifier_;
};

RSAVerifier::RSAVerifier(const std::string& str_pub_key, KeyFormat fmt) : key_str_{str_pub_key} {
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

bool RSAVerifier::verify(const std::string& data, const std::string& sig) const { return impl_->verify(data, sig); }

uint32_t RSAVerifier::signatureLength() const { return impl_->signatureLength(); }

RSAVerifier::~RSAVerifier() = default;

}  // namespace concord::crypto::cryptopp
