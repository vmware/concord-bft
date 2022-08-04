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

#include "crypto/cryptopp/signers.hpp"

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

class ECDSASigner::Impl {
  std::unique_ptr<ECDSA<ECP, CryptoPP::SHA256>::Signer> signer_;
  AutoSeededRandomPool prng_;

 public:
  Impl(ECDSA<ECP, CryptoPP::SHA256>::PrivateKey& privateKey) {
    signer_ = std::make_unique<ECDSA<ECP, CryptoPP::SHA256>::Signer>(std::move(privateKey));
  }

  std::string sign(const std::string& data_to_sign) {
    size_t siglen = signer_->MaxSignatureLength();
    std::string signature(siglen, 0x00);
    siglen = signer_->SignMessage(
        prng_, (const CryptoPP::byte*)&data_to_sign[0], data_to_sign.size(), (CryptoPP::byte*)&signature[0]);
    signature.resize(siglen);
    return signature;
  }
  uint32_t signatureLength() const { return signer_->SignatureLength(); }
};

std::string ECDSASigner::sign(const std::string& data) { return impl_->sign(data); }

ECDSASigner::ECDSASigner(const std::string& str_priv_key, KeyFormat fmt) : key_str_{str_priv_key} {
  ECDSA<ECP, CryptoPP::SHA256>::PrivateKey privateKey;
  if (fmt == KeyFormat::PemFormat) {
    StringSource s(str_priv_key, true);
    PEM_Load(s, privateKey);
  } else {
    StringSource s(str_priv_key, true, new HexDecoder());
    privateKey.Load(s);
  }
  impl_.reset(new Impl(privateKey));
}

uint32_t ECDSASigner::signatureLength() const { return impl_->signatureLength(); }

ECDSASigner::~ECDSASigner() = default;

class RSASigner::Impl {
 public:
  Impl(CryptoPP::RSA::PrivateKey& private_key) {
    signer_ = std::make_unique<RSASS<PKCS1v15, CryptoPP::SHA256>::Signer>(std::move(private_key));
  }
  std::string sign(const std::string& data_to_sign) {
    size_t siglen = signer_->MaxSignatureLength();
    std::string signature(siglen, 0x00);
    siglen = signer_->SignMessage(
        prng_, (const CryptoPP::byte*)&data_to_sign[0], data_to_sign.size(), (CryptoPP::byte*)&signature[0]);
    signature.resize(siglen);
    return signature;
  }
  uint32_t signatureLength() const { return signer_->SignatureLength(); }

 private:
  std::unique_ptr<RSASS<PKCS1v15, CryptoPP::SHA256>::Signer> signer_;
  AutoSeededRandomPool prng_;
};

RSASigner::RSASigner(const std::string& str_priv_key, KeyFormat fmt) : key_str_{str_priv_key} {
  CryptoPP::RSA::PrivateKey private_key;
  if (fmt == KeyFormat::PemFormat) {
    StringSource s(str_priv_key, true);
    PEM_Load(s, private_key);
  } else {
    StringSource s(str_priv_key, true, new HexDecoder());
    private_key.Load(s);
  }
  impl_.reset(new RSASigner::Impl(private_key));
}

std::string RSASigner::sign(const std::string& data) { return impl_->sign(data); }

uint32_t RSASigner::signatureLength() const { return impl_->signatureLength(); }

RSASigner::~RSASigner() = default;

}  // namespace concord::crypto::cryptopp
