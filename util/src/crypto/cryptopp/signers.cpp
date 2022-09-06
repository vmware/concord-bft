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
using concord::Byte;

class ECDSASigner::Impl {
  std::unique_ptr<ECDSA<ECP, CryptoPP::SHA256>::Signer> signer_;
  AutoSeededRandomPool prng_;

 public:
  Impl(ECDSA<ECP, CryptoPP::SHA256>::PrivateKey& privateKey) {
    signer_ = std::make_unique<ECDSA<ECP, CryptoPP::SHA256>::Signer>(std::move(privateKey));
  }

  size_t sign(const Byte* dataIn, size_t dataLen, Byte* sigOutBuffer) {
    return signer_->SignMessage(prng_, reinterpret_cast<const byte*>(dataIn), dataLen, (CryptoPP::byte*)sigOutBuffer);
  }

  size_t signatureLength() const { return signer_->SignatureLength(); }
};

std::string ECDSASigner::sign(const std::string& data) {
  size_t siglen = impl_->signatureLength();
  std::string signature(siglen, 0x00);
  siglen = signBuffer(
      reinterpret_cast<const Byte* const>(data.data()), data.size(), reinterpret_cast<Byte*>(signature.data()));
  signature.resize(siglen);
  return signature;
}

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

size_t ECDSASigner::signatureLength() const { return impl_->signatureLength(); }
size_t ECDSASigner::signBuffer(const Byte* dataIn, size_t dataLen, Byte* sigOutBuffer) {
  return impl_->sign(dataIn, dataLen, sigOutBuffer);
}

ECDSASigner::~ECDSASigner() = default;

class RSASigner::Impl {
 public:
  Impl(CryptoPP::RSA::PrivateKey& private_key) {
    signer_ = std::make_unique<RSASS<PKCS1v15, CryptoPP::SHA256>::Signer>(std::move(private_key));
  }

  uint32_t sign(const Byte* const dataIn, uint32_t dataLen, Byte* sigOutBuffer) {
    return signer_->SignMessage(prng_, (const CryptoPP::byte*)dataIn, dataLen, (CryptoPP::byte*)sigOutBuffer);
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

size_t RSASigner::signBuffer(const Byte* const dataIn, size_t dataLen, Byte* sigOutBuffer) {
  return impl_->sign(dataIn, dataLen, sigOutBuffer);
}

size_t RSASigner::signatureLength() const { return impl_->signatureLength(); }

RSASigner::~RSASigner() = default;

}  // namespace concord::crypto::cryptopp
