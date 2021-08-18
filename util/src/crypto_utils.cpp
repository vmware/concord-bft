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
#include <cryptopp/oids.h>
#include "Logger.hpp"
using namespace CryptoPP;
namespace concord::util::crypto {
class ECDSAVerifier::Impl {
  std::unique_ptr<ECDSA<ECP, SHA256>::Verifier> verifier_;

 public:
  Impl(ECDSA<ECP, SHA256>::PublicKey& publicKey) {
    verifier_ = std::make_unique<ECDSA<ECP, SHA256>::Verifier>(std::move(publicKey));
  }

  bool verify(const std::string& data_to_verify, const std::string& signature) {
    return verifier_->VerifyMessage((const CryptoPP::byte*)&data_to_verify[0],
                                    data_to_verify.size(),
                                    (const CryptoPP::byte*)&signature[0],
                                    signature.size());
  }

  uint32_t signatureLength() const { return verifier_->SignatureLength(); }
};
bool ECDSAVerifier::verify(const std::string& data, const std::string& sig) { return impl_->verify(data, sig); }
ECDSAVerifier::ECDSAVerifier(const std::string& str_pub_key, KeyFormat fmt) {
  ECDSA<ECP, SHA256>::PublicKey publicKey;
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

class ECDSASigner::Impl {
  std::unique_ptr<ECDSA<ECP, SHA256>::Signer> signer_;
  AutoSeededRandomPool prng_;

 public:
  Impl(ECDSA<ECP, SHA256>::PrivateKey& privateKey) {
    signer_ = std::make_unique<ECDSA<ECP, SHA256>::Signer>(std::move(privateKey));
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
ECDSASigner::ECDSASigner(const std::string& str_pub_key, KeyFormat fmt) {
  ECDSA<ECP, SHA256>::PrivateKey privateKey;
  if (fmt == KeyFormat::PemFormat) {
    StringSource s(str_pub_key, true);
    PEM_Load(s, privateKey);
  } else {
    StringSource s(str_pub_key, true, new HexDecoder());
    privateKey.Load(s);
  }
  impl_.reset(new Impl(privateKey));
}
uint32_t ECDSASigner::signatureLength() const { return impl_->signatureLength(); }
ECDSASigner::~ECDSASigner() = default;

class RSAVerifier::Impl {
 public:
  Impl(RSA::PublicKey& public_key) {
    verifier_ = std::make_unique<RSASS<PKCS1v15, SHA256>::Verifier>(std::move(public_key));
  }
  bool verify(const std::string& data_to_verify, const std::string& signature) {
    return verifier_->VerifyMessage((const CryptoPP::byte*)&data_to_verify[0],
                                    data_to_verify.size(),
                                    (const CryptoPP::byte*)&signature[0],
                                    signature.size());
  }

  uint32_t signatureLength() const { return verifier_->SignatureLength(); }

 private:
  std::unique_ptr<RSASS<PKCS1v15, SHA256>::Verifier> verifier_;
};

class RSASigner::Impl {
 public:
  Impl(RSA::PrivateKey& private_key) {
    signer_ = std::make_unique<RSASS<PKCS1v15, SHA256>::Signer>(std::move(private_key));
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
  std::unique_ptr<RSASS<PKCS1v15, SHA256>::Signer> signer_;
  AutoSeededRandomPool prng_;
};

RSASigner::RSASigner(const std::string& str_priv_key, KeyFormat fmt) {
  RSA::PrivateKey private_key;
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

RSAVerifier::RSAVerifier(const std::string& str_pub_key, KeyFormat fmt) {
  RSA::PublicKey public_key;
  if (fmt == KeyFormat::PemFormat) {
    StringSource s(str_pub_key, true);
    PEM_Load(s, public_key);
  } else {
    StringSource s(str_pub_key, true, new HexDecoder());
    public_key.Load(s);
  }
  impl_.reset(new RSAVerifier::Impl(public_key));
}
bool RSAVerifier::verify(const std::string& data, const std::string& sig) { return impl_->verify(data, sig); }
uint32_t RSAVerifier::signatureLength() const { return impl_->signatureLength(); }
RSAVerifier::~RSAVerifier() = default;

class Crypto::Impl {
 public:
  std::pair<std::string, std::string> RsaHexToPem(const std::pair<std::string, std::string>& key_pair) {
    std::pair<std::string, std::string> out;
    if (!key_pair.first.empty()) {
      StringSource priv_str(key_pair.first, true, new HexDecoder());
      RSA::PrivateKey priv;
      priv.Load(priv_str);
      StringSink priv_string_sink(out.first);
      PEM_Save(priv_string_sink, priv);
      priv_string_sink.MessageEnd();
    }

    if (!key_pair.second.empty()) {
      StringSource pub_str(key_pair.second, true, new HexDecoder());
      RSA::PublicKey pub;
      pub.Load(pub_str);
      StringSink pub_string_sink(out.second);
      PEM_Save(pub_string_sink, pub);
      pub_string_sink.MessageEnd();
    }
    return out;
  }

  std::pair<std::string, std::string> ECDSAHexToPem(const std::pair<std::string, std::string>& key_pair) {
    std::pair<std::string, std::string> out;
    if (!key_pair.first.empty()) {
      StringSource priv_str(key_pair.first, true, new HexDecoder());
      ECDSA<ECP, SHA256>::PrivateKey priv;
      priv.Load(priv_str);
      StringSink priv_string_sink(out.first);
      PEM_Save(priv_string_sink, priv);
    }
    if (!key_pair.second.empty()) {
      StringSource pub_str(key_pair.second, true, new HexDecoder());
      ECDSA<ECP, SHA256>::PublicKey pub;
      pub.Load(pub_str);
      StringSink pub_string_sink(out.second);
      PEM_Save(pub_string_sink, pub);
    }
    return out;
  }

  std::pair<std::string, std::string> generateRsaKeyPairs(uint32_t sig_length, KeyFormat fmt) {
    AutoSeededRandomPool rng;
    std::pair<std::string, std::string> keyPair;

    RSAES<OAEP<SHA256>>::Decryptor priv(rng, sig_length);
    RSAES<OAEP<SHA256>>::Encryptor pub(priv);
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
  std::pair<std::string, std::string> generateECDSAKeyPair(const KeyFormat fmt) {
    AutoSeededRandomPool prng;
    ECDSA<ECP, SHA256>::PrivateKey privateKey;
    ECDSA<ECP, SHA256>::PublicKey publicKey;
    privateKey.Initialize(prng, ASN1::secp256k1());
    privateKey.MakePublicKey(publicKey);
    std::pair<std::string, std::string> keyPair;
    HexEncoder privEncoder(new StringSink(keyPair.first));
    privateKey.Save(privEncoder);
    HexEncoder pubEncoder(new StringSink(keyPair.second));
    publicKey.Save(pubEncoder);
    if (fmt == KeyFormat::PemFormat) {
      keyPair = ECDSAHexToPem(keyPair);
    }
    return keyPair;
  }

  ~Impl() = default;
};

std::pair<std::string, std::string> Crypto::generateRsaKeyPair(const uint32_t sig_length, const KeyFormat fmt) const {
  return impl_->generateRsaKeyPairs(sig_length, fmt);
}

std::pair<std::string, std::string> Crypto::RsaHexToPem(const std::pair<std::string, std::string>& key_pair) const {
  return impl_->RsaHexToPem(key_pair);
}

std::pair<std::string, std::string> Crypto::generateECDSAKeyPair(KeyFormat fmt) const {
  return impl_->generateECDSAKeyPair(fmt);
}

std::pair<std::string, std::string> Crypto::ECDSAHexToPem(const std::pair<std::string, std::string>& key_pair) const {
  return impl_->ECDSAHexToPem(key_pair);
}

Crypto::Crypto() : impl_{new Impl()} {}

Crypto::~Crypto() = default;
}  // namespace concord::util::crypto
