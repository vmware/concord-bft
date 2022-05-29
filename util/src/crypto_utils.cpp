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
#include <cryptopp/cryptlib.h>
#include <cryptopp/oids.h>
#pragma GCC diagnostic pop

#include <openssl/bio.h>
#include <openssl/ec.h>
#include <openssl/pem.h>
#include <openssl/x509.h>
#include <openssl/evp.h>
#include <regex>
#include "Logger.hpp"

using namespace CryptoPP;
namespace concord::util::crypto {
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

class Crypto::Impl {
 public:
  std::pair<std::string, std::string> RsaHexToPem(const std::pair<std::string, std::string>& key_pair) {
    std::pair<std::string, std::string> out;
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

  std::pair<std::string, std::string> ECDSAHexToPem(const std::pair<std::string, std::string>& key_pair) {
    std::pair<std::string, std::string> out;
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

  std::pair<std::string, std::string> generateRsaKeyPairs(uint32_t sig_length, KeyFormat fmt) {
    AutoSeededRandomPool rng;
    std::pair<std::string, std::string> keyPair;

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
  std::pair<std::string, std::string> generateECDSAKeyPair(const KeyFormat fmt, CurveType curve_types) {
    AutoSeededRandomPool prng;
    ECDSA<ECP, CryptoPP::SHA256>::PrivateKey privateKey;
    ECDSA<ECP, CryptoPP::SHA256>::PublicKey publicKey;

    privateKey.Initialize(prng, curve_types == CurveType::secp256k1 ? ASN1::secp256k1() : ASN1::secp384r1());
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

std::pair<std::string, std::string> Crypto::generateECDSAKeyPair(KeyFormat fmt, CurveType curve_types) const {
  return impl_->generateECDSAKeyPair(fmt, curve_types);
}

std::pair<std::string, std::string> Crypto::ECDSAHexToPem(const std::pair<std::string, std::string>& key_pair) const {
  return impl_->ECDSAHexToPem(key_pair);
}

KeyFormat Crypto::getFormat(const std::string& key) const {
  return key.find("BEGIN") != std::string::npos ? KeyFormat::PemFormat : KeyFormat::HexaDecimalStrippedFormat;
}

Crypto::Crypto() : impl_{new Impl()} {}

Crypto::~Crypto() = default;

bool CertificateUtils::verifyCertificate(X509* cert_to_verify,
                                         const std::string& cert_root_directory,
                                         uint32_t& remote_peer_id,
                                         std::string& conn_type,
                                         bool use_unified_certs) {
  // First get the source ID
  static constexpr size_t SIZE = 512;
  std::string subject(SIZE, 0);
  X509_NAME_oneline(X509_get_subject_name(cert_to_verify), subject.data(), SIZE);

  int peerIdPrefixLength = 3;
  std::regex r("OU=\\d*", std::regex_constants::icase);
  std::smatch sm;
  regex_search(subject, sm, r);

  LOG_DEBUG(GL, "Subject from certificate " << subject.data());
  if (sm.length() <= peerIdPrefixLength) {
    LOG_ERROR(GL, "OU not found or empty: " << subject);
    return false;
  }

  auto remPeer = sm.str().substr(peerIdPrefixLength, sm.str().length() - peerIdPrefixLength);
  if (0 == remPeer.length()) {
    LOG_ERROR(GL, "OU empty " << subject);
    return false;
  }

  uint32_t remotePeerId;
  try {
    remotePeerId = stoul(remPeer, nullptr);
  } catch (const std::invalid_argument& ia) {
    LOG_ERROR(GL, "cannot convert OU, " << subject << ", " << ia.what());
    return false;
  } catch (const std::out_of_range& e) {
    LOG_ERROR(GL, "cannot convert OU, " << subject << ", " << e.what());
    return false;
  }
  remote_peer_id = remotePeerId;
  LOG_INFO(GL, "Peer: " << remote_peer_id);
  std::string CN;
  CN.resize(SIZE);
  X509_NAME_get_text_by_NID(X509_get_subject_name(cert_to_verify), NID_commonName, CN.data(), SIZE);

  LOG_INFO(GL, "Field CN: " << CN.data());
  std::string cert_type = "server";
  if (CN.find("cli") != std::string::npos) cert_type = "client";
  conn_type = cert_type;

  // Get the local stored certificate for this peer
  std::string local_cert_path =
      (use_unified_certs)
          ? cert_root_directory + "/" + std::to_string(remotePeerId) + "/" + "node.cert"
          : cert_root_directory + "/" + std::to_string(remotePeerId) + "/" + cert_type + "/" + cert_type + ".cert";
  auto deleter = [](FILE* fp) {
    if (fp) fclose(fp);
  };
  std::unique_ptr<FILE, decltype(deleter)> fp(fopen(local_cert_path.c_str(), "r"), deleter);
  if (!fp) {
    LOG_ERROR(GL, "Certificate file not found, path: " << local_cert_path);
    return false;
  }

  X509* localCert = PEM_read_X509(fp.get(), NULL, NULL, NULL);
  if (!localCert) {
    LOG_ERROR(GL, "Cannot parse certificate, path: " << local_cert_path);
    return false;
  }

  // this is actual comparison, compares hash of 2 certs
  bool res = (X509_cmp(cert_to_verify, localCert) == 0);
  X509_free(localCert);
  return res;
}
std::string CertificateUtils::generateSelfSignedCert(const std::string& origin_cert_path,
                                                     const std::string& public_key,
                                                     const std::string& signing_key) {
  auto deleter = [](FILE* fp) {
    if (fp) fclose(fp);
  };
  std::unique_ptr<FILE, decltype(deleter)> fp(fopen(origin_cert_path.c_str(), "r"), deleter);
  if (!fp) {
    LOG_ERROR(GL, "Certificate file not found, path: " << origin_cert_path);
    return std::string();
  }

  X509* cert = PEM_read_X509(fp.get(), NULL, NULL, NULL);
  if (!cert) {
    LOG_ERROR(GL, "Cannot parse certificate, path: " << origin_cert_path);
    return std::string();
  }

  EVP_PKEY* priv_key = EVP_PKEY_new();
  BIO* priv_bio = BIO_new(BIO_s_mem());
  int priv_bio_write_ret = BIO_write(priv_bio, static_cast<const char*>(signing_key.c_str()), signing_key.size());
  if (priv_bio_write_ret <= 0) {
    EVP_PKEY_free(priv_key);
    BIO_free(priv_bio);
    X509_free(cert);
    LOG_ERROR(GL, "Unable to create private key object");
    return std::string();
  }
  if (!PEM_read_bio_PrivateKey(priv_bio, &priv_key, NULL, NULL)) {
    EVP_PKEY_free(priv_key);
    BIO_free(priv_bio);
    X509_free(cert);
    LOG_ERROR(GL, "Unable to create private key object");
    return std::string();
  }
  EVP_PKEY* pub_key = EVP_PKEY_new();
  BIO* pub_bio = BIO_new(BIO_s_mem());
  int pub_bio_write_ret = BIO_write(pub_bio, static_cast<const char*>(public_key.c_str()), public_key.size());
  if (pub_bio_write_ret <= 0) {
    EVP_PKEY_free(priv_key);
    EVP_PKEY_free(pub_key);
    BIO_free(priv_bio);
    BIO_free(pub_bio);
    X509_free(cert);
    LOG_ERROR(GL, "Unable to create public key object");
    return std::string();
  }
  if (!PEM_read_bio_PUBKEY(pub_bio, &pub_key, NULL, NULL)) {
    EVP_PKEY_free(priv_key);
    EVP_PKEY_free(pub_key);
    BIO_free(priv_bio);
    BIO_free(pub_bio);
    X509_free(cert);
    LOG_ERROR(GL, "Unable to create public key object");
    return std::string();
  }

  X509_set_pubkey(cert, pub_key);
  X509_sign(cert, priv_key, EVP_sha256());

  BIO* outbio = BIO_new(BIO_s_mem());
  if (!PEM_write_bio_X509(outbio, cert)) {
    BIO_free(outbio);
    EVP_PKEY_free(priv_key);
    EVP_PKEY_free(pub_key);
    BIO_free(priv_bio);
    BIO_free(pub_bio);
    X509_free(cert);
    LOG_ERROR(GL, "Unable to create certificate object");
    return std::string();
  }
  std::string certStr;
  int certLen = BIO_pending(outbio);
  certStr.resize(certLen);
  BIO_read(outbio, (void*)&(certStr.front()), certLen);
  // free all pointers
  BIO_free(outbio);
  EVP_PKEY_free(priv_key);
  BIO_free(priv_bio);
  EVP_PKEY_free(pub_key);
  BIO_free(pub_bio);
  X509_free(cert);
  return certStr;
}
bool CertificateUtils::verifyCertificate(X509* cert, const std::string& public_key) {
  EVP_PKEY* pub_key = EVP_PKEY_new();
  BIO* pub_bio = BIO_new(BIO_s_mem());
  int pub_bio_write_ret = BIO_write(pub_bio, static_cast<const char*>(public_key.c_str()), public_key.size());
  if (pub_bio_write_ret <= 0) {
    EVP_PKEY_free(pub_key);
    BIO_free(pub_bio);
    return false;
  }
  if (!PEM_read_bio_PUBKEY(pub_bio, &pub_key, NULL, NULL)) {
    EVP_PKEY_free(pub_key);
    BIO_free(pub_bio);
    return false;
  }
  int r = X509_verify(cert, pub_key);
  EVP_PKEY_free(pub_key);
  BIO_free(pub_bio);
  return (bool)r;
}
}  // namespace concord::util::crypto
