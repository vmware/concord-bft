// Concord
//
// Copyright (c) 2018 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").  You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

// TODO(GG): clean and review this file

#include <set>
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wpedantic"
#include <cryptopp/dll.h>
#include <cryptopp/pem.h>
#pragma GCC diagnostic pop

#include "Crypto.hpp"

#define VERIFY(exp)                                                                                            \
  {                                                                                                            \
    if (!(exp)) {                                                                                              \
      std::ostringstream oss;                                                                                  \
      oss << "Assertion failed: " << (char*)(__FILE__) << "(" << (int)(__LINE__) << "): " << (char*)(__func__) \
          << std::endl;                                                                                        \
      std::cerr << oss.str();                                                                                  \
    }                                                                                                          \
  }

#include "DigestType.h"
#include <cryptopp/cryptlib.h>
#include "cryptopp/ida.h"

using namespace CryptoPP;
using namespace std;

#if defined MD5_DIGEST
#include <cryptopp/md5.h>
#define DigestType Weak1::MD5
#elif defined SHA256_DIGEST
#define DigestType SHA256
#elif defined SHA512_DIGEST
#define DigestType SHA512
#endif

// TODO(GG): TBD
#include <iostream>
#include <sstream>

namespace bftEngine {
namespace impl {

#define RSA_STANDARD OAEP<SHA256>
//#define RSA_STANDARD PKCS1v15
//#define RSA_STANDARD OAEP<SHA>

static RandomPool sGlobalRandGen;  // not thread-safe !!

void CryptographyWrapper::init(const char* randomSeed) {
  string s(randomSeed);
  if (s.length() < 16) s.resize(16, ' ');
  sGlobalRandGen.IncorporateEntropy((CryptoPP::byte*)s.c_str(), s.length());

  VERIFY(DigestUtil::digestLength() == DIGEST_SIZE);

  // Initialize RELIC library for BLS threshold signatures
  BLS::Relic::Library::Get();
}

void CryptographyWrapper::init() {
  std::string seed = IntToString(time(NULL));
  CryptographyWrapper::init(seed.c_str());
}

void convert(const Integer& in, string& out) {
  out.clear();
  HexEncoder encoder(new StringSink(out));
  in.DEREncode(encoder);
  encoder.MessageEnd();
}

void convert(const string& in, Integer& out) {
  StringSource strSrc(in, true, new HexDecoder);
  out = Integer(strSrc);
}

size_t DigestUtil::digestLength() { return DigestType::DIGESTSIZE; }

bool DigestUtil::compute(const char* input,
                         size_t inputLength,
                         char* outBufferForDigest,
                         size_t lengthOfBufferForDigest) {
  DigestType dig;

  size_t size = dig.DigestSize();

  if (lengthOfBufferForDigest < size) return false;

  SecByteBlock digest(size);

  dig.Update((CryptoPP::byte*)input, inputLength);
  dig.Final(digest);
  const CryptoPP::byte* h = digest;
  memcpy(outBufferForDigest, h, size);

  return true;
}

DigestUtil::Context::Context() {
  DigestType* p = new DigestType();
  internalState = p;
}

void DigestUtil::Context::update(const char* data, size_t len) {
  VERIFY(internalState != NULL);
  DigestType* p = (DigestType*)internalState;
  p->Update((CryptoPP::byte*)data, len);
}

void DigestUtil::Context::writeDigest(char* outDigest) {
  VERIFY(internalState != NULL);
  DigestType* p = (DigestType*)internalState;
  SecByteBlock digest(digestLength());
  p->Final(digest);
  const CryptoPP::byte* h = digest;
  memcpy(outDigest, h, digestLength());

  delete p;
  internalState = NULL;
}

DigestUtil::Context::~Context() {
  if (internalState != NULL) {
    DigestType* p = (DigestType*)internalState;
    delete p;
    internalState = NULL;
  }
}

class RSASigner::Impl {
 public:
  Impl(BufferedTransformation& privateKey) : rand(sGlobalRandGen), priv(privateKey){};
  Impl(RSA::PrivateKey& privateKey) : rand(sGlobalRandGen), priv(privateKey){};

  size_t signatureLength() const { return priv.SignatureLength(); }

  bool sign(const char* inBuffer,
            size_t lengthOfInBuffer,
            char* outBuffer,
            size_t lengthOfOutBuffer,
            size_t& lengthOfReturnedData) const {
    const size_t sigLen = priv.SignatureLength();
    if (lengthOfOutBuffer < sigLen) return false;
    lengthOfReturnedData =
        priv.SignMessage(rand, (CryptoPP::byte*)inBuffer, lengthOfInBuffer, (CryptoPP::byte*)outBuffer);
    VERIFY(lengthOfReturnedData == sigLen);

    return true;
  }

 private:
  RandomPool& rand;
  RSASS<PKCS1v15, SHA256>::Signer priv;
};

class RSAVerifier::Impl {
 public:
  Impl(BufferedTransformation& publicKey) : pub(publicKey){};
  Impl(RSA::PublicKey& publicKey) : pub(publicKey){};

  size_t signatureLength() const { return pub.SignatureLength(); }

  bool verify(const char* data, size_t lengthOfData, const char* signature, size_t lengthOfOSignature) const {
    bool ok = pub.VerifyMessage((CryptoPP::byte*)data, lengthOfData, (CryptoPP::byte*)signature, lengthOfOSignature);
    return ok;
  }

 private:
  RSASS<PKCS1v15, SHA256>::Verifier pub;  // TODO 77777
};

RSASigner::RSASigner(const char* privateKey) {
  StringSource s(privateKey, true, new HexDecoder);
  impl = std::make_unique<Impl>(s);
}

RSASigner::RSASigner(const string& private_key_pem) {
  StringSource ss(private_key_pem, true);
  RSA::PrivateKey priv_key;
  PEM_Load(ss, priv_key);
  impl = std::make_unique<Impl>(priv_key);
}

RSASigner::RSASigner(RSASigner&&) = default;

RSASigner::~RSASigner() = default;

RSASigner& RSASigner::operator=(RSASigner&&) = default;

size_t RSASigner::signatureLength() const { return impl->signatureLength(); }

bool RSASigner::sign(const char* inBuffer,
                     size_t lengthOfInBuffer,
                     char* outBuffer,
                     size_t lengthOfOutBuffer,
                     size_t& lengthOfReturnedData) const {
  return impl->sign(inBuffer, lengthOfInBuffer, outBuffer, lengthOfOutBuffer, lengthOfReturnedData);
}

RSAVerifier::RSAVerifier(const char* publicKey) {
  StringSource s(publicKey, true, new HexDecoder);
  impl = std::make_unique<Impl>(s);
}

RSAVerifier::RSAVerifier(const string& publicKeyPath) {
  FileSource fs(publicKeyPath.c_str(), true);
  RSA::PublicKey pub_key;
  PEM_Load(fs, pub_key);
  impl = std::make_unique<Impl>(pub_key);
}

RSAVerifier::RSAVerifier(RSAVerifier&&) = default;

RSAVerifier::~RSAVerifier() = default;

RSAVerifier& RSAVerifier::operator=(RSAVerifier&&) = default;

size_t RSAVerifier::signatureLength() const { return impl->signatureLength(); }

bool RSAVerifier::verify(const char* data,
                         size_t lengthOfData,
                         const char* signature,
                         size_t lengthOfOSignature) const {
  return impl->verify(data, lengthOfData, signature, lengthOfOSignature);
}

}  // namespace impl
}  // namespace bftEngine
