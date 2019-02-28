// Concord
//
// Copyright (c) 2018 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

// TODO(GG): clean and review this file

#include <set>

#include "Crypto.hpp"

#define VERIFY(exp)                                                      \
  {                                                                      \
    if (!(exp)) {                                                        \
      std::ostringstream oss;                                            \
      oss << "Assertion failed: " << (char*)(__FILE__) << "("            \
          << (int)(__LINE__) << "): " << (char*)(__func__) << std::endl; \
      std::cerr << oss.str();                                            \
    }                                                                    \
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
const unsigned int rsaKeyLength = 2048;  // Key length in bits

int RSAKeysGenerator::getModulusBits() {
  return static_cast<int>(rsaKeyLength);
}

#define RSA_STANDARD OAEP<SHA256>
//#define RSA_STANDARD PKCS1v15
//#define RSA_STANDARD OAEP<SHA>

static RandomPool sGlobalRandGen;  // not thread-safe !!

void CryptographyWrapper::init(const char* randomSeed) {
  string s(randomSeed);
  if (s.length() < 16) s.resize(16, ' ');
  sGlobalRandGen.IncorporateEntropy((byte*)s.c_str(), s.length());

  VERIFY(DigestUtil::digestLength() == DIGEST_SIZE);

  // Initialize RELIC library for BLS threshold signatures
  BLS::Relic::Library::Get();
}

void CryptographyWrapper::init() {
  std::string seed = IntToString(time(NULL));
  CryptographyWrapper::init(seed.c_str());
}

void CryptographyWrapper::generateRandomBlock(char* output, size_t size) {
  sGlobalRandGen.GenerateBlock((byte*)output, size);
}

void convert(Integer in, string& out) {
  out.clear();
  HexEncoder encoder(new StringSink(out));
  in.DEREncode(encoder);
  encoder.MessageEnd();
}

void convert(string in, Integer& out) {
  StringSource strSrc(in, true, new HexDecoder);
  out = Integer(strSrc);
}

bool RSAKeysGenerator::generateKeys(string& outPublicKey,
                                    string& outPrivateKey) {
  outPrivateKey.clear();
  RSAES<RSA_STANDARD>::Decryptor priv(sGlobalRandGen, rsaKeyLength);
  HexEncoder privEncoder(new StringSink(outPrivateKey));
  priv.DEREncode(privEncoder);
  privEncoder.MessageEnd();

  outPublicKey.clear();
  RSAES<RSA_STANDARD>::Encryptor pub(priv);
  HexEncoder pubEncoder(new StringSink(outPublicKey));
  pub.DEREncode(pubEncoder);
  pubEncoder.MessageEnd();

  return true;
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

  dig.Update((byte*)input, inputLength);
  dig.Final(digest);
  const byte* h = digest;
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
  p->Update((byte*)data, len);
}

void DigestUtil::Context::writeDigest(char* outDigest) {
  VERIFY(internalState != NULL);
  DigestType* p = (DigestType*)internalState;
  SecByteBlock digest(digestLength());
  p->Final(digest);
  const byte* h = digest;
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

class RSASignerInternal {
 public:
  RSASignerInternal(BufferedTransformation& privateKey)
      : rand(sGlobalRandGen), priv(privateKey) {}

  size_t signatureLength() { return priv.SignatureLength(); }

  bool sign(const char* inBuffer,
            size_t lengthOfInBuffer,
            char* outBuffer,
            size_t lengthOfOutBuffer,
            size_t& lengthOfReturnedData) {
    const size_t sigLen = priv.SignatureLength();
    if (lengthOfOutBuffer < sigLen) return false;
    lengthOfReturnedData = priv.SignMessage(
        rand, (byte*)inBuffer, lengthOfInBuffer, (byte*)outBuffer);
    VERIFY(lengthOfReturnedData == sigLen);

    return true;
  }

 private:
  RandomPool& rand;
  RSASS<PKCS1v15, SHA256>::Signer priv;
};

class RSAVerifierInternal {
 public:
  RSAVerifierInternal(BufferedTransformation& publicKey) : pub(publicKey) {}

  size_t signatureLength() { return pub.SignatureLength(); }

  bool verify(const char* data,
              size_t lengthOfData,
              const char* signature,
              size_t lengthOfOSignature) {
    bool ok = pub.VerifyMessage(
        (byte*)data, lengthOfData, (byte*)signature, lengthOfOSignature);
    return ok;
  }

 private:
  RSASS<PKCS1v15, SHA256>::Verifier pub;  // TODO 77777
};

// RSASigner::RSASigner(const char* privteKey, const char* randomSeed)
//{
//}

RSASigner::RSASigner(const char* privateKey) {
  StringSource s(privateKey, true, new HexDecoder);
  d = new RSASignerInternal(s);
}

RSASigner::~RSASigner() {
  RSASignerInternal* p = (RSASignerInternal*)d;
  delete p;
}

size_t RSASigner::signatureLength() {
  RSASignerInternal* p = (RSASignerInternal*)d;
  return p->signatureLength();
}

bool RSASigner::sign(const char* inBuffer,
                     size_t lengthOfInBuffer,
                     char* outBuffer,
                     size_t lengthOfOutBuffer,
                     size_t& lengthOfReturnedData) {
  RSASignerInternal* p = (RSASignerInternal*)d;
  bool succ = p->sign(inBuffer,
                      lengthOfInBuffer,
                      outBuffer,
                      lengthOfOutBuffer,
                      lengthOfReturnedData);
  return succ;
}

// RSAVerifier::RSAVerifier(const char* publicKey, const char* randomSeed)
//{
//}

RSAVerifier::RSAVerifier(const char* publicKey) {
  StringSource s(publicKey, true, new HexDecoder);
  d = new RSAVerifierInternal(s);
}

RSAVerifier::~RSAVerifier() {
  RSAVerifierInternal* p = (RSAVerifierInternal*)d;
  delete p;
}

size_t RSAVerifier::signatureLength() {
  RSAVerifierInternal* p = (RSAVerifierInternal*)d;
  return p->signatureLength();
}

bool RSAVerifier::verify(const char* data,
                         size_t lengthOfData,
                         const char* signature,
                         size_t lengthOfOSignature) {
  RSAVerifierInternal* p = (RSAVerifierInternal*)d;
  return p->verify(data, lengthOfData, signature, lengthOfOSignature);
}

void SecretSharingOperations::splitBinaryString(
    uint16_t threshold,
    uint16_t nShares,
    string binaryData,
    const char* seed,
    string* outBinaryStringArray,
    uint16_t lenOutBinaryStringArray) {
  VERIFY(threshold <= nShares);
  VERIFY(threshold > 1);
  VERIFY(nShares > 1);
  VERIFY(nShares <= 1000);
  VERIFY(lenOutBinaryStringArray == nShares);

  CryptoPP::RandomPool rng;
  rng.IncorporateEntropy((byte*)seed, strlen(seed));

  CryptoPP::ChannelSwitch* channelSwitch = new CryptoPP::ChannelSwitch;
  CryptoPP::SecretSharing* secretSharing =
      new CryptoPP::SecretSharing(rng, threshold, nShares, channelSwitch);

  CryptoPP::StringSource source(binaryData, false, secretSharing);

  CryptoPP::vector_member_ptrs<CryptoPP::StringSink> strSinks(nShares);
  string channel;

  for (uint16_t i = 0; i < nShares; i++) {
    CryptoPP::StringSink* s = new CryptoPP::StringSink(outBinaryStringArray[i]);
    strSinks[i].reset(s);
    channel = CryptoPP::WordToString<CryptoPP::word32>(i);
    strSinks[i]->Put((const byte*)channel.data(), 4);
    channelSwitch->AddRoute(channel, *strSinks[i], CryptoPP::DEFAULT_CHANNEL);
  }

  source.PumpAll();
}

void SecretSharingOperations::recoverBinaryString(uint16_t threshold,
                                                  string inBinaryStringArray[],
                                                  string& outBinaryData) {
  VERIFY(threshold > 1);
  VERIFY(threshold <= 1000);

  CryptoPP::SecretRecovery recovery(threshold,
                                    new CryptoPP::StringSink(outBinaryData));

  CryptoPP::vector_member_ptrs<CryptoPP::StringSource> strSources(threshold);
  CryptoPP::SecByteBlock channel(4);

  uint16_t i;
  for (i = 0; i < threshold; i++) {
    strSources[i].reset(
        new CryptoPP::StringSource(inBinaryStringArray[i], false));
    strSources[i]->Pump(4);
    strSources[i]->Get(channel, 4);
    strSources[i]->Attach(new CryptoPP::ChannelSwitch(
        recovery, string((char*)channel.begin(), 4)));
  }

  while (strSources[0]->Pump(256))
    for (i = 1; i < threshold; i++) strSources[i]->Pump(256);

  for (i = 0; i < threshold; i++) strSources[i]->PumpAll();
}

void SecretSharingOperations::splitHexString(uint16_t threshold,
                                             uint16_t nShares,
                                             string hexData,
                                             const char* seed,
                                             string* outHexStringArray,
                                             uint16_t lenOutHexStringArray) {
  VERIFY(threshold <= nShares);
  VERIFY(threshold > 1);
  VERIFY(nShares > 1);
  VERIFY(nShares <= 1000);
  VERIFY(lenOutHexStringArray == nShares);

  string binData;
  CryptoPP::StringSource ss(
      hexData,
      true,
      new CryptoPP::HexDecoder(new CryptoPP::StringSink(binData)));

  for (uint16_t i = 0; i < nShares; i++) outHexStringArray[i].clear();

  splitBinaryString(
      threshold,
      nShares,
      binData,
      seed,
      outHexStringArray,
      nShares);  // we use outHexStringArray to store the binary strings (to
                 // avoid additional allocation of strings)

  for (uint16_t i = 0; i < nShares; i++) {
    string binString = outHexStringArray[i];
    outHexStringArray[i].clear();
    CryptoPP::StringSource ss((const byte*)binString.data(),
                              binString.length(),
                              true,
                              new CryptoPP::HexEncoder(new CryptoPP::StringSink(
                                  outHexStringArray[i])));
  }
}

void SecretSharingOperations::recoverHexString(uint16_t threshold,
                                               string inHexStringArray[],
                                               string& outHexData) {
  VERIFY(threshold > 1);
  VERIFY(threshold <= 1000);

  outHexData.clear();

  string* binaryStringArray = new string[threshold];

  for (uint16_t i = 0; i < threshold; i++) {
    binaryStringArray[i].clear();
    CryptoPP::StringSource ss(inHexStringArray[i],
                              true,
                              new CryptoPP::HexDecoder(new CryptoPP::StringSink(
                                  binaryStringArray[i])));
  }

  string binData;

  recoverBinaryString(threshold, binaryStringArray, binData);

  CryptoPP::StringSource ss(
      binData,
      true,
      new CryptoPP::HexEncoder(new CryptoPP::StringSink(outHexData)));

  delete[] binaryStringArray;
}

}  // namespace impl
}  // namespace bftEngine
