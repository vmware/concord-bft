//
// Created by yflum on 26/04/2022.
//
#include <openssl/crypto.h>
#include <openssl/evp.h>
#include <boost/algorithm/hex.hpp>
#include "threshsign/ISecretKey.h"
#include "openssl_crypto.hpp"
#include "threshsign/eddsa/SSLEdDSAPrivateKey.h"

SSLEdDSAPrivateKey::SSLEdDSAPrivateKey(const EdDSAPrivateKeyBytes& bytes) : bytes_(bytes) {}
std::string SSLEdDSAPrivateKey::sign(const uint8_t* msg, size_t len) const {
  std::string signature(64, 0);
  size_t sigLen_ = 64;
  sign(msg, len, reinterpret_cast<uint8_t*>(signature.data()), sigLen_);
  ConcordAssertEQ(sigLen_, 64);
  return signature;
}

void SSLEdDSAPrivateKey::sign(const uint8_t* msg, size_t len, uint8_t* signature, size_t& signatureLength) const {
  EVP_PKEY* pkey =
      EVP_PKEY_new_raw_private_key(NID_ED25519, nullptr, (const unsigned char*)bytes_.data(), bytes_.size());
  EVP_MD_CTX* edCtx = EVP_MD_CTX_new();
  EVP_DigestSignInit(edCtx, nullptr, nullptr, nullptr, pkey);
  ConcordAssertEQ(EVP_DigestSign(edCtx, signature, &signatureLength, msg, len), 1);
}

std::string SSLEdDSAPrivateKey::sign(const std::string& message) const {
  return sign(reinterpret_cast<const uint8_t*>(message.data()), message.size());
}

std::string SSLEdDSAPrivateKey::toString() const {
  std::string ret;
  boost::algorithm::hex(bytes_.begin(), bytes_.end(), std::back_inserter(ret));
  ConcordAssertEQ(ret.size(), KeyByteSize * 2);
  return ret;
}
SSLEdDSAPrivateKey SSLEdDSAPrivateKey::fromHexString(const std::string& hexString) {
  std::string keyBytes = boost::algorithm::unhex(hexString);
  ConcordAssertEQ(keyBytes.size(), KeyByteSize);
  SSLEdDSAPrivateKey result;
  std::memcpy(result.bytes_.data(), keyBytes.data(), keyBytes.size());
  return result;
}
