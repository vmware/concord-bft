//
// Created by yflum on 26/04/2022.
//
#include <openssl/crypto.h>
#include <openssl/evp.h>
#include <boost/algorithm/hex.hpp>
#include "threshsign/IPublicKey.h"
#include "openssl_crypto.hpp"
#include "threshsign/eddsa/SSLEdDSAPublicKey.h"

SSLEdDSAPublicKey::SSLEdDSAPublicKey(const EdDSAPublicKeyBytes& bytes) : bytes_(bytes) {}

std::string SSLEdDSAPublicKey::serialize() const { return std::string(bytes_.begin(), bytes_.end()); }

bool SSLEdDSAPublicKey::verify(const uint8_t* message,
                               const size_t messageLen,
                               const uint8_t* signature,
                               const size_t signatureLen) const {
  EVP_PKEY* pkey =
      EVP_PKEY_new_raw_public_key(NID_ED25519, nullptr, (const unsigned char*)bytes_.data(), bytes_.size());
  EVP_MD_CTX* edCtx = EVP_MD_CTX_new();
  ConcordAssertEQ(EVP_DigestVerifyInit(edCtx, nullptr, nullptr, nullptr, pkey), 1);

  if (1 != EVP_DigestVerify(edCtx, signature, signatureLen, message, messageLen)) {
    return false;
  }
  return true;
}

bool SSLEdDSAPublicKey::verify(const std::string& message, const std::string& signature) const {
  return verify(reinterpret_cast<const uint8_t*>(message.data()),
                message.size(),
                reinterpret_cast<const uint8_t*>(signature.data()),
                signature.size());
}
std::string SSLEdDSAPublicKey::toString() const {
  std::string ret;
  boost::algorithm::hex(bytes_.begin(), bytes_.end(), std::back_inserter(ret));
  ConcordAssertEQ(ret.size(), KeyByteSize * 2);
  return ret;
}
SSLEdDSAPublicKey SSLEdDSAPublicKey::fromHexString(const std::string& hexString) {
  std::string keyBytes = boost::algorithm::unhex(hexString);
  ConcordAssertEQ(keyBytes.size(), KeyByteSize);
  SSLEdDSAPublicKey result;
  std::memcpy(result.bytes_.data(), keyBytes.data(), keyBytes.size());
  return result;
}
