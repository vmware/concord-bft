// Concord
//
// Copyright (c) 2018-2022 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the
// LICENSE file.
#include <boost/algorithm/hex.hpp>
#include "threshsign/ISecretKey.h"
#include "threshsign/eddsa/SSLEdDSAPrivateKey.h"
#include "threshsign/eddsa/OpenSSLWrappers.h"
#include "assertUtils.hpp"

SSLEdDSAPrivateKey::SSLEdDSAPrivateKey(const EdDSAPrivateKeyBytes& bytes) : bytes_(bytes) {}
std::string SSLEdDSAPrivateKey::sign(const uint8_t* msg, size_t len) const {
  std::string signature(SignatureByteSize, 0);
  size_t sigLen_ = SignatureByteSize;
  sign(msg, len, reinterpret_cast<uint8_t*>(signature.data()), sigLen_);
  ConcordAssertEQ(sigLen_, SignatureByteSize);
  return signature;
}

void SSLEdDSAPrivateKey::sign(const uint8_t* msg, size_t len, uint8_t* signature, size_t& signatureLength) const {
  UniquePKEY pkey{EVP_PKEY_new_raw_private_key(NID_ED25519, nullptr, bytes_.data(), bytes_.size())};
  UniqueOpenSSLContext ctx{EVP_MD_CTX_new()};
  ConcordAssertEQ(EVP_DigestSignInit(ctx.get(), nullptr, nullptr, nullptr, pkey.get()), OPENSSL_SUCCESS);
  ConcordAssertEQ(EVP_DigestSign(ctx.get(), signature, &signatureLength, msg, len), OPENSSL_SUCCESS);
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
