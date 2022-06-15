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
//
#pragma once
#include "EdDSA.h"
#include "openssl_crypto.hpp"

/**
 * @tparam PublicKeyType The type of the public key - expected to be a SerializableByteArray
 *
 */
template <typename PublicKeyType>
class EdDSAVerifier {
 public:
  using VerifierKeyType = PublicKeyType;
  EdDSAVerifier(const PublicKeyType &publicKey) : publicKey_(publicKey) {}

  bool verify(const std::string &message, const std::string &signature) const {
    return verify(message.data(), message.size(), signature.data(), signature.size());
  }

  bool verify(const char *msg, int msgLen, const char *sig, int sigLen) const {
    return verify(reinterpret_cast<const uint8_t *>(msg),
                  static_cast<size_t>(msgLen),
                  reinterpret_cast<const uint8_t *>(sig),
                  static_cast<size_t>(sigLen));
  }

  bool verify(const uint8_t *msg, size_t msgLen, const uint8_t *sig, size_t sigLen) const {
    ConcordAssertEQ(sigLen, EdDSASignatureByteSize);
    concord::util::openssl_utils::UniquePKEY pkey{
        EVP_PKEY_new_raw_public_key(NID_ED25519, nullptr, publicKey_.getBytes().data(), publicKey_.getBytes().size())};
    concord::util::openssl_utils::UniqueOpenSSLContext ctx{EVP_MD_CTX_new()};
    ConcordAssertEQ(EVP_DigestVerifyInit(ctx.get(), nullptr, nullptr, nullptr, pkey.get()),
                    concord::util::openssl_utils::OPENSSL_SUCCESS);
    return concord::util::openssl_utils::OPENSSL_SUCCESS == EVP_DigestVerify(ctx.get(), sig, sigLen, msg, msgLen);
  }
  const PublicKeyType &getPubKey() const { return publicKey_; }
  virtual ~EdDSAVerifier() = default;

 private:
  PublicKeyType publicKey_;
};