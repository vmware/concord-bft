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

#include "EdDSA.hpp"
#include "crypto.hpp"
#include "crypto/verifier.hpp"

namespace concord::crypto::openssl {

/**
 * @tparam PublicKeyType The type of the public key, expected to be a SerializableByteArray.
 */
template <typename PublicKeyType>
class EdDSAVerifier : public IVerifier {
 public:
  using VerifierKeyType = PublicKeyType;

  /**
   * @brief Construct a new EdDSA Verifier object
   *
   * @param publicKey
   */
  explicit EdDSAVerifier(const PublicKeyType &publicKey) : publicKey_(publicKey) {}

  bool verifyBuffer(const Byte *msg, size_t msgLen, const Byte *sig, size_t sigLen) const override {
    UniquePKEY pkey{
        EVP_PKEY_new_raw_public_key(NID_ED25519, nullptr, publicKey_.getBytes().data(), publicKey_.getBytes().size())};
    UniqueContext ctx{EVP_MD_CTX_new()};
    ConcordAssertEQ(EVP_DigestVerifyInit(ctx.get(), nullptr, nullptr, nullptr, pkey.get()), OPENSSL_SUCCESS);
    return (OPENSSL_SUCCESS == EVP_DigestVerify(ctx.get(), sig, sigLen, msg, msgLen));
  }

  uint32_t signatureLength() const override { return Ed25519SignatureByteSize; }

  std::string getPubKey() const override { return publicKey_.toString(); }

  virtual ~EdDSAVerifier() = default;

 public:
  const PublicKeyType publicKey_;
};
}  // namespace concord::crypto::openssl
