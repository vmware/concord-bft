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
 *
 * @tparam PrivateKeyType The type of the private key, expected to be a SerializableByteArray
 *
 */
template <typename PrivateKeyType>
class EdDSASigner {
 public:
  using SignerKeyType = PrivateKeyType;
  EdDSASigner(const PrivateKeyType &privateKey) : privateKey_(privateKey) {}

  std::string sign(const uint8_t *msg, size_t len) const {
    std::string signature(EdDSASignatureByteSize, 0);
    size_t sigLen_ = EdDSASignatureByteSize;
    sign(msg, len, reinterpret_cast<uint8_t *>(signature.data()), sigLen_);
    ConcordAssertEQ(sigLen_, EdDSASignatureByteSize);
    return signature;
  }

  void sign(const uint8_t *msg, size_t len, uint8_t *signature, size_t &signatureLength) const {
    concord::util::openssl_utils::UniquePKEY pkey{EVP_PKEY_new_raw_private_key(
        NID_ED25519, nullptr, privateKey_.getBytes().data(), privateKey_.getBytes().size())};
    concord::util::openssl_utils::UniqueOpenSSLContext ctx{EVP_MD_CTX_new()};
    ConcordAssertEQ(EVP_DigestSignInit(ctx.get(), nullptr, nullptr, nullptr, pkey.get()),
                    concord::util::openssl_utils::OPENSSL_SUCCESS);
    ConcordAssertEQ(EVP_DigestSign(ctx.get(), signature, &signatureLength, msg, len),
                    concord::util::openssl_utils::OPENSSL_SUCCESS);
  }

  std::string sign(const std::string &message) const {
    return sign(reinterpret_cast<const uint8_t *>(message.data()), message.size());
  }

  const PrivateKeyType &getPrivKey() const { return privateKey_; }
  virtual ~EdDSASigner() = default;

 private:
  PrivateKeyType privateKey_;
};