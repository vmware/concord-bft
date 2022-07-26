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
#include "openssl_crypto.hpp"
#include "crypto_utils.hpp"
#include "crypto/interface/signer.hpp"

namespace concord::crypto::openssl {

/**
 * @tparam PrivateKeyType The type of the private key, expected to be a SerializableByteArray.
 *
 */
template <typename PrivateKeyType>
class EdDSASigner : public ISigner {
 public:
  using SignerKeyType = PrivateKeyType;

  /**
   * @brief Construct a new EdDSA Signer object.
   *
   * @param privateKey
   */
  explicit EdDSASigner(const PrivateKeyType &privateKey) : privateKey_(privateKey) {
    pkey_.reset(EVP_PKEY_new_raw_private_key(
        NID_ED25519, nullptr, privateKey_.getBytes().data(), privateKey_.getBytes().size()));
    ConcordAssertNE(pkey_, nullptr);
  }

  std::string sign(const uint8_t *msg, size_t len) const {
    std::string signature(EdDSASignatureByteSize, 0);
    size_t sigLen_ = EdDSASignatureByteSize;
    sign(msg, len, reinterpret_cast<uint8_t *>(signature.data()), sigLen_);
    ConcordAssertEQ(sigLen_, EdDSASignatureByteSize);
    return signature;
  }

  void sign(const uint8_t *msg, size_t len, uint8_t *signature, size_t &signatureLength) const {
    using concord::util::openssl_utils::OPENSSL_SUCCESS;
    concord::util::openssl_utils::UniqueOpenSSLContext ctx{EVP_MD_CTX_new()};
    ConcordAssertEQ(EVP_DigestSignInit(ctx.get(), nullptr, nullptr, nullptr, pkey_.get()), OPENSSL_SUCCESS);
    ConcordAssertEQ(EVP_DigestSign(ctx.get(), signature, &signatureLength, msg, len), OPENSSL_SUCCESS);
  }

  std::string sign(const std::string &message) override {
    return sign(reinterpret_cast<const uint8_t *>(message.data()), message.size());
  }

  uint32_t signatureLength() const override { return EdDSASignatureByteSize; }

  std::string getPrivKey() const override { return privateKey_.toString(); }

  virtual ~EdDSASigner() = default;

 private:
  concord::util::openssl_utils::UniquePKEY pkey_;

 protected:
  const PrivateKeyType privateKey_;
};
}  // namespace concord::crypto::openssl
