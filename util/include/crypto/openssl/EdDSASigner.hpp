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
#include "crypto/signer.hpp"
#include "crypto/crypto.hpp"

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
  explicit EdDSASigner(const PrivateKeyType &privateKey) : privateKey_(privateKey) {}

  size_t signBuffer(const concord::Byte *msg, size_t len, concord::Byte *signature) override {
    using concord::util::openssl_utils::UniquePKEY;
    UniquePKEY pkey(EVP_PKEY_new_raw_private_key(
        NID_ED25519, nullptr, privateKey_.getBytes().data(), privateKey_.getBytes().size()));
    ConcordAssertNE(pkey, nullptr);

    size_t signatureLength = concord::crypto::openssl::EdDSASignatureByteSize;
    using concord::util::openssl_utils::OPENSSL_SUCCESS;
    concord::util::openssl_utils::UniqueOpenSSLContext ctx{EVP_MD_CTX_new()};
    ConcordAssertEQ(EVP_DigestSignInit(ctx.get(), nullptr, nullptr, nullptr, pkey.get()), OPENSSL_SUCCESS);
    ConcordAssertEQ(EVP_DigestSign(ctx.get(),
                                   reinterpret_cast<unsigned char *>(signature),
                                   &signatureLength,
                                   reinterpret_cast<const unsigned char *>(msg),
                                   len),
                    OPENSSL_SUCCESS);
    return signatureLength;
  }

  size_t signatureLength() const override { return concord::crypto::openssl::EdDSASignatureByteSize; }

  std::string getPrivKey() const override { return privateKey_.toString(); }

  virtual ~EdDSASigner() = default;

 protected:
  const PrivateKeyType privateKey_;
};
}  // namespace concord::crypto::openssl
