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
#include <iostream>
#include <boost/algorithm/hex.hpp>

#include <openssl/crypto.h>
#include <openssl/evp.h>

#include "crypto/openssl/crypto.hpp"
#include "crypto/threshsign/eddsa/EdDSAMultisigFactory.h"
#include "crypto/threshsign/eddsa/EdDSAThreshsignKeys.h"
#include "crypto/threshsign/eddsa/EdDSAMultisigSigner.h"
#include "crypto/threshsign/eddsa/EdDSAMultisigVerifier.h"

using concord::crypto::openssl::UniquePKEY;
using concord::crypto::openssl::UniquePKEYContext;
using concord::crypto::openssl::OPENSSL_SUCCESS;

IThresholdVerifier *EdDSAMultisigFactory::newVerifier(ShareID reqSigners,
                                                      ShareID totalSigners,
                                                      [[maybe_unused]] const char *publicKeyStr,
                                                      const std::vector<std::string> &verifKeysStr) const {
  using SingleVerifier = EdDSAMultisigVerifier::SingleVerifier;
  using PublicKey = SingleVerifier::VerifierKeyType;
  ConcordAssertEQ(verifKeysStr.size(), static_cast<std::vector<std::string>::size_type>(totalSigners));
  std::vector<SingleVerifier> verifiers;
  std::transform(
      verifKeysStr.begin(), verifKeysStr.end(), std::back_inserter(verifiers), [](const std::string &publicKeyHex) {
        LOG_DEBUG(EDDSA_MULTISIG_LOG, KVLOG(publicKeyHex));
        return EdDSAMultisigVerifier::SingleVerifier(fromHexString<PublicKey>(publicKeyHex));
      });
  auto newVerifier =
      new EdDSAMultisigVerifier(verifiers, static_cast<size_t>(totalSigners), static_cast<size_t>(reqSigners));
  return newVerifier;
}

IThresholdSigner *EdDSAMultisigFactory::newSigner(ShareID id, const char *secretKeyStr) const {
  auto privateKey =
      fromHexString<EdDSAThreshsignPrivateKey>(std::string(secretKeyStr, EdDSAThreshsignPrivateKey::ByteSize * 2));
  return new EdDSAMultisigSigner(privateKey, (uint32_t)id);
}

IThresholdFactory::SignersVerifierTuple EdDSAMultisigFactory::newRandomSigners(NumSharesType reqSigners,
                                                                               NumSharesType numSigners) const {
  std::vector<EdDSAThreshsignPrivateKey> allPrivateKeys;
  std::vector<EdDSAMultisigVerifier::SingleVerifier> allVerifiers;
  std::vector<std::unique_ptr<IThresholdSigner>> signers(static_cast<size_t>(numSigners));

  ConcordAssertLE(reqSigners, numSigners);
  for (size_t i = 0; i < static_cast<size_t>(numSigners); i++) {
    auto [privateKey, publicKey] = newKeyPair();
    const auto &priv = *dynamic_cast<EdDSAThreshsignPrivateKey *>(privateKey.get());
    const auto &pub = *dynamic_cast<EdDSAThreshsignPublicKey *>(publicKey.get());
    allPrivateKeys.push_back(priv);
    allVerifiers.emplace_back(pub);
    signers[i].reset(new EdDSAMultisigSigner(allPrivateKeys[static_cast<size_t>(i)], static_cast<uint32_t>(i)));
  }

  auto verifier = std::make_unique<EdDSAMultisigVerifier>(allVerifiers, (size_t)numSigners, (size_t)reqSigners);
  return {std::move(signers), std::move(verifier)};
}

std::pair<std::unique_ptr<IShareSecretKey>, std::unique_ptr<IShareVerificationKey>> EdDSAMultisigFactory::newKeyPair()
    const {
  UniquePKEY uniquePKEY;
  EVP_PKEY *pkey = nullptr;
  UniquePKEYContext pctx{EVP_PKEY_CTX_new_id(EVP_PKEY_ED25519, NULL)};
  if (EVP_PKEY_keygen_init(pctx.get()) != OPENSSL_SUCCESS) {
    throw std::invalid_argument("EVP_PKEY_keygen_init failed");
  }

  if (EVP_PKEY_keygen(pctx.get(), &pkey) != OPENSSL_SUCCESS) {
    throw std::invalid_argument("EVP_PKEY_keygen failed");
  }
  uniquePKEY.reset(pkey);

  EdDSAThreshsignPrivateKey::ByteArray privateKey;
  EdDSAThreshsignPublicKey::ByteArray publicKey;
  size_t len = EdDSAThreshsignPrivateKey::ByteSize;
  ConcordAssertEQ(EVP_PKEY_get_raw_private_key(pkey, privateKey.data(), &len), OPENSSL_SUCCESS);
  ConcordAssertEQ(len, EdDSAThreshsignPrivateKey::ByteSize);
  len = EdDSAThreshsignPublicKey::ByteSize;
  ConcordAssertEQ(EVP_PKEY_get_raw_public_key(pkey, reinterpret_cast<uint8_t *>(publicKey.data()), &len),
                  OPENSSL_SUCCESS);
  ConcordAssertEQ(len, EdDSAThreshsignPublicKey::ByteSize);
  return {std::make_unique<EdDSAThreshsignPrivateKey>(privateKey),
          std::make_unique<EdDSAThreshsignPublicKey>(publicKey)};
}
EdDSAMultisigFactory::EdDSAMultisigFactory() {}
