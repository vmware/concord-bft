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
#include <openssl/crypto.h>
#include <openssl/evp.h>
#include "threshsign/eddsa/EdDSAMultisigFactory.h"
#include "openssl_crypto.hpp"
#include "threshsign/eddsa/SSLEdDSAPrivateKey.h"
#include "threshsign/eddsa/SSLEdDSAPublicKey.h"
#include "threshsign/eddsa/EdDSAMultisigSigner.h"
#include "threshsign/eddsa/EdDSAMultisigVerifier.h"
#include "threshsign/eddsa/OpenSSLWrappers.h"
#include <boost/algorithm/hex.hpp>
#include <iostream>

IThresholdVerifier *EdDSAMultisigFactory::newVerifier(ShareID reqSigners,
                                                      ShareID totalSigners,
                                                      const char *publicKeyStr,
                                                      const std::vector<std::string> &verifKeysStr) const {
  UNUSED(publicKeyStr);
  ConcordAssertEQ(verifKeysStr.size(), static_cast<std::vector<std::string>::size_type>(totalSigners + 1));
  std::vector<SSLEdDSAPublicKey> publicKeys;
  publicKeys.push_back(SSLEdDSAPublicKey(SSLEdDSAPublicKey::EdDSAPublicKeyBytes{}));
  std::transform(verifKeysStr.begin() + 1,
                 verifKeysStr.end(),
                 std::back_inserter(publicKeys),
                 [](const std::string &publicKeyHex) {
                   LOG_DEBUG(EDDSA_MULTISIG_LOG, KVLOG(publicKeyHex));
                   return SSLEdDSAPublicKey::fromHexString(publicKeyHex);
                 });
  auto newVerifier =
      new EdDSAMultisigVerifier(publicKeys, static_cast<size_t>(totalSigners), static_cast<size_t>(reqSigners));
  return newVerifier;
}

IThresholdSigner *EdDSAMultisigFactory::newSigner(ShareID id, const char *secretKeyStr) const {
  return new EdDSAMultisigSigner(
      SSLEdDSAPrivateKey::fromHexString(std::string(secretKeyStr, SSLEdDSAPrivateKey::KeyByteSize * 2)), (uint32_t)id);
}

std::tuple<std::vector<IThresholdSigner *>, IThresholdVerifier *> EdDSAMultisigFactory::newRandomSigners(
    NumSharesType reqSigners, NumSharesType numSigners) const {
  std::vector<SSLEdDSAPrivateKey> allPrivateKeys;
  std::vector<SSLEdDSAPublicKey> allPublicKeys;
  std::vector<IThresholdSigner *> signers;

  // One-based indices
  allPrivateKeys.push_back(SSLEdDSAPrivateKey{{0}});
  allPublicKeys.push_back(SSLEdDSAPublicKey{{0}});
  signers.push_back(new EdDSAMultisigSigner(allPrivateKeys[0], (uint32_t)0));

  ConcordAssertLE(reqSigners, numSigners);
  for (int i = 0; i < numSigners; i++) {
    auto [privateKey, publicKey] = newKeyPair();
    const auto &priv = *dynamic_cast<SSLEdDSAPrivateKey *>(privateKey.get());
    const auto &pub = *dynamic_cast<SSLEdDSAPublicKey *>(publicKey.get());
    allPrivateKeys.push_back(priv);
    allPublicKeys.push_back(pub);
    signers.push_back(new EdDSAMultisigSigner(allPrivateKeys[(size_t)i + 1], (uint32_t)i + 1));
  }

  EdDSAMultisigVerifier *verifier = new EdDSAMultisigVerifier(allPublicKeys, (size_t)numSigners, (size_t)reqSigners);
  return {signers, verifier};
}

std::pair<std::unique_ptr<IShareSecretKey>, std::unique_ptr<IShareVerificationKey>> EdDSAMultisigFactory::newKeyPair()
    const {
  UniquePKEY uniquePKEY;
  EVP_PKEY *pkey = nullptr;
  UniqueOpenSSLPKEYContext pctx{EVP_PKEY_CTX_new_id(EVP_PKEY_ED25519, NULL)};
  if (EVP_PKEY_keygen_init(pctx.get()) != OPENSSL_SUCCESS) {
    throw std::invalid_argument("EVP_PKEY_keygen_init failed");
  }

  if (EVP_PKEY_keygen(pctx.get(), &pkey) != OPENSSL_SUCCESS) {
    throw std::invalid_argument("EVP_PKEY_keygen failed");
  }
  uniquePKEY.reset(pkey);

  SSLEdDSAPrivateKey::EdDSAPrivateKeyBytes privateKey;
  SSLEdDSAPublicKey::EdDSAPublicKeyBytes publicKey;
  size_t len = SSLEdDSAPrivateKey::KeyByteSize;
  ConcordAssertEQ(EVP_PKEY_get_raw_private_key(pkey, privateKey.data(), &len), OPENSSL_SUCCESS);
  ConcordAssertEQ(len, SSLEdDSAPrivateKey::KeyByteSize);
  len = SSLEdDSAPublicKey::KeyByteSize;
  ConcordAssertEQ(EVP_PKEY_get_raw_public_key(pkey, (uint8_t *)publicKey.data(), &len), OPENSSL_SUCCESS);
  ConcordAssertEQ(len, SSLEdDSAPublicKey::KeyByteSize);
  return {std::make_unique<SSLEdDSAPrivateKey>(privateKey), std::make_unique<SSLEdDSAPublicKey>(publicKey)};
}
EdDSAMultisigFactory::EdDSAMultisigFactory() { /*EDDSA_MULTISIG_LOG.setLogLevel(log4cplus::DEBUG_LOG_LEVEL);*/
}
