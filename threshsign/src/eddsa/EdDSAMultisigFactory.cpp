//
// Created by yflum on 26/04/2022.
//

#include <openssl/crypto.h>
#include <openssl/evp.h>
#include "threshsign/eddsa/EdDSAMultisigFactory.h"
#include "openssl_crypto.hpp"
#include "threshsign/eddsa/SSLEdDSAPrivateKey.h"
#include "threshsign/eddsa/SSLEdDSAPublicKey.h"
#include "threshsign/eddsa/EdDSAMultisigSigner.h"
#include "threshsign/eddsa/EdDSAMultisigVerifier.h"
#include "../Utils.h"
#include <boost/algorithm/hex.hpp>
#include <iostream>

IThresholdVerifier *EdDSAMultisigFactory::newVerifier(ShareID reqSigners,
                                                      ShareID totalSigners,
                                                      const char *publicKeyStr,
                                                      const std::vector<std::string> &verifKeysStr) const {
  UNUSED(reqSigners);
  UNUSED(publicKeyStr);
  ConcordAssert(verifKeysStr.size() - 1 == (size_t)totalSigners);
  std::vector<SSLEdDSAPublicKey> publicKeys;
  publicKeys.push_back(SSLEdDSAPublicKey(SSLEdDSAPublicKey::EdDSAPublicKeyBytes{}));
  std::transform(verifKeysStr.begin() + 1,
                 verifKeysStr.end(),
                 std::back_inserter(publicKeys),
                 [](const std::string &publicKeyHex) {
                   std::cout << "new verifier using public key: " << publicKeyHex << std::endl;
                   return SSLEdDSAPublicKey::fromHexString(publicKeyHex);
                 });
  auto newVerifier = new EdDSAMultisigVerifier(publicKeys, (size_t)totalSigners, (size_t)reqSigners);
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

  ConcordAssert(reqSigners == numSigners);
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
  EVP_PKEY *pkey = NULL;
  EVP_PKEY_CTX *pctx = EVP_PKEY_CTX_new_id(EVP_PKEY_ED25519, NULL);
  if (EVP_PKEY_keygen_init(pctx) <= 0) {
    throw std::invalid_argument("EVP_PKEY_keygen_init failed");
  }

  if (EVP_PKEY_keygen(pctx, &pkey) <= 0) {
    throw std::invalid_argument("EVP_PKEY_keygen failed");
  }

  std::array<uint8_t, 32> private_key;
  std::array<uint8_t, 32> public_key;
  size_t len = 256;
  EVP_PKEY_get_raw_private_key(pkey, private_key.data(), &len);
  ConcordAssertEQ(len, 32u);
  auto priv = std::make_unique<SSLEdDSAPrivateKey>(private_key);
  len = 256;
  EVP_PKEY_get_raw_public_key(pkey, (uint8_t *)public_key.data(), &len);
  ConcordAssertEQ(len, 32u);
  auto pub = std::make_unique<SSLEdDSAPublicKey>(public_key);
  EVP_PKEY_CTX_free(pctx);
  return {std::move(priv), std::move(pub)};
}
