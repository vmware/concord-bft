// UTT Client API
//
// Copyright (c) 2020-2022 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the sub-component's license, as noted in the LICENSE
// file.

#include <openssl/x509.h>
#include <openssl/pem.h>
#include <openssl/rsa.h>
#include <openssl/evp.h>
#include <openssl/sha.h>
#include <xutils/Utils.h>
#include <cstdio>
#include "utils/crypto.hpp"

namespace utt::client::utils::crypto {
std::string getCertificatePublicKey(const std::string& cert_str) {
  BIO* cert_bio = BIO_new(BIO_s_mem());
  BIO_puts(cert_bio, cert_str.c_str());
  X509* cert = PEM_read_bio_X509(cert_bio, nullptr, nullptr, nullptr);
  BIO_free(cert_bio);
  EVP_PKEY* public_key = X509_get_pubkey(cert);
  X509_free(cert);

  BIO* bio = BIO_new(BIO_s_mem());
  PEM_write_bio_PUBKEY(bio, public_key);
  BUF_MEM* buf;
  BIO_get_mem_ptr(bio, &buf);
  std::string public_key_pem(buf->data, buf->length - 1);
  BIO_free(bio);

  EVP_PKEY_free(public_key);
  return public_key_pem;
}

std::vector<uint8_t> signData(const std::vector<uint8_t>& data, const std::string& pem_private_key) {
  BIO* key_bio = BIO_new(BIO_s_mem());
  EVP_PKEY* pkey = NULL;
  BIO_write(key_bio, pem_private_key.c_str(), (int)pem_private_key.size());
  pkey = PEM_read_bio_PrivateKey(key_bio, NULL, NULL, NULL);
  if (!pkey) {
    throw std::runtime_error("Failed to create private key");
  }
  std::vector<uint8_t> sig;
  EVP_MD_CTX* ctx = EVP_MD_CTX_create();
  EVP_SignInit(ctx, EVP_sha256());
  EVP_SignUpdate(ctx, data.data(), data.size());
  uint32_t siglen = 0;
  EVP_SignFinal(ctx, nullptr, &siglen, pkey);
  sig.resize(siglen);
  EVP_SignFinal(ctx, sig.data(), &siglen, pkey);
  EVP_MD_CTX_destroy(ctx);
  BIO_free(key_bio);
  EVP_PKEY_free(pkey);
  return sig;
}

std::string sha256(const std::vector<uint8_t>& data) {
  uint8_t hash[SHA256_DIGEST_LENGTH];
  SHA256_CTX sha256;
  SHA256_Init(&sha256);
  SHA256_Update(&sha256, data.data(), data.size());
  SHA256_Final(hash, &sha256);
  return libutt::Utils::bin2hex(hash, SHA256_DIGEST_LENGTH);
}
}  // namespace utt::client::utils::crypto