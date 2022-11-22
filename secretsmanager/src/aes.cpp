// Concord
//
// Copyright (c) 2020 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the
// "License").  You may not use this product except in compliance with the
// Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.
//
// This convenience header combines different block implementations.

#include "aes.h"
#include "crypto/openssl/crypto.hpp"
#include "assertUtils.hpp"
#include <openssl/aes.h>

namespace concord::secretsmanager {
using std::vector;
using std::string;
using concord::crypto::openssl::OPENSSL_SUCCESS;
using concord::crypto::openssl::UniqueCipherContext;

vector<uint8_t> AES_CBC::encrypt(std::string_view input) {
  if (algo_ == concord::crypto::SignatureAlgorithm::EdDSA) {
    if (input.empty()) {
      return {};
    }

    auto ciphertext = std::make_unique<unsigned char[]>(input.size() + AES_BLOCK_SIZE);
    auto plaintext = std::make_unique<unsigned char[]>(input.size());

    std::copy(input.begin(), input.end(), plaintext.get());

    UniqueCipherContext ctx(EVP_CIPHER_CTX_new());
    ConcordAssert(nullptr != ctx);

    int c_len{0};
    int f_len{0};

    ConcordAssert(OPENSSL_SUCCESS ==
                  EVP_EncryptInit_ex(ctx.get(), EVP_aes_256_cbc(), nullptr, params_.key.data(), params_.iv.data()));
    ConcordAssert(OPENSSL_SUCCESS ==
                  EVP_EncryptUpdate(ctx.get(), ciphertext.get(), &c_len, plaintext.get(), input.size()));
    ConcordAssert(OPENSSL_SUCCESS == EVP_EncryptFinal_ex(ctx.get(), ciphertext.get() + c_len, &f_len));

    const int encryptedMsgLen = c_len + f_len;
    vector<uint8_t> cipher(encryptedMsgLen);
    memcpy(cipher.data(), ciphertext.get(), encryptedMsgLen);
    return cipher;
  }
  return {};
}

string AES_CBC::decrypt(const vector<uint8_t>& cipher) {
  if (algo_ == concord::crypto::SignatureAlgorithm::EdDSA) {
    if (cipher.capacity() == 0) {
      return {};
    }

    const int cipherLength = cipher.capacity();
    int c_len{0}, f_len{0};

    auto plaintext = std::make_unique<unsigned char[]>(cipherLength);
    UniqueCipherContext ctx(EVP_CIPHER_CTX_new());
    ConcordAssert(nullptr != ctx);

    ConcordAssert(OPENSSL_SUCCESS ==
                  EVP_DecryptInit_ex(ctx.get(), EVP_aes_256_cbc(), nullptr, params_.key.data(), params_.iv.data()));
    EVP_CIPHER_CTX_set_key_length(ctx.get(), EVP_MAX_KEY_LENGTH);
    ConcordAssert(
        OPENSSL_SUCCESS ==
        EVP_DecryptUpdate(ctx.get(), plaintext.get(), &c_len, (const unsigned char*)cipher.data(), cipherLength));
    ConcordAssert(OPENSSL_SUCCESS == EVP_DecryptFinal_ex(ctx.get(), plaintext.get() + c_len, &f_len));

    const int plainMsgLen = c_len + f_len;
    plaintext.get()[plainMsgLen] = 0;

    vector<uint8_t> plaintxt(plainMsgLen);
    memcpy(plaintxt.data(), plaintext.get(), plainMsgLen);
    return string(plaintxt.begin(), plaintxt.end());
  }
  return {};
}
}  // namespace concord::secretsmanager
