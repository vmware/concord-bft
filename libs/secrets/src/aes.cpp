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
#include <nlohmann/json.hpp>

namespace concord::secretsmanager {
using std::vector;
using std::string;
using concord::crypto::openssl::OPENSSL_SUCCESS;
using concord::crypto::openssl::UniqueCipherContext;
using json = nlohmann::json;

vector<uint8_t> AES_CBC::encrypt(std::string_view input) {
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

  ConcordAssert(
      OPENSSL_SUCCESS ==
      EVP_EncryptInit_ex(ctx.get(), EVP_aes_256_cbc(), nullptr, getKeyParams().key.data(), getKeyParams().iv.data()));
  ConcordAssert(OPENSSL_SUCCESS ==
                EVP_EncryptUpdate(ctx.get(), ciphertext.get(), &c_len, plaintext.get(), input.size()));
  ConcordAssert(OPENSSL_SUCCESS == EVP_EncryptFinal_ex(ctx.get(), ciphertext.get() + c_len, &f_len));

  const int encryptedMsgLen = c_len + f_len;
  vector<uint8_t> cipher(encryptedMsgLen);
  memcpy(cipher.data(), ciphertext.get(), encryptedMsgLen);
  return cipher;
}

string AES_CBC::decrypt(const vector<uint8_t>& cipher) {
  if (cipher.capacity() == 0) {
    return {};
  }

  const int cipherLength = cipher.capacity();
  int c_len{0}, f_len{0};

  auto plaintext = std::make_unique<unsigned char[]>(cipherLength);
  UniqueCipherContext ctx(EVP_CIPHER_CTX_new());
  ConcordAssert(nullptr != ctx);

  ConcordAssert(
      OPENSSL_SUCCESS ==
      EVP_DecryptInit_ex(ctx.get(), EVP_aes_256_cbc(), nullptr, getKeyParams().key.data(), getKeyParams().iv.data()));
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

std::vector<uint8_t> AES_GCM::encrypt(std::string_view input) {
  if (input.empty()) {
    return {};
  }
  int tagLength = 16;
  if (!getAdditionalInfo().empty()) {
    auto j = json::parse(getAdditionalInfo());
    tagLength = (j["TAG_LENGTH_BITS"].get<int>()) / 8;  // from bits to bytes
  }

  auto ciphertext = std::make_unique<unsigned char[]>(input.size() + AES_BLOCK_SIZE);
  auto plaintext = std::make_unique<unsigned char[]>(input.size());

  std::copy(input.begin(), input.end(), plaintext.get());

  UniqueCipherContext ctx(EVP_CIPHER_CTX_new());
  ConcordAssert(nullptr != ctx);

  int c_len{0};
  int f_len{0};

  /* Set cipher type and mode */
  ConcordAssert(OPENSSL_SUCCESS == EVP_EncryptInit_ex(ctx.get(), EVP_aes_256_gcm(), NULL, NULL, NULL));
  /* Set IV length if default 96 bits is not appropriate */
  ConcordAssert(OPENSSL_SUCCESS ==
                EVP_CIPHER_CTX_ctrl(ctx.get(), EVP_CTRL_AEAD_SET_IVLEN, getKeyParams().iv.size(), NULL));
  /* Initialise key and IV */
  ConcordAssert(OPENSSL_SUCCESS ==
                EVP_EncryptInit_ex(ctx.get(), NULL, NULL, getKeyParams().key.data(), getKeyParams().iv.data()));
  /*
   * Provide the message to be encrypted, and obtain the encrypted output.
   * EVP_EncryptUpdate can be called multiple times if necessary
   */
  ConcordAssert(OPENSSL_SUCCESS ==
                EVP_EncryptUpdate(ctx.get(), ciphertext.get(), &c_len, plaintext.get(), input.size()));

  /*
   * Finalise the encryption. Normally ciphertext bytes may be written at
   * this stage, but this does not occur in GCM mode
   */
  ConcordAssert(OPENSSL_SUCCESS == EVP_EncryptFinal_ex(ctx.get(), ciphertext.get() + c_len, &f_len));

  const int encryptedMsgLen = c_len + f_len;
  // allocating memory for cipher as well as tag
  vector<uint8_t> cipher(encryptedMsgLen + tagLength);
  memcpy(cipher.data(), ciphertext.get(), encryptedMsgLen);
  /* Get the tag */
  auto c_tag = std::make_unique<unsigned char[]>(tagLength);
  EVP_CIPHER_CTX_ctrl(ctx.get(), EVP_CTRL_GCM_GET_TAG, tagLength, c_tag.get());
  // adding tag to cipher text
  memcpy(cipher.data() + encryptedMsgLen, c_tag.get(), tagLength);
  return cipher;
}
/* In GCM mode, cipher includes cipher text  + authenticated tag
 */
string AES_GCM::decrypt(const vector<uint8_t>& cipher) {
  if (cipher.capacity() == 0) {
    return {};
  }
  int tagLength = 16;
  if (!getAdditionalInfo().empty()) {
    auto j = json::parse(getAdditionalInfo());
    tagLength = (j["TAG_LENGTH_BITS"].get<int>()) / 8;  // from bits to bytes
  }
  const unsigned int cipherLength = cipher.size() - tagLength;
  std::vector<uint8_t> cipherText(cipher.begin(), cipher.end() - tagLength);
  std::vector<uint8_t> tag(cipher.begin() + cipherLength, cipher.end());
  int c_len{0}, f_len{0};
  auto plaintext = std::make_unique<unsigned char[]>(cipher.capacity());
  UniqueCipherContext ctx(EVP_CIPHER_CTX_new());
  ConcordAssert(nullptr != ctx);
  /* Select cipher */
  ConcordAssert(OPENSSL_SUCCESS == EVP_DecryptInit_ex(ctx.get(), EVP_aes_256_gcm(), NULL, NULL, NULL));
  /* Set IV length, omit for 96 bits */
  ConcordAssert(OPENSSL_SUCCESS ==
                EVP_CIPHER_CTX_ctrl(ctx.get(), EVP_CTRL_GCM_SET_IVLEN, getKeyParams().iv.size(), NULL));
  /* Specify key and IV */
  ConcordAssert(OPENSSL_SUCCESS ==
                EVP_DecryptInit_ex(ctx.get(), NULL, NULL, getKeyParams().key.data(), getKeyParams().iv.data()));
  /* Decrypt plaintext */
  ConcordAssert(
      OPENSSL_SUCCESS ==
      EVP_DecryptUpdate(ctx.get(), plaintext.get(), &c_len, (const unsigned char*)cipherText.data(), cipherLength));
  /* Set expected tag value. */
  ConcordAssert(OPENSSL_SUCCESS == EVP_CIPHER_CTX_ctrl(ctx.get(), EVP_CTRL_GCM_SET_TAG, tagLength, tag.data()));

  ConcordAssert(OPENSSL_SUCCESS == EVP_DecryptFinal_ex(ctx.get(), plaintext.get() + c_len, &f_len));
  const int plainMsgLen = c_len + f_len;
  plaintext.get()[plainMsgLen] = 0;

  vector<uint8_t> plaintxt(plainMsgLen);
  memcpy(plaintxt.data(), plaintext.get(), plainMsgLen);
  return string(plaintxt.begin(), plaintxt.end());
}

}  // namespace concord::secretsmanager
