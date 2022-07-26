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
#include "openssl_crypto.hpp"
#include "assertUtils.hpp"

#include <cryptopp/filters.h>

namespace concord::secretsmanager {

using std::vector;
using std::string;
using std::unique_ptr;
using concord::util::openssl_utils::OPENSSL_SUCCESS;
using concord::util::openssl_utils::UniqueOpenSSLCipherContext;

AES_CBC::AES_CBC(const KeyParams& params) {
#ifdef USE_CRYPTOPP_RSA
  ConcordAssertEQ(params.key.size(), 256 / 8);
  aesEncryption = CryptoPP::AES::Encryption(params.key.data(), params.key.size());
  aesDecryption = CryptoPP::AES::Decryption(params.key.data(), params.key.size());
  enc = CryptoPP::CBC_Mode_ExternalCipher::Encryption(aesEncryption, params.iv.data());
  dec = CryptoPP::CBC_Mode_ExternalCipher::Decryption(aesDecryption, params.iv.data());
#elif USE_EDDSA_SINGLE_SIGN
  key = params.key;
  iv = params.iv;
#endif
}

vector<uint8_t> AES_CBC::encrypt(const string& input) const {
#ifdef USE_CRYPTOPP_RSA
  vector<uint8_t> cipher;
  CryptoPP::StringSource ss(
      input, true, new CryptoPP::StreamTransformationFilter(enc, new CryptoPP::VectorSink(cipher)));
  return cipher;
#elif USE_EDDSA_SINGLE_SIGN
  if (input.empty()) {
    return {};
  }

  unique_ptr<unsigned char[]> ciphertext(new unsigned char[input.size() + AES_BLOCK_SIZE]);
  unique_ptr<unsigned char[]> plaintext(new unsigned char[input.size() + 1]);

  for (size_t i{0UL}; i < input.size(); ++i) {
    plaintext.get()[i] = (unsigned char)input[i];
  }

  UniqueOpenSSLCipherContext ctx(EVP_CIPHER_CTX_new());
  ConcordAssert(nullptr != ctx);

  int c_len{0};
  int f_len{0};

  ConcordAssert(OPENSSL_SUCCESS == EVP_EncryptInit_ex(ctx.get(), EVP_aes_256_cbc(), nullptr, key.data(), iv.data()));
  ConcordAssert(OPENSSL_SUCCESS ==
                EVP_EncryptUpdate(ctx.get(), ciphertext.get(), &c_len, plaintext.get(), input.size()));
  ConcordAssert(OPENSSL_SUCCESS == EVP_EncryptFinal_ex(ctx.get(), ciphertext.get() + c_len, &f_len));

  const int encryptedMsgLen = c_len + f_len;
  vector<uint8_t> ciphertxt(encryptedMsgLen);
  memcpy(&ciphertxt[0], ciphertext.get(), encryptedMsgLen);
  return ciphertxt;
#endif
}

string AES_CBC::decrypt(const vector<uint8_t>& cipher) const {
#ifdef USE_CRYPTOPP_RSA
  string pt;
  CryptoPP::VectorSource ss(cipher, true, new CryptoPP::StreamTransformationFilter(dec, new CryptoPP::StringSink(pt)));
  return pt;
#elif USE_EDDSA_SINGLE_SIGN
  if (cipher.capacity() == 0) {
    return {};
  }
  const int cipherLength = cipher.capacity();
  int c_len{0}, f_len{0};

  unique_ptr<unsigned char[]> plaintext(new unsigned char[cipherLength]);
  UniqueOpenSSLCipherContext ctx(EVP_CIPHER_CTX_new());
  ConcordAssert(nullptr != ctx);

  ConcordAssert(OPENSSL_SUCCESS == EVP_DecryptInit_ex(ctx.get(), EVP_aes_256_cbc(), nullptr, key.data(), iv.data()));
  EVP_CIPHER_CTX_set_key_length(ctx.get(), EVP_MAX_KEY_LENGTH);
  ConcordAssert(
      OPENSSL_SUCCESS ==
      EVP_DecryptUpdate(ctx.get(), plaintext.get(), &c_len, (const unsigned char*)cipher.data(), cipherLength));
  ConcordAssert(OPENSSL_SUCCESS == EVP_DecryptFinal_ex(ctx.get(), plaintext.get() + c_len, &f_len));

  const int plainMsgLen = c_len + f_len;
  plaintext.get()[plainMsgLen] = 0;

  vector<uint8_t> plaintxt(plainMsgLen);
  memcpy(&plaintxt[0], plaintext.get(), plainMsgLen);
  return string(plaintxt.begin(), plaintxt.end());
#endif
}
}  // namespace concord::secretsmanager
