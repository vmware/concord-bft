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

#include <cryptopp/filters.h>

#include "assertUtils.hpp"

namespace concord::secretsmanager {

AES_CBC::AES_CBC(KeyParams& params) {
  ConcordAssertEQ(params.key.size(), 256 / 8);
  aesEncryption = CryptoPP::AES::Encryption(params.key.data(), params.key.size());
  aesDecryption = CryptoPP::AES::Decryption(params.key.data(), params.key.size());
  enc = CryptoPP::CBC_Mode_ExternalCipher::Encryption(aesEncryption, params.iv.data());
  dec = CryptoPP::CBC_Mode_ExternalCipher::Decryption(aesDecryption, params.iv.data());
}

std::vector<uint8_t> AES_CBC::encrypt(const std::string& input) {
  std::vector<uint8_t> cipher;
  CryptoPP::StringSource ss(
      input, true, new CryptoPP::StreamTransformationFilter(enc, new CryptoPP::VectorSink(cipher)));
  return cipher;
}
std::string AES_CBC::decrypt(const std::vector<uint8_t>& cipher) {
  std::string pt;
  CryptoPP::VectorSource ss(cipher, true, new CryptoPP::StreamTransformationFilter(dec, new CryptoPP::StringSink(pt)));
  return pt;
}

}  // namespace concord::secretsmanager