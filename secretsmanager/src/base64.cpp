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

#include "base64.h"

#include <cryptopp/cryptlib.h>
#include <cryptopp/base64.h>

namespace concord::secretsmanager {

const std::string salt_prefix{"Salted__"};
const uint8_t salt_size = 8;

std::string base64Enc(const std::vector<uint8_t>& salt, const std::vector<uint8_t>& cipher_text) {
  if (salt.size() != salt_size) {
    throw std::runtime_error("Bad salt size " + std::to_string(salt.size()) + ". Expected value " +
                             std::to_string(salt_size));
  }

  CryptoPP::Base64Encoder encoder;
  encoder.Put((unsigned char*)salt_prefix.data(), salt_prefix.length());
  encoder.Put(salt.data(), salt.size());
  encoder.Put(cipher_text.data(), cipher_text.size());
  encoder.MessageEnd();

  uint64_t output_size = encoder.MaxRetrievable();
  std::string output(output_size, '0');
  encoder.Get((unsigned char*)output.data(), output.size());

  return output;
}

std::string base64Enc(const std::vector<uint8_t>& cipher_text) {
  CryptoPP::Base64Encoder encoder;
  encoder.Put(cipher_text.data(), cipher_text.size());
  encoder.MessageEnd();
  uint64_t output_size = encoder.MaxRetrievable();
  std::string output(output_size, '0');
  encoder.Get((unsigned char*)output.data(), output.size());

  return output;
}

SaltedCipher base64Dec(const std::string& input) {
  std::vector<uint8_t> decoded;
  CryptoPP::StringSource ss(input, true, new CryptoPP::Base64Decoder(new CryptoPP::VectorSink(decoded)));

  SaltedCipher result;

  if (decoded.size() < salt_prefix.length()) {
    throw std::runtime_error("Bad salt header length " + std::to_string(decoded.size()) +
                             ". Expected length to be at least " + std::to_string(salt_prefix.length()));
  }

  std::string header(decoded.begin(), decoded.begin() + salt_prefix.length());
  if (header != salt_prefix) {
    throw std::runtime_error("Bad salt header '" + header + "'. Expected value " + salt_prefix);
  }

  auto salt_it = decoded.begin() + salt_prefix.length();
  auto ct_it = salt_it + salt_size;
  result.salt = std::vector<uint8_t>(salt_it, salt_it + salt_size);
  result.cipher_text = std::vector<uint8_t>(ct_it, decoded.end());

  return result;
}

std::vector<uint8_t> base64DecNoSalt(const std::string& input) {
  std::vector<uint8_t> dec;
  CryptoPP::StringSource ss(input, true, new CryptoPP::Base64Decoder(new CryptoPP::VectorSink(dec)));

  return dec;
}
}  // namespace concord::secretsmanager