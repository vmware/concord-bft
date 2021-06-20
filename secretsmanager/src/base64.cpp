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

std::string base64Enc(const std::vector<uint8_t>& cipher_text) {
  CryptoPP::Base64Encoder encoder;
  encoder.Put(cipher_text.data(), cipher_text.size());
  encoder.MessageEnd();
  uint64_t output_size = encoder.MaxRetrievable();
  std::string output(output_size, '0');
  encoder.Get((unsigned char*)output.data(), output.size());

  return output;
}

std::vector<uint8_t> base64Dec(const std::string& input) {
  std::vector<uint8_t> dec;
  CryptoPP::StringSource ss(input, true, new CryptoPP::Base64Decoder(new CryptoPP::VectorSink(dec)));

  return dec;
}
}  // namespace concord::secretsmanager