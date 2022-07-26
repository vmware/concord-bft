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

using std::string;
using std::vector;
using std::string_view;

string base64Enc(const vector<uint8_t>& msgBytes) {
#ifdef USE_CRYPTOPP_RSA
  CryptoPP::Base64Encoder encoder;
  encoder.Put(msgBytes.data(), msgBytes.size());
  encoder.MessageEnd();
  uint64_t output_size = encoder.MaxRetrievable();
  string output(output_size, '0');
  encoder.Get((unsigned char*)output.data(), output.size());

  return output;
#elif USE_EDDSA_SINGLE_SIGN
  if (msgBytes.capacity() == 0) {
    return {};
  }
  BIO* b64 = BIO_new(BIO_f_base64());
  BIO* bio = BIO_new(BIO_s_mem());
  bio = BIO_push(b64, bio);
  BIO_write(bio, msgBytes.data(), msgBytes.capacity());

  BUF_MEM* bufferPtr{nullptr};
  BIO_get_mem_ptr(bio, &bufferPtr);
  BIO_set_close(bio, BIO_NOCLOSE);
  BIO_flush(bio);
  BIO_free_all(bio);

  const auto msgLen = (*bufferPtr).length;
  vector<uint8_t> encodedMsg(msgLen);
  memcpy(&encodedMsg[0], (*bufferPtr).data, msgLen);
  BUF_MEM_free(bufferPtr);
  return string(encodedMsg.begin(), encodedMsg.end());
#endif
}

vector<uint8_t> base64Dec(const string& b64message) {
#ifdef USE_CRYPTOPP_RSA
  vector<uint8_t> dec;
  CryptoPP::StringSource ss(b64message, true, new CryptoPP::Base64Decoder(new CryptoPP::VectorSink(dec)));
  return dec;
#elif USE_EDDSA_SINGLE_SIGN
  const string b64msg(stripPemHeaderFooter(b64message, "-----BEGIN PRIVATE KEY-----", "-----END PRIVATE KEY-----"));

  if (b64msg.empty()) {
    return {};
  }
  vector<uint8_t> decodedOutput(calcDecodeLength(b64msg.data()));

  BIO* bio = BIO_new_mem_buf(b64msg.data(), -1);
  BIO* b64 = BIO_new(BIO_f_base64());
  bio = BIO_push(b64, bio);

  const int outputLen = BIO_read(bio, decodedOutput.data(), b64msg.size());
  vector<uint8_t> dec(outputLen);
  memcpy(&dec[0], decodedOutput.data(), outputLen);
  BIO_free_all(bio);
  return dec;
#endif
}

size_t calcDecodeLength(const char* b64message) {
  const size_t len{strlen(b64message)};
  size_t padding{0};

  if ((b64message[len - 1] == '=') && (b64message[len - 2] == '=')) {  // Check if the last 2 characters are '=='
    padding = 2;
  } else if (b64message[len - 1] == '=') {  // Check if the last characters is '='
    padding = 1;
  }
  return (((len * 3) / 4) - padding);
}

string stripPemHeaderFooter(const string_view b64message, const string_view header, const string_view footer) {
  string strippedPemMsg(b64message);
  auto pos1 = b64message.find(header);

  if (pos1 != string::npos) {
    auto pos2 = b64message.find(footer, pos1 + 1);

    // Start position and header's length.
    pos1 = pos1 + header.length();
    pos2 = pos2 - pos1 - 1;
    strippedPemMsg = b64message.substr(pos1 + 1, pos2 - 1);
  }
  return strippedPemMsg;
}
}  // namespace concord::secretsmanager
