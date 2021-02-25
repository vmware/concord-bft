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

#include "openssl_pass.h"

#include <string>
#include <vector>

#include <cryptopp/cryptlib.h>
#include <cryptopp/sha.h>
#include "misc.h"

namespace concord::secretsmanager {

KeyParams deriveKeyPass(const std::string_view pass,
                        const std::vector<uint8_t>& salt,
                        const uint32_t key_size,
                        const uint32_t iv_size) {
  auto res = KeyParams(key_size, iv_size);
  auto h = CryptoPP::SHA256();
  OPENSSL_EVP_BytesToKey(h,
                         salt.data(),
                         (const unsigned char*)pass.data(),
                         pass.length(),
                         1,
                         res.key.data(),
                         res.key.size(),
                         res.iv.data(),
                         res.iv.size());

  return res;
}

}  // namespace concord::secretsmanager