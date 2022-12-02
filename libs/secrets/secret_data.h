// Concord
//
// Copyright (c) 2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the
// LICENSE file.

#pragma once

#include <string>
#include <stdint.h>

namespace concord::secretsmanager {

// This struct contains the secret parameters needed to initialise the SecretsManagerEnc
struct SecretData {
  std::string key;              // symmetric key (as hex string)
  std::string iv;               // iv to use with the key (as hex string)
  std::string algo;             // Encryption algorithm used by SecretsManagerEnc
  uint32_t key_length;          // key size in bytes
  std::string additional_info;  // contains extra information needed
                                // including error reasons when get API errors out
};

}  // namespace concord::secretsmanager