// Concord
//
// Copyright (c) 2018-2022 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the
// LICENSE file.
#pragma once
#include <array>
#include <cstdint>
#include "types.hpp"
#include "crypto/openssl/EdDSA.hpp"

/**
 * Used as an interface between signers and verifiers
 */
struct SingleEdDSASignature {
  uint64_t id;
  std::array<concord::Byte, concord::crypto::Ed25519SignatureByteSize> signatureBytes;
};
