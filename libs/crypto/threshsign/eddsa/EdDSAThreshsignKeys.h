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
#include "crypto/threshsign/IPublicKey.h"
#include "crypto/threshsign/ISecretKey.h"
#include "crypto/openssl/EdDSA.hpp"

class EdDSAThreshsignPrivateKey : public IShareSecretKey, public concord::crypto::openssl::EdDSAPrivateKey {
 public:
  EdDSAThreshsignPrivateKey(const EdDSAThreshsignPrivateKey::ByteArray& arr)
      : concord::crypto::openssl::EdDSAPrivateKey(arr) {}
  std::string toString() const override { return toHexString(); }
};

class EdDSAThreshsignPublicKey : public IShareVerificationKey, public concord::crypto::openssl::EdDSAPublicKey {
 public:
  EdDSAThreshsignPublicKey(const EdDSAThreshsignPublicKey::ByteArray& arr)
      : concord::crypto::openssl::EdDSAPublicKey(arr) {}
  std::string toString() const override { return toHexString(); }
};
