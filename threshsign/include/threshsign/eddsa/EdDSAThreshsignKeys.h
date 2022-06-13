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
#include "threshsign/IPublicKey.h"
#include "threshsign/ISecretKey.h"
#include "crypto/eddsa/EdDSA.h"

class EdDSAThreshsignPrivateKey : public IShareSecretKey, public EdDSAPrivateKey {
 public:
  EdDSAThreshsignPrivateKey(const EdDSAThreshsignPrivateKey::ByteArray& arr) : EdDSAPrivateKey(arr) {}
  std::string toString() const override { return toHexString(); }
};

class EdDSAThreshsignPublicKey : public IShareVerificationKey, public EdDSAPublicKey {
 public:
  EdDSAThreshsignPublicKey(const EdDSAThreshsignPublicKey::ByteArray& arr) : EdDSAPublicKey(arr) {}
  std::string toString() const override { return toHexString(); }
};