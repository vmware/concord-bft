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
//
#pragma once
#include "SerializableByteArray.hpp"

static constexpr const size_t EdDSAPrivateKeyByteSize = 32;
static constexpr const size_t EdDSAPublicKeyByteSize = 32;
static constexpr const size_t EdDSASignatureByteSize = 64;

class EdDSAPrivateKey : public SerializableByteArray<EdDSAPrivateKeyByteSize> {
 public:
  EdDSAPrivateKey(const EdDSAPrivateKey::ByteArray& arr) : SerializableByteArray<EdDSAPrivateKeyByteSize>(arr) {}
};

class EdDSAPublicKey : public SerializableByteArray<EdDSAPublicKeyByteSize> {
 public:
  EdDSAPublicKey(const EdDSAPublicKey::ByteArray& arr) : SerializableByteArray<EdDSAPublicKeyByteSize>(arr) {}
};