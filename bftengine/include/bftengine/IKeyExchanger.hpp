// Concord
//
// Copyright (c) 2020 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License"). You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the sub-component's license, as noted in the LICENSE
// file.

#pragma once
#include <string>
#include <cstdint>
#include <tuple>
#include "crypto/crypto.hpp"

// Interface for objects that need to be notified on key rotation
class IKeyExchanger {
 public:
  virtual ~IKeyExchanger() {}
  virtual void onPublicKeyExchange(const std::string& pub, const std::uint16_t& signerIndex, const int64_t& seqnum) = 0;
  virtual void onPrivateKeyExchange(const std::string& privKey, const std::string& pubKey, const int64_t& seqnum) = 0;
};

class IMultiSigKeyGenerator {
 public:
  virtual ~IMultiSigKeyGenerator() {}
  virtual std::tuple<std::string, std::string, concord::crypto::SignatureAlgorithm> generateMultisigKeyPair() = 0;
};

class IClientPublicKeyStore {
 public:
  virtual ~IClientPublicKeyStore() = default;
  virtual void setClientPublicKey(uint16_t clientId, const std::string& key, concord::crypto::KeyFormat) = 0;
};
