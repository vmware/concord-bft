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
#include "Serializable.h"

struct KeyExchangeMsg : public concord::serialize::SerializableFactory<KeyExchangeMsg> {
  constexpr static uint8_t EXCHANGE{0};
  constexpr static uint8_t HAS_KEYS{1};
  constexpr static std::string_view hasKeysTrueReply{"true"};
  constexpr static std::string_view hasKeysFalseReply{"false"};
  uint8_t op{EXCHANGE};
  std::string key;
  uint16_t repID;

  std::string toString() const {
    std::stringstream ss;
    ss << "key [" << key << "] replica id [" << repID << "]";
    return ss.str();
  }
  static KeyExchangeMsg deserializeMsg(const char* serializedMsg, const int& size) {
    std::stringstream ss;
    KeyExchangeMsg ke;
    ss.write(serializedMsg, std::streamsize(size));
    deserialize(ss, ke);
    return ke;
  }

 protected:
  const std::string getVersion() const override { return "1"; }
  void serializeDataMembers(std::ostream& outStream) const override {
    serialize(outStream, op);
    serialize(outStream, key);
    serialize(outStream, repID);
  }
  void deserializeDataMembers(std::istream& inStream) override {
    deserialize(inStream, op);
    deserialize(inStream, key);
    deserialize(inStream, repID);
  }
};
