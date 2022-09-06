// Concord
//
// Copyright (c) 2022 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the sub-component's license, as noted in the LICENSE
// file.
#pragma once

#include <string>

namespace concord::crypto {

// Interface for verifier.
class IVerifier {
 public:
  virtual bool verifyBuffer(const Byte* data,
                            size_t dataByteSize,
                            const Byte* signature,
                            size_t signatureByteSize) const = 0;
  template <typename DataContainer, typename SignatureContainer>
  bool verify(const DataContainer& data, const SignatureContainer& signature) const {
    static_assert(sizeof(typename DataContainer::value_type) == sizeof(Byte), "data elements are not byte-sized");
    static_assert(sizeof(typename SignatureContainer::value_type) == sizeof(Byte),
                  "signature elements are not byte-sized");
    return verifyBuffer(reinterpret_cast<const Byte*>(data.data()),
                        data.size(),
                        reinterpret_cast<const Byte*>(signature.data()),
                        signature.size());
  }
  virtual uint32_t signatureLength() const = 0;
  virtual ~IVerifier() = default;
  virtual std::string getPubKey() const = 0;
};
}  // namespace concord::crypto
