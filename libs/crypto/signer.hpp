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
#include "util/types.hpp"

namespace concord::crypto {

// Interface for signer.
// Signers expect the memory for the signature to be allocated by the caller
// to prevent redundant memory allocations
class ISigner {
 public:
  // This function's name need to be different from ISigner::sign, otherwise inheriting
  // classes will hide ISigner::sign
  virtual size_t signBuffer(const Byte* dataIn, size_t dataLen, Byte* sigOutBuffer) const = 0;
  template <typename Container>
  size_t sign(const Container& dataIn, Byte* sigOutBuffer) {
    static_assert(sizeof(typename Container::value_type) == sizeof(Byte),
                  "Attempting to sign a container whose elements are not byte-sized");
    return signBuffer(reinterpret_cast<const Byte*>(dataIn.data()), dataIn.size(), sigOutBuffer);
  }
  virtual size_t signatureLength() const = 0;
  virtual ~ISigner() = default;
  virtual std::string getPrivKey() const = 0;
};
}  // namespace concord::crypto
