
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
#include <cstdint>

namespace bftEngine {
class IReservedPages {
 public:
  virtual ~IReservedPages(){};
  virtual uint32_t numberOfReservedPages() const = 0;
  virtual uint32_t sizeOfReservedPage() const = 0;
  virtual bool loadReservedPage(uint32_t reservedPageId, uint32_t copyLength, char *outReservedPage) const = 0;
  virtual void saveReservedPage(uint32_t reservedPageId, uint32_t copyLength, const char *inReservedPage) = 0;
  virtual void zeroReservedPage(uint32_t reservedPageId) = 0;
};
}  // namespace bftEngine
