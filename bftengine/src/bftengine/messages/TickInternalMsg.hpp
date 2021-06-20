// Concord
//
// Copyright (c) 2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").  You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#pragma once

#include <cstdint>

namespace bftEngine::impl {

struct TickInternalMsg {
  const std::uint32_t component_id{0};

  TickInternalMsg(std::uint32_t c) : component_id{c} {}
};

inline bool operator==(const TickInternalMsg& l, const TickInternalMsg& r) {
  return (l.component_id == r.component_id);
}

}  // namespace bftEngine::impl
