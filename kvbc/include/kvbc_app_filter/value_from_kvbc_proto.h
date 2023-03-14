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
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#pragma once

#include "concord_kvbc.pb.h"

#include <memory>
#include <stdexcept>
#include <string>
#include <utility>

namespace concord::kvbc {

inline std::string valueFromKvbcProto(std::string&& in) {
  using com::vmware::concord::kvbc::ValueWithTrids;

  auto proto = ValueWithTrids{};
  if (!proto.ParseFromArray(in.data(), in.size())) {
    throw std::runtime_error{"Parsing of ValueWithTrids failed"};
  }

  if (!proto.has_value()) {
    return std::string{};
  }

  auto value = std::unique_ptr<std::string>(proto.release_value());
  return std::move(*value);
}

}  // namespace concord::kvbc
