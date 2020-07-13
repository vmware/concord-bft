// Concord
//
// Copyright (c) 2018-2020 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License"). You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#pragma once

#include <string>
#include <map>
#include <memory>

namespace bftEngine {
namespace controlStateController {
class IControlStateController {
 public:
  typedef enum {
    STOP_AT_NEXT_CHECKPOINT,
  } ControlStateId;

  virtual bool saveControlState(const ControlStateId id, const std::string& state) = 0;
  virtual ~IControlStateController(){};
};

class ReservedPagesControlStateController : public IControlStateController {
 public:
  bool saveControlState(const ControlStateId id, const std::string& state) override { return true; }
  virtual ~ReservedPagesControlStateController() = default;
};

}  // namespace controlStateController
}  // namespace bftEngine
