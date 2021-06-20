// Concord
//
// Copyright (c) 2019 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the
// LICENSE file.

#pragma once

#include <cxxabi.h>
#include <memory>
#include <assert.h>
#include "assertUtils.hpp"

struct demangler {
  static std::string demangle(const char* name) {
    int status = -4;
    std::unique_ptr<char, decltype(&std::free)> res{abi::__cxa_demangle(name, NULL, NULL, &status), std::free};
    ConcordAssert(status == 0);
    return res.get();
  }

  static std::string demangle(const std::type_info& ti) { return demangle(ti.name()); }

  template <typename T>
  static std::string demangle() {
    return demangle(typeid(T).name());
  }
};
