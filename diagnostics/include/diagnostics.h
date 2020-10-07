// Concord
//
// Copyright (c) 2020 VMware, Inc. All Rights Reserved.
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
#include <functional>
#include <map>
#include <mutex>
#include <stdexcept>

#include "performance_handler.h"
#include "status_handlers.h"

namespace concord::diagnostics {

class Registrar {
 public:
  PerformanceHandler perf;
  StatusHandlers status;
};

// Singleton wrapper class for a Registrar.
class RegistrarSingleton {
 public:
  static Registrar& getInstance() {
    static Registrar registrar_;
    return registrar_;
  }

  RegistrarSingleton(const RegistrarSingleton&) = delete;
  RegistrarSingleton& operator=(const RegistrarSingleton&) = delete;
  RegistrarSingleton(RegistrarSingleton&&) = delete;
  RegistrarSingleton& operator=(RegistrarSingleton&&) = delete;

 private:
  RegistrarSingleton() = default;
  ~RegistrarSingleton() = default;
};

}  // namespace concord::diagnostics
