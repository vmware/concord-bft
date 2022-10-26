// Concord
//
// Copyright (c) 2018-2022 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the
// LICENSE file.
//
#pragma once
#include <memory>

template <auto delete_function>
struct deleter_from_fn {
  template <typename T>
  constexpr void operator()(T* arg) const {
    delete_function(arg);
  }
};

template <typename T, auto fn>
using custom_deleter_unique_ptr = std::unique_ptr<T, deleter_from_fn<fn>>;
