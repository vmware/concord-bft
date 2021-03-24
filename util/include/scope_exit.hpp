// Concord
//
// Copyright (c) 2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the
// LICENSE file.

// Modelled after std::experimental::scope_exit
// https://en.cppreference.com/w/cpp/experimental/scope_exit

#pragma once

#include <type_traits>
#include <utility>

namespace concord::util {

// The ScopeExit utility class calls a user-provided function when a scope is exited.
template <typename EF>
class ScopeExit {
 public:
  // Disable this perferct forwarding constructor as it can hide the move constructor in case the passed
  // type is ScopeExit.
  // Reported by clang-tidy:
  // https://releases.llvm.org/5.0.1/tools/clang/tools/extra/docs/clang-tidy/checks/misc-forwarding-reference-overload.html
  template <typename Fn, typename = std::enable_if_t<!std::is_same_v<std::decay_t<Fn>, ScopeExit>>>
  explicit ScopeExit(Fn&& fn) noexcept : fn_{std::forward<Fn>(fn)} {}

  ScopeExit(ScopeExit&& other) noexcept : active_{other.active_}, fn_{std::move(other.fn_)} { other.release(); }

  ~ScopeExit() noexcept {
    if (active_) {
      fn_();
    }
  }

  void release() noexcept { active_ = false; }

  ScopeExit(const ScopeExit&) = delete;
  ScopeExit& operator=(const ScopeExit&) = delete;
  ScopeExit& operator=(ScopeExit&&) = delete;

 private:
  bool active_{true};
  EF fn_;
};

// Deduction guide, allowing code such as:
//   auto s = ScopeExit{[] () {}};
template <typename EF>
ScopeExit(EF)->ScopeExit<EF>;

}  // namespace concord::util
