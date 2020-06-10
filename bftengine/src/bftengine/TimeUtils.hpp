// Concord
//
// Copyright (c) 2018-2019 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").  You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

// This file contains simple wrapper types around a steady_clock. Its
// replacement code for prior type wrappers around uint64_t and int64_t that
// were less safe. It shouldn't be used outside the bftEngine and only exists to
// allow making the minimal possible changes to allow using std::chrono. It may
// be removed in the future and to use the std::types directly. However, it's
// nice to force the use of steady_clock to avoid mistakes in using the wrong
// clock.

#pragma once

#include <chrono>
#include <ctime>
#include <iomanip>

namespace bftEngine {
namespace impl {

typedef std::chrono::steady_clock::time_point Time;

const Time MaxTime = std::chrono::steady_clock::time_point::max();

// Don't use time_point::min() as it returns a time_since_epoch that is
// negative. This is because the underlying representation is a signed long.
// The default constructor on the other hand returns a time_since_epoch of 0,
// which is what we want.
const Time MinTime = std::chrono::steady_clock::time_point();

#define getMonotonicTime std::chrono::steady_clock::now

// Make a steady clock printable in UTC
inline std::string utcstr(const std::chrono::steady_clock::time_point &time_point) {
  time_t systime = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now() +
                                                        (time_point - std::chrono::steady_clock::now()));
  std::ostringstream os;
  os << std::put_time(std::gmtime(&systime), "%c %Z");
  return os.str();
}

}  // namespace impl
}  // namespace bftEngine
