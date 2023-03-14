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
#include <sstream>

namespace bftEngine {
namespace impl {

using std::chrono::steady_clock;
using std::chrono::duration_cast;
using std::chrono::seconds;
using std::chrono::milliseconds;
using std::chrono::microseconds;
using std::chrono::time_point;
using std::chrono::system_clock;

typedef steady_clock::time_point Time;

const Time MaxTime = steady_clock::time_point::max();

// Don't use time_point::min() as it returns a time_since_epoch that is
// negative. This is because the underlying representation is a signed long.
// The default constructor on the other hand returns a time_since_epoch of 0,
// which is what we want.
const Time MinTime = steady_clock::time_point();

#define getMonotonicTime steady_clock::now

// Make a steady clock printable in UTC
inline std::string utcstr(const steady_clock::time_point &time_point) {
  using namespace std::chrono;
  time_t systime = system_clock::to_time_t(system_clock::now() +
                                           duration_cast<system_clock::duration>(time_point - steady_clock::now()));
  std::ostringstream os;
  os << std::put_time(std::gmtime(&systime), "%c %Z");
  return os.str();
}

inline uint64_t getMonotonicTimeMicro() {
  steady_clock::time_point curTimePoint = steady_clock::now();
  auto timeSinceEpoch = curTimePoint.time_since_epoch();
  uint64_t micro = duration_cast<microseconds>(timeSinceEpoch).count();
  return micro;
}

inline uint64_t getMonotonicTimeMilli() {
  steady_clock::time_point curTimePoint = steady_clock::now();
  auto timeSinceEpoch = curTimePoint.time_since_epoch();
  uint64_t milli = duration_cast<milliseconds>(timeSinceEpoch).count();
  return milli;
}

}  // namespace impl
}  // namespace bftEngine
