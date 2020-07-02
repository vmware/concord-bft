// Concord
//
// Copyright (c) 2018 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").  You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#pragma once

#include <cmath>  // sqrt
#include "RollingAvgAndVar.hpp"

namespace bftEngine {
namespace impl {

template <typename T>
class DynamicUpperLimitWithSimpleFilter {
 public:
  DynamicUpperLimitWithSimpleFilter(T initialUpperLimit,
                                    int16_t s,
                                    T maxUpperLimit,
                                    T minUpperLimit,
                                    int16_t evalPeriod,
                                    int16_t resetPoint,
                                    double maxIncreasingFactor,
                                    double maxDecreasingFactor)
      : numOfStdDeviations{s},
        maxLimit{maxUpperLimit},
        minLimit{minUpperLimit},
        evalPeriod{evalPeriod},
        resetPoint{resetPoint},
        maxIncreasing{maxIncreasingFactor},
        maxDecreasing{maxDecreasingFactor} {
    currentUpperLimit = initialUpperLimit;

    // TODO(GG): add asserts
  }

  virtual ~DynamicUpperLimitWithSimpleFilter() {}

  virtual void add(T x) {
    if (x > maxLimit)
      x = maxLimit;
    else if (x < minLimit)
      x = minLimit;

    double val = (double)x;
    avgAndVar.add(val);

    if (avgAndVar.numOfElements() % evalPeriod != 0) return;

    const T maxVal = std::min(maxLimit, (T)(currentUpperLimit * maxIncreasing));
    const T minVal = std::max(minLimit, (T)(currentUpperLimit / maxDecreasing));

    const double avg = avgAndVar.avg();
    const double var = avgAndVar.var();
    const double sd = ((var > 0) ? sqrt(var) : 0);

    double newLimit = avg + numOfStdDeviations * sd;

    if (((T)newLimit) > maxVal)
      currentUpperLimit = maxVal;
    else if (((T)newLimit) < minVal)
      currentUpperLimit = minVal;
    else
      currentUpperLimit = (T)newLimit;

    if (avgAndVar.numOfElements() >= resetPoint) avgAndVar.reset();
  }

  virtual T upperLimit() { return currentUpperLimit; }

 protected:
  const int16_t numOfStdDeviations;

  const T maxLimit;
  const T minLimit;

  const int16_t evalPeriod;
  const int16_t resetPoint;
  const double maxIncreasing;
  const double maxDecreasing;

  RollingAvgAndVar avgAndVar;
  T currentUpperLimit;
};
}  // namespace impl
}  // namespace bftEngine
