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

#include <stdint.h>

namespace bftEngine {
namespace impl {

// Based on Knuth TAOCP vol 2, 2nd edition, page 216 (see also https://www.johndcook.com/blog/standard_deviation/)
class RollingAvgAndVar {
 public:
  void reset() {
    k = 0;
    prevM = 0;
    prevS = 0;
    currM = 0;
    currS = 0;
  }

  void add(double x) {
    k++;

    if (k == 1) {
      prevM = currM = x;
      prevS = 0.0;
    } else {
      currM = prevM + (x - prevM) / k;
      currS = prevS + (x - prevM) * (x - currM);
      prevM = currM;
      prevS = currS;
    }
  }

  double avg() const { return (k > 0) ? currM : 0.0; }

  double var() const { return ((k > 1) ? currS / (k - 1) : 0.0); }

  int numOfElements() const { return k; }

 private:
  uint32_t k{0};
  double prevM{0};
  double prevS{0};
  double currM{0};
  double currS{0};
};

}  // namespace impl
}  // namespace bftEngine
