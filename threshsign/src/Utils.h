// Concord
//
// Copyright (c) 2018 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the
// LICENSE file.

#pragma once

#include <cstdint>
#include <string>
#include <vector>
#include <sstream>
#include <iterator>
#include <string>
#include <set>

#include "XAssert.h"

namespace threshsign {
class Utils {
 public:
  template <class T, class E>
  class MinMaxAvg {
   public:
    T min, max;
    E avg;

   public:
    static MinMaxAvg<T, E> compute(const std::vector<T>& v) {
      MinMaxAvg<T, E> mma;
      mma.min = v[0];
      mma.max = v[0];
      mma.avg = 0;

      for (auto it = v.begin(); it != v.end(); it++) {
        if (*it < mma.min) {
          mma.min = *it;
        }
        if (*it > mma.max) {
          mma.max = *it;
        }
        mma.avg += static_cast<E>(*it);
      }

      mma.avg = mma.avg / static_cast<E>(v.size());

      return mma;
    }
  };

 public:
  static bool fileExists(const std::string& filename);

  /**
   * Returns the number of bits needed to represent a positive integer n:
   * Specifically, return 1 if n == 0 and ceil(log_2(n)) otherwise.
   * e.g., numBits(1) = 1
   * e.g., numBits(2) = 2
   * e.g., numBits(3) = 2
   * e.g., numBits(4) = 3
   */
  static int numBits(int n) {
    assertGreaterThanOrEqual(n, 0);
    if (n == 0) return 1;

    int l = 0;
    while (n > 0) {
      n = n / 2;
      l++;
    }

    return l;
  }

  /**
   * Returns 2^exp
   */
  static int pow2(int exp) {
    if (exp >= 0) {
      return 1 << exp;
    } else {
      throw std::logic_error("Power must be positive");
    }
  }

  static int nearestPowerOfTwo(int n) {
    assertGreaterThanOrEqual(n, 0);

    int power = 1;
    while (power < n) {
      power *= 2;
    }
    return power;
  }

  template <class T>
  static std::string humanizeBytes(T bytes) {
    std::ostringstream str;
    long double result = static_cast<long double>(bytes);
    const char* units[] = {"bytes", "KB", "MB", "GB", "TB"};
    unsigned int i = 0;
    while (result >= 1024.0) {
      result /= 1024.0;
      i++;
    }

    str << result;
    str << " ";
    str << units[i];

    return str.str();
  }

  /**
   * Returns k random numbers in the range [0, n)
   */
  template <class T>
  static void randomSubset(std::set<T>& rset, int n, int k) {
    // NOTE: Does not need cryptographically secure RNG
    while (rset.size() < static_cast<typename std::set<T>::size_type>(k)) {
      T i = static_cast<T>(rand() % n);

      rset.insert(i);
    }
  }

  static void hex2bin(const char* hexBuf, int hexBufLen, unsigned char* bin, int binCapacity);
  static void bin2hex(const void* data, int dataLen, char* hexBuf, int hexBufCapacity);

  static std::string bin2hex(const void* data, int dataLen);
  static void hex2bin(const std::string& hexStr, unsigned char* bin, int binCapacity);
};

}  // namespace threshsign
