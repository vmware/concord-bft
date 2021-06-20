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

#include <cstddef>
#include <vector>

#include "ThresholdSignaturesTypes.h"

/**
 * NOTE: This class is NOT the fastest implementation. We just use it when we generate keys.
 */
template <class Integer>
class SecretSharingPolynomial {
 protected:
  std::vector<Integer> coefs;
  int degree;
  const Integer& modBase;

 public:
  SecretSharingPolynomial(const Integer& secret, int degree, const Integer& modBase)
      : degree(degree), modBase(modBase) {
    coefs.resize(static_cast<size_t>(degree + 1));
    coefs[0] = secret % this->modBase;
  }

  virtual ~SecretSharingPolynomial() {}

 public:
  // TODO: If you want to eliminate this method (can't right now because C++ does not let you call virtual methods in
  // the constructor) then add a functor template parameter for randomCoeff instead of a pure virtual method
  void generate() { generate(modBase); }

  void generate(const Integer& coefLimit) {
    for (int i = 1; i <= degree; i++) {
      size_t idx = static_cast<size_t>(i);
      coefs[idx] = randomCoeff(coefLimit);
    }
  }

  virtual Integer randomCoeff(const Integer& coefLimit) const = 0;

  Integer get(ShareID x) const {
    Integer res = Integer::Zero();
    Integer xVal(x);

    // Computes f(i) as f(i)_{old} * x + a_k in reverse order k = { degree + 1, degree, ..., 2, 1, 0 }
    for (int i = 0; i <= this->degree; i++) {
      size_t k = static_cast<size_t>(this->degree - i);

      res = (res * xVal + coefs[k]) % modBase;
    }

    return res;
  }
};
