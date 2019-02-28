// Concord
//
// Copyright (c) 2018 VMware, Inc. All Rights Reserved.
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

#include <string>

/**
 * This class is used to represent the parameters of a cryptosystem like RSA or
 * BLS. For example, this class can store the value of N = pq, for RSA or the
 * value of the prime p for BLS.
 */
class IPublicParameters {
 protected:
  /**
   * Security level.
   */
  int k;

  /**
   * Name of the scheme and the library.
   */
  std::string name, library;

 public:
  IPublicParameters(int k,
                    const std::string& name,
                    const std::string& lib = "unknown")
      : k(k), name(name), library(lib) {}
  virtual ~IPublicParameters() {}

 public:
  /**
   * The security parameter of the cryptosystem. This should be 128, 256 or
   * larger. The subclassing cryptosystem is responsible for generating its own
   * strong enough parameters. For example, in RSA, a 2048-bit prime or larger
   * should be generated when k is 128 whereas in BLS, a smaller elliptic curve
   * group of order close to 2^256 can be used.
   */
  virtual int getSecurityLevel() const { return k; }

  virtual const std::string& getName() const { return name; }

  virtual const std::string& getLibrary() const { return library; }
};
