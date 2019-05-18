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

#include "threshsign/Serializable.h"
#include <string>
#include <map>
#include <fstream>

/**
 * This class is used to represent the parameters of a cryptosystem like RSA or BLS.
 * For example, this class can store the value of N = pq, for RSA or the value of
 * the prime p for BLS.
 */

class IPublicParameters : public Serializable {
 protected:
  /**
   * Security level.
   */
  int securityLevel_ = 0;

  /**
   * Name of the scheme and the library.
   */
  std::string schemeName_, library_ = "unknown";

 public:
  IPublicParameters(int securityLevel, std::string schemeName,
                    std::string library);
  ~IPublicParameters() override = default;

  bool operator==(const IPublicParameters& other) const;

  bool compare(const IPublicParameters& other) const {
    return *this == other;
  }

  /**
   * The security parameter of the cryptosystem. This should be 128,
   * 256 or larger. The subclassing cryptosystem is responsible for
   * generating its own strong enough parameters. For example,
   * in RSA, a 2048-bit prime or larger should be generated when k is 128
   * whereas in BLS, a smaller elliptic curve group of order close to 2^256
   * can be used.
   */
  int getSecurityLevel() const { return securityLevel_; }

  const std::string &getName() const { return schemeName_; }

  const std::string &getLibrary() const { return library_; }

  // Serialization/deserialization
  // Two functions below should be implemented by all derived classes.
  virtual void serialize(std::ostream &outStream) const;

  UniquePtrToClass create(std::istream &inStream) override;

  // To be used ONLY during deserialization. Could not become private/protected,
  // as there is a composition relationship between IPublicParameters and
  // signer/verifier classes.
  IPublicParameters() = default;

 private:
  void serializeDataMembers(std::ostream &outStream) const;

 private:
  static const std::string className_;
  static const uint32_t classVersion_;
};
