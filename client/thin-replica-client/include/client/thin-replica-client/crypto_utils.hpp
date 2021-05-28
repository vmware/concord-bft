// Concord
//
// Copyright (c) 2020-2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.
//
// Header file for wrappers of cryptographic utilities used by the Thin Replica
// Client. Actual cryptographic implementations used are provided by trusted
// cryptographic libraries (ex: OpenSSL Crypto); the wrappers declared in this
// file abstract those implementation(s) from the Thin Replica Client and
// present an interface stylistically consistent with the Thin Replica Client
// Library.

#ifndef TRC_CRYPTO_UTILS_HPP_
#define TRC_CRYPTO_UTILS_HPP_

#include <climits>
#include <string>

#include "assertUtils.hpp"

namespace client::thin_replica_client {

// Class of exception wrappers declared in this file may throw in the event they
// unexpectedly get a failure from some call into the underlying trusted
// cryptographic library.
class UnexpectedCryptoImplementationFailureException : public std::exception {
 private:
  std::string message;

 public:
  explicit UnexpectedCryptoImplementationFailureException(const std::string& what) : message(what) {}
  virtual const char* what() const noexcept override { return message.c_str(); }
};

// Crypto wrapper function(s) declared in this file assume char is an 8 bit
// type.
static_assert(CHAR_BIT == 8);
const size_t kExpectedSHA256HashLengthInBytes = (256 / 8);

// Compute the SHA256 Hash of a given byte string (stored in an std::string);
// the hash will also be returned as a byte string stored in an std::string. The
// length of the returned byte string will always be
// kExpectedSHA256HashLengthInBytes. If the underlying implementation of the
// hash function reports a hash value shorter than
// kExpectedSHA256HashLengthInBytes, the byte string this function returns will
// be padded at its end with extra 0 byte(s).
//
// May throw an UnexpectedCryptoImplementationFailureException if the trusted
// library providing the underlying implementation of the hash function reports
// a failure unexpectedly.
std::string ComputeSHA256Hash(const std::string& data);

}  // namespace client::thin_replica_client

#endif  // TRC_CRYPTO_UTILS_HPP_
