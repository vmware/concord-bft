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
//
//
// Thin wrapper over selected cryptographic function utilities from OpenSSL's
// crypto library, which we consider a reasonably trusted source of
// cryptographic implementations. This wrapper is intended to provide a cleaner
// and more convenient interface to the OpenSSL crypto library to fit better
// with the rest of the Concord codebase, as the OpenSSL crypto library itself
// has a C interface.
#pragma once

#include <climits>
#include <memory>
#include <ostream>
#include <string>
#include <vector>
#include "crypto/crypto.hpp"

#include "memory.hpp"
#include "assertUtils.hpp"

// OpenSSL includes.
#include <openssl/crypto.h>
#include <openssl/evp.h>
#include <openssl/bio.h>
#include <openssl/pem.h>
#include <openssl/x509.h>

namespace concord::crypto::openssl {

// unique_ptr custom deleter
void _openssl_free(void* ptr);

using UniqueContext = custom_deleter_unique_ptr<EVP_MD_CTX, EVP_MD_CTX_free>;
using UniquePKEYContext = custom_deleter_unique_ptr<EVP_PKEY_CTX, EVP_PKEY_CTX_free>;
using UniquePKEY = custom_deleter_unique_ptr<EVP_PKEY, EVP_PKEY_free>;
using UniqueX509 = custom_deleter_unique_ptr<X509, X509_free>;
using UniqueBIO = custom_deleter_unique_ptr<BIO, BIO_free_all>;
using UniqueCipherContext = custom_deleter_unique_ptr<EVP_CIPHER_CTX, EVP_CIPHER_CTX_free>;
using UniqueBIGNUM = custom_deleter_unique_ptr<BIGNUM, BN_clear_free>;
using UniqueBNCTX = custom_deleter_unique_ptr<BN_CTX, BN_CTX_free>;
using UniqueECKEY = custom_deleter_unique_ptr<EC_KEY, EC_KEY_free>;
using UniqueECPOINT = custom_deleter_unique_ptr<EC_POINT, EC_POINT_clear_free>;
using UniqueOpenSSLString = custom_deleter_unique_ptr<char[], _openssl_free>;

constexpr int OPENSSL_SUCCESS = 1;
constexpr int OPENSSL_FAILURE = 0;
constexpr int OPENSSL_ERROR = -1;

// Note these utilities may use std::strings to pass around byte strings; note
// this should work since C++ should guarantee std::string is a string of chars
// specifically and that the char type is exactly 1 byte in size. Some of these
// utilities may assume that chars are 8-bit bytes, which C++ does not
// guarantee, however.
static_assert(CHAR_BIT == 8);

const size_t kExpectedSHA256HashLengthInBytes = (256 / 8);

// Compute the SHA256 Hash of a given byte string (given either a reference to a
// string object or a char pointer to the start of that string and a length for
// it); The length of the returned byte string will always be
// kExpectedSHA256HashLengthInBytes. If the underlying implementation of the
// hash function reports a hash value shorter than
// kExpectedSHA256HashLengthInBytes, the byte string this function returns will
// be padded at its end with extra 0 byte(s).
//
// May throw an OpenSSLError if the underlying
// OpenSSL Crypto library unexpectedly reports a failure while attempting this
// operation.
std::string computeSHA256Hash(const std::string& data);
std::string computeSHA256Hash(const char* data, size_t length);

// Specialized Exception types that may be thrown by the above utilities.

// Exception that may be thrown if a call into OpenSSLCrypto returns a failure
// unexpectedly.
class OpenSSLError : public std::exception {
 private:
  std::string message;

 public:
  explicit OpenSSLError(const std::string& what) : message(what) {}
  virtual const char* what() const noexcept override { return message.c_str(); }
};

// Deleter classes for using smart pointers that correctly manage objects from
// the OpenSSL Crypto library, given OpenSSL Crypto objects use free functions
// provided by the library rather than normal destructors.
class OPENSSLStringDeleter {
 public:
  void operator()(char* string) const { OPENSSL_free(string); }
};
/**
 * @brief Generates an EdDSA asymmetric key pair (private-public key pair).
 *
 * @param fmt Output key format.
 * @return pair<string, string> Private-Public key pair.
 */
std::pair<std::string, std::string> generateEdDSAKeyPair(const KeyFormat fmt);
/**
 * @brief Generates an EdDSA PEM file from hexadecimal key pair (private-public key pair).
 *
 * @param key_pair Key pair in hexa-decimal format.
 * @return pair<string, string>
 */
std::pair<std::string, std::string> EdDSAHexToPem(const std::pair<std::string, std::string>& hex_key_pair);

}  // namespace concord::crypto::openssl
