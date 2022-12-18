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
#include "crypto/openssl/crypto.hpp"
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

constexpr int OPENSSL_SUCCESS = 1;
constexpr int OPENSSL_FAILURE = 0;
constexpr int OPENSSL_ERROR = -1;

// Note these utilities may use std::strings to pass arond byte strings; note
// this should work since C++ should guarantee std::string is a string of chars
// specifically and that the char type is exactly 1 byte in size. Some of these
// utilities may assume that chars are 8-bit bytes, which C++ does not
// guarantee, however.
static_assert(CHAR_BIT == 8);

// Wrapper class for OpenSSL Crypto's EVP_PKEY objects implementing
// AsymmetricPrivateKey.
class EVPPKEYPrivateKey : public AsymmetricPrivateKey {
 private:
  UniquePKEY pkey;
  std::string scheme_name;

 public:
  // Copying and moving EVPPKEYPrivateKey objects is currently unimplemented;
  // note the default implementations for copy and move operations would not be
  // appropriate for this class since the EVP_PKEY object it contains must be
  // memory-managed through the OpenSSL library rather than by default
  // mechanisms.
  EVPPKEYPrivateKey(const EVPPKEYPrivateKey& other) = delete;
  EVPPKEYPrivateKey(const EVPPKEYPrivateKey&& other) = delete;
  EVPPKEYPrivateKey& operator=(const EVPPKEYPrivateKey& other) = delete;
  EVPPKEYPrivateKey& operator=(const EVPPKEYPrivateKey&& other) = delete;

  // Note the constructed EVPPKEYPrivateKey takes ownership of the pointer it is
  // constructed with. A precondition of this constructor is that the provided
  // EVP_PKEY object must have both its private and public keys initialized;
  // future behavior of this EVPPKEYPrivateKey object is undefined if this
  // precondition is not met.
  EVPPKEYPrivateKey(UniquePKEY&& pkey_ptr, const std::string& scheme);
  virtual ~EVPPKEYPrivateKey() override;

  virtual std::string serialize() const override;

  virtual std::string sign(const std::string& message) const override;
};

// Wrapper class for OpenSSL Crypto's EVP_PKEY objects implementing
// concord::util::openssl_crypto::AsymmetricPublicKey.
class EVPPKEYPublicKey : public AsymmetricPublicKey {
 private:
  UniquePKEY pkey;
  std::string scheme_name;

 public:
  // Copying and moving EVPPKEYPublicKey objects is currently unimplemented;
  // note the default implementations for copy and move operations would not be
  // appropriate for this class since the EVP_PKEY object it contains must be
  // memory-managed through the OpenSSL library rather than by default
  // mechanisms.
  EVPPKEYPublicKey(const EVPPKEYPublicKey& other) = delete;
  EVPPKEYPublicKey(const EVPPKEYPublicKey&& other) = delete;
  EVPPKEYPublicKey& operator=(const EVPPKEYPublicKey& other) = delete;
  EVPPKEYPublicKey& operator=(const EVPPKEYPublicKey&& other) = delete;

  // Note the constructed EVPPKEYPublicKey takes ownership of the pointer it is
  // constructed with. A precondition of this constructor is that the provided
  // EVP_PKEY object must its public key initialized; future behavior of this
  // EVPPKEYPublicKey object is undefined if this precondition is not met.
  EVPPKEYPublicKey(UniquePKEY&& pkey_ptr, const std::string& scheme);
  virtual ~EVPPKEYPublicKey() override;

  virtual std::string serialize() const override;

  virtual bool verify(const std::string& message, const std::string& signature) const override;
};

std::pair<std::unique_ptr<AsymmetricPrivateKey>, std::unique_ptr<AsymmetricPublicKey>>
generateAsymmetricCryptoKeyPairById(int id, const std::string& scheme_name);

// Function for pseudorandomly generating a key pair for asymmetric
// cryptography, given an asymmetric cryptography scheme listed in
// KPermittedAsymmetricCryptoSchemes.
// Throws an std::invalid_argument if the scheme_name parameter is not permitted
// and supported (that is, if scheme_name is not listed in
// kPermittedAsymmetricCryptoSchemes). May throw an
// OpenSSLError if the underlying OpenSSL Crypto
// library unexpectedly reports a failure while attempting this operation.
std::pair<std::unique_ptr<AsymmetricPrivateKey>, std::unique_ptr<AsymmetricPublicKey>> generateAsymmetricCryptoKeyPair(
    const std::string& scheme_name);

// Deserialize a private key serialized with an implementation of
// AsymmetricPrivateKey's implementation of serialize from a given string.
// This function will handle determining what scheme the key is for and
// constructing an AsymmetricPrivateKey object of the appropriate type. This
// function may throw an invalid_argument exception if it fails to parse the
// serialized key. It may also throw an OpenSSLError
// if the underlying OpenSSL Crypto library unexpectedly reports a failure while
// attempting this operation.
std::unique_ptr<AsymmetricPrivateKey> deserializePrivateKey(const std::string& input);

std::unique_ptr<AsymmetricPrivateKey> deserializePrivateKeyFromPem(const std::string& path_to_file,
                                                                   const std::string& spec);
// Reads a private key in PEM format, saved in a string. This function is needed
// for the encrypted configuration case. There the PK is saved in encrypted
// file. It is decrytped into a string and this function loads it in
// AsymmetricPrivateKey.
std::unique_ptr<AsymmetricPrivateKey> deserializePrivateKeyFromPemString(const std::string& pem_contents,
                                                                         const std::string& scheme);
std::unique_ptr<AsymmetricPublicKey> deserializePublicKeyFromPem(const std::string& path_to_file,
                                                                 const std::string& spec);

// Deserialize a public key serialized with an implementation of
// AsymmetricPublicKey's implementation of serialize from a given string.
// This function will handle determining what scheme the key is for and
// constructing an AsymmetricPublicKey object of the appropriate type. This
// function may throw an invalid_argument exception if it fails to parse the
// serialized key. It may also throw an OpenSSLError
// if the underlying OpenSSL Crypto library unexpectedly reports a failure while
// attempting this operation.
std::unique_ptr<AsymmetricPublicKey> deserializePublicKey(const std::string& input);

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

}  // namespace concord::crypto::openssl
