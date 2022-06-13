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

#ifndef UTILS_OPENSSL_CRYPTO_HPP
#define UTILS_OPENSSL_CRYPTO_HPP

#include <climits>
#include <memory>
#include <ostream>
#include <string>
#include <vector>
#include <openssl/crypto.h>
#include <openssl/evp.h>
#include "memory.hpp"
#include "assertUtils.hpp"

namespace concord {
namespace util {
namespace openssl_utils {

// Note these utilities may use std::strings to pass arond byte strings; note
// this should work since C++ should guarantee std::string is a string of chars
// specifically and that the char type is exactly 1 byte in size. Some of these
// utilities may assume that chars are 8-bit bytes, which C++ does not
// guarantee, however.
static_assert(CHAR_BIT == 8);

// Interface for wrapper class for private keys in asymmetric cryptography
// schemes.
class AsymmetricPrivateKey {
 public:
  // As this is an abstract class written with the intention it should be used
  // only through polymorphic pointers, copying or moving AsymmetricPrivateKey
  // instances directly is not supported.
  AsymmetricPrivateKey(const AsymmetricPrivateKey& other) = delete;
  AsymmetricPrivateKey(const AsymmetricPrivateKey&& other) = delete;
  AsymmetricPrivateKey& operator=(const AsymmetricPrivateKey& other) = delete;
  AsymmetricPrivateKey& operator=(const AsymmetricPrivateKey&& other) = delete;

  virtual ~AsymmetricPrivateKey() {}

  // Serialize the private key to a printable format, returning the serialized
  // object as a string. May throw an UnexpectedOpenSSLCryptoFailureException if
  // the underlying OpenSSL Crypto library unexpectedly reports a failure while
  // attempting this operation.
  virtual std::string serialize() const = 0;

  // Produce and return a signature of a given message. Note both the message
  // and signature are logically byte strings handled via std::strings. May
  // throw an UnexpectedOpenSSLCryptoFailureException if the underlying OpenSSL
  // Crypto library unexpectedly reports a failure while attempting this
  // operation.
  virtual std::string sign(const std::string& message) const = 0;

 protected:
  AsymmetricPrivateKey() {}
};

// Interface for wrapper classes for public keys in asymmetric cryptography
// schemes.
class AsymmetricPublicKey {
 public:
  // As this is an abstract class written with the intention it should be used
  // only through polymorphic pointers, copying or moving AsymmetricPublicKey
  // instances directly is not supported.
  AsymmetricPublicKey(const AsymmetricPublicKey& other) = delete;
  AsymmetricPublicKey(const AsymmetricPublicKey&& other) = delete;
  AsymmetricPublicKey& operator=(const AsymmetricPublicKey& other) = delete;
  AsymmetricPublicKey& operator=(const AsymmetricPublicKey&& other) = delete;

  virtual ~AsymmetricPublicKey() {}

  // Serialize the public key to a printable format, returning the serialized
  // object as a string. May throw an UnexpectedOpenSSLCryptoFailureException if
  // the underlying OpenSSL Crypto library unexpectedly reports a failure while
  // attempting this operation.
  virtual std::string serialize() const = 0;

  // Verify a signature of a message allegedly signed with the private key
  // corresponding to this public key. Note both the message and alleged
  // signature are logically byte strings handled via std::strings. Returns
  // true if this private key validates the signature as being a valid signature
  // of the message under the corresponding public key, and false otherwise. May
  // throw an UnexpectedOpenSSLCryptoFailureException if the underlying OpenSSL
  // Crypto library unexpectedly reports a failure while attempting this
  // operation.
  virtual bool verify(const std::string& message, const std::string& signature) const = 0;

 protected:
  AsymmetricPublicKey() {}
};

// List of currently permitted and supported asymmetric cryptography schemes
// Concord might use. Note Concord maintainers should not add new schemes to
// this list and implement support for them unless those new schemes have been
// vetted for acceptability and adequacy for our purposes, considering our
// security requirements. May throw an UnexpectedOpenSSLCryptoFailureException
// if the underlying OpenSSL Crypto library unexpectedly reports a failure while
// attempting this operation.
const static std::vector<std::string> kPermittedAsymmetricCryptoSchemes({
    "secp256r1"  // secp256r1 is an ECDSA scheme. Apparently it is
                 // NIST-approved.
                 // TODO (Alex): (I am using this curve as it was recommended by
                 //              Ittai) Find reputable sources vouching for the
                 //              security of this curve and cite them here.
                 // TODO (Alex): Also consider seeing if VMware's security
                 //              department has any comments on the security of
                 //              this curve.
});

// Function for pseudorandomly generating a key pair for asymmetric
// cryptography, given an asymmetric cryptography scheme listed in
// KPermittedAsymmetricCryptoSchemes.
// Throws an std::invalid_argument if the scheme_name parameter is not permitted
// and supported (that is, if scheme_name is not listed in
// kPermittedAsymmetricCryptoSchemes). May throw an
// UnexpectedOpenSSLCryptoFailureException if the underlying OpenSSL Crypto
// library unexpectedly reports a failure while attempting this operation.
std::pair<std::unique_ptr<AsymmetricPrivateKey>, std::unique_ptr<AsymmetricPublicKey>> generateAsymmetricCryptoKeyPair(
    const std::string& scheme_name);

// Deserialize a private key serialized with an implementation of
// AsymmetricPrivateKey's implementation of serialize from a given string.
// This function will handle determining what scheme the key is for and
// constructing an AsymmetricPrivateKey object of the appropriate type. This
// function may throw an invalid_argument exception if it fails to parse the
// serialized key. It may also throw an UnexpectedOpenSSLCryptoFailureException
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
// serialized key. It may also throw an UnexpectedOpenSSLCryptoFailureException
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
// May throw an UnexpectedOpenSSLCryptoFailureException if the underlying
// OpenSSL Crypto library unexpectedly reports a failure while attempting this
// operation.
std::string computeSHA256Hash(const std::string& data);
std::string computeSHA256Hash(const char* data, size_t length);

// Specialized Exception types that may be thrown by the above utilities.

// Exception that may be thrown if a call into OpenSSLCrypto returns a failure
// unexpectedly.
class UnexpectedOpenSSLCryptoFailureException : public std::exception {
 private:
  std::string message;

 public:
  explicit UnexpectedOpenSSLCryptoFailureException(const std::string& what) : message(what) {}
  virtual const char* what() const noexcept override { return message.c_str(); }
};

using UniqueOpenSSLContext = custom_deleter_unique_ptr<EVP_MD_CTX, EVP_MD_CTX_free>;
using UniqueOpenSSLPKEYContext = custom_deleter_unique_ptr<EVP_PKEY_CTX, EVP_PKEY_CTX_free>;
using UniquePKEY = custom_deleter_unique_ptr<EVP_PKEY, EVP_PKEY_free>;

constexpr int OPENSSL_SUCCESS = 1;

}  // namespace openssl_utils
}  // namespace util
}  // namespace concord

#endif  // UTILS_OPENSSL_CRYPTO_HPP
