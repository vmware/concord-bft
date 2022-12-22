// Concord
//
// Copyright (c) 2022 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the sub-component's license, as noted in the LICENSE
// file.

#pragma once

#include <cstdint>
#include <string>
#include <cstddef>

namespace concord::crypto {

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
  // object as a string. May throw an OpenSSLError if
  // the underlying OpenSSL Crypto library unexpectedly reports a failure while
  // attempting this operation.
  virtual std::string serialize() const = 0;

  // Produce and return a signature of a given message. Note both the message
  // and signature are logically byte strings handled via std::strings. May
  // throw an OpenSSLError if the underlying OpenSSL
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
  // object as a string. May throw an OpenSSLError if
  // the underlying OpenSSL Crypto library unexpectedly reports a failure while
  // attempting this operation.
  virtual std::string serialize() const = 0;

  // Verify a signature of a message allegedly signed with the private key
  // corresponding to this public key. Note both the message and alleged
  // signature are logically byte strings handled via std::strings. Returns
  // true if this private key validates the signature as being a valid signature
  // of the message under the corresponding public key, and false otherwise. May
  // throw an OpenSSLError if the underlying OpenSSL
  // Crypto library unexpectedly reports a failure while attempting this
  // operation.
  virtual bool verify(const std::string& message, const std::string& signature) const = 0;

 protected:
  AsymmetricPublicKey() {}
};

enum SignatureAlgorithm : uint32_t { Uninitialized = 0, EdDSA = 2, RSA = 3 };

enum class KeyFormat : uint16_t { HexaDecimalStrippedFormat, PemFormat };
enum class CurveType : uint16_t { secp256k1, secp384r1 };

static constexpr const size_t Ed25519PrivateKeyByteSize = 32UL;
static constexpr const size_t Ed25519PublicKeyByteSize = 32UL;
static constexpr const size_t Ed25519SignatureByteSize = 64UL;

/**
 * @brief Generates an EdDSA asymmetric key pair (private-public key pair).
 *
 * @param fmt Output key format.
 * @return pair<string, string> Private-Public key pair.
 */
std::pair<std::string, std::string> generateEdDSAKeyPair(const KeyFormat fmt = KeyFormat::HexaDecimalStrippedFormat);

/**
 * @brief Generates an EdDSA PEM file from hexadecimal key pair (private-public key pair).
 *
 * @param key_pair Key pair in hexa-decimal format.
 * @return pair<string, string>
 */
std::pair<std::string, std::string> EdDSAHexToPem(const std::pair<std::string, std::string>& hex_key_pair);

/**
 * @brief If the key string contains 'BEGIN' token, then it is PEM format, else HEX format.
 *
 * @param key
 * @return KeyFormat Returns the key's format.
 * @todo The check to identify the format is not generic. Need to implement some generic way
 * identifying the input format.
 */
KeyFormat getFormat(const std::string& key_str);

/**
 * @brief Validates the key.
 *
 * @param keyType Key type to be validated.
 * @param key Key to be validate.
 * @param expectedSize Size of the key to be validated.
 * @return Validation result.
 */
bool isValidKey(const std::string& keyType, const std::string& key, size_t expectedSize);
}  // namespace concord::crypto
