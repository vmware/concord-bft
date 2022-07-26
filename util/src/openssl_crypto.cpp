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

#include "openssl_crypto.hpp"

#include <openssl/ec.h>
#include <openssl/pem.h>
#include <stdio.h>
#include <cassert>

using concord::util::openssl_utils::AsymmetricPrivateKey;
using concord::util::openssl_utils::AsymmetricPublicKey;
using concord::util::openssl_utils::kExpectedSHA256HashLengthInBytes;
using concord::util::openssl_utils::UnexpectedOpenSSLCryptoFailureException;
using concord::util::openssl_utils::UniquePKEY;
using concord::util::openssl_utils::UniqueOpenSSLContext;
using concord::util::openssl_utils::UniqueOpenSSLBIGNUM;
using concord::util::openssl_utils::UniqueOpenSSLBNCTX;
using concord::util::openssl_utils::UniqueOpenSSLECKEY;
using concord::util::openssl_utils::UniqueOpenSSLECPOINT;
using std::invalid_argument;
using std::pair;
using std::string;
using std::unique_ptr;

// Deleter classes for using smart pointers that correctly manage objects from
// the OpenSSL Crypto library, given OpenSSL Crypto objects use free functions
// provided by the library rather than normal destructors.
class OPENSSLStringDeleter {
 public:
  void operator()(char* string) const { OPENSSL_free(string); }
};

// Wrapper class for OpenSSL Crypto's EVP_PKEY objects implementing
// concord::util::openssl_crypto::AsymmetricPrivateKey.
class EVPPKEYPrivateKey : public AsymmetricPrivateKey {
 private:
  UniquePKEY pkey;
  string scheme_name;

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
  EVPPKEYPrivateKey(UniquePKEY&& pkey_ptr, const string& scheme) : pkey(move(pkey_ptr)), scheme_name(scheme) {}
  virtual ~EVPPKEYPrivateKey() override {}

  virtual string serialize() const override {
    if (EVP_PKEY_get0_EC_KEY(pkey.get())) {
      const EC_KEY* ec_key = EVP_PKEY_get0_EC_KEY(pkey.get());
      const BIGNUM* private_key = EC_KEY_get0_private_key(ec_key);
      if (!private_key) {
        throw UnexpectedOpenSSLCryptoFailureException(
            "OpenSSL Crypto unexpectedly failed to fetch the private key from "
            "an elliptic curve key pair.");
      }
      unique_ptr<char, OPENSSLStringDeleter> hex_chars(BN_bn2hex(private_key), OPENSSLStringDeleter());
      if (!hex_chars) {
        throw UnexpectedOpenSSLCryptoFailureException(
            "OpenSSL Crypto unexpectedly failed to convert a private key to "
            "hexadecimal for serialization.");
      }
      string private_key_data = string(hex_chars.get());
      return "PRIVATE_KEY:" + scheme_name + ":" + private_key_data;
    } else {
      // This case should not be reachable as all currently supported schemes
      // should be handled above.
      throw invalid_argument(
          "Failed to serialize AsymmetricPrivateKey object; failed to identify "
          "supported key type; key serialization logic may be out of sync with "
          "key creation logic.");
    }
  }

  virtual std::string sign(const std::string& message) const override {
    UniqueOpenSSLContext digest_context(EVP_MD_CTX_new());
    if (!digest_context) {
      throw UnexpectedOpenSSLCryptoFailureException(
          "OpenSSL Crypto unexpectedly failed to allocate a message digest "
          "context object.");
    }
    if (!EVP_DigestSignInit(digest_context.get(), nullptr, EVP_sha256(), nullptr, pkey.get())) {
      throw UnexpectedOpenSSLCryptoFailureException(
          "OpenSSL Crypto unexpectedly failed to initialize a message digest "
          "context object.");
    }
    if (!EVP_DigestSignUpdate(digest_context.get(), message.data(), message.length())) {
      throw UnexpectedOpenSSLCryptoFailureException(
          "OpenSSL Crypto unexpectedly failed to hash a message for signing.");
    }
    size_t signature_length;
    if (!EVP_DigestSignFinal(digest_context.get(), nullptr, &signature_length)) {
      throw UnexpectedOpenSSLCryptoFailureException(
          "OpenSSL Crypto unexpectedly failed to determine the maximum length "
          "for a signature signed with asymmetric cryptography.");
    }
    string signature(signature_length, (char)0);
    if (!EVP_DigestSignFinal(
            digest_context.get(), reinterpret_cast<unsigned char*>(signature.data()), &signature_length)) {
      throw UnexpectedOpenSSLCryptoFailureException(
          "OpenSSL Crypto unexpectedly failed to sign a message digest with "
          "asymmetric cryptography.");
    }
    if (signature_length > signature.length()) {
      // This should probably never happen.
      throw UnexpectedOpenSSLCryptoFailureException(
          "OpenSSL Crypto unexpectedly reports having produced an asymmetric "
          "cryptography signature of length greater than the maximum signature "
          "length it previously reported.");
    }
    // It is, at least theoretically, possible for the produced signature to be
    // shorter than the maximum signature length.
    signature.resize(signature_length);

    return signature;
  }
};

// Wrapper class for OpenSSL Crypto's EVP_PKEY objects implementing
// concord::util::openssl_crypto::AsymmetricPublicKey.
class EVPPKEYPublicKey : public AsymmetricPublicKey {
 private:
  UniquePKEY pkey;
  string scheme_name;

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
  EVPPKEYPublicKey(UniquePKEY&& pkey_ptr, const string& scheme) : pkey(move(pkey_ptr)), scheme_name(scheme) {}
  virtual ~EVPPKEYPublicKey() override {}

  virtual string serialize() const override {
    if (EVP_PKEY_get0_EC_KEY(pkey.get())) {
      const EC_KEY* ec_key = EVP_PKEY_get0_EC_KEY(pkey.get());
      const EC_POINT* public_key = EC_KEY_get0_public_key(ec_key);
      if (!public_key) {
        throw UnexpectedOpenSSLCryptoFailureException(
            "OpenSSL Crypto unexpectedly failed to fetch the public key from "
            "an elliptic curve key object.");
      }
      const EC_GROUP* ec_group = EC_KEY_get0_group(ec_key);
      if (!ec_group) {
        throw UnexpectedOpenSSLCryptoFailureException(
            "OpenSSL Crypto unexpectedly failed to fetch the elliptic curve "
            "group from an elliptic curve key object.");
      }
      UniqueOpenSSLBNCTX big_num_context(BN_CTX_new());
      if (!big_num_context) {
        throw UnexpectedOpenSSLCryptoFailureException(
            "OpenSSL Crypto unexpectedly failed to allocate a big number "
            "context object needed to convert an elliptic curve point to "
            "hexadecimal for serialization.");
      }
      unique_ptr<char, OPENSSLStringDeleter> hex_chars(
          EC_POINT_point2hex(ec_group, public_key, EC_GROUP_get_point_conversion_form(ec_group), big_num_context.get()),
          OPENSSLStringDeleter());
      if (!hex_chars) {
        throw UnexpectedOpenSSLCryptoFailureException(
            "OpenSSL Crypto unexpectedly failed to convert a public key to "
            "hexadecimal for serialization.");
      }
      string public_key_data = string(hex_chars.get());
      return "PUBLIC_KEY:" + scheme_name + ":" + public_key_data;
    } else {
      // This case should not be reachable as all currently supported schemes
      // should be handled above.
      throw invalid_argument(
          "Failed to serialize AsymmetricPublicKey object; failed to identify "
          "supported key type; key serialization logic may be out of sync with "
          "key creation logic.");
    }
  }

  virtual bool verify(const std::string& message, const std::string& signature) const override {
    UniqueOpenSSLContext digest_context(EVP_MD_CTX_new());
    if (!digest_context) {
      throw UnexpectedOpenSSLCryptoFailureException(
          "OpenSSL Crypto unexpectedly failed to allocate a message digest "
          "context object.");
    }
    if (!EVP_DigestVerifyInit(digest_context.get(), nullptr, EVP_sha256(), nullptr, pkey.get())) {
      throw UnexpectedOpenSSLCryptoFailureException(
          "OpenSSL Crypto unexpectedly failed to initialize a message digest "
          "context object.");
    }
    if (!EVP_DigestVerifyUpdate(digest_context.get(), message.data(), message.length())) {
      throw UnexpectedOpenSSLCryptoFailureException(
          "OpenSSL Crypto unexpectedly failed to hash a message to validate a "
          "signature over it.");
    }

    // Note we only return true if EVP_DigestVerifyFinal returns 1, and not if
    // it returns a non-1, non-0 value; only 1 indicates a verification success,
    // other non-1, non-0 values indicate unexpected failures (and 0 indicates
    // that the signature was not correct cryptographically).
    return (EVP_DigestVerifyFinal(digest_context.get(),
                                  reinterpret_cast<const unsigned char*>(signature.data()),
                                  signature.length()) == 1);
  }
};

pair<unique_ptr<AsymmetricPrivateKey>, unique_ptr<AsymmetricPublicKey>>
concord::util::openssl_utils::generateAsymmetricCryptoKeyPair(const string& scheme_name) {
  if (scheme_name == "secp256r1") {
    // prime256v1 is an alternative name for the same curve parameters as
    // secp256r1; prime256v1 happens to be the name OpenSSL's Crypto library
    // uses for a possible parameter to EC_KEY_new_by_curve_name.
    UniqueOpenSSLECKEY key_pair(EC_KEY_new_by_curve_name(NID_X9_62_prime256v1));
    UniqueOpenSSLECKEY public_key(EC_KEY_new_by_curve_name(NID_X9_62_prime256v1));
    if (!key_pair || !public_key) {
      throw UnexpectedOpenSSLCryptoFailureException(
          "OpenSSL Crypto unexpectedly failed to allocate and prepare an "
          "elliptic curve key object.");
    }
    if (!EC_KEY_generate_key(key_pair.get())) {
      throw UnexpectedOpenSSLCryptoFailureException(
          "OpenSSL Crypto unexpectedly failed to generate a new Elliptic Curve "
          "key pair.");
    }
    const EC_POINT* public_key_raw = EC_KEY_get0_public_key(key_pair.get());
    if (!public_key_raw) {
      throw UnexpectedOpenSSLCryptoFailureException(
          "OpenSSL Crypto unexpectedly failed to get a pointer to the public "
          "key for a generated elliptic curve key pair.");
    }
    if (!EC_KEY_set_public_key(public_key.get(), public_key_raw)) {
      throw UnexpectedOpenSSLCryptoFailureException(
          "OpenSSL Crypto unexpectedly failed to set the public key for an "
          "empty allocated elliptic curve key object.");
    }
    UniquePKEY private_pkey(EVP_PKEY_new());
    UniquePKEY public_pkey(EVP_PKEY_new());
    if (!private_pkey || !public_pkey) {
      throw UnexpectedOpenSSLCryptoFailureException(
          "OpenSSL Crypto unexpectedly failed to allocate and prepare a "
          "high-level key object.");
    }
    if (!EVP_PKEY_set1_EC_KEY(private_pkey.get(), key_pair.get()) ||
        !EVP_PKEY_set1_EC_KEY(public_pkey.get(), key_pair.get())) {
      throw UnexpectedOpenSSLCryptoFailureException(
          "OpenSSL Crypto unexpectedly failed to initialize a high-level key "
          "object given an elliptic curve key object.");
    }

    return pair<unique_ptr<AsymmetricPrivateKey>, unique_ptr<AsymmetricPublicKey>>(
        new EVPPKEYPrivateKey(move(private_pkey), scheme_name), new EVPPKEYPublicKey(move(public_pkey), scheme_name));
  } else {
    throw invalid_argument(
        "Cannot generate asymmetric cryptography key pair for cryptography "
        "scheme \"" +
        scheme_name +
        "\"; this scheme is either not recognized, not supported, or has not "
        "been permitted for use in Concord in consideration of our security "
        "requirements (which could be because it does not meet them, or could "
        "simply be because we have not assessed this particular scheme).");
  }
}

const string kLegalHexadecimalDigits = "0123456789ABCDEFabcdef";

unique_ptr<AsymmetricPrivateKey> concord::util::openssl_utils::deserializePrivateKey(const string& input) {
  size_t first_split_point = input.find(':');
  size_t second_split_point = input.rfind(':');
  if ((first_split_point == string::npos) || (first_split_point == second_split_point)) {
    throw invalid_argument(
        "Failed to deserialize private key: input is malformatted and could "
        "not be parsed.");
  }
  string private_key_label = input.substr(0, first_split_point);
  string scheme_name = input.substr(first_split_point + 1, second_split_point - first_split_point - 1);
  string private_key_data = input.substr(second_split_point + 1);
  if (private_key_label != "PRIVATE_KEY") {
    throw invalid_argument(
        "Failed to deserialize private key: input does not appear to be "
        "labeled as a private key.");
  }
  if (scheme_name == "secp256r1") {
    if (private_key_data.find_first_not_of(kLegalHexadecimalDigits) != string::npos) {
      throw invalid_argument(
          "Failed to deserialize private key of secp256r1 scheme: given key "
          "data (\"" +
          private_key_data + "\") is not purely hexadecimal.");
    }

    // prime256v1 is an alternative name for the same curve parameters as
    // secp256r1; prime256v1 happens to be the name OpenSSL's Crypto library
    // uses for a possible parameter to EC_KEY_new_by_curve_name.
    UniqueOpenSSLECKEY ec_key(EC_KEY_new_by_curve_name(NID_X9_62_prime256v1));
    if (nullptr == ec_key.get()) {
      throw UnexpectedOpenSSLCryptoFailureException(
          "OpenSSL Crypto unexpectedly failed to allocate and prepare an "
          "elliptic curve key object.");
    }
    UniqueOpenSSLBIGNUM private_key(nullptr);
    BIGNUM* allocated_private_key = nullptr;
    int hex_parse_return_code = BN_hex2bn(&allocated_private_key, private_key_data.c_str());
    private_key.reset(allocated_private_key);
    if (!hex_parse_return_code) {
      throw invalid_argument(
          "Failed to deserialize private key of secp256r1 scheme: OpenSSL was "
          "unable to parse a private elliptic curve key from the private "
          "hexadecimal key data.");
    }
    if (!private_key) {
      throw UnexpectedOpenSSLCryptoFailureException(
          "OpenSSL Crypto unexpectedly failed to allocate a big number object "
          "and initialize it from hexadecimal data.");
    }
    if (!EC_KEY_set_private_key(ec_key.get(), private_key.get())) {
      throw UnexpectedOpenSSLCryptoFailureException(
          "OpenSSL Crypto unexpectedly failed to load the private key into an "
          "elliptic curve key object.");
    }

    // OpenSSL Crypto expects us to load the public key if the private key is
    // loaded, so we compute the public key from the private key here.
    const EC_GROUP* ec_group = EC_KEY_get0_group(ec_key.get());
    if (!ec_group) {
      throw UnexpectedOpenSSLCryptoFailureException(
          "OpenSSL Crypto unexpectedly failed to fetch the elliptic curve "
          "group for an elliptic curve key object.");
    }
    UniqueOpenSSLECPOINT public_key(EC_POINT_new(ec_group));
    if (!public_key) {
      throw UnexpectedOpenSSLCryptoFailureException(
          "OpenSSL Crypto unexpectedly failed to allocate an elliptic curve "
          "point object.");
    }
    UniqueOpenSSLBNCTX public_key_derivation_context(BN_CTX_new());
    if (!public_key_derivation_context) {
      throw UnexpectedOpenSSLCryptoFailureException(
          "OpenSSL Crypto unexpectedly failed to allocate a big number context "
          "object needed to derive a public elliptic curve key from the "
          "corresponding private key.");
    }
    if (!EC_POINT_mul(
            ec_group, public_key.get(), private_key.get(), nullptr, nullptr, public_key_derivation_context.get())) {
      throw UnexpectedOpenSSLCryptoFailureException(
          "OpenSSL Crypto unexpectedly failed to derive a public elliptic "
          "curve key from the corresponding private key.");
    }
    if (!EC_KEY_set_public_key(ec_key.get(), public_key.get())) {
      throw UnexpectedOpenSSLCryptoFailureException(
          "OpenSSL Crypto unexpectedly failed to a public key to an elliptic "
          "curve key object.");
    }

    UniquePKEY private_pkey(EVP_PKEY_new());
    if (!private_pkey) {
      throw UnexpectedOpenSSLCryptoFailureException(
          "OpenSSL Crypto unexpectedly failed to allocate and prepare a "
          "high-level key object.");
    }
    if (!EVP_PKEY_set1_EC_KEY(private_pkey.get(), ec_key.get())) {
      throw UnexpectedOpenSSLCryptoFailureException(
          "OpenSSL Crypto unexpectedly failed to initialize a high-level key "
          "object given an elliptic curve key object.");
    }

    return unique_ptr<AsymmetricPrivateKey>(new EVPPKEYPrivateKey(move(private_pkey), scheme_name));
  } else {
    throw invalid_argument("Failed to deserialize private key: scheme \"" + scheme_name +
                           "\" is either not recognized, not supported, or has not been permitted "
                           "for use in Concord in consideration of our security requirements "
                           "(which could be because it does not meet them, or could simply be "
                           "because we have not assessed this particular scheme).");
  }
}

std::unique_ptr<AsymmetricPrivateKey> concord::util::openssl_utils::deserializePrivateKeyFromPem(
    const std::string& path_to_file, const std::string& scheme) {
  if (scheme != "secp256r1") {
    throw invalid_argument("Failed to deserialize private key: scheme \"" + scheme +
                           "\" is either not recognized, not supported, or has not been permitted "
                           "for use in Concord in consideration of our security requirements "
                           "(which could be because it does not meet them, or could simply be "
                           "because we have not assessed this particular scheme).");
  }
  unique_ptr<FILE, decltype(&fclose)> fp(fopen(path_to_file.c_str(), "r"), fclose);
  if (nullptr == fp) {
    return nullptr;
  }
  UniqueOpenSSLECKEY pkey(EC_KEY_new());

  if (!PEM_read_ECPrivateKey(fp.get(), reinterpret_cast<EC_KEY**>(pkey.get()), nullptr, nullptr)) {
    throw UnexpectedOpenSSLCryptoFailureException(
        "OpenSSL Crypto unexpectedly failed to parse the private key file "
        "for " +
        path_to_file);
  }

  UniquePKEY private_pkey(EVP_PKEY_new());
  if (!EVP_PKEY_set1_EC_KEY(private_pkey.get(), pkey.get())) {
    throw UnexpectedOpenSSLCryptoFailureException(
        "OpenSSL Crypto unexpectedly failed to initialize a high-level key "
        "object given an elliptic curve key object.");
  }
  return std::make_unique<EVPPKEYPrivateKey>(std::move(private_pkey), "secp256r1");
}

std::unique_ptr<AsymmetricPrivateKey> concord::util::openssl_utils::deserializePrivateKeyFromPemString(
    const std::string& pem_contents, const std::string& scheme) {
  if (scheme != "secp256r1") {
    throw invalid_argument("Failed to deserialize private key from string: scheme \"" + scheme +
                           "\" is either not recognized, not supported, or has not been permitted "
                           "for use in Concord in consideration of our security requirements "
                           "(which could be because it does not meet them, or could simply be "
                           "because we have not assessed this particular scheme).");
  }

  EVP_PKEY* pkey = EVP_PKEY_new();
  if (!pkey) {
    throw UnexpectedOpenSSLCryptoFailureException("Memory allocation errror: EVP_PKEY_new()");
  }

  BIO* bio = BIO_new(BIO_s_mem());
  if (!bio) {
    EVP_PKEY_free(pkey);
    throw UnexpectedOpenSSLCryptoFailureException("Memory allocation error: BIO_new()");
  }

  int bio_write_ret = BIO_write(bio, pem_contents.c_str(), pem_contents.size());
  if (bio_write_ret <= 0) {
    EVP_PKEY_free(pkey);
    BIO_free(bio);
    throw UnexpectedOpenSSLCryptoFailureException("Error writing PEM string to BIO object.");
  }

  if (!PEM_read_bio_PrivateKey(bio, &pkey, NULL, NULL)) {
    EVP_PKEY_free(pkey);
    BIO_free(bio);
    throw UnexpectedOpenSSLCryptoFailureException("Error reading PEM string.");
  }

  EC_KEY* eckey = EVP_PKEY_get1_EC_KEY(pkey);
  if (!eckey) {
    BIO_free(bio);
    throw UnexpectedOpenSSLCryptoFailureException("Error getting EC KEY: EVP_PKEY_get1_EC_KEY()");
  }
  EVP_PKEY_free(pkey);
  BIO_free(bio);

  UniquePKEY private_pkey(EVP_PKEY_new());
  if (!EVP_PKEY_set1_EC_KEY(private_pkey.get(), eckey)) {
    EC_KEY_free(eckey);
    throw UnexpectedOpenSSLCryptoFailureException(
        "OpenSSL Crypto unexpectedly failed to initialize a high-level key "
        "object given an elliptic curve key object.");
  }
  EC_KEY_free(eckey);
  return std::make_unique<EVPPKEYPrivateKey>(std::move(private_pkey), "secp256r1");
}

std::unique_ptr<AsymmetricPublicKey> concord::util::openssl_utils::deserializePublicKeyFromPem(
    const std::string& path_to_file, const std::string& scheme) {
  if (scheme != "secp256r1") {
    throw invalid_argument("Failed to deserialize private key: scheme \"" + scheme +
                           "\" is either not recognized, not supported, or has not been permitted "
                           "for use in Concord in consideration of our security requirements "
                           "(which could be because it does not meet them, or could simply be "
                           "because we have not assessed this particular scheme).");
  }
  unique_ptr<FILE, decltype(&fclose)> fp(fopen(path_to_file.c_str(), "r"), fclose);
  if (nullptr == fp) {
    throw UnexpectedOpenSSLCryptoFailureException(
        "OpenSSL Crypto unexpectedly failed to open the public key file for " + path_to_file);
  }
  UniqueOpenSSLECKEY pkey(EC_KEY_new());

  if (!PEM_read_EC_PUBKEY(fp.get(), reinterpret_cast<EC_KEY**>(pkey.get()), nullptr, nullptr)) {
    throw UnexpectedOpenSSLCryptoFailureException(
        "OpenSSL Crypto unexpectedly failed to parse the public key file for " + path_to_file);
  }

  UniquePKEY public_pkey(EVP_PKEY_new());
  if (!EVP_PKEY_set1_EC_KEY(public_pkey.get(), pkey.get())) {
    throw UnexpectedOpenSSLCryptoFailureException(
        "OpenSSL Crypto unexpectedly failed to initialize a high-level key "
        "object given an elliptic curve key object.");
  }
  return std::make_unique<EVPPKEYPublicKey>(std::move(public_pkey), "secp256r1");
}

unique_ptr<AsymmetricPublicKey> concord::util::openssl_utils::deserializePublicKey(const string& input) {
  size_t first_split_point = input.find(':');
  size_t second_split_point = input.rfind(':');
  if ((first_split_point == string::npos) || (first_split_point == second_split_point)) {
    throw invalid_argument(
        "Failed to deserialize public key: input is malformatted and could not "
        "be parsed.");
  }
  string public_key_label = input.substr(0, first_split_point);
  string scheme_name = input.substr(first_split_point + 1, second_split_point - first_split_point - 1);
  string public_key_data = input.substr(second_split_point + 1);
  if (public_key_label != "PUBLIC_KEY") {
    throw invalid_argument(
        "Failed to deserialize public key: input does not appear to be labeled "
        "as a public key.");
  }
  if (scheme_name == "secp256r1") {
    if (public_key_data.find_first_not_of(kLegalHexadecimalDigits) != string::npos) {
      throw invalid_argument(
          "Failed to deserialize public key of secp256r1 scheme: given key "
          "data (\"" +
          public_key_data + "\") is not purely hexadecimal.");
    }

    // prime256v1 is an alternative name for the same curve parameters as
    // secp256r1; prime256v1 happens to be the name OpenSSL's Crypto library
    // uses for a possible parameter to EC_KEY_new_by_curve_name.
    UniqueOpenSSLECKEY ec_key(EC_KEY_new_by_curve_name(NID_X9_62_prime256v1));
    if (nullptr == ec_key.get()) {
      throw UnexpectedOpenSSLCryptoFailureException(
          "OpenSSL Crypto unexpectedly failed to allocate and prepare an "
          "elliptic curve key object.");
    }
    const EC_GROUP* ec_group = EC_KEY_get0_group(ec_key.get());
    if (!ec_group) {
      throw UnexpectedOpenSSLCryptoFailureException(
          "OpenSSL Crypto unexpectedly failed to fetch the elliptic curve "
          "group from an elliptic curve key object.");
    }
    UniqueOpenSSLECPOINT public_key(EC_POINT_new(ec_group));
    if (!public_key) {
      throw UnexpectedOpenSSLCryptoFailureException(
          "OpenSSL Crypto unexpectedly failed to allocate an elliptic curve "
          "point object.");
    }
    UniqueOpenSSLBNCTX big_num_context(BN_CTX_new());
    if (!big_num_context) {
      throw UnexpectedOpenSSLCryptoFailureException(
          "OpenSSL Crypto unexpectedly failed to allocate a big number context "
          "object needed to convert hexadecimal data to an elliptic cuve point "
          "for deserialization.");
    }
    if (!EC_POINT_hex2point(ec_group, public_key_data.c_str(), public_key.get(), big_num_context.get())) {
      throw invalid_argument(
          "Failed to deserialize public key of secp256r1 scheme: OpenSSL "
          "failed to parse an elliptic curve private key from hexadecimal "
          "data..");
    }
    if (!EC_KEY_set_public_key(ec_key.get(), public_key.get())) {
      throw UnexpectedOpenSSLCryptoFailureException(
          "OpenSSL Crypto unexpectedly failed to load the public key to an "
          "elliptic curve key object.");
    }

    UniquePKEY public_pkey(EVP_PKEY_new());
    if (!public_pkey) {
      throw UnexpectedOpenSSLCryptoFailureException(
          "OpenSSL Crypto unexpectedly failed to allocate and prepare a "
          "high-level key object.");
    }
    if (!EVP_PKEY_set1_EC_KEY(public_pkey.get(), ec_key.get())) {
      throw UnexpectedOpenSSLCryptoFailureException(
          "OpenSSL Crypto unexpectedly failed to initialize a high-level key "
          "object given an elliptic curve key object.");
    }

    return unique_ptr<AsymmetricPublicKey>(new EVPPKEYPublicKey(move(public_pkey), scheme_name));
  } else {
    throw invalid_argument("Failed to deserialize public key: scheme \"" + scheme_name +
                           "\" is either not recognized, not supported, or has not been permitted "
                           "for use in Concord in consideration of our security requirements "
                           "(which could be because it does not meet them, or could simply be "
                           "because we have not assessed this particular scheme).");
  }
}

string concord::util::openssl_utils::computeSHA256Hash(const string& data) {
  return computeSHA256Hash(data.data(), data.length());
}

string concord::util::openssl_utils::computeSHA256Hash(const char* data, size_t length) {
  UniqueOpenSSLContext digest_context(EVP_MD_CTX_new());
  if (!digest_context) {
    throw UnexpectedOpenSSLCryptoFailureException(
        "OpenSSL Crypto unexpectedly failed to allocate a message digest "
        "context object.");
  }
  if (!EVP_DigestInit_ex(digest_context.get(), EVP_sha256(), nullptr)) {
    throw UnexpectedOpenSSLCryptoFailureException(
        "OpenSSL Crypto unexpectedly failed to initialize a message digest "
        "context object for SHA-256.");
  }
  if (!EVP_DigestUpdate(digest_context.get(), data, length)) {
    throw UnexpectedOpenSSLCryptoFailureException("OpenSSL Crypto unexpectedly failed to hash data with SHA-256.");
  }

  ConcordAssert(kExpectedSHA256HashLengthInBytes == EVP_MD_size(EVP_sha256()));

  string hash(kExpectedSHA256HashLengthInBytes, (char)0);
  unsigned int hash_bytes_written;
  if (!EVP_DigestFinal_ex(digest_context.get(), reinterpret_cast<unsigned char*>(hash.data()), &hash_bytes_written)) {
    throw UnexpectedOpenSSLCryptoFailureException(
        "OpenSSL Crypto unexpectedly failed to retrieve the resulting hash "
        "from a message digest context object for an SHA-256 hash operation.");
  }
  if (hash_bytes_written > kExpectedSHA256HashLengthInBytes) {
    // This should probably never happen.
    throw UnexpectedOpenSSLCryptoFailureException(
        "OpenSSL Crypto unexpectedly reported retrieving a hash value longer "
        "than that expected for an SHA-256 hash from an SHA-256 hash "
        "operation.");
  }

  return hash;
}