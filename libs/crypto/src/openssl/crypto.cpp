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

#include "crypto/openssl/crypto.hpp"

#include <openssl/ec.h>
#include <openssl/pem.h>
#include <stdio.h>
#include <cassert>

namespace concord::crypto::openssl {

using std::invalid_argument;
using std::pair;
using std::string;
using std::unique_ptr;

void OpenSSLAssert(bool expr, const std::string& msg) {
  if (!expr) {
    throw OpenSSLError(msg);
  }
}

// Note the constructed EVPPKEYPrivateKey takes ownership of the pointer it is
// constructed with. A precondition of this constructor is that the provided
// EVP_PKEY object must have both its private and public keys initialized;
// future behavior of this EVPPKEYPrivateKey object is undefined if this
// precondition is not met.
EVPPKEYPrivateKey::EVPPKEYPrivateKey(UniquePKEY&& pkey_ptr, const string& scheme)
    : pkey(move(pkey_ptr)), scheme_name(scheme) {}
EVPPKEYPrivateKey::~EVPPKEYPrivateKey() {}

string EVPPKEYPrivateKey::serialize() const {
  if (EVP_PKEY_get0_EC_KEY(pkey.get())) {
    const EC_KEY* ec_key = EVP_PKEY_get0_EC_KEY(pkey.get());
    const BIGNUM* private_key = EC_KEY_get0_private_key(ec_key);
    OpenSSLAssert(private_key, "failed to fetch the private key from an elliptic curve key pair.");

    unique_ptr<char, OPENSSLStringDeleter> hex_chars(BN_bn2hex(private_key), OPENSSLStringDeleter());
    OpenSSLAssert(hex_chars.get() != nullptr, "failed to convert a private key to hexadecimal for serialization.");

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

std::string EVPPKEYPrivateKey::sign(const std::string& message) const {
  UniqueContext digest_context(EVP_MD_CTX_new());
  OpenSSLAssert(digest_context.get() != nullptr, "failed to allocate a message digest context object.");
  OpenSSLAssert(EVP_DigestSignInit(digest_context.get(), nullptr, EVP_sha256(), nullptr, pkey.get()),
                "failed to initialize a message digest context object.");

  OpenSSLAssert(EVP_DigestSignUpdate(digest_context.get(), message.data(), message.length()),
                "failed to hash a message for signing.");
  size_t signature_length;
  OpenSSLAssert(EVP_DigestSignFinal(digest_context.get(), nullptr, &signature_length),
                "failed to determine the maximum length for a signature signed with asymmetric cryptography.");

  string signature(signature_length, (char)0);
  OpenSSLAssert(
      EVP_DigestSignFinal(digest_context.get(), reinterpret_cast<unsigned char*>(signature.data()), &signature_length),
      "failed to sign a message digest with asymmetric cryptography.");

  OpenSSLAssert(signature_length <= signature.length(),
                "reports having produced an asymmetric cryptography signature of length greater than the maximum "
                "signature length it previously reported.");

  // It is, at least theoretically, possible for the produced signature to be
  // shorter than the maximum signature length.
  signature.resize(signature_length);

  return signature;
}

EVPPKEYPublicKey::EVPPKEYPublicKey(UniquePKEY&& pkey_ptr, const string& scheme)
    : pkey(move(pkey_ptr)), scheme_name(scheme) {}
EVPPKEYPublicKey::~EVPPKEYPublicKey() {}

string EVPPKEYPublicKey::serialize() const {
  if (EVP_PKEY_get0_EC_KEY(pkey.get())) {
    const EC_KEY* ec_key = EVP_PKEY_get0_EC_KEY(pkey.get());
    const EC_POINT* public_key = EC_KEY_get0_public_key(ec_key);
    OpenSSLAssert(public_key != nullptr, "failed to fetch the public key from an elliptic curve key object.");

    const EC_GROUP* ec_group = EC_KEY_get0_group(ec_key);
    OpenSSLAssert(ec_group != nullptr, "failed to fetch the elliptic curve group from an elliptic curve key object.");

    UniqueBNCTX big_num_context(BN_CTX_new());
    OpenSSLAssert(big_num_context.get() != nullptr,
                  "failed to allocate a big number context object needed to convert an elliptic curve point to "
                  "hexadecimal for serialization.");

    unique_ptr<char, OPENSSLStringDeleter> hex_chars(
        EC_POINT_point2hex(ec_group, public_key, EC_GROUP_get_point_conversion_form(ec_group), big_num_context.get()),
        OPENSSLStringDeleter());
    OpenSSLAssert(hex_chars.get() != nullptr, "failed to convert a public key to hexadecimal for serialization.");

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

bool EVPPKEYPublicKey::verify(const std::string& message, const std::string& signature) const {
  UniqueContext digest_context(EVP_MD_CTX_new());
  OpenSSLAssert(digest_context.get() != nullptr, "failed to allocate a message digest context object.");
  OpenSSLAssert(EVP_DigestVerifyInit(digest_context.get(), nullptr, EVP_sha256(), nullptr, pkey.get()),
                "failed to initialize a message digest context object.");
  OpenSSLAssert(EVP_DigestVerifyUpdate(digest_context.get(), message.data(), message.length()),
                "failed to hash a message to validate a signature over it.");

  // Note we only return true if EVP_DigestVerifyFinal returns 1, and not if
  // it returns a non-1, non-0 value; only 1 indicates a verification success,
  // other non-1, non-0 values indicate unexpected failures (and 0 indicates
  // that the signature was not correct cryptographically).
  return (EVP_DigestVerifyFinal(
              digest_context.get(), reinterpret_cast<const unsigned char*>(signature.data()), signature.length()) == 1);
}

pair<unique_ptr<AsymmetricPrivateKey>, unique_ptr<AsymmetricPublicKey>> generateAsymmetricCryptoKeyPairById(
    int id, const std::string& scheme_name) {
  UniqueECKEY key_pair(EC_KEY_new_by_curve_name(id));
  UniqueECKEY public_key(EC_KEY_new_by_curve_name(id));
  OpenSSLAssert(key_pair.get() != nullptr && public_key.get() != nullptr,
                "failed to allocate and prepare an elliptic curve key object.");
  OpenSSLAssert(EC_KEY_generate_key(key_pair.get()), "failed to generate a new Elliptic Curve key pair.");

  const EC_POINT* public_key_raw = EC_KEY_get0_public_key(key_pair.get());
  OpenSSLAssert(public_key_raw, "failed to get a pointer to the public key for a generated elliptic curve key pair.");

  OpenSSLAssert(EC_KEY_set_public_key(public_key.get(), public_key_raw),
                "failed to set the public key for an empty allocated elliptic curve key object.");
  UniquePKEY private_pkey(EVP_PKEY_new());
  UniquePKEY public_pkey(EVP_PKEY_new());
  OpenSSLAssert(private_pkey.get() != nullptr && public_pkey.get() != nullptr,
                "failed to allocate and prepare a high-level key object.");

  OpenSSLAssert(EVP_PKEY_set1_EC_KEY(private_pkey.get(), key_pair.get()) == OPENSSL_SUCCESS &&
                    EVP_PKEY_set1_EC_KEY(public_pkey.get(), key_pair.get()) == OPENSSL_SUCCESS,
                "failed to initialize a high-level key object given an elliptic curve key object.");

  return pair<unique_ptr<AsymmetricPrivateKey>, unique_ptr<AsymmetricPublicKey>>(
      new EVPPKEYPrivateKey(move(private_pkey), scheme_name), new EVPPKEYPublicKey(move(public_pkey), scheme_name));
}

pair<unique_ptr<AsymmetricPrivateKey>, unique_ptr<AsymmetricPublicKey>> generateAsymmetricCryptoKeyPair(
    const string& scheme_name) {
  if (scheme_name == "secp256r1") {
    return generateAsymmetricCryptoKeyPairById(NID_X9_62_prime256v1, scheme_name);
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

unique_ptr<AsymmetricPrivateKey> deserializePrivateKey(const string& input) {
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
    UniqueECKEY ec_key(EC_KEY_new_by_curve_name(NID_X9_62_prime256v1));
    OpenSSLAssert(ec_key.get() != nullptr, "failed to allocate and prepare an elliptic curve key object.");

    UniqueBIGNUM private_key(nullptr);
    BIGNUM* allocated_private_key = nullptr;
    int hex_parse_return_code = BN_hex2bn(&allocated_private_key, private_key_data.c_str());
    private_key.reset(allocated_private_key);
    if (!hex_parse_return_code) {
      throw invalid_argument(
          "Failed to deserialize private key of secp256r1 scheme: OpenSSL was "
          "unable to parse a private elliptic curve key from the private "
          "hexadecimal key data.");
    }
    OpenSSLAssert(private_key.get() != nullptr,
                  "failed to allocate a big number object and initialize it from hexadecimal data.");
    OpenSSLAssert(EC_KEY_set_private_key(ec_key.get(), private_key.get()),
                  "failed to load the private key into an elliptic curve key object.");

    // OpenSSL Crypto expects us to load the public key if the private key is
    // loaded, so we compute the public key from the private key here.
    const EC_GROUP* ec_group = EC_KEY_get0_group(ec_key.get());
    OpenSSLAssert(ec_group != nullptr, "failed to fetch the elliptic curve group for an elliptic curve key object.");

    UniqueECPOINT public_key(EC_POINT_new(ec_group));
    OpenSSLAssert(public_key.get() != nullptr, "failed to allocate an elliptic curve point object.");

    UniqueBNCTX public_key_derivation_context(BN_CTX_new());
    OpenSSLAssert(public_key_derivation_context.get() != nullptr,
                  "failed to allocate a big number context object needed to derive a public elliptic curve key from "
                  "the corresponding private key.");
    OpenSSLAssert(
        EC_POINT_mul(
            ec_group, public_key.get(), private_key.get(), nullptr, nullptr, public_key_derivation_context.get()),
        "failed to derive a public elliptic curve key from the corresponding private key.");
    OpenSSLAssert(EC_KEY_set_public_key(ec_key.get(), public_key.get()),
                  "failed to a public key to an elliptic curve key object.");

    UniquePKEY private_pkey(EVP_PKEY_new());
    OpenSSLAssert(private_pkey.get() != nullptr, "failed to allocate and prepare a high-level key object.");
    OpenSSLAssert(EVP_PKEY_set1_EC_KEY(private_pkey.get(), ec_key.get()),
                  "failed to initialize a high-level key object given an elliptic curve key object.");

    return unique_ptr<AsymmetricPrivateKey>(new EVPPKEYPrivateKey(move(private_pkey), scheme_name));
  } else {
    throw invalid_argument("Failed to deserialize private key: scheme \"" + scheme_name +
                           "\" is either not recognized, not supported, or has not been permitted "
                           "for use in Concord in consideration of our security requirements "
                           "(which could be because it does not meet them, or could simply be "
                           "because we have not assessed this particular scheme).");
  }
}

std::unique_ptr<AsymmetricPrivateKey> deserializePrivateKeyFromPem(const std::string& path_to_file,
                                                                   const std::string& scheme) {
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
  UniqueECKEY pkey(EC_KEY_new());
  OpenSSLAssert(PEM_read_ECPrivateKey(fp.get(), reinterpret_cast<EC_KEY**>(pkey.get()), nullptr, nullptr),
                "failed to parse the private key file for " + path_to_file);

  UniquePKEY private_pkey(EVP_PKEY_new());
  OpenSSLAssert(EVP_PKEY_set1_EC_KEY(private_pkey.get(), pkey.get()),
                "failed to initialize a high-level key object given an elliptic curve key object.");
  return std::make_unique<EVPPKEYPrivateKey>(std::move(private_pkey), "secp256r1");
}

std::unique_ptr<AsymmetricPrivateKey> deserializePrivateKeyFromPemString(const std::string& pem_contents,
                                                                         const std::string& scheme) {
  if (scheme != "secp256r1") {
    throw invalid_argument("Failed to deserialize private key from string: scheme \"" + scheme +
                           "\" is either not recognized, not supported, or has not been permitted "
                           "for use in Concord in consideration of our security requirements "
                           "(which could be because it does not meet them, or could simply be "
                           "because we have not assessed this particular scheme).");
  }

  EVP_PKEY* pkey = EVP_PKEY_new();
  OpenSSLAssert(pkey, "Memory allocation errror: EVP_PKEY_new()");

  BIO* bio = BIO_new(BIO_s_mem());
  if (!bio) {
    EVP_PKEY_free(pkey);
    throw OpenSSLError("Memory allocation error: BIO_new()");
  }

  int bio_write_ret = BIO_write(bio, pem_contents.c_str(), pem_contents.size());
  if (bio_write_ret <= 0) {
    EVP_PKEY_free(pkey);
    BIO_free(bio);
    throw OpenSSLError("Error writing PEM string to BIO object.");
  }

  if (!PEM_read_bio_PrivateKey(bio, &pkey, NULL, NULL)) {
    EVP_PKEY_free(pkey);
    BIO_free(bio);
    throw OpenSSLError("Error reading PEM string.");
  }

  EC_KEY* eckey = EVP_PKEY_get1_EC_KEY(pkey);
  if (!eckey) {
    BIO_free(bio);
    throw OpenSSLError("Error getting EC KEY: EVP_PKEY_get1_EC_KEY()");
  }
  EVP_PKEY_free(pkey);
  BIO_free(bio);

  UniquePKEY private_pkey(EVP_PKEY_new());
  if (!EVP_PKEY_set1_EC_KEY(private_pkey.get(), eckey)) {
    EC_KEY_free(eckey);
    throw OpenSSLError(
        "failed to initialize a high-level key "
        "object given an elliptic curve key object.");
  }
  EC_KEY_free(eckey);
  return std::make_unique<EVPPKEYPrivateKey>(std::move(private_pkey), "secp256r1");
}

std::unique_ptr<AsymmetricPublicKey> deserializePublicKeyFromPem(const std::string& path_to_file,
                                                                 const std::string& scheme) {
  if (scheme != "secp256r1") {
    throw invalid_argument("Failed to deserialize private key: scheme \"" + scheme +
                           "\" is either not recognized, not supported, or has not been permitted "
                           "for use in Concord in consideration of our security requirements "
                           "(which could be because it does not meet them, or could simply be "
                           "because we have not assessed this particular scheme).");
  }
  unique_ptr<FILE, decltype(&fclose)> fp(fopen(path_to_file.c_str(), "r"), fclose);
  OpenSSLAssert(nullptr != fp, "failed to open the public key file for " + path_to_file);
  UniqueECKEY pkey(EC_KEY_new());

  OpenSSLAssert(PEM_read_EC_PUBKEY(fp.get(), reinterpret_cast<EC_KEY**>(pkey.get()), nullptr, nullptr),
                "failed to parse the public key file for " + path_to_file);

  UniquePKEY public_pkey(EVP_PKEY_new());
  OpenSSLAssert(EVP_PKEY_set1_EC_KEY(public_pkey.get(), pkey.get()),
                "failed to initialize a high-level key object given an elliptic curve key object.");
  return std::make_unique<EVPPKEYPublicKey>(std::move(public_pkey), "secp256r1");
}

unique_ptr<AsymmetricPublicKey> deserializePublicKey(const string& input) {
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
    UniqueECKEY ec_key(EC_KEY_new_by_curve_name(NID_X9_62_prime256v1));
    OpenSSLAssert(nullptr != ec_key.get(), "failed to allocate and prepare an elliptic curve key object.");
    const EC_GROUP* ec_group = EC_KEY_get0_group(ec_key.get());
    OpenSSLAssert(ec_group, "failed to fetch the elliptic curve group from an elliptic curve key object.");

    UniqueECPOINT public_key(EC_POINT_new(ec_group));
    OpenSSLAssert(public_key.get() != nullptr, "failed to allocate an elliptic curve point object.");
    UniqueBNCTX big_num_context(BN_CTX_new());
    OpenSSLAssert(big_num_context.get() != nullptr,
                  "failed to allocate a big number context object needed to convert hexadecimal data to an elliptic "
                  "curve point for deserialization.");
    if (!EC_POINT_hex2point(ec_group, public_key_data.c_str(), public_key.get(), big_num_context.get())) {
      throw invalid_argument(
          "Failed to deserialize public key of secp256r1 scheme: OpenSSL failed to parse an elliptic curve private key "
          "from hexadecimal data..");
    }
    OpenSSLAssert(EC_KEY_set_public_key(ec_key.get(), public_key.get()),
                  "failed to load the public key to an elliptic curve key object.");

    UniquePKEY public_pkey(EVP_PKEY_new());
    OpenSSLAssert(public_pkey.get() != nullptr, "failed to allocate and prepare a high-level key object.");
    OpenSSLAssert(EVP_PKEY_set1_EC_KEY(public_pkey.get(), ec_key.get()),
                  "failed to initialize a high-level key object given an elliptic curve key object.");
    return unique_ptr<AsymmetricPublicKey>(new EVPPKEYPublicKey(move(public_pkey), scheme_name));
  } else {
    throw invalid_argument("Failed to deserialize public key: scheme \"" + scheme_name +
                           "\" is either not recognized, not supported, or has not been permitted "
                           "for use in Concord in consideration of our security requirements "
                           "(which could be because it does not meet them, or could simply be "
                           "because we have not assessed this particular scheme).");
  }
}

string computeSHA256Hash(const string& data) { return computeSHA256Hash(data.data(), data.length()); }

string computeSHA256Hash(const char* data, size_t length) {
  UniqueContext digest_context(EVP_MD_CTX_new());
  OpenSSLAssert(digest_context.get() != nullptr, "failed to allocate a message digest context object.");
  OpenSSLAssert(EVP_DigestInit_ex(digest_context.get(), EVP_sha256(), nullptr),
                "failed to initialize a message digest context object for SHA-256.");
  OpenSSLAssert(EVP_DigestUpdate(digest_context.get(), data, length), "failed to hash data with SHA-256.");
  ConcordAssert(kExpectedSHA256HashLengthInBytes == EVP_MD_size(EVP_sha256()));

  string hash(kExpectedSHA256HashLengthInBytes, (char)0);
  unsigned int hash_bytes_written;
  OpenSSLAssert(
      EVP_DigestFinal_ex(digest_context.get(), reinterpret_cast<unsigned char*>(hash.data()), &hash_bytes_written),
      "failed to retrieve the resulting hash from a message digest context object for an SHA-256 hash operation.");
  OpenSSLAssert(
      hash_bytes_written <= kExpectedSHA256HashLengthInBytes,
      "reported retrieving a hash value longer than that expected for an SHA-256 hash from a SHA-256 hash operation.");

  return hash;
}

// Unique_ptr custom deleter. OPENSSL_FREE macro should be wrapped because of CRYPTO_free's signature inside
void _openssl_free(void* ptr) { OPENSSL_free(ptr); }

}  // namespace concord::crypto::openssl
