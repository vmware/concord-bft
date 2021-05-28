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
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.
//
// Definitions of cryptographic wrappers declared in
// thin-replica-client/lib/crypto_utils.hpp. All actual cryptographic
// implementations should be provided by trusted libraries such as OpenSSL
// Crypto.

#include "client/thin-replica-client/crypto_utils.hpp"

#include <openssl/evp.h>
#include <cassert>
#include <memory>

using std::string;
using std::unique_ptr;

namespace client::thin_replica_client {

// Deleter class for deleting EVP_MD_CTX objects; this is needed to construct
// smart pointers that correctly memory manage EVP_MD_CTX objects as they use a
// free function from the OpenSSL library rather than a C++ destructor.
class EVPMDCTXDeleter {
 public:
  void operator()(EVP_MD_CTX* obj) const { EVP_MD_CTX_free(obj); }
};

string ComputeSHA256Hash(const string& data) {
  unique_ptr<EVP_MD_CTX, EVPMDCTXDeleter> digest_context(EVP_MD_CTX_new(), EVPMDCTXDeleter());
  if (!digest_context) {
    throw UnexpectedCryptoImplementationFailureException(
        "OpenSSL Crypto unexpectedly failed to allocate a message digest "
        "context object.");
  }
  if (!EVP_DigestInit_ex(digest_context.get(), EVP_sha256(), nullptr)) {
    throw UnexpectedCryptoImplementationFailureException(
        "OpenSSL Crypto unexpectedly failed to initialize a message digest "
        "context object for SHA-256.");
  }
  if (!EVP_DigestUpdate(digest_context.get(), data.data(), data.length())) {
    throw UnexpectedCryptoImplementationFailureException(
        "OpenSSL Crypto unexpectedly failed to hash data with SHA-256.");
  }

  ConcordAssert(kExpectedSHA256HashLengthInBytes == EVP_MD_size(EVP_sha256()));

  string hash(kExpectedSHA256HashLengthInBytes, (char)0);
  unsigned int hash_bytes_written;
  if (!EVP_DigestFinal_ex(digest_context.get(), reinterpret_cast<unsigned char*>(hash.data()), &hash_bytes_written)) {
    throw UnexpectedCryptoImplementationFailureException(
        "OpenSSL Crypto unexpectedly failed to retrieve the resulting hash "
        "from a message digest context object for an SHA-256 hash operation.");
  }
  if (hash_bytes_written > kExpectedSHA256HashLengthInBytes) {
    // This should probably never happen.
    throw UnexpectedCryptoImplementationFailureException(
        "OpenSSL Crypto unexpectedly reported retrieving a hash value longer "
        "than that expected for an SHA-256 hash from an SHA-256 hash "
        "operation.");
  }

  return hash;
}

}  // namespace client::thin_replica_client
