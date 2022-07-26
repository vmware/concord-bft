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

#include <string>
#include <memory>
#include <fstream>

#include <openssl/bio.h>
#include <openssl/ec.h>
#include <openssl/pem.h>
#include <openssl/x509.h>
#include <openssl/evp.h>

#include "crypto_utils.hpp"

namespace concord::crypto::openssl {

class CertificateUtils {
 public:
  static std::string generateSelfSignedCert(const std::string& origin_cert_path,
                                            const std::string& pub_key,
                                            const std::string& signing_key);
  /**
   * @brief Verifies the signature of certificate 'cert' using public key 'pub_key'.
   *
   * @param cert [input] Certificate to be validated.
   * @param pub_key [input] Public key to be used to validate the certificate.
   * @return bool Verification result.
   */
  static bool verifyCertificate(X509& cert, const std::string& pub_key);

  /**
   * @brief Verifies the certificate 'cert_to_verify' with another certificate present in 'cert_root_directory'.
   *
   * @param cert_to_verify [input] Certificate to be validated.
   * @param cert_root_directory [input] Location of the other certificate to be verified.
   * @param remote_peer_id [output]
   * @param conn_type [output] Certificate type (server or client).
   * @param use_unified_certs [input]
   * @return Verification result.
   */
  static bool verifyCertificate(const X509& cert_to_verify,
                                const std::string& cert_root_directory,
                                uint32_t& remote_peer_id,
                                std::string& conn_type,
                                bool use_unified_certs);
};

class OpenSSLCryptoImpl {
 public:
  static OpenSSLCryptoImpl& instance() {
    static OpenSSLCryptoImpl crypto;
    return crypto;
  }

  OpenSSLCryptoImpl() = default;
  ~OpenSSLCryptoImpl() = default;

  /**
   * @brief Generates an EdDSA asymmetric key pair (private-public key pair).
   *
   * @param fmt Output key format.
   * @return pair<string, string> Private-Public key pair.
   */
  std::pair<std::string, std::string> generateEdDSAKeyPair(
      const concord::util::crypto::KeyFormat fmt = concord::util::crypto::KeyFormat::HexaDecimalStrippedFormat) const;

  /**
   * @brief Generates an EdDSA PEM file from hexadecimal key pair (private-public key pair).
   *
   * @param key_pair Key pair in hexa-decimal format.
   * @return pair<string, string>
   */
  std::pair<std::string, std::string> EdDSAHexToPem(const std::pair<std::string, std::string>& hex_key_pair) const;

  /**
   * @brief If the key string contains 'BEGIN' token, then it is PEM format, else HEX format.
   *
   * @param key
   * @return KeyFormat Returns the key's format.
   * @todo The check to identify the format is not generic. Need to implement some generic way
   * identifying the input format.
   */
  concord::util::crypto::KeyFormat getFormat(const std::string& key) const;
};
}  // namespace concord::crypto::openssl
