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
#include <openssl/bio.h>
#include <openssl/ec.h>
#include <openssl/x509.h>
#include <openssl/evp.h>
#include "crypto/factory.hpp"

namespace concord::crypto {

std::string generateSelfSignedCert(const std::string& origin_cert_path,
                                   const std::string& pub_key,
                                   const std::string& signing_key,
                                   const SignatureAlgorithm signingAlgo);
/**
 * @brief Verifies the signature of certificate 'cert' using public key 'pub_key'.
 *
 * @param cert [input] Certificate to be validated.
 * @param pub_key [input] Public key to be used to validate the certificate.
 * @return bool Verification result.
 */
bool verifyCertificate(X509& cert, const std::string& pub_key);

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
bool verifyCertificate(const X509& cert_to_verify,
                       const std::string& cert_root_directory,
                       uint32_t& remote_peer_id,
                       std::string& conn_type,
                       bool use_unified_certs);

// valid field_name: "C"/"L"/"ST"/"O"/"OU"/"CN"
std::string getSubjectFieldByName(const std::string& cert_path, const std::string& attribute_name);
// This function accepts path to a cert bundle
std::vector<std::string> getSubjectFieldListByName(const std::string& cert_bundle_path,
                                                   const std::string& attribute_name);

/**
 * @brief Validates a certificate bundle chain. The certificate is composed of chained certificated in the following
 * form "-----BEGIN CERTIFICATE-----MIIC...-----END CERTIFICATE-----/n-----BEGIN CERTIFICATE-----MIIC...-----END
 * CERTIFICATE-----...-----BEGIN CERTIFICATE-----MIIC...-----END CERTIFICATE-----". The first certificate in the bundle
 * is the user certificate and the reset are the ordered chain such that cert1 <- cert2 <- ... <- ca_cert, where "<-"
 * represent a signature.
 *
 * @param x509_certs_chain_pem a certificate bundle
 * @return true if valid
 * @return false if not valid
 */
bool verifyCertificatesChain(const std::string& certificates_bundle);

/**
 * @brief Get the Certificate Public Key object
 *
 * @param cert_str a certificate bundle.
 * @param index the index of the certificate in the certificate bundle
 * @return std::string the encoded public key in pem format of the first certificate in the bundle
 */
std::string getCertificatePublicKey(const std::string& cert_str, uint64_t index);

/**
 * @brief Get the Subject Fields object
 *
 * @param cert_data a certificates bundle.
 * @param index the certificate index in the bundle
 * @return std::map<std::string, std::string>
 */
std::map<std::string, std::string> getSubjectFields(const std::string& cert_data, uint64_t index);

}  // namespace concord::crypto
