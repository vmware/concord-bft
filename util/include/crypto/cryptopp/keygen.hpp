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
#include "crypto/crypto.hpp"
#include <string>

namespace concord::crypto {
/**
 * @brief Generates an RSA PEM file from hexadecimal key pair (private-public key pair).
 *
 * @param key_pair Key pair in hexa-decimal format.
 * @return pair<string, string>
 */
std::pair<std::string, std::string> RsaHexToPem(const std::pair<std::string, std::string>& key_pair);

/**
 * @brief Generates an ECDSA PEM file from hexadecimal key pair (private-public key pair).
 *
 * @param key_pair Key pair in hexa-decimal format.
 * @return pair<string, string>
 */
std::pair<std::string, std::string> ECDSAHexToPem(const std::pair<std::string, std::string>& key_pair);

/**
 * @brief Generates an RSA asymmetric key pair (private-public key pair).
 *
 * @param fmt Output key format.
 * @return pair<string, string> Private-Public key pair.
 */
std::pair<std::string, std::string> generateRsaKeyPair(const KeyFormat fmt = KeyFormat::HexaDecimalStrippedFormat);

/**
 * @brief Generates an ECDSA asymmetric key pair (private-public key
 * pair).
 *
 * @param fmt Output key format.
 * @return pair<string, string> Private-Public key pair.
 */
std::pair<std::string, std::string> generateECDSAKeyPair(
    const KeyFormat fmt, concord::crypto::CurveType curve_type = concord::crypto::CurveType::secp256k1);

}  // namespace concord::crypto