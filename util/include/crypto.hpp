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

#include <stdint.h>
#include <string>

namespace concord::crypto {

enum class KeyFormat : uint16_t { HexaDecimalStrippedFormat, PemFormat };
enum class CurveType : uint16_t { secp256k1, secp384r1 };

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
 * @brief Generates an RSA asymmetric key pair (private-public key pair).
 *
 * @param fmt Output key format.
 * @return pair<string, string> Private-Public key pair.
 */
std::pair<std::string, std::string> generateRsaKeyPair(const uint32_t sig_length,
                                                       const KeyFormat fmt = KeyFormat::HexaDecimalStrippedFormat);

/**
 * @brief Generates an ECDSA asymmetric key pair (private-public key pair).
 *
 * @param fmt Output key format.
 * @return pair<string, string> Private-Public key pair.
 */
std::pair<std::string, std::string> generateECDSAKeyPair(
    const KeyFormat fmt, concord::crypto::CurveType curve_type = concord::crypto::CurveType::secp256k1);

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
