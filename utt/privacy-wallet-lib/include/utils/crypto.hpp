// UTT Client API
//
// Copyright (c) 2020-2022 VMware, Inc. All Rights Reserved.
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
#include <map>
#include <vector>
namespace utt::client::utils::crypto {
/**
 * @brief Get the Certificate Public Key object
 *
 * @param cert a single certificate im pem format
 * @return std::string
 */
std::string getCertificatePublicKey(const std::string& cert);

/**
 * @brief Sign a given data vector using a given private key
 *
 * @param data bytes vector to sign on
 * @param pem_private_key a private key in pem format
 * @return std::vector<uint8_t>
 */
std::vector<uint8_t> signData(const std::vector<uint8_t>& data, const std::string& pem_private_key);

/**
 * @brief generate a hex string that represents a hash256 on a given string
 *
 * @param data the data to hash
 * @return std::string hex string of the hashed data
 */
std::string sha256(const std::vector<uint8_t>& data);
}  // namespace utt::client::utils::crypto