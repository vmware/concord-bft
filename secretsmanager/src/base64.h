// Concord
//
// Copyright (c) 2020 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the
// "License").  You may not use this product except in compliance with the
// Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.
//
// This convenience header combines different block implementations.

#pragma once

#include <string>
#include <vector>

#include <openssl/ec.h>
#include <openssl/evp.h>
#include <openssl/pem.h>

namespace concord::secretsmanager {

/*
 * Encode message to Base64 string.
 * @param msgBytes (input) Message to be encoded.
 * @return Base64 encoded message.
 */
std::string base64Enc(const std::vector<uint8_t>& msgBytes);

/*
 * Decode Base64 string.
 * @param b64message (input) Base64 encoded message.
 * @return Decoded, but encrypted message.
 */
std::vector<uint8_t> base64Dec(const std::string& b64message);

size_t calcDecodeLength(const char* b64message);

/**
 * @brief Extracts the message part of PEM file by deleting the HEADER and FOOTER parts, if exist.
 *
 * @param b64message Base64 encoded message.
 * @param header Header to be removed.
 * @param footer Footer to be removed.
 * @return std::string Extracted message.
 */
std::string stripPemHeaderFooter(const std::string_view b64message,
                                 const std::string_view header,
                                 const std::string_view footer);
}  // namespace concord::secretsmanager
