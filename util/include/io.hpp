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

namespace concord::io {
/**
 * @brief Reads a file.
 *
 * @param file File stream to be read from.
 * @param maxBytes Maximum bytes of data to be read from the file.
 * @return std::string Contents of the file.
 */
std::string readFile(FILE* file, uint64_t maxBytes);
}  // namespace concord::io
