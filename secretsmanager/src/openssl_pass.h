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

#include <vector>
#include <string>

#include "key_params.h"

namespace concord::secretsmanager {

KeyParams deriveKeyPass(const std::string_view pass,
                        const std::vector<uint8_t>& salt,
                        const uint32_t key_size,
                        const uint32_t iv_size);

}  // namespace concord::secretsmanager