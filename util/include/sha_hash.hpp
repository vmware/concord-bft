// Concord
//
// Copyright (c) 2020 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the
// LICENSE file.

#pragma once

#include "evp_hash.hpp"

namespace concord {
namespace util {

using SHA3_256 = detail::EVPHash<EVP_sha3_256, 32>;
using SHA2_256 = detail::EVPHash<EVP_sha256, 32>;

}  // namespace util
}  // namespace concord
