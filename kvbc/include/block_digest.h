// Copyright (c) 2020 VMware, Inc. All Rights Reserved.

#pragma once

#include "bcstatetransfer/SimpleBCStateTransfer.hpp"

#include <array>
#include <cstdint>

namespace concord::kvbc {

inline constexpr auto BLOCK_DIGEST_SIZE = bftEngine::bcst::BLOCK_DIGEST_SIZE;

using BlockDigest = std::array<std::uint8_t, BLOCK_DIGEST_SIZE>;

}  // namespace concord::kvbc
