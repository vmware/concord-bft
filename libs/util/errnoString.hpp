// Concord
//
// Copyright (c) 2020 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").  You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#pragma once

#include <string.h>

#include <string>

namespace concordUtils {

// Thread-safe errno string generation.
inline std::string errnoString(int errNum) {
  constexpr auto size = 128 + 1;
  char buf[size];
#ifdef _GNU_SOURCE
  const auto ret = strerror_r(errNum, buf, size);
  // Documentation on the return value of the GNU version is not clear - assume it can return nullptr.
  if (ret) {
    return ret;
  }
#else
  if (strerror_r(errNum, buf, size) == 0) {
    // POSIX documentation is not clear on whether the buffer is always null-terminated. Be conservative and terminate
    // it, even if it will contain some garbage data.
    buf[size - 1] = '\0';
    return buf;
  }
#endif
  return std::string{};
}

}  // namespace concordUtils
