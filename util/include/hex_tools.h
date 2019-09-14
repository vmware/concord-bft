// Copyright 2018 VMware, all rights reserved

#ifndef CONCORD_BFT_UTIL_HEX_TOOLS_H_
#define CONCORD_BFT_UTIL_HEX_TOOLS_H_

#include <stdio.h>
#include <string>

namespace concordUtils {

std::ostream &hexPrint(std::ostream &s, const char* data, size_t size);

}  // namespace concordUtils

#endif  // CONCORD_BFT_UTIL_HEX_TOOLS_H_
