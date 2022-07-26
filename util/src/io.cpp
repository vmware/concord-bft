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

#include "io.hpp"

namespace concord::io {

using std::string;

string readFile(FILE* file, uint64_t maxBytes) {
  string result;

  while (true) {
    auto ch = std::fgetc(file);

    if ((ch == EOF) || (result.size() == maxBytes)) {
      break;
    }
    result += static_cast<char>(ch);
  }
  return result;
}
}  // namespace concord::io
