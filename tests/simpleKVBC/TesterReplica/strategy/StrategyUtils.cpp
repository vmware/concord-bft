// Concord
//
// Copyright (c) 2018-2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include "StrategyUtils.hpp"

namespace concord::kvbc {
namespace strategy {
std::string StrategyUtils::getRandomStingOfLength(size_t len) {
  std::vector<char> alphabet{'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'p',
                             'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z', 'a', 'b', 'c', 'd', 'e', 'f',
                             'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v',
                             'w', 'x', 'y', 'z', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', ' '};

  std::mt19937_64 eng{std::random_device{}()};
  std::uniform_int_distribution<> dist{0, static_cast<int>(alphabet.size())};
  std::string res = "";
  while (len > 0) {
    res += alphabet[dist(eng)];
    len--;
  }
  return res;
}

}  // end of namespace strategy
}  // end of namespace concord::kvbc
