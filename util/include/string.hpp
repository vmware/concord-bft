// Concord
//
// Copyright (c) 2018 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#pragma once

#include <string>

namespace concord {
namespace util {

/**
 * conversion from string to integral types
 * TODO float, double
 */
template<typename T>
T to(const std::string& s){assert(false && "no suitable specialization"); return static_cast<T>(0);}

template<> bool               to<> (const std::string& s) {return std::stoi(s);  }
template<> std::uint16_t      to<> (const std::string& s) {return std::stoi(s);  }
template<> int                to<> (const std::string& s) {return std::stoi(s);  }
template<> long               to<> (const std::string& s) {return std::stol(s);  }
template<> unsigned long      to<> (const std::string& s) {return std::stoul(s); }
template<> long long          to<> (const std::string& s) {return std::stoll(s); }
template<> unsigned long long to<> (const std::string& s) {return std::stoull(s);}
}
}
