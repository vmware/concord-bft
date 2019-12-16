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
#include <assert.h>
#include <algorithm>

namespace concord {
namespace util {

/**
 * conversion from string to integral types
 * TODO float, double
 */
template <typename T>
T to(const std::string& s) {
  assert(false && "no suitable specialization");
  return static_cast<T>(0);
}

template <>
inline bool to<>(const std::string& s) {
  return std::stoi(s);
}
template <>
inline std::uint16_t to<>(const std::string& s) {
  return std::stoi(s);
}
template <>
inline int to<>(const std::string& s) {
  return std::stoi(s);
}
template <>
inline unsigned int to<>(const std::string& s) {
  return std::stoul(s);
}
template <>
inline long to<>(const std::string& s) {
  return std::stol(s);
}
template <>
inline unsigned long to<>(const std::string& s) {
  return std::stoul(s);
}
template <>
inline long long to<>(const std::string& s) {
  return std::stoll(s);
}
template <>
inline unsigned long long to<>(const std::string& s) {
  return std::stoull(s);
}

inline std::string& ltrim_inplace(std::string& s){
  s.erase(s.begin(), std::find_if(s.begin(), s.end(), [](int ch) { return !std::isspace(ch);}));
  return s;
}
inline std::string& rtrim_inplace(std::string &s) {
    s.erase(std::find_if(s.rbegin(), s.rend(), [](int ch) { return !std::isspace(ch);}).base(), s.end());
    return s;
}
inline std::string& trim_inplace(std::string& str){return ltrim_inplace(rtrim_inplace(str));}
inline std::string ltrim(const std::string& s)
{
  auto it =  std::find_if( s.begin() , s.end() , [](char ch){ return !std::isspace(ch);});
  return std::string(it, s.end());
}
inline std::string rtrim(const std::string& s)
{
  auto it =  std::find_if( s.rbegin() , s.rend() , [](char ch){ return !std::isspace(ch);});
  return std::string( s.begin(), it.base());
}
inline std::string trim(const std::string& s){return ltrim(rtrim(s));}

}  // namespace util
}  // namespace concord
