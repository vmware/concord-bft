// Concord
//
// Copyright (c) 2019 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the sub-component's license, as noted in the
// LICENSE file.

#pragma once
#include <set>
#include <vector>
#include <type_traits>
#include <deque>

namespace concord {

/**
 *  type traits
 *  may be extended with other container types to meet future needs
 */
template <typename T>
struct is_set : std::false_type {};
template <typename T>
struct is_set<std::set<T>> : std::true_type {};
template <typename T>
struct is_vector : std::false_type {};
template <typename T>
struct is_vector<std::vector<T>> : std::true_type {};
template <typename T>
struct is_deque : std::false_type {};
template <typename T>
struct is_deque<std::deque<T>> : std::true_type {};

template <class T, T v>
struct std_container {
  static constexpr const T value = v;
  constexpr T operator()() const noexcept { return value; }
};
template <class T>
struct is_std_container : std_container<bool, is_set<T>::value || is_vector<T>::value || is_deque<T>::value> {};

template <class T, T v>
constexpr const T std_container<T, v>::value;

// Taken from https://stackoverflow.com/a/22759544
template <typename S, typename T>
class is_streamable {
  template <typename SS, typename TT>
  static auto test(int) -> decltype(std::declval<SS&>() << std::declval<TT>(), std::true_type());

  template <typename, typename>
  static auto test(...) -> std::false_type;

 public:
  static const bool value = decltype(test<S, T>(0))::value;
};

}  // namespace concord
