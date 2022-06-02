// Concord
//
// Copyright (c) 2018-2022 VMware, Inc. All Rights Reserved.
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

#include <sstream>

#include "Logger.hpp"

#include "UTTClientApp.hpp"

#define INDENT(width) std::setw(width) << ' '

/////////////////////////////////////////////////////////////////////////////////////////////////////
struct PrintContextError : std::runtime_error {
  PrintContextError(const std::string& msg) : std::runtime_error(msg) {}
};

struct UnexpectedPathTokenError : PrintContextError {
  UnexpectedPathTokenError(const std::string& token) : PrintContextError("Unexpected path token: " + token) {}
};

struct IndexEmptyObjectError : PrintContextError {
  IndexEmptyObjectError(const std::string& object) : PrintContextError("Indexed object is empty: " + object) {}
};

struct IndexOutOfBoundsError : PrintContextError {
  IndexOutOfBoundsError(const std::string& object) : PrintContextError("Index out of bounds for object: " + object) {}
};

/////////////////////////////////////////////////////////////////////////////////////////////////////
struct PrintContext {
  static constexpr const char* const s_PrefixComment = "# ";
  static constexpr const char* const s_PrefixList = "- ";
  static constexpr const char* const s_InfixKV = ": ";

  static void printList(const std::vector<std::string>& v, const std::string& delim = ", ") {
    if (!v.empty()) {
      for (int i = 0; i < (int)v.size() - 1; ++i) std::cout << v[i] << delim;
      std::cout << v.back();
    }
  }

  PrintContext() = default;

  void handleCommand(const UTTClientApp& app, const std::vector<std::string>& tokens);

  // PrintContext& push(std::string token) {
  //   //path_.emplace_back(std::move(token));
  //   // trace_.emplace_back(std::move(trace));
  //   return *this;
  // }

  // PrintContext& push(size_t idx) {
  //   //path_.emplace_back(std::to_string(idx));
  //   // trace_.emplace_back(std::move(trace));
  //   return *this;
  // }

  // PrintContext& pop() {
  //   //path_.pop_back();
  //   // trace_.pop_back();
  //   return *this;
  // }

  size_t getIndent() const { return path_.size() * 2; }

  std::string getCurrentPath() const { return path_; }

  void printComment(const char* comment) const {
    std::cout << INDENT(getIndent()) << s_PrefixComment << comment << '\n';
  }

  void printLink(const std::string& to, const std::string& preview = "<...>") const {
    std::cout << INDENT(getIndent()) << s_PrefixList << to << s_InfixKV << preview << " [";
    // printList(path_, "/");
    std::cout << '/' << to << "]\n";
  }

  void printKey(const char* key) const { std::cout << INDENT(getIndent()) << s_PrefixList << key << ":\n"; }

  template <typename T>
  void printValue(const T& value) const {
    std::cout << INDENT(getIndent()) << s_PrefixList << value << '\n';
  }

  void printKeyValue(const char* key) const {
    std::cout << INDENT(getIndent()) << s_PrefixList << key << s_InfixKV << "<...>\n";
  }

  template <typename T>
  void printKeyValue(const char* key, const T& value) const {
    std::cout << INDENT(getIndent()) << s_PrefixList << key << s_InfixKV << value << '\n';
  }

 private:
  static std::optional<std::string> extractPathToken(std::stringstream& ss);
  static size_t getValidIdx(size_t size, const std::string& object, const std::string& tokenIdx);

  std::string path_;
};