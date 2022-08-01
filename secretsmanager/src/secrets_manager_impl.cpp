// Concord
//
// Copyright (c) 2020 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the
// "License").  You may not use this product except in compliance with the
// Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.
//
// This convenience header combines different block implementations.

#include "secrets_manager_impl.h"

#include <string>
#include <fstream>
#include <sstream>

namespace concord::secretsmanager {

std::string ISecretsManagerImpl::readFile(std::string_view path) {
  std::ifstream in(path.data());

  if (!in) throw std::runtime_error("Error opening file " + std::string{path});

  return readFile(in);
}

std::string ISecretsManagerImpl::readFile(const std::ifstream& file) {
  if (!file) throw std::runtime_error("Bad ifstream");

  std::stringstream stream;
  stream << file.rdbuf();

  return stream.str();
}

void ISecretsManagerImpl::writeFile(std::string_view file_path, const std::string& content) {
  std::ofstream out(file_path.data());
  if (!out.is_open()) {
    throw std::runtime_error("Failed to open file for writing: " + std::string(file_path));
  }
  out << content;
}

}  // namespace concord::secretsmanager
