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

#include "utt_config.hpp"

using namespace libutt;

namespace utt_config {
////////////////////////////////////////////////////////////////////////
std::string JoinStr(const std::vector<std::string>& v, char delim) {
  if (v.empty()) return std::string{};
  if (v.size() == 1) return v[0];
  std::stringstream ss;
  for (size_t i = 0; i < v.size() - 1; ++i) ss << v[i] << delim;
  ss << v.back();
  return ss.str();
}

////////////////////////////////////////////////////////////////////////
std::vector<std::string> SplitStr(const std::string& s, char delim) {
  if (s.empty()) return std::vector<std::string>{};
  std::string token;
  std::vector<std::string> result;
  std::stringstream ss(s);
  while (std::getline(ss, token, delim)) result.emplace_back(std::move(token));
  return result;
}
}  // namespace utt_config

using namespace utt_config;

////////////////////////////////////////////////////////////////////////
bool UTTClientConfig::operator==(const UTTClientConfig& other) const {
  return pids_ == other.pids_ && wallet_ == other.wallet_;
}
bool UTTClientConfig::operator!=(const UTTClientConfig& other) const { return !(*this == other); }

std::ostream& operator<<(std::ostream& os, const UTTClientConfig& cfg) {
  os << JoinStr(cfg.pids_, ',') << '\n';
  os << cfg.wallet_;
  return os;
}

std::istream& operator>>(std::istream& is, UTTClientConfig& cfg) {
  std::string pids;
  std::getline(is, pids);
  if (pids.empty()) throw std::runtime_error("Trying to deserialize UTTClientConfig with no pids!");
  cfg.pids_ = SplitStr(pids, ',');

  is >> cfg.wallet_;

  return is;
}

////////////////////////////////////////////////////////////////////////
bool UTTReplicaConfig::operator==(const UTTReplicaConfig& other) const {
  return p_ == other.p_ && rpk_ == other.rpk_ && bskShare_ == other.bskShare_;
}
bool UTTReplicaConfig::operator!=(const UTTReplicaConfig& other) const { return !(*this == other); }

std::ostream& operator<<(std::ostream& os, const UTTReplicaConfig& cfg) {
  os << JoinStr(cfg.pids_, ',') << '\n';
  os << cfg.p_;
  os << cfg.rpk_;
  os << cfg.bpk_;
  os << cfg.bskShare_;
  return os;
}

std::istream& operator>>(std::istream& is, UTTReplicaConfig& cfg) {
  std::string pids;
  std::getline(is, pids);
  if (pids.empty()) throw std::runtime_error("Trying to deserialize UTTReplicaConfig with no pids!");
  cfg.pids_ = SplitStr(pids, ',');

  is >> cfg.p_;
  is >> cfg.rpk_;
  is >> cfg.bpk_;
  is >> cfg.bskShare_;
  return is;
}