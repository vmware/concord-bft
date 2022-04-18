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

#pragma once

#include <iostream>
#include <vector>

#include <utt/Params.h>
#include <utt/RegAuth.h>
#include <utt/Wallet.h>

////////////////////////////////////////////////////////////////////////
namespace utt_config {
std::string JoinStr(const std::vector<std::string>& v, char delim = ' ');
std::vector<std::string> SplitStr(const std::string& s, char delim = ' ');
}  // namespace utt_config

////////////////////////////////////////////////////////////////////////
struct UTTClientConfig {
  bool operator==(const UTTClientConfig& other) const;
  bool operator!=(const UTTClientConfig& other) const;

  std::vector<std::string> pids_;
  int initPublicBalance_ = 0;
  libutt::Wallet wallet_;
};
std::ostream& operator<<(std::ostream& os, const UTTClientConfig& cfg);
std::istream& operator>>(std::istream& is, UTTClientConfig& cfg);

////////////////////////////////////////////////////////////////////////
struct UTTReplicaConfig {
  bool operator==(const UTTReplicaConfig& other) const;
  bool operator!=(const UTTReplicaConfig& other) const;

  std::vector<std::string> pids_;
  int initPublicBalance_ = 0;
  libutt::Params p_;
  libutt::RegAuthPK rpk_;
  libutt::RandSigPK bpk_;
  libutt::RandSigShareSK bskShare_;
};
std::ostream& operator<<(std::ostream& os, const UTTReplicaConfig& cfg);
std::istream& operator>>(std::istream& is, UTTReplicaConfig& cfg);