// UTT Client API
//
// Copyright (c) 2020-2022 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the sub-component's license, as noted in the LICENSE
// file.

#pragma once

#include <string>
#include <memory>
#include <tuple>
#include <optional>
#include <grpcpp/grpcpp.h>
#include "wallet-api.grpc.pb.h"  // Generated from privacy-wallet-library/proto/api
#include <utt-client-api/ClientApi.hpp>

namespace utt::walletservice {
class Wallet {
 public:
  struct RegistrationInput {
    std::vector<uint8_t> rcm1;
    std::vector<uint8_t> rcm1_sig;
  };

  Wallet(std::string userId,
         const std::string& private_key,
         const std::string& public_key,
         const std::string& storage_path,
         const utt::PublicConfig& config);

  std::optional<RegistrationInput> generateRegistrationInput();
  bool updateRegistrationCommitment(const RegistrationSig& sig, const S2& s2);
  const std::string& getUserId() const;

 private:
  std::unique_ptr<utt::client::IStorage> storage_;
  std::string userId_;
  std::string private_key_;
  std::unique_ptr<utt::client::User> user_;
  bool registered_ = false;
};
}  // namespace utt::walletservice