// UTT Common API
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

#include <map>
#include <string>
#include <vector>

namespace utt {

// [TODO-UTT] All types are tentative

/// @brief The complete configuration required to deploy a UTT instance, contains public and secret data
using Configuration = std::vector<uint8_t>;

/// @brief The public part of a UTT instance configuration visible to all users
using PublicConfig = std::vector<uint8_t>;

/// @brief A user generated commitment used as input for registration in the UTT instance
using UserRegistrationInput = std::vector<uint8_t>;

/// @brief A signature on the user's full registration commitment
using RegistrationSig = std::vector<uint8_t>;

/// @brief A system generated part of the user's PRF key
// [TODO-UTT] Should be a byte vector similar to other opaque data types
using S2 = std::vector<uint64_t>;

/// @brief The privacy budget determines how much in value can a user transfer anonymously to other users.
using PrivacyBudget = std::vector<uint8_t>;

/// @brief Signature on a privacy budget object
using PrivacyBudgetSig = std::vector<uint8_t>;

/// @brief A signature on a single output of a transaction
using TxOutputSig = std::vector<uint8_t>;

/// @brief The signatures associated with the ordered outputs of a transaction
using TxOutputSigs = std::vector<TxOutputSig>;

/// @brief A transaction and its type
struct Transaction {
  enum class Type { Undefined = 0, Mint = 1, Transfer = 2, Burn = 3 } type_ = Type::Undefined;
  std::vector<uint8_t> data_;
};

}  // namespace utt