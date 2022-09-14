// UTT Server API
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

namespace utt::server {

// [TODO-UTT] Move the configuration to a common source file
struct Configuration {
  std::map<std::string, std::string> encryptedCommitSecrets;
  std::map<std::string, std::string> encryptedRegistrationSecrets;
  std::string commitVerificationKey;
  std::string registrationVerificationKey;
  std::vector<uint8_t> publicParams;
};

struct Registrar {
  std::string secretKey;
  std::string publicKey;
  std::vector<uint8_t> publicParams;
};

struct Committer {
  std::string secretKey;
  std::string publicKey;
  std::vector<uint8_t> publicParams;
};

// Generate S2 based on a given s2 hint.
// hint is an unpredictable hash. s2 is computed deterministically from hint.
std::vector<uint8_t> generateS2(const std::vector<uint8_t>& hint);

// Creates the full RCM for a user with userId using the given rcm1 and s2.
std::vector<uint8_t> createRCM(const Registrar& registrar,
                               const std::string& userId,
                               const std::vector<uint8_t>& rcm1,
                               const std::vector<uint8_t>& s2);

// [TODO-UTT] Why don't we pass in the computed RCM instead of userId, s2 and rcm1?
std::vector<uint8_t> signRCM(const Registrar& registrar,
                             const std::string& userId,
                             const std::vector<uint8_t>& s2,
                             const std::vector<uint8_t>& rcm1);

// [TODO-UTT] When do we verify an rcm against the sig?
bool verifyRCM(const Registrar& registrar, const std::vector<uint8_t>& rcm, const std::vector<uint8_t>& rs);

bool verifyPartialRcmSig(const Registrar& registrar, const std::vector<uint8_t>& rcm, const std::vector<uint8_t>& rs);

// Returns the nullifiers of the input coins in a transaction
std::vector<std::string> getNullifiers(const std::vector<uint8_t>& tx);

// Verifies the correctness of the transactions
bool validateTx(const Committer& committer, const std::vector<uint8_t>& tx);

bool verifyTxOutputSig(const Committer& committer, const std::vector<uint8_t>& sig, const std::vector<uint8_t>& output);

bool verifyTxOutputSigShare(const Committer& committer,
                            const std::vector<uint8_t>& sigShare,
                            const std::vector<uint8_t>& output);

// Verifies the correctness of the transactions
// [TODO-UTT] Returns a sig share for each output
std::vector<std::vector<uint8_t>> signTxOutputs(const Committer& committer, const std::vector<uint8_t>& tx);

// Creates a budget token for a given amount, serial number nonce, expiration date and amount.
// The nonce is a value used to create a unique sn for the token.
std::vector<uint8_t> createBudgetToken(const Committer& committer,
                                       const std::string& nonce,
                                       const std::vector<uint8_t>& userId,
                                       uint64_t expire_time,
                                       uint64_t amount);

// returns true if the transaction has a budget
bool hasBudget(const std::vector<uint8_t>& tx);

// returns the expiration time of the budget coin within a transaction
// [TODO-UTT] What should be the type of the time object?
uint64_t getExpireTime(const std::vector<uint8_t>& tx);

// returns the pidHash, value and sequence number of the given mint operation
struct MintOpData {
  std::vector<uint8_t> pidHash;  // [TODO-UTT] Why the pid hash? How it is used?
  uint64_t value = 0;
  uint64_t seqNum = 0;  // [TODO-UTT] Why is it returning the seqNum? How it is used?
};
MintOpData getMintData(const std::vector<uint8_t>& mintOp);

// Gets the Burned coin nullifier
std::string getNullifier(const std::vector<uint8_t>& burnTx);

}  // namespace utt::server