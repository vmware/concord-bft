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

#include "UTTServerApi.hpp"

namespace utt::server {

std::vector<uint8_t> generateS2(const std::vector<uint8_t>& hint) {
  // [TODO-UTT] Implement generateS2
  (void)hint;
  return std::vector<uint8_t>{};
}

std::vector<uint8_t> createRCM(const Registrar& registrar,
                               const std::string& userId,
                               const std::vector<uint8_t>& rcm1,
                               const std::vector<uint8_t>& s2) {
  // [TODO-UTT] Implement createRCM
  (void)registrar;
  (void)userId;
  (void)rcm1;
  (void)s2;
  return std::vector<uint8_t>{};
}

// [TODO-UTT] Why don't we pass in the computed RCM instead of userId, s2 and rcm1?
std::vector<uint8_t> signRCM(const Registrar& registrar,
                             const std::string& userId,
                             const std::vector<uint8_t>& s2,
                             const std::vector<uint8_t>& rcm1) {
  // [TODO-UTT] Implement signRCM
  (void)registrar;
  (void)userId;
  (void)s2;
  (void)rcm1;
  return std::vector<uint8_t>{};
}

// [TODO-UTT] When do we verify an rcm against the sig?
bool verifyRCM(const Registrar& registrar, const std::vector<uint8_t>& rcm, const std::vector<uint8_t>& rs) {
  // [TODO-UTT] Implement verifyRCM
  (void)registrar;
  (void)rcm;
  (void)rs;
  return false;
}

bool verifyPartialRcmSig(const Registrar& registrar, const std::vector<uint8_t>& rcm, const std::vector<uint8_t>& rs) {
  // [TODO-UTT] Implement verifyPartialRcmSig
  (void)registrar;
  (void)rcm;
  (void)rs;
  return false;
}

std::vector<std::string> getNullifiers(const std::vector<uint8_t>& tx) {
  // [TODO-UTT] Implement getNullifiers
  (void)tx;
  return std::vector<std::string>{};
}

// Verifies the correctness of the transactions
bool validateTx(const Committer& committer, const std::vector<uint8_t>& tx) {
  // [TODO-UTT] Implement validateTx
  (void)committer;
  (void)tx;
  return false;
}

bool verifyTxOutputSig(const Committer& committer,
                       const std::vector<uint8_t>& txOutput,
                       const std::vector<uint8_t>& sig) {
  // [TODO-UTT] Implement verifyTxOutputSig
  (void)committer;
  (void)txOutput;
  (void)sig;
  return false;
}

bool verifyTxOutputSigShare(const Committer& committer,
                            const std::vector<uint8_t>& txOutput,
                            const std::vector<uint8_t>& sigShare) {
  // [TODO-UTT] Implement verifyTxOutputSigShare
  (void)committer;
  (void)txOutput;
  (void)sigShare;
  return false;
}

// Verifies the correctness of the transactions
// [TODO-UTT] Returns a sig share for each output
std::vector<std::vector<uint8_t>> signTxOutputs(const Committer& committer, const std::vector<uint8_t>& tx) {
  // [TODO-UTT] Implement signTxOutputs
  (void)committer;
  (void)tx;
  return std::vector<std::vector<uint8_t>>{};
}

// Creates a budget token for a given amount, serial number nonce, expiration date and amount.
// The nonce is a value used to create a unique sn for the token.
std::vector<uint8_t> createBudgetToken(const Committer& committer,
                                       const std::string& nonce,
                                       const std::vector<uint8_t>& userId,
                                       uint64_t expireTime,
                                       uint64_t amount) {
  // [TODO-UTT] Implement createBudgetToken
  (void)committer;
  (void)nonce;
  (void)userId;
  (void)expireTime;
  (void)amount;
  return std::vector<uint8_t>{};
}

// returns true if the transaction has a budget
bool hasBudget(const std::vector<uint8_t>& tx) {
  // [TODO-UTT] Implement hasBudget
  (void)tx;
  return false;
}

// returns the expiration time of the budget coin within a transaction
// [TODO-UTT] What should be the type of the time object?
uint64_t getExpireTime(const std::vector<uint8_t>& tx) {
  // [TODO-UTT] Implement getExpireTime
  (void)tx;
  return 0;
}

MintOpData getMintData(const std::vector<uint8_t>& mintOp) {
  // [TODO-UTT] Implement getMintData
  (void)mintOp;
  return MintOpData{};
}

// Gets the Burned coin nullifier
std::string getNullifier(const std::vector<uint8_t>& burnTx) {
  // [TODO-UTT] Implement getNullifier
  (void)burnTx;
  return std::string{};
}

}  // namespace utt::server