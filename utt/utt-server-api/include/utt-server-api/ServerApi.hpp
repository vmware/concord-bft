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

#include "utt-common-api/CommonApi.hpp"

namespace utt::server {

// [TODO-UTT] Properly annotate functions for automated doc generation.

// [TODO-UTT] All types are tentative

// [TODO-UTT] Consider using strong types for std::vector<uint8_t> or strings.
// This will allow not only type safety, but since we don't have binary serialization yet and
// internally libutt uses strings for serialize/deserialize these strong types can use strings as well and then be
// changed to byte vectors. Example: "struct Rcm { std::string bytes; }" becomes "struct Rcm { std::vector<uint8_t>
// bytes; }"

// Registrars comprise a multi-party authority that registers users.
struct Registrar {
  std::string secretKeyShare;
  std::string verificationKey;
  std::map<std::string, std::string>
      verificationKeyShares;  // [TODO-UTT] Check: I think we need this for partial sig verification
  std::vector<uint8_t> publicParams;
};

// Committers comprise a multi-party authority that issues tokens.
struct Committer {
  std::string secretKeyShare;
  std::string verificationKey;
  std::map<std::string, std::string>
      verificationKeyShares;  // [TODO-UTT] Check: I think we need this for partial sig verification
  std::vector<uint8_t> publicParams;
};

// Given a configuration create a registrar/committer
// [TODO-UTT] Add a separate function to validate a configuration?
Registrar createRegistrar(const utt::Configuration& config,
                          const std::string& registrarId,
                          const std::string& registrarSecret);
Committer createCommitter(const utt::Configuration& config,
                          const std::string& committerId,
                          const std::string& committerSecret);

// Generates S2 from a nonce value.
// S2 is the server-side generated part of the user's PRF key.
// The nonce should be generated server-side in a deterministic but unpredictable by the client way.
std::vector<uint8_t> generateS2(const std::vector<uint8_t>& nonce);

// Creates the full RCM for a user with userId using the given rcm1 and s2.
std::vector<uint8_t> createRcm(const Registrar& registrar,
                               const std::string& userId,
                               const std::vector<uint8_t>& rcm1,
                               const std::vector<uint8_t>& s2);

// [TODO-UTT] Why don't we pass in the computed RCM instead of userId, s2 and rcm1?
// Returns an rcm sign share
std::vector<uint8_t> signRcm(const Registrar& registrar,
                             const std::string& userId,
                             const std::vector<uint8_t>& s2,
                             const std::vector<uint8_t>& rcm1);

bool verifyRcmSig(const Registrar& registrar, const std::vector<uint8_t>& rcm, const std::vector<uint8_t>& rs);

bool verifyRcmSigShare(const Registrar& registrar,
                       const std::vector<uint8_t>& rcm,
                       const std::vector<uint8_t>& rsShare);

// Returns the nullifiers of the input coins in a transaction
std::vector<std::string> getNullifiers(const std::vector<uint8_t>& tx);

// Verifies the correctness of the transaction
bool validateTx(const Committer& committer, const std::vector<uint8_t>& tx);

bool verifyTxOutputSig(const Committer& committer,
                       const std::vector<uint8_t>& txOutput,
                       const std::vector<uint8_t>& sig);

bool verifyTxOutputSigShare(const Committer& committer,
                            const std::vector<uint8_t>& txOutput,
                            const std::vector<uint8_t>& sigShare);

// Returns a sig share for each output
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
// [TODO-UTT] What should be the type of the time object? We may decide that it's a 64-bit unix timestamp, but we can
// also provide N-bytes for time data. Example "TimeData getExpireTimeData(tx)" "struct TimeData { std::array<uint8_t,
// N> bytes; }" Need to decide how many bytes to provide, which could depend on the size of a single curve point inside
// libutt With a 64-bit unix timestamp we could simplify things, but we could create an obstacle for high-resolution
// timestamps.
uint64_t getExpireTime(const std::vector<uint8_t>& tx);

// returns the pidHash, value and sequence number of the given mint operation
struct MintOpData {
  std::vector<uint8_t> pidHash;  // [TODO-UTT] Why the pid hash? How it is used? A hash of the userId is used internally
                                 // by libutt no need to expose this outside.
  uint64_t value = 0;
  uint64_t seqNum = 0;  // [TODO-UTT] Why is it returning the seqNum? How it is used?
};
MintOpData getMintData(const std::vector<uint8_t>& mintOp);

// Gets the Burned coin nullifier
// [TODO-UTT] This can be handled by a generalized getNullifier that works on all applicable transaction types.
std::string getNullifier(const std::vector<uint8_t>& burnTx);

}  // namespace utt::server