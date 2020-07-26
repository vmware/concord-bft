// Concord
//
// Copyright (c) 2018 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the
// LICENSE file.

#ifdef ERROR  // TODO(GG): should be fixed by encapsulating relic (or windows) definitions in cpp files
#undef ERROR
#endif

#include "threshsign/Configuration.h"

#include "threshsign/bls/relic/BlsThresholdVerifier.h"
#include "threshsign/bls/relic/BlsThresholdAccumulator.h"
#include "threshsign/bls/relic/BlsPublicKey.h"
#include "threshsign/bls/relic/BlsPublicParameters.h"

#include "BlsAlmostMultisigAccumulator.h"

#include <algorithm>
#include <iterator>

#include "Logger.hpp"
#include "XAssert.h"

using namespace std;
using namespace concord::serialize;

namespace BLS {
namespace Relic {

BlsThresholdVerifier::BlsThresholdVerifier(const BlsPublicParameters &params,
                                           const G2T &pk,
                                           NumSharesType reqSigners,
                                           NumSharesType numSigners,
                                           const vector<BlsPublicKey> &verificationKeys)
    : params_(params),
      publicKey_(pk),
      publicKeysVector_(verificationKeys.begin(), verificationKeys.end()),
      generator2_(params.getGenerator2()),
      reqSigners_(reqSigners),
      numSigners_(numSigners) {
  assertEqual(verificationKeys.size(), static_cast<vector<BlsPublicKey>::size_type>(numSigners + 1));
  // verifKeys[0] was copied as well, but it's set to a dummy PK so it does not matter
  assertEqual(publicKeysVector_.size(), static_cast<vector<BlsPublicKey>::size_type>(numSigners + 1));

#ifdef TRACE
  LOG_TRACE(BLS_LOG, "VKs (array has size " << vks.size() << ")");
  copy(vks.begin(), vks.end(), ostream_iterator<BlsPublicKey>(cout, "\n"));
#endif
}

const IShareVerificationKey &BlsThresholdVerifier::getShareVerificationKey(ShareID signer) const {
  return publicKeysVector_.at(static_cast<size_t>(signer));
}

IThresholdAccumulator *BlsThresholdVerifier::newAccumulator(bool withShareVerification) const {
  if (reqSigners_ == numSigners_ - 1) {
    return new BlsAlmostMultisigAccumulator(publicKeysVector_, numSigners_);
  } else {
    return new BlsThresholdAccumulator(publicKeysVector_, reqSigners_, numSigners_, withShareVerification);
  }
}

bool BlsThresholdVerifier::verify(const char *msg, int msgLen, const char *sigBuf, int sigLen) const {
  G1T h, sig;
  // Convert hash to elliptic curve point
  g1_map(h, reinterpret_cast<const unsigned char *>(msg), msgLen);
  // Convert signature to elliptic curve point
  sig.fromBytes(reinterpret_cast<const unsigned char *>(sigBuf), sigLen);

  return verify(h, sig, publicKey_.y);
}

bool BlsThresholdVerifier::verify(const G1T &msgHash, const G1T &sigShare, const G2T &pk) const {
  // FIXME: RELIC: Dealing with library peculiarities here by using a const cast
  // Pair hash with PK
  GTT e1, e2;
  pc_map(e1, const_cast<G1T &>(msgHash), const_cast<G2T &>(pk));

  // Pair signature with group's generator
  pc_map(e2, const_cast<G1T &>(sigShare), const_cast<G2T &>(generator2_));

  // Make sure the two pairings are equal
  return (gt_cmp(e1, e2) == CMP_EQ);
}

/************** Serialization **************/

void BlsThresholdVerifier::serializePublicKey(const BlsPublicKey &key, std::ostream &outStream) const {
  int publicKeySize = key.y.getByteCount();
  LOG_TRACE(logger(), "<<< public key size: " << publicKeySize);
  serialize(outStream, publicKeySize);
  unsigned char *publicKeyBuf = new unsigned char[static_cast<size_t>(publicKeySize)];
  key.y.toBytes(publicKeyBuf, publicKeySize);
  outStream.write((char *)publicKeyBuf, publicKeySize);
  LOG_TRACE(logger(), "<<< public key buf: [" << key.y.toString() << "]");
  delete[] publicKeyBuf;
}

void BlsThresholdVerifier::serializeDataMembers(ostream &outStream) const {
  params_.serialize(outStream);
  serializePublicKey(publicKey_, outStream);

  serialize(outStream, publicKeysVector_.size());
  for (const auto &elem : publicKeysVector_) serializePublicKey(elem, outStream);

  serialize(outStream, reqSigners_);
  serialize(outStream, numSigners_);
}

bool BlsThresholdVerifier::operator==(const BlsThresholdVerifier &other) const {
  bool result = ((other.params_ == params_) && (other.publicKey_ == publicKey_) &&
                 (other.publicKeysVector_ == publicKeysVector_) && (other.generator2_ == generator2_) &&
                 (other.reqSigners_ == reqSigners_) && (other.numSigners_ == numSigners_));
  return result;
}

/************** Deserialization **************/
// static
G2T BlsThresholdVerifier::deserializePublicKey(istream &inStream) {
  int publicKeySize = 0;
  deserialize(inStream, publicKeySize);
  LOG_TRACE(logging::getLogger("serialize"), ">>> public key size: " << publicKeySize);
  unsigned char *publicKeyBuf = new unsigned char[static_cast<size_t>(publicKeySize)];
  inStream.read((char *)publicKeyBuf, publicKeySize);
  G2T g2t_(publicKeyBuf, publicKeySize);
  LOG_TRACE(logging::getLogger("serialize"), ">>> public key buf: [" << g2t_.toString() << "]");
  delete[] publicKeyBuf;
  return g2t_;
}

void BlsThresholdVerifier::deserializeDataMembers(istream &inStream) {
  BlsPublicParameters *params = nullptr;
  deserialize(inStream, params);
  params_ = BlsPublicParameters(*params);
  generator2_ = G2T(params_.getGenerator2());

  publicKey_ = BlsPublicKey(deserializePublicKey(inStream));

  vector<BlsPublicKey>::size_type publicKeysVectorNum = 0;
  deserialize(inStream, publicKeysVectorNum);
  for (std::vector<BlsPublicKey>::size_type i = 0; i < publicKeysVectorNum; ++i)
    publicKeysVector_.emplace_back(deserializePublicKey(inStream));

  deserialize(inStream, reqSigners_);
  deserialize(inStream, numSigners_);
  delete params;
}

} /* namespace Relic */
} /* namespace BLS */
