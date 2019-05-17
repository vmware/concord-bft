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

#ifdef ERROR // TODO(GG): should be fixed by encapsulating relic (or windows) definitions in cpp files
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

#include "Log.h"
#include "XAssert.h"

using namespace std;

namespace BLS {
namespace Relic {

const string BlsThresholdVerifier::className_ = "BlsThresholdVerifier";
const uint32_t BlsThresholdVerifier::classVersion_ = 1;
bool BlsThresholdVerifier::registered_ = false;

void BlsThresholdVerifier::registerClass() {
  if (!registered_) {
    classNameToObjectMap_[className_] =
        UniquePtrToClass(new BlsThresholdVerifier);
    registered_ = true;
  }
}

BlsThresholdVerifier::BlsThresholdVerifier(
    const BlsPublicParameters &params, const G2T &pk, NumSharesType reqSigners,
    NumSharesType numSigners, const vector<BlsPublicKey> &verificationKeys)
    : params_(params), publicKey_(pk),
      publicKeysVector_(verificationKeys.begin(), verificationKeys.end()),
      generator2_(params.getGenerator2()), reqSigners_(reqSigners),
      numSigners_(numSigners) {
  assertEqual(verificationKeys.size(),
              static_cast<vector<BlsPublicKey>::size_type>(numSigners + 1));
  // verifKeys[0] was copied as well, but it's set to a dummy PK so it does not matter
  assertEqual(publicKeysVector_.size(),
              static_cast<vector<BlsPublicKey>::size_type>(numSigners + 1));
  registerClass();

#ifdef TRACE
  logtrace << "VKs (array has size " << vks.size() << ")" << endl;
  copy(vks.begin(), vks.end(), ostream_iterator<BlsPublicKey>(cout, "\n"));
#endif
}

const IShareVerificationKey &BlsThresholdVerifier::getShareVerificationKey(
    ShareID signer) const {
  return publicKeysVector_.at(static_cast<size_t>(signer));
}

IThresholdAccumulator *BlsThresholdVerifier::newAccumulator(
    bool withShareVerification) const {
  if (reqSigners_ == numSigners_ - 1) {
    return new BlsAlmostMultisigAccumulator(publicKeysVector_, numSigners_);
  } else {
    return new BlsThresholdAccumulator(publicKeysVector_, reqSigners_,
                                       numSigners_, withShareVerification);
  }
}

bool BlsThresholdVerifier::verify(const char *msg,
                                  int msgLen,
                                  const char *sigBuf,
                                  int sigLen) const {
  G1T h, sig;
  // Convert hash to elliptic curve point
  g1_map(h, reinterpret_cast<const unsigned char *>(msg), msgLen);
  // Convert signature to elliptic curve point
  sig.fromBytes(reinterpret_cast<const unsigned char *>(sigBuf), sigLen);

  return verify(h, sig, publicKey_.y);
}

bool BlsThresholdVerifier::verify(const G1T &msgHash,
                                  const G1T &sigShare,
                                  const G2T &pk) const {
  // FIXME: RELIC: Dealing with library peculiarities here by using a const cast
  // Pair hash with PK
  GTT e1, e2;pc_map(e1, const_cast<G1T &>(msgHash), const_cast<G2T &>(pk));

  // Pair signature with group's generator
  pc_map(e2, const_cast<G1T &>(sigShare), const_cast<G2T &>(generator2_));

  // Make sure the two pairings are equal
  return (gt_cmp(e1, e2) == CMP_EQ);
}

/************** Serialization **************/

void BlsThresholdVerifier::serialize(ostream &outStream) const {
  // Serialize first the class name.
  serializeClassName(className_, outStream);
  serializeDataMembers(outStream);
}

void BlsThresholdVerifier::serialize(UniquePtrToChar &outBuf,
                                     int64_t &outBufSize) const {
  ofstream outStream(className_.c_str(), ofstream::binary | ofstream::trunc);
  serialize(outStream);
  outStream.close();
  retrieveSerializedBuffer(className_, outBuf, outBufSize);
}

void BlsThresholdVerifier::serializePublicKey(
    ostream &outStream, const BlsPublicKey &key) {
  int32_t publicKeySize = key.y.getByteCount();
  UniquePtrToUChar publicKeyBuf(new unsigned char[publicKeySize]);
  key.y.toBytes(publicKeyBuf.get(), publicKeySize);
  outStream.write((char *) &publicKeySize, sizeof(publicKeySize));
  outStream.write((char *) publicKeyBuf.get(), publicKeySize);
}

void BlsThresholdVerifier::serializeDataMembers(ostream &outStream) const {
  // Serialize class version
  outStream.write((char *) &classVersion_, sizeof(classVersion_));

  // Serialize params
  params_.serialize(outStream);

  // Serialize publicKey
  serializePublicKey(outStream, publicKey_);

  // Serialize publicKeysVector
  uint64_t publicKeysVectorNum = publicKeysVector_.size();
  outStream.write((char *) &publicKeysVectorNum, sizeof(publicKeysVectorNum));
  for (const auto &elem : publicKeysVector_) {
    serializePublicKey(outStream, elem);
  }

  // Serialize reqSigners
  outStream.write((char *) &reqSigners_, sizeof(reqSigners_));

  // Serialize numSigners
  outStream.write((char *) &numSigners_, sizeof(numSigners_));
}

bool BlsThresholdVerifier::operator==(const BlsThresholdVerifier &other) const {
  bool result = ((other.params_ == params_) &&
      (other.publicKey_ == publicKey_) &&
      (other.publicKeysVector_ == publicKeysVector_) &&
      (other.generator2_ == generator2_) &&
      (other.reqSigners_ == reqSigners_) &&
      (other.numSigners_ == numSigners_));
  return result;
}

/************** Deserialization **************/

G2T BlsThresholdVerifier::deserializePublicKey(istream &inStream) {
  int32_t sizeOfPublicKey = 0;
  inStream.read((char *) &sizeOfPublicKey, sizeof(sizeOfPublicKey));
  UniquePtrToUChar publicKey(new unsigned char[sizeOfPublicKey]);
  inStream.read((char *) publicKey.get(), sizeOfPublicKey);
  return G2T(publicKey.get(), sizeOfPublicKey);
}

UniquePtrToClass BlsThresholdVerifier::create(istream &inStream) {
  // Deserialize class version
  verifyClassVersion(classVersion_, inStream);

  // Deserialize params
  BlsPublicParameters params;
  UniquePtrToClass paramsObj(params.create(inStream));

  // Deserialize publicKey
  G2T publicKey = deserializePublicKey(inStream);

  // Deserialize publicKeysVector
  uint64_t publicKeysVectorNum = 0;
  inStream.read((char *) &publicKeysVectorNum, sizeof(publicKeysVectorNum));
  vector<BlsPublicKey> publicKeysVector;
  for (uint64_t i = 0; i < publicKeysVectorNum; ++i) {
    publicKeysVector.emplace_back(deserializePublicKey(inStream));
  }

  // Deserialize reqSigners
  inStream.read((char *) &reqSigners_, sizeof(reqSigners_));

  // Deserialize numSigners
  inStream.read((char *) &numSigners_, sizeof(numSigners_));

  return UniquePtrToClass(new BlsThresholdVerifier(
      *((BlsPublicParameters *) paramsObj.get()), publicKey, reqSigners_,
      numSigners_, publicKeysVector));
}

} /* namespace Relic */
} /* namespace BLS */
