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

#include "threshsign/Configuration.h"
#include "threshsign/bls/relic/BlsMultisigVerifier.h"
#include "threshsign/bls/relic/BlsMultisigAccumulator.h"
#include "threshsign/bls/relic/BlsPublicParameters.h"
#include "threshsign/VectorOfShares.h"

#include "Log.h"
#include "XAssert.h"

using namespace std;

namespace BLS {
namespace Relic {

const string BlsMultisigVerifier::className_ = "BlsThresholdSigner";
const uint32_t BlsMultisigVerifier::classVersion_ = 1;
bool BlsMultisigVerifier::registered_ = false;

void BlsMultisigVerifier::registerClass() {
  if (!registered_) {
    classNameToObjectMap_[className_] =
        UniquePtrToClass(new BlsMultisigVerifier);
    registered_ = true;
  }
}

BlsMultisigVerifier::BlsMultisigVerifier(
    const BlsPublicParameters &params, NumSharesType reqSigners,
    NumSharesType numSigners, const vector<BlsPublicKey> &verificationKeys)
    : BlsThresholdVerifier(params, G2T::Identity(), reqSigners, numSigners,
                           verificationKeys) {
  if (reqSigners == numSigners) {
    // the PK is the aggregate PK of all numSigners and is needed to verify
    // n-out-of-n threshold
    for (auto &vk : verificationKeys) {
      publicKey_.y.Add(vk.getPoint());
    }
  } else {
    // the PK is computed dynamically based on the signer IDs in the signature
  }
  registerClass();
}

BlsMultisigVerifier::BlsMultisigVerifier(const BlsThresholdVerifier &base) :
    BlsThresholdVerifier(base.getParams(), base.getKey().y,
                         base.getNumRequiredShares(),
                         base.getNumTotalShares(),
                         base.getPublicKeysVector()) {}

IThresholdAccumulator *BlsMultisigVerifier::newAccumulator(
    bool withShareVerification) const {
  if (reqSigners_ == numSigners_ && withShareVerification) {
    logwarn << "BLS n-out-of-n multisig typically has share verification "
               "disabled in Concord. Are you sure you need this?" << endl;
  }
  return new BlsMultisigAccumulator(publicKeysVector_, reqSigners_,
                                    numSigners_, withShareVerification);
}

/**
 * NOTE(Alin): There are many other ways of encoding the signer IDs along the signature. 
 * For simplicity, we just serialize the bit vector of signer IDs. 
 * However, if more efficient variable-length encodings are to be used, then the API must change. 
 * Right now, IThresholdVerifier::requiredLengthForSignedData() is used to fetch the
 * signature size, which is "too early": not enough info about the signer IDs to 
 * determine the variable size of the signature.
 */
int BlsMultisigVerifier::requiredLengthForSignedData() const {
  int sigSize = params_.getSignatureSize();

  if (reqSigners_ != numSigners_)
    sigSize += VectorOfShares::getByteCount();

  return sigSize;
}

bool BlsMultisigVerifier::verify(const char *msg, int msgLen,
                                 const char *sigBuf, int sigLen) const {
  // Parse the signer IDs from sigBuf and adjust the PK
  if (reqSigners_ != numSigners_) {
    if (sigLen != requiredLengthForSignedData()) {
      throw runtime_error("Signature does not have the right size");
    }

    // need to parse out signer IDs
    VectorOfShares signers;
    const char *idbuf = sigBuf + params_.getSignatureSize();
    int idbufLen = VectorOfShares::getByteCount();
    signers.fromBytes(reinterpret_cast<const unsigned char *>(idbuf), idbufLen);

    // for reqSigners != numSigners, need to derive PK from signer IDs
    if (reqSigners_ != numSigners_) {
      publicKey_ = G2T::Identity();
      for (ShareID id = signers.first(); !signers.isEnd(id);
           id = signers.next(id)) {
        auto idx = static_cast<size_t>(id);
        publicKey_.y.Add(publicKeysVector_[idx].getPoint());
      }
    }
  }

  // Once the PK is set in 'pk' can call parent BlsThresholdVerifier to verify the sig
  return BlsThresholdVerifier::verify(msg, msgLen, sigBuf,
                                      params_.getSignatureSize());
}

/************** Serialization **************/

void BlsMultisigVerifier::serialize(UniquePtrToChar &outBuf,
                                    int64_t &outBufSize)
const {
  ofstream outStream(className_.c_str(), ofstream::binary | ofstream::trunc);
  // Serialize the base class
  BlsThresholdVerifier::serialize(outStream);

  // Serialize the class name.
  serializeClassName(className_, outStream);
  serializeDataMembers(outStream);
  outStream.close();
  retrieveSerializedBuffer(className_, outBuf, outBufSize);
}

void BlsMultisigVerifier::serializeDataMembers(ostream &outStream) const {
  // Serialize class version
  outStream.write((char *) &classVersion_, sizeof(classVersion_));
}

bool BlsMultisigVerifier::operator==(const BlsMultisigVerifier &other) const {
  bool result = BlsThresholdVerifier::compare(other);
  return result;
}

/************** Deserialization **************/

UniquePtrToClass BlsMultisigVerifier::create(istream &inStream) {
  // Retrieve the base class
  UniquePtrToClass baseClass(BlsThresholdVerifier::create(inStream));

  verifyClassName(className_, inStream);
  verifyClassVersion(classVersion_, inStream);

  auto &baseClassObj = *(BlsThresholdVerifier *) baseClass.get();
  return UniquePtrToClass(new BlsMultisigVerifier(baseClassObj));
}

} /* namespace Relic */
} /* namespace BLS */
