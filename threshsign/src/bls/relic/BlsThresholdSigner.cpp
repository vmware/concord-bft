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
#include "threshsign/bls/relic/BlsThresholdSigner.h"
#include "threshsign/bls/relic/BlsPublicParameters.h"
#include <sstream>
#include <iostream>

using namespace std;

namespace BLS {
namespace Relic {

const string BlsThresholdSigner::className_ = "BlsThresholdSigner";
const uint32_t BlsThresholdSigner::classVersion_ = 1;
bool BlsThresholdSigner::registered_ = false;

void BlsThresholdSigner::registerClass() {
  if (!registered_) {
    classNameToObjectMap_[className_] =
        UniquePtrToClass(new BlsThresholdSigner);
    registered_ = true;
  }
}

BlsThresholdSigner::BlsThresholdSigner(const BlsPublicParameters &params,
                                       ShareID id, const BNT &secretKey)
    : params_(params), secretKey_(secretKey), publicKey_(secretKey), id_(id),
      sigSize_(params.getSignatureSize()) {
  // Serialize signer's ID to a buffer
  BNT idNum(id);
  idNum.toBytes(serializedId_, sizeof(id));
  registerClass();
}

void BlsThresholdSigner::signData(const char *hash, int hashLen, char *outSig,
                                  int outSigLen) {
  // TODO: ALIN: If the signer has some time to waste before signing,
  //  we can precompute multiplication tables on H(m) to speed up signing.
  G1T hTmp_, sigTmp_;

  // Map the specified 'hash' to an elliptic curve point
  g1_map(hTmp_, reinterpret_cast<const unsigned char *>(hash), hashLen);

  // sig = h^{sk} (except RELIC uses multiplication notation)
  g1_mul(sigTmp_, hTmp_, secretKey_.x);

  // Include the signer's ID in the sigshare
  memcpy(outSig, serializedId_, sizeof(id_));
  // Serialize the signature to a byte array
  sigTmp_.toBytes(reinterpret_cast<unsigned char *>(outSig) + sizeof(id_),
                  outSigLen - static_cast<int>(sizeof(id_)));
}

/************** Serialization **************/

void BlsThresholdSigner::serialize(UniquePtrToChar &outBuf, int64_t &outBufSize)
const {
  ofstream outStream(className_.c_str(), ofstream::binary | ofstream::trunc);
  // Serialize first the class name.
  serializeClassName(className_, outStream);
  serializeDataMembers(outStream);
  outStream.close();
  retrieveSerializedBuffer(className_, outBuf, outBufSize);
}

void BlsThresholdSigner::serializeDataMembers(ostream &outStream) const {
  // Serialize class version
  outStream.write((char *) &classVersion_, sizeof(classVersion_));

  // Serialize params
  params_.serialize(outStream);

  // Serialize secretKey
  int32_t secretKeySize = secretKey_.x.getByteCount();
  UniquePtrToUChar secretKeyBuf(new unsigned char[secretKeySize]);
  secretKey_.x.toBytes(secretKeyBuf.get(), secretKeySize);
  outStream.write((char *) &secretKeySize, sizeof(secretKeySize));
  outStream.write((char *) secretKeyBuf.get(), secretKeySize);

  // Serialize id
  outStream.write((char *) &id_, sizeof(id_));
}

bool BlsThresholdSigner::operator==(const BlsThresholdSigner &other) const {
  bool result = ((other.id_ == id_) &&
      (other.params_ == params_) &&
      (other.sigSize_ == sigSize_) &&
      !memcmp(other.serializedId_, serializedId_, sizeof(ShareID)) &&
      (other.secretKey_ == secretKey_) &&
      (other.publicKey_ == publicKey_)
  );
  if (other.secretKey_ == secretKey_);
  else
    cout << "secretKeys are not the same" << endl;
  if (other.publicKey_ == publicKey_);
  else
    cout << "publicKeys are not the same" << endl;
  return result;
}

/************** Deserialization **************/

UniquePtrToClass BlsThresholdSigner::create(istream &inStream) {
  // Deserialize class version
  verifyClassVersion(classVersion_, inStream);

  // Deserialize params
  UniquePtrToClass params(params_.create(inStream));

  // Deserialize secretKey
  int32_t sizeOfSecretKey = 0;
  inStream.read((char *) &sizeOfSecretKey, sizeof(sizeOfSecretKey));
  UniquePtrToUChar secretKey(new unsigned char[sizeOfSecretKey]);
  inStream.read((char *) secretKey.get(), sizeOfSecretKey);
  BNT key(secretKey.get(), sizeOfSecretKey);

  // Deserialize id
  inStream.read((char *) &id_, sizeof(id_));

  return UniquePtrToClass(new BlsThresholdSigner(
      *((BlsPublicParameters *) params.get()), id_, key));
}

} /* namespace Relic */
} /* namespace BLS */
