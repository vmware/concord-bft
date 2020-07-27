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
#include "Logger.hpp"

using namespace std;
using namespace concord::serialize;

namespace BLS {
namespace Relic {

BlsThresholdSigner::BlsThresholdSigner(const BlsPublicParameters &params, ShareID id, const BNT &secretKey)
    : params_(params), secretKey_(secretKey), publicKey_(secretKey), id_(id), sigSize_(params.getSignatureSize()) {
  // Serialize signer's ID to a buffer
  BNT idNum(id);
  idNum.toBytes(serializedId_, sizeof(id));
}

void BlsThresholdSigner::signData(const char *hash, int hashLen, char *outSig, int outSigLen) {
  // TODO: ALIN: If the signer has some time to waste before signing,
  //  we can precompute multiplication tables on H(m) to speed up signing.

  // Map the specified 'hash' to an elliptic curve point
  g1_map(hTmp_, reinterpret_cast<const unsigned char *>(hash), hashLen);

  // sig = h^{sk} (except RELIC uses multiplication notation)
  g1_mul(sigTmp_, hTmp_, secretKey_.x);

  // Include the signer's ID in the sigshare
  memcpy(outSig, serializedId_, sizeof(id_));
  // Serialize the signature to a byte array
  sigTmp_.toBytes(reinterpret_cast<unsigned char *>(outSig) + sizeof(id_), outSigLen - static_cast<int>(sizeof(id_)));
  LOG_TRACE(BLS_LOG, "id: " << id_);
}

/************** Serialization **************/

void BlsThresholdSigner::serializeDataMembers(ostream &outStream) const {
  params_.serialize(outStream);
  int32_t secretKeySize = secretKey_.x.getByteCount();
  UniquePtrToUChar secretKeyBuf(new unsigned char[static_cast<size_t>(secretKeySize)]);
  secretKey_.x.toBytes(secretKeyBuf.get(), secretKeySize);
  serialize(outStream, secretKeySize);
  outStream.write((char *)secretKeyBuf.get(), secretKeySize);
  serialize(outStream, id_);
}

bool BlsThresholdSigner::operator==(const BlsThresholdSigner &other) const {
  bool result = ((other.id_ == id_) && (other.params_ == params_) && (other.sigSize_ == sigSize_) &&
                 !memcmp(other.serializedId_, serializedId_, sizeof(ShareID)) && (other.hTmp_ == hTmp_) &&
                 (other.sigTmp_ == sigTmp_) && (other.secretKey_ == secretKey_) && (other.publicKey_ == publicKey_));

  if (other.id_ != id_) cout << "id_" << endl;
  if (other.params_ == params_)
    ;
  else
    cout << "params_" << endl;
  if (other.sigSize_ != sigSize_) cout << "sigSize_" << endl;
  if (memcmp(other.serializedId_, serializedId_, sizeof(ShareID)) != 0) cout << "serializedId_" << endl;
  if (other.hTmp_ != hTmp_) cout << "hTmp_" << endl;
  if (other.sigTmp_ != sigTmp_) cout << "sigTmp_" << endl;
  if (other.secretKey_ == secretKey_)
    ;
  else
    cout << "secretKeys are not the same" << endl;
  if (other.publicKey_ == publicKey_)
    ;
  else
    cout << "publicKeys are not the same" << endl;
  return result;
}

/************** Deserialization **************/
void BlsThresholdSigner::deserializeDataMembers(istream &inStream) {
  BlsPublicParameters *params = nullptr;
  deserialize(inStream, params);
  params_ = BlsPublicParameters(*params);
  sigSize_ = params_.getSignatureSize();
  std::int32_t sizeOfSecretKey = 0;
  deserialize(inStream, sizeOfSecretKey);
  UniquePtrToUChar secretKey(new unsigned char[static_cast<size_t>(sizeOfSecretKey)]);
  inStream.read((char *)secretKey.get(), sizeOfSecretKey);
  BNT key(secretKey.get(), sizeOfSecretKey);
  secretKey_ = BlsSecretKey(BNT(secretKey.get(), sizeOfSecretKey));
  publicKey_ = BlsPublicKey(BNT(secretKey.get(), sizeOfSecretKey));
  deserialize(inStream, id_);
  BNT idNum(id_);
  idNum.toBytes(serializedId_, sizeof(id_));
  delete params;
}

} /* namespace Relic */
} /* namespace BLS */
