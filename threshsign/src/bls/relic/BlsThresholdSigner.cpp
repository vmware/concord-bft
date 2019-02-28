// Concord
//
// Copyright (c) 2018 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the
// LICENSE file.

#include "threshsign/Configuration.h"

#include "threshsign/bls/relic/BlsThresholdSigner.h"
#include "threshsign/bls/relic/BlsPublicParameters.h"

namespace BLS {
namespace Relic {

BlsThresholdSigner::BlsThresholdSigner(const BlsPublicParameters& params,
                                       ShareID id,
                                       const BNT& sk)
    : params(params),
      sk(sk),
      pk(sk),
      id(id),
      sigSize(params.getSignatureSize()) {
  // Serialize signer's ID to a buffer
  BNT idNum(id);
  idNum.toBytes(idBuf, sizeof(id));
}

void BlsThresholdSigner::signData(const char* hash,
                                  int hashLen,
                                  char* outSig,
                                  int outSigLen) {
  // TODO: ALIN: If the signer has some time to waste before signing, we can
  // precompute multiplication tables on H(m) to speed up signing.

  // Map the specified 'hash' to an elliptic curve point
  g1_map(hTmp, reinterpret_cast<const unsigned char*>(hash), hashLen);

  // sig = h^{sk} (except RELIC uses multiplication notation)
  g1_mul(sigTmp, hTmp, sk.x);

  // Include the signer's ID in the sigshare
  memcpy(outSig, idBuf, sizeof(id));
  // Serialize the signature to a byte array
  sigTmp.toBytes(reinterpret_cast<unsigned char*>(outSig) + sizeof(id),
                 outSigLen - static_cast<int>(sizeof(id)));
}

} /* namespace Relic */
} /* namespace BLS */
