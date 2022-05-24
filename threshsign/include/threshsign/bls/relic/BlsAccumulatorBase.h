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

#pragma once

#include "threshsign/IThresholdAccumulator.h"
#include "threshsign/ThresholdAccumulatorBase.h"
#include "threshsign/ThresholdSignaturesTypes.h"
#include "threshsign/VectorOfShares.h"

#include "FastMultExp.h"
#include "BlsNumTypes.h"
#include "BlsPublicKey.h"

#include <vector>

namespace BLS {
namespace Relic {

class BlsSigshareParser {
 public:
  std::pair<ShareID, G1T> operator()(const char* sigShare, int len);
};

class BlsAccumulatorBase : public ThresholdAccumulatorBase<BlsPublicKey, G1T, BlsSigshareParser> {
 protected:
  G1T threshSig;  // the final assembled threshold signature
  G1T hash;       // expected hash of the message being signed

  G2T gen2;  // the generate of group G_2 (needed when verifying sigshares)

  // True if share verification is enabled
  bool shareVerificationEnabled;

 public:
  BlsAccumulatorBase(const std::vector<BlsPublicKey>& vks,
                     NumSharesType reqSigners,
                     NumSharesType totalSigners,
                     bool withShareVerification);
  virtual ~BlsAccumulatorBase() {}

  // IThresholdAccumulator overloads.
 public:
  /**
   * Returns true if shares are verified once the expected message is set via setExpectedDigest()
   * or false otherwise (i.e., when shares are optimistically assumed to be valid)
   *
   * @return true when shares are verified by this class and false otherwise
   */
  virtual bool hasShareVerificationEnabled() const { return shareVerificationEnabled; }

  // ThresholdAccumulatorBase overloads
 public:
  /**
   * Automagically called when a new signature share is added to the accumulator via add().
   * In this case does nothing because we defer aggregation to subclasses.
   *
   * @param   id          the signer ID
   * @param   sigShare    a signature share from that signer ID on the expected message.
   */
  virtual void onNewSigShareAdded(ShareID id, const G1T& sigShare) {
    // Do nothing
    (void)id;
    (void)sigShare;
  }

  // ThresholdAccumulatorBase overloads
 protected:
  /**
   * Verifies a signature share from the specified signer ID on the expected message.
   * Assumes the expected message has been already set in 'hash' via setExpectedDigest() and onExpectedDigestSet().
   * Automagically called by parent ThresholdAccumulatorBase class when hasShareVerificationEnabled() returns true
   *
   * @param   id          the signer ID
   * @param   sigShare    a signature share from that signer ID on the expected message.
   *
   * @return true if the share verified correctly against the signer's verification key, false otherwise
   */
  virtual bool verifyShare(ShareID id, const G1T& sigShare);

  /**
   * Simply maps the message/digest to the group G1, storing it in 'hash.'
   * Automagically called when setExpectedDigest() is called on the accumulator.
   */
  virtual void onExpectedDigestSet();

  // Used internally or for testing
 public:
  /**
   * NOTE: Used to fetch the signature as a group element when benchmarking threshold BLS.
   * Assumes caller already called computeLagrangeCoeff(), exponentiateLagrangeCoeff() and aggregateShares().
   * Otherwise, threshSig will not be computed (i.e., will be G1T::Identity()).
   *
   * @return the assembled threshold signature
   */
  // const G1T& getThresholdSignature() const { return threshSig; }

  /**
   * Converts the threshold signature to a byte sequence.
   * Like getThresholdSignature() assumes the threshold signature has been assembled.
   *
   * @param   buf buffer to store the threshold signature
   * @param   len capacity of the buffer
   * @return  The amount of occupied bytes in the buffer
   */
  virtual size_t sigToBytes(unsigned char* buf, int len) const { return threshSig.toBytes(buf, len); }

  virtual void aggregateShares() = 0;
};

} /* namespace Relic */
} /* namespace BLS */
