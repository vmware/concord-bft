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

#include "IThresholdAccumulator.h"
#include "VectorOfShares.h"

#include <memory>
#include <vector>

/**
 * TODO: Threshold accumulators store too much state that could be kept in the parent IThresholdVerifier:
 * 	reqSigners, totalSigners, vks
 *
 * TODO: Add ParentThresholdVerifier typename to template and take it as arg in constructor.
 * Then access vks, reqSigners and totalSigners from parent verifier.
 */
template <class VerificationKey, class NumType, typename SigShareParserFunc>
class ThresholdAccumulatorBase : public IThresholdAccumulator {
 protected:
  // The expected digest that is being threshold-signed and its size
  std::shared_ptr<unsigned char[]> expectedDigest;
  int expectedDigestLen;

  // The # of required/threshold signers and # of total signers
  NumSharesType reqSigners, totalSigners;

  // Reference to the verification keys stored in the corresponding IThresholdVerifier object
  const std::vector<VerificationKey>& vks;

  // Pending unverified shares (because setExpectedDigest() has not been called yet)
  std::vector<NumType> pendingShares;

  // Valid shares indexed by signer ID
  std::vector<NumType> validShares;

  // Bit vectors of pending (unverified) shares and valid shares (|validSharesBits| <= reqSigners)
  VectorOfShares validSharesBits, pendingSharesBits;

 public:
  ThresholdAccumulatorBase(const std::vector<VerificationKey>& vks,
                           NumSharesType reqSigners,
                           NumSharesType totalSigners)
      : expectedDigest(nullptr),
        expectedDigestLen(0),
        reqSigners(reqSigners),
        totalSigners(totalSigners),
        vks(vks),
        pendingShares(static_cast<size_t>(totalSigners + 1)),
        validShares(static_cast<size_t>(totalSigners + 1)) {}

  virtual ~ThresholdAccumulatorBase() {}

  /**
   * New virtual methods introduced by this class (both public and protected)
   */
 public:
  const std::vector<VerificationKey>& getVKs() const { return vks; }

  /**
   * Adds the share either to the pending shares or valid shares and returns their updated count (depending on
   * hasExpectedDigest()). (We make this public because we call it in our tests/benchmarks)
   */
  virtual int addNumById(ShareID signer, const NumType& sigShare);

 protected:
  /**
   * Called to verify a share when added by addNumById() (and hasExpectedDigest() is true).
   * Or, called by verifyPendingShares() to move pending shares into valid shares when setExpectedDigest() is called.
   *
   * For example, BLS accumulators will compute the pairing here.
   */
  virtual bool verifyShare(ShareID id, const NumType& sigShare) = 0;

  /**
   * Called after the share is verified succesfully if share verification is enabled, or called after the unverified
   * share is added otherwise.
   *
   * For example, multisig accumulators will implement this to algebraically accumulate the newly added share.
   */
  virtual void onNewSigShareAdded(ShareID id, const NumType& sigShare) = 0;

  /**
   * 	Called after setExpectedDigest() is called with a valid digest, but before verifyPendingShares() is called.
   *
   *	For example, the BLS accumulator will want to start precomputing pairings e(H(m), g^{vk_i}), \forall signers i
   *	to speed up share verification!
   */
  virtual void onExpectedDigestSet() = 0;

  /**
   * Utility methods.
   */
 protected:
  /**
   * Verifies all the pendingShares and moves the valid ones into validShares for accumulation later.
   * After it returns, the 'signers' bit vector will be for the validShares, not pendingShares.
   */
  void verifyPendingShares();

  /**
   * Returns true if setExpectedDigest() has been called with a valid digest.
   */
  bool hasExpectedDigest() const { return expectedDigest != nullptr; }

  /**
   * Implementing some IThresholdAccumulator virtual methods here.
   */
 public:
  /**
   * Keeps track of the digest and makes sure it's never changed.
   * Verifies and moves the pending shares into the list of valid shares.
   */
  virtual void setExpectedDigest(const unsigned char* msg, int len);

  /**
   * Parses signer ID out of sigShare and calls internal addNumById() method.
   */
  virtual int add(const char* sigShare, int len);

  /**
   * When verification is enabled, returns the number of valid shares. Otherwise
   * returns 0.
   */
  virtual int getNumValidShares() const {
    if (!hasShareVerificationEnabled() || hasExpectedDigest())
      return validSharesBits.count();
    else
      return 0;
  }
};
