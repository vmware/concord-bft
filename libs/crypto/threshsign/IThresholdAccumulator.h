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

#include <cstddef>
#include <set>
#include "ThresholdSignaturesTypes.h"

/**
 * NOTE: Can't take msg and length as constructor arguments because, in SBFT, message is not known beforehand
 */
class IThresholdAccumulator {
 public:
  virtual ~IThresholdAccumulator() {}

 public:
  /**
   * Adds a signature share to the accumulator with or without verifying it, depending on the implementation
   * and on whether setExpectedDigest() has been called beforehand.
   *
   * @param sigShareWithId  the sigShare buffer; contains the ID of the signer.
   * @param len             the length of the sigShare buffer
   *
   * @return  the size of the accumulator (will have increased by one if the added sigShare was validated successfully)
   */
  virtual int add(const char* sigShareWithId, int len) = 0;

  /**
   * Sets the message being threshold-signed. Should only be called with the same message or will throw.
   * This message will be used to verify signature shares against!
   *
   * @param msg
   * @param len
   *
   * @throws std::runtime_error if ever called with two different messages
   */
  virtual void setExpectedDigest(const unsigned char* msg, int len) = 0;

  /**
   * Returns true if share verification is performed.
   */
  virtual bool hasShareVerificationEnabled() const = 0;

  /**
   * After setExpectedDigest is called, returns the number of valid shares.
   * Before that, always returns 0, since shares can't be verified without a digest.
   */
  virtual int getNumValidShares() const = 0;

  /**
   * if share verification enabled, returns invalid share ids.
   */
  virtual std::set<ShareID> getInvalidShareIds() const = 0;

  /**
   * Computes and returns the final threshold signature on the digest specified in setExpectedDigest().
   * WARNING: If called twice, might recompute the signature which will be slow.
   *
   * TODO: rename to getThresholdSignature()
   */
  virtual size_t getFullSignedData(char* outThreshSig, int threshSigLen) = 0;
};
