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

#include "threshsign/IThresholdFactory.h"

#include "BlsPublicParameters.h"

namespace BLS::Relic {

class BlsThresholdKeygenBase;

class BlsThresholdFactory : public IThresholdFactory {
 protected:
  BlsPublicParameters params;
  /**
   * When set to true, uses a multisig-based scheme for implementing threshold signatures. Otherwise, uses the
   * secret-sharing-based scheme The multisig-based scheme is much faster when assembling the threshold signature but
   * produces larger signatures that are a bit slower to verify.
   */
  bool useMultisig;

 public:
  /**
   * Create a factory for building objects used to create BLS-based threshold signatures.
   *
   * @param params        the public parameters for the BLS-based scheme (e.g., elliptic curve groups)
   * @param useMultisig   creates a multisig-based threshold scheme, rather than a secret-sharing-based threshold scheme
   */
  BlsThresholdFactory(const BlsPublicParameters& params, bool useMultisig = false);

  virtual ~BlsThresholdFactory() {}

 public:
  const BlsPublicParameters& getPublicParameters() const { return params; }

  /**
   * Returns a key generation object that can be used to generate secret keys for each signer.
   *
   * @param reqSigners   the threshold number of signers required to assemble a threshold signature
   * @param numSigners   the total number of signers (>= reqSigners)
   *
   * @return a unique pointer to a key generator for the threshold scheme
   */
  std::unique_ptr<BlsThresholdKeygenBase> newKeygen(NumSharesType reqSigners, NumSharesType numSigners) const;

  /**
   * Creates a new IThresholdVerifier for a (reqSigners, numSigners) threshold scheme
   * with the specified public key, as well as verification keys of individual signers.
   *
   * @param reqSigners   the threshold number of signers required to assemble a threshold signature
   * @param numSigners   the total number of signers (>= reqSigners)
   * @param publicKeyStr the public key of the threshold scheme, against which any threshold signature can be verified
   * @param verifKeysStr the verification keys of individual signers against which their signature shares can be
   * verified
   *
   * @return an IThresholdVerifier object that can be used to verify signature shares, assemble the threshold signature
   * and verify it as well
   */
  IThresholdVerifier* newVerifier(NumSharesType reqSigners,
                                  NumSharesType numSigners,
                                  const char* publicKeyStr,
                                  const std::vector<std::string>& verifKeysStr) const override;

  /**
   * Creates a new IThresholdSigner for the specified signer ID with the specified secret key
   *
   * @param id            the signer's ID
   * @param secretKeyStr  the hex-encoded secret key of this signer
   *
   * @return an IThresholdSigner object that can be used to sign
   */
  IThresholdSigner* newSigner(ShareID id, const char* secretKeyStr) const override;

  /**
   * Creates random IThresholdSigner objects for *all* signer IDs from 1 to numSigners.
   * Internally calls newKeygen method from above to generate secret keys for the signers.
   * Also creates an IThresholdVerifier that can verify threshold signatures from these signers.
   *
   * @param reqSigners    the threshold number of signers required to assemble a threshold signature
   * @param numSigners    the total number of signers (>= reqSigners)
   *
   * @return a vector of IThresholdSigner's (indexed from 1 to reqSigners) and the IThresholdVerifier object
   *         (caller is responsible for deleting them)
   */
  IThresholdFactory::SignersVerifierTuple newRandomSigners(NumSharesType reqSigners,
                                                           NumSharesType numSigners) const override;

  /**
   * Generates a single BLS key pair
   */
  std::pair<std::unique_ptr<IShareSecretKey>, std::unique_ptr<IShareVerificationKey>> newKeyPair() const override;
};

}  // namespace BLS::Relic
