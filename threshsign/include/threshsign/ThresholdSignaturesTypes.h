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

#include <string>
#include <vector>

/**
 * The current implementation sends the share ID (i.e., the signer ID) with the share signature.
 * Here we define its data type so that we can easily support an arbitrary number of signers.
 *
 * WARNING: Do not set this to an unsigned type! You will run into C/C++ signed vs unsigned problems
 * (see http://soundsoftware.ac.uk/c-pitfall-unsigned)
 */
using ShareID = int;
using NumSharesType = ShareID;

constexpr int MAX_NUM_OF_SHARES = 2048;
constexpr const char* MULTISIG_BLS_SCHEME = "multisig-bls";
constexpr const char* THRESHOLD_BLS_SCHEME = "threshold-bls";
constexpr const char* MULTISIG_EDDSA_SCHEME = "multisig-eddsa";

class IThresholdFactory;
class IThresholdSigner;
class IThresholdVerifier;

/**
 * A class for representing threshold cryptosystems of different types,
 * numbers of signers, and threshold levels.
 */
class Cryptosystem {
 private:
  std::string type_{""};
  std::string subtype_{""};

  uint16_t numSigners_{0};
  uint16_t threshold_{0};
  bool forceMultisig_{false};  // is true if  signers == threshold

  // If only one signer's private key is known and stored in this cryptosystem,
  // this field records that signer's ID; otherwise (if no or all private keys
  // are known to this cryptosystem), this field stores Cryptosystem::NID to
  // represent it is inapplicable.
  uint16_t signerID_{0};

  // Note that 0 is not a valid signer ID because signer IDs are 1-indexed.
  static const uint16_t NID = 0;

  std::string publicKey_{""};
  std::vector<std::string> verificationKeys_;
  std::vector<std::string> privateKeys_;

  // Internally used helper functions.
  IThresholdFactory* createThresholdFactory();

  void validateKey(const std::string& key, size_t expectedSize) const;
  void validatePublicKey(const std::string& key) const;
  void validateVerificationKey(const std::string& key) const;
  void validatePrivateKey(const std::string& key) const;
  bool isValidCryptosystemSelection(const std::string& type, const std::string& subtype);
  bool isValidCryptosystemSelection(const std::string& type,
                                    const std::string& subtype,
                                    uint16_t numSigners,
                                    uint16_t threshold);

 protected:
  Cryptosystem() = default;

 public:
  /**
   * Constructor for the Cryptosystem class.
   *
   * @param sysType       The type of threshold cryptosystem to create. A list
   *                      of currently supported types can be obtained with the
   *                      static getAvailableCryptosystemTypes function of
   *                      Cryptosystem.
   * @param sysSubtype    Subtype of threshold cryptosystem to create. The
   *                      meaning of this parameter is dependent on the type of
   *                      cryptosystem selected with sysType. For example, if
   *                      sysType specifies a type of elliptic curve
   *                      cryptography, then sysSubtype should specify an
   *                      elliptic curve type.
   * @param sysNumSigners The total number of signers in this threshold
   *                      cryptosystem.
   * @param sysThreshold  The threshold for this threshold cryptosystem, that
   *                      is, the number of signatures needed from different
   *                      individual signers to produce a complete signature
   *                      under this cryptosystem.
   *
   * @throws std::runtime_error           If sysType is unrecognized or
   *                                      unsupported, if sysSubtype is
   *                                      invalid, unrecognized, or unsupported
   *                                      for the type of cryptosystem
   *                                      specified by sysType, if sysThreshold
   *                                      is greater than sysNumSigners, or if
   *                                      constraints on sysNumSigners and/or
   *                                      sysThreshold specific to the type
   *                                      and/or subtype of cryptosystem
   *                                      selected are not met.
   */
  Cryptosystem(const std::string& sysType,
               const std::string& sysSubtype,
               uint16_t sysNumSigners,
               uint16_t sysThreshold);

  /**
   * Destructor for Cryptosystem.
   */
  virtual ~Cryptosystem() {}

  // Functions for checking how a cryptosystem is configured.

  /**
   * Get the type of this cryptosystem.
   *
   * @return A string representing the type of this cryptosystem.
   */
  const std::string& getType() const { return type_; }

  /**
   * Get the type-dependent subtype of this crytposystem.
   *
   * @return A string representing the subtype of this cryptosystem.
   */
  const std::string& getSubtype() const { return subtype_; }

  /**
   * Get the number of signers in this cryptosystem.
   *
   * @return The number of signers in this cryptosystem.
   */
  uint16_t getNumSigners() const { return numSigners_; }

  /**
   * Get the threshold for this threshold cryptosystem.
   *
   * @return The threshold for this cryptosystem.
   */
  uint16_t getThreshold() const { return threshold_; }

  /**
   * Pseudorandomly generate a complete set of keys for this cryptosystem and
   * store them in this Cryptosystem object. Note that this function may take a
   * while to run depending on the cryptosystem type and number of signers, as
   * it performs pseudorandom key generation, which is computationally
   * expensive. Any existing keys loaded in this cryptosystem will be
   * overwritten.
   */
  void generateNewPseudorandomKeys();

  /**
   *  Generate a single key pair.
   *  Used for key rotation.
   *
   *  @return pair<shareSecretKey, shareVerificationKey>
   */
  std::pair<std::string, std::string> generateNewKeyPair();
  /**
   * Get the public key for this threshold cryptosystem, represented as a
   * string. The format of the string is cryptosystem type-dependent.
   *
   * @return The public key for this threshold cryptosystem.
   *
   * @throws std::runtime_error                 If this cryptosystem does not
   *                                            currently have a public key
   *                                            because keys for it have not
   *                                            been either generated or
   *                                            loaded.
   */
  std::string getSystemPublicKey() const;

  /**
   * Get a list of verification keys for this threshold cryptosystem,
   * represented as strings. Their format is cryptosystem type-dependent.
   *
   * @return A vector containing the verification keys, in order of which signer
   *         they correspond to. To comply with the convention of 1-indexing
   *         signer IDs, verification keys will begin at index 1 of the vector.
   *         The contents of index 0 of the vector is left undefined.
   *
   * @throws std::runtime_error                 If this cryptosystem does not
   *                                            currently have verification
   *                                            keys because keys for it have
   *                                            not been either generated or
   *                                            loaded.
   */
  std::vector<std::string> getSystemVerificationKeys() const;

  /**
   * Get a list of private keys for this threshold cryptosystem, represented as
   * strings. Their format is cryptosystem type-dependent.
   *
   * @return A vector containing the private keys, in order of which signer they
   *         correspond to. To comply with the convention of 1-indexing signer
   *         IDs, the private keys will begin at index 1 of the vector. The
   *         contents of index 0 are left undefined.
   *
   * @throws std::runtime_error If this cryptosystem does not
   *                                            currently have private keys
   *                                            because keys for it have not
   *                                            been generated and private keys
   *                                            have not been loaded.
   */
  std::vector<std::string> getSystemPrivateKeys() const;

  /**
   * Get the private key for a specific signer in this threshold cryptosystem,
   * represented as a string of cryptosystem type-dependent format.
   *
   * @param signerIndex The index for the signer to get the private key for;
   *                    signers are indexed from [1, numSigners].
   *
   * @return The private key for the specified signer.
   *
   * @throws std::runtime_error If this cryptosystem does not
   *                                            currently have the private key
   *                                            for the specified replica
   *                                            because it has not been
   *                                            generated or loaded.
   * @throws std::out_of_range                  If signerIndex > numSigners or
   *                                            signerIndex < 1.
   */
  std::string getPrivateKey(uint16_t signerIndex) const;

  /**
   * Load an existing set of keys to this cryptosystem. If this cryptosystem is
   * currently holding a set of keys, they will be overwritten.
   *
   * @param publicKey        Public key to load for this cryptosystem,
   *                         represented as a string.
   * @param verificationKeys List of verification keys to load for this
   *                         cryptosystem, represented as strings, in order of
   *                         signer they correspond to. To comply with the
   *                         convention of 1-indexing signer IDs, the
   *                         verification keys should beginat index 1 of the
   *                         vector. The content at index 0 of the vector will
   *                         not be used by this function.
   *
   * @throws std::runtime_error If the set of keys given is not valid
   *                                      for this cryptosystem.
   */
  void loadKeys(const std::string& publicKey, const std::vector<std::string>& verificationKeys);

  /**
   * Load the private key for a specific signer using this cryptosystem. Any
   * existing private keys will be overwritten.
   *
   * @param signerIndex The signer ID for the signer to which this private key
   *                    belongs, which should be in the range
   *                    [1, numSigners].
   * @param key         The private key to load belonging to this signer.
   *
   * @throws std::runtime_error If the given private key is invalid.
   * @throws std::out_of_range            If signerID is not in the range
   *                                      [1, numReplicas].
   */
  void loadPrivateKey(uint16_t signerIndex, const std::string& key);

  /**
   * Create a threshod verifier for this cryptosystem.
   *
   * @param threshold required threshold for multisig scheme
   * @return A pointer to a newly created IThresholdVerifier object for this
   *         cryptosystem.
   *
   * @throws std::runtime_error If this cryptosystem does not
   *                                            have the required public and
   *                                            verification keys loaded to
   *                                            create a verifier.
   */
  virtual IThresholdVerifier* createThresholdVerifier(uint16_t threshold = 0);

  /**
   * Create a threshold signer with the private key loaded for this system.
   * This function requires that a single private key (the one belonging to the
   * signer of interest) is currently loaded to this cryptosystem.
   *
   * @return A pointer to a newly created IThresholdSigner object with the
   *         private key loaded to this cryptosystem.
   *
   * @throws std::runtime_error If this cryptosystem does not
   *                                            have a private key loaded, or if
   *                                            it has all private keys loaded.
   */
  virtual IThresholdSigner* createThresholdSigner();

  /**
   * Get a list of supported cryptosystem types and descriptions of what
   * type-specific parameters they require.
   *
   * @return    A vector to which to append this functions output, as pairs of
   *            strings. The first string in each pair is the name for a
   *            supported cryptosystem type, and the second string is a
   *            description of what the type-specific subtype parameter
   *            specifies for that cryptosytem type.
   */
  static const std::vector<std::pair<std::string, std::string>>& getAvailableCryptosystemTypes();
  /**
   * Output configuration to stream
   */
  void writeConfiguration(std::ostream&, const std::string& prefix, const uint16_t& replicaId);
  /**
   * Create Cryptosystem from configuration
   */
  static Cryptosystem* fromConfiguration(std::istream&,
                                         const std::string& prefix,
                                         const uint16_t& replicaId,
                                         std::string& type,
                                         std::string& subtype,
                                         std::string& thrPrivateKey,
                                         std::string& thrPublicKey,
                                         std::vector<std::string>& thrVerificationKeys);
  /**
   * Update a key pair
   */
  void updateKeys(const std::string& shareSecretKey, const std::string& shareVerificationKey);

  /**
   * Update a shareVerificationKey for signer
   */
  void updateVerificationKey(const std::string& shareVerificationKey, const std::uint16_t& signerIndex);
};
