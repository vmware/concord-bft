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
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include <fstream>
#include <iostream>
#include <unordered_map>
#include <unordered_set>

#include "threshsign/ThresholdSignaturesTypes.h"
#include "threshsign/IThresholdAccumulator.h"
#include "threshsign/IThresholdSigner.h"
#include "threshsign/IThresholdVerifier.h"
#include "KeyfileIOUtils.hpp"
#include "crypto_utils.hpp"

// How often to output status when testing cryptosystems, measured as an
// interval measured in tested signaturs.
static const size_t kTestProgressReportingInterval = 128;

// Hash(es) to test for each signer combination that will be validated.
static const std::string kHashesToTest[1] = {
    "986699702ad64879360bbaccf7df4b2255a777f85a57c699c895992ea6fff2ac3c5e386c634f"
    "9f9588be531154368468d8f454e82bee8d780ea13f7466f90a30ae0627fab054798ab217dfd6"
    "2a988ca5875b555df5392b07664dcf4e947034f66e0ec51a9e469d78badf9c9c357d6ea14bcf"
    "cd86d5f23e7f19ed11f9c18c859a9600320700d23e6a935fbf58c2310aeba2faaac00a7075de"
    "02e6fd1cc94448daa7ead2aa6b4750b571211b01af392e8f100c0cd8011709690c2ef44847a6"
    "47f60cfd8dd0005cd39fb599f9f8ad5be5a939c542707fc3d7c55c1b4e8dcde5ab02b9c9f421"
    "2ae2d9bad10bf6299e2e02979e2c6ecc4d9989986d01ef58da511f671e983fe16169895534c3"
    "ead052e9fd0d3b2963e64601362cc441c0e3a5c5a892edb3dab2b8db54c8d2025e4e9ba2aef1"
    "ba2054751c7473d47fac4ccc407752fcbb30596c1815e806b3a88860e5e0d1f340556b55fb19"
    "4b3b9761108bc14c33b4f957d5c3cbb74f7f7531dfbd37e14e773d453ea51209d485d1623df9"
    "13875038247edb228b7b34075ee93d970f73e768fc2982c33e091a1a29fd2c29760b46aa1a63"
    "d26992682b906b96f98dc5140babeb98dbccadceaf8f5e31b6f0906c773b6426df866dae32a2"
    "1d88c54ee130f6f60761bc49a5b0864ac8f89bd464e5e2c8e8e8938570a47933d83e934d6b39"
    "6e45479128168ce36f61880e93038a7895b1fb4303b50a59b21f92c721ca7ebdafcd96b9f23e"
    "d6b0e276417034a564061c3d8c055afd9afbf6d933b41504c9fa39fe7c100ad538ea41be536c"
    "b1e4e7eee0644c4d02beb00ff2be77ed84e17e65105c25a5caf74aca6c4204df6cacf07fa914"
    "3260767dd8f6c8b794b16cf22cf2353da51840e0d20348b48907c44da481298dfa3f45b55d08"
    "69acd530f82d06542970d15753ae1806a4229341b7af785175998944d5b575b00196fa01ed10"
    "f4ffef06912a1b5964eb0604c8633929e7e056bdeb814cd0a719149c164a464bbc855e38f9aa"
    "7bd19505dd85e487a55fff1bfc579c12f3816e87776273c6e3e72222c6a61132fac441e3af3b"
    "db465f44dac867c66c2e83d925cdc976ebac4569945532ffbed26693ec61ad54b2897097dd67"
    "88e6d5da2390a2cf0842783779e39478a91b06c32911fdd3466562a4cef2ff490bba670e20fe"
    "6122a4d936c703e9cf6f5847c4d4e202074326ad953b37d97264c9b7d36ba26413d14ca38108"
    "f4ceabe84b52768653168c54ff4d25c05c99c653e3cd606b87d5dbae249f57dc406969a5fcf0"
    "eb3b1a893c5111853bed1cc60fe074a55aa51a975cc3217594ff1479036a05d01c01b2f610b4"
    "e4adbc629e06dc636b65872072cdf084ee5a7e0a63afe42931025d4e5ed60d069cfa71d91bc7"
    "451a8a2109529181fd150fc055ad42ce5c30c9512cd64ee3789f8c0d0069501be65f1950"};

// Since ReplicaConfig is a singleton and the test requires several instances,
// we need to define a replica config for tests.
class TestReplicaConfig : public bftEngine::ReplicaConfig {};

// Helper functions to main acting as sub-components of the test.

// validateFundamentalFields and validateConfigStructIntegrity are sanity checks
// for the partially-filled-out ReplicaConfig structs read from the keyfiles to
// validate they are in order before attempting tests of the actual
// cryptographic keys.

static bool validateFundamentalFields(const std::vector<TestReplicaConfig>& configs) {
  uint16_t numReplicas = configs.size() - configs.front().numRoReplicas;
  uint16_t fVal = configs.front().fVal;
  uint16_t cVal = configs.front().cVal;

  // Note we validate agreement of numReplicas, fVal, and cVal with 32-bit
  // integers in case 3F + 2C + 1 overflows a 16-bit integer.
  uint32_t expectedNumReplicas = 3 * (uint32_t)fVal + 2 * (uint32_t)cVal + 1;

  // Note that check fVal >= 1 before enforcing agreement of fVal and cVal with
  // numReplicas to give precedence to complaining about invalid F over
  // disagreement with numReplicas.
  if ((fVal >= 1) && (expectedNumReplicas != (uint32_t)numReplicas)) {
    std::cout << "FAILURE: fVal (" << fVal << ") and cVal (" << cVal
              << ") (according to key file 0) do not agree with the number"
                 " of replicas ("
              << numReplicas
              << "). It is required that numReplicas ="
                 " (3 * fVal + 2 * cVal + 1).\n";
    return false;
  }

  for (uint16_t i = 0; i < numReplicas; ++i) {
    const TestReplicaConfig& config = configs[i];
    if (config.replicaId != i) {
      std::cout << "FAILURE: Key file " << i << " specifies a replica ID disagreeing with its filename.\n";
      return false;
    }
    if (config.fVal < 1) {
      std::cout << "FAILURE: Replica " << i
                << " has an"
                   " invalid F value: "
                << config.fVal << ".\n.";
      return false;
    }
    if (config.fVal != fVal) {
      std::cout << "FAILURE: Replica " << i
                << " has an F"
                   " value inconsistent with replica(s) 0 through "
                << (i - 1) << ".\n";
      return false;
    }
    if (config.cVal != cVal) {
      std::cout << "FAILURE: Replica " << i
                << " has a C"
                   " value inconsistent with replica(s) 0 through "
                << (i - 1) << ".\n";
      return false;
    }
  }

  return true;
}

// Helper function to test RSA keys to test the compatibility of a single key
// pair.
static bool testRSAKeyPair(const std::string& privateKey, const std::string& publicKey, uint16_t replicaID) {
  // The signer and verifier are stored with unique pointers rather than by
  // value so that they can be constructed in try/catch statements without
  // limiting their scope to those statements; declaring them by value is not
  // possible in this case becuause they lack paramter-less default
  // constructors.
  std::unique_ptr<concord::util::crypto::RSASigner> signer;
  std::unique_ptr<concord::util::crypto::RSAVerifier> verifier;

  std::string invalidPrivateKey = "FAILURE: Invalid RSA private key for replica " + std::to_string(replicaID) + ".\n";
  std::string invalidPublicKey = "FAILURE: Invalid RSA public key for replica " + std::to_string(replicaID) + ".\n";

  try {
    signer.reset(
        new concord::util::crypto::RSASigner(privateKey, concord::util::crypto::KeyFormat::HexaDecimalStrippedFormat));
  } catch (std::exception& e) {
    std::cout << invalidPrivateKey;
    return false;
  }
  try {
    verifier.reset(
        new concord::util::crypto::RSAVerifier(publicKey, concord::util::crypto::KeyFormat::HexaDecimalStrippedFormat));
  } catch (std::exception& e) {
    std::cout << invalidPublicKey;
    return false;
  }

  for (auto iter = std::begin(kHashesToTest); iter != std::end(kHashesToTest); ++iter) {
    const std::string& hash = *iter;

    std::string sig;
    try {
      sig = signer->sign(hash);
      if (sig.empty()) {
        std::cout << "FAILURE: Failed to sign data with"
                     " replica "
                  << replicaID << "'s RSA private key.\n";
        return false;
      }
    } catch (std::exception& e) {
      std::cout << invalidPrivateKey;
      return false;
    }

    try {
      if (!verifier->verify(hash, sig)) {
        std::cout << "FAILURE: A signature with replica " << replicaID
                  << "'s RSA private key could not be verified with"
                     " replica "
                  << replicaID << "'s RSA public key.\n";
        return false;
      }
    } catch (std::exception& e) {
      std::cout << invalidPublicKey;
      return false;
    }
  }

  return true;
}

// Test that the RSA key pairs given in the keyfiles work, that the keyfiles
// agree on what the public keys are, and that there are no duplicates.
static bool testRSAKeys(const std::vector<TestReplicaConfig>& configs) {
  uint16_t numReplicas = configs.size();

  std::cout << "Testing " << numReplicas << " RSA key pairs...\n";
  std::unordered_map<uint16_t, std::string> expectedPublicKeys;
  std::unordered_set<std::string> rsaPublicKeysSeen;

  // Test that a signature produced with each replica's private key can be
  // verified with that replica's public key.
  for (uint16_t i = 0; i < numReplicas; ++i) {
    std::string privateKey = configs[i].replicaPrivateKey;
    std::string publicKey;

    for (const auto& publicKeyEntry : configs[i].publicKeysOfReplicas) {
      if (publicKeyEntry.first == i) {
        publicKey = publicKeyEntry.second;
      }
    }

    if (!testRSAKeyPair(privateKey, publicKey, i)) {
      return false;
    }

    if (rsaPublicKeysSeen.count(publicKey) > 0) {
      uint16_t existingKeyholder;
      for (const auto& publicKeyEntry : expectedPublicKeys) {
        if (publicKeyEntry.second == publicKey) {
          existingKeyholder = publicKeyEntry.first;
        }
      }
      std::cout << "FAILURE: Replicas " << existingKeyholder << " and " << i << " share the same RSA public key.\n";
      return false;
    }
    expectedPublicKeys[i] = publicKey;

    if (((i + 1) % kTestProgressReportingInterval) == 0) {
      std::cout << "Tested " << (i + 1) << " out of " << numReplicas << " RSA key pairs...\n";
    }
  }

  std::cout << "Verifying that all replicas agree on RSA public keys...\n";

  // Verify that all replicas' keyfiles agree on the RSA public keys.
  for (uint16_t i = 0; i < numReplicas; ++i) {
    for (const auto& publicKeyEntry : configs[i].publicKeysOfReplicas) {
      if (publicKeyEntry.second != expectedPublicKeys[publicKeyEntry.first]) {
        std::cout << "FAILURE: Replica " << i
                  << " has an"
                     " incorrect RSA public key for replica "
                  << publicKeyEntry.first << ".\n";
        return false;
      }
    }
  }

  std::cout << "All RSA key tests were successful.\n";

  return true;
}

// Testing the threshold cryptosystem keys is not as straightforward as testing
// the RSA keys because each signature may have multiple signers. Testing all
// possible signer combinations, or even only all signer combinations that we
// expect to produce a complete valid threshold signature, is not feasible
// (unless the number of signers is rather small), as the number of possible
// signer combinations grows combinatorially with the number of signers; this
// can get even worth if we consider testing whether signer order makes a
// difference.
//
// For this case, which is intended specifically as a test of the keyset
// generated by GenerateConcordKeys, we deliberately choose to limit ourselves
// to testing that GenerateConcordKeys generates a valid keyset under our
// threshold cryptography implementations in the threshsign directory; we do NOT
// thoroughly test the correctness of the implementations in threshsign here.
//
// To this end, we try to test that the thresholds for the generated
// cryptosystems are correct and to cover all keys in the systems with the test.

// Function for selecting what combinations of signers to test for a given
// number of signers and threshold; note the isolation of this functions makes
// it less difficult to reconfigure what tests we want to do on each thresold
// cryptosystem.
static std::vector<std::vector<uint16_t>> getThresholdSignerCombinationsToTest(uint16_t numSigners,
                                                                               uint16_t threshold) {
  std::vector<std::vector<uint16_t>> signerCombinations;

  // Try signer sets of sizes 1, threshold - 1, and threshold to validate that
  // threshold is as expected and working correctly for this cryptosystem.
  std::vector<uint16_t> signerNumsToTest;
  signerNumsToTest.push_back(1);
  if ((threshold - 1) > signerNumsToTest.back()) {
    signerNumsToTest.push_back(threshold - 1);
  }
  if (threshold > signerNumsToTest.back()) {
    signerNumsToTest.push_back(threshold);
  }

  for (auto numSignersToTest : signerNumsToTest) {
    std::vector<uint16_t> signers;
    signers.reserve(numSignersToTest);
    for (uint16_t i = 0; i < numSignersToTest; ++i) {
      signers.push_back(i);
    }
    signerCombinations.push_back(std::move(signers));
  }

  // Ensure that every private/verification key pair is tested. Since the
  // threshold is less than the number of signers, we achieve this by testing
  // consecutive (by signer index) bocks of keys of size threshold until we have
  // tested all keys. To handle that the number of signers may not be divisible
  // by the threshold, we wrap around the final block to the front again if it
  // goes past the end of the keys.

  // We use a 32 bit integer to keep track of how many signers we have tested so
  // far, even though the signers are indexed with 16 bit integers, because
  // signersTested will likely exceed the number of signers during the last
  // block, at which point we will continue selecting as many signers as we need
  // by wraping it back around to the front with modulus; we cannot immediately
  // set signersTested itself to itself mod numSigners once it exceeds the
  // number of signers because whether it has yet exceeded the number of signers
  // is used as the condition to determine whether we have covered all the keys
  // or whether we need to add more threshold-sized blocks of them.
  uint32_t signersTested = 0;

  while (signersTested < (uint32_t)numSigners) {
    std::vector<uint16_t> signers;
    signers.reserve(threshold);
    for (uint16_t i = 0; i < threshold; ++i) {
      signers.push_back((uint16_t)(signersTested % (uint32_t)numSigners));
      ++signersTested;
    }
    signerCombinations.push_back(std::move(signers));
  }

  return signerCombinations;
}

// Helper function to testThresholdCryptosystem for testing a single signature.
//
// Note we cannot accept signers and verifier as const because
// IThresholdSigner.signData and IThresholdVerifier.release are not marked as
// const, even though threshold signers and verifiers are theoretically
// reusable.
static bool testThresholdSignature(const std::string& cryptosystemName,
                                   std::vector<IThresholdSigner*> signers,
                                   IThresholdVerifier* verifier,
                                   const std::vector<uint16_t>& signersToTest,
                                   uint16_t numSigners,
                                   uint16_t threshold) {
  std::string invalidPublicConfig =
      "FAILURE: Invalid public and verification keyset for " + cryptosystemName + " threshold cryptosystem.\n";
  std::string invalidKeyset = "FAILURE: Invalid keyset for the " + cryptosystemName + " threshold cryptosystem.\n";

  uint16_t participatingSigners = signersToTest.size();

  for (const auto& hash : kHashesToTest) {
    int hashLength = hash.length();

    std::shared_ptr<IThresholdAccumulator> accumulator = nullptr;

    try {
      accumulator.reset(verifier->newAccumulator(true));

      // It is necessary to try to catch an exception from newAccumulator and
      // then construct an accumulator with verification disabled (and make sure
      // we do not attempt to use verification below if we had to do this)
      // because some implementations of IThresholdAccumulator have the behavior
      // of failing to support verification and throwing an exception when
      // attempting to construct them with verification. At the time thiw code
      // was written, IThresholdVerifier did not include a means of checking
      // whether the accumulators an implementation of it creates support
      // verification other than handling the exception that occurs if they do
      // not.
    } catch (std::exception& e) {
      try {
        accumulator.reset(verifier->newAccumulator(false));
      } catch (std::exception& e) {
        std::cout << invalidPublicConfig;
        return false;
      }
    }

    bool verificationEnabled;
    try {
      verificationEnabled = accumulator->hasShareVerificationEnabled();
    } catch (std::exception& e) {
      std::cout << invalidPublicConfig;
      return false;
    }

    if (verificationEnabled) {
      try {
        accumulator->setExpectedDigest(reinterpret_cast<const unsigned char*>(hash.c_str()), hashLength);
      } catch (std::exception& e) {
        std::cout << invalidPublicConfig;
        return false;
      }
    }

    int accumulatorSize;
    if (verificationEnabled) {
      try {
        accumulatorSize = accumulator->getNumValidShares();
      } catch (std::exception& e) {
        std::cout << invalidPublicConfig;
        return false;
      }
      if (accumulatorSize != 0) {
        std::cout << "FAILURE: Newly created signature accumulator for " << cryptosystemName
                  << " threshold cryptosystem reports already having valid signatures.\n";
        return false;
      }
    }

    for (auto signerID : signersToTest) {
      IThresholdSigner* signer = signers[signerID];
      std::string invalidPrivateKey = "FAILURE: Invalid private key for signer " + std::to_string(signerID) +
                                      " under the " + cryptosystemName + " threshold cryptosystem.\n";
      int sigShareLen;
      try {
        sigShareLen = signer->requiredLengthForSignedData();
      } catch (std::exception& e) {
        std::cout << invalidPrivateKey;
        return false;
      }
      char* sigShareBuf = new char[sigShareLen];
      try {
        signer->signData(hash.c_str(), hashLength, sigShareBuf, sigShareLen);
      } catch (std::exception& e) {
        std::cout << invalidPrivateKey;
        delete[] sigShareBuf;
        return false;
      }
      int addRes;
      try {
        addRes = accumulator->add(sigShareBuf, sigShareLen);
      } catch (std::exception& e) {
        std::cout << invalidKeyset;
        delete[] sigShareBuf;
        return false;
      }
      delete[] sigShareBuf;

      if (verificationEnabled) {
        // Note we do not necessarily expect the accumulator to accept the
        // signature if it already has enough signatures to satisfy the
        // threshold, as extra signatures are not actually helpful to it for
        // producing the composite signature it was created to produce.
        if (((accumulatorSize + 1) <= threshold) && (addRes != (accumulatorSize + 1))) {
          std::cout << "FAILURE: Signature accumulator could not validate replica " << signerID << "'s signature for "
                    << cryptosystemName << " threshold cryptosystem.\n";
          return false;
        } else {
          accumulatorSize = addRes;
        }
      }
    }

    int signatureLength;
    try {
      signatureLength = verifier->requiredLengthForSignedData();
    } catch (std::exception& e) {
      std::cout << invalidPublicConfig;
      return false;
    }
    char* sigBuf = new char[signatureLength];
    try {
      accumulator->getFullSignedData(sigBuf, signatureLength);
    } catch (std::exception& e) {
      std::cout << invalidKeyset << e.what() << std::endl;
      delete[] sigBuf;
      return false;
    }

    bool signatureAccepted;
    try {
      signatureAccepted = verifier->verify(hash.c_str(), hashLength, sigBuf, signatureLength);
    } catch (std::exception& e) {
      std::cout << invalidKeyset;
      delete[] sigBuf;
      return false;
    }

    delete[] sigBuf;

    if (signatureAccepted != (participatingSigners >= threshold)) {
      std::cout << "FAILURE: Threshold signer with " << participatingSigners << " signatures unexpectedly "
                << (signatureAccepted ? "accepted" : "rejected") << " under the " << cryptosystemName
                << " threshold cryptosystem, which has threshold " << threshold << " out of " << numSigners << ".\n";
      return false;
    }
  }

  return true;
}

// Run all tests we want to do on each threshold cryptosystem on a given
// threshold cryptosystem.
static bool testThresholdCryptosystem(const std::string& name,
                                      const std::vector<IThresholdSigner*>& signers,
                                      const std::vector<IThresholdVerifier*>& verifiers,
                                      uint16_t numSigners,
                                      uint16_t threshold) {
  std::cout << "Testing " << name << " threshold cryptosystem.\n";

  size_t testsCompleted = 0;

  IThresholdVerifier* referenceVerifier = verifiers.front();
  std::vector<std::vector<uint16_t>> signerCombinationsToTest =
      getThresholdSignerCombinationsToTest(numSigners, threshold);
  size_t totalTests = signerCombinationsToTest.size();

  // Test that the threshold cryptosystem functions as expected.
  for (const auto& signerCombination : signerCombinationsToTest) {
    if (!testThresholdSignature(name, signers, referenceVerifier, signerCombination, numSigners, threshold)) {
      return false;
    }
    ++testsCompleted;
    if ((testsCompleted % kTestProgressReportingInterval) == 0) {
      std::cout << "Tested " << testsCompleted << " out of " << totalTests << " signer combinations to test on the "
                << name
                << " threshold"
                   " cryptosystem.\n";
    }
  }

  // Verify that all replicas' keyfiles agree on the public and verification
  // keys for this cryptosystem.
  for (uint16_t i = 0; i < numSigners; ++i) {
    IThresholdVerifier* verifier = verifiers[i];
    try {
      if (verifier->getPublicKey().toString() != referenceVerifier->getPublicKey().toString()) {
        std::cout << "FAILURE: Replica " << i
                  << "'s key file"
                     " has the wrong public key for the "
                  << name
                  << " threshold"
                     " cryptosystem.\n";
        return false;
      }
    } catch (std::exception& e) {
      // BLSMultisigVerifier::getPublicKey() throws if k-out-of-n, k < n
    }

    for (uint16_t j = 0; j < numSigners; ++j) {
      if (verifier->getShareVerificationKey(j).toString() != referenceVerifier->getShareVerificationKey(j).toString()) {
        std::cout << "FAILURE: Replica " << i
                  << "'s key"
                     " file has the wrong share verification key for replica "
                  << j << " under the " << name << " threshold cryptosystem.\n";
        return false;
      }
    }
  }

  // Verify that each replica's private key is unique.
  std::unordered_set<std::string> privKeys;
  for (uint16_t i = 0; i < numSigners; ++i) {
    std::string key = signers[i]->getShareSecretKey().toString();
    if (privKeys.count(key) > 0) {
      uint16_t existingKeyHolder;
      for (uint16_t j = 0; j < i; ++j) {
        if (signers[j]->getShareSecretKey().toString() == key) {
          existingKeyHolder = j;
        }
      }
      std::cout << "FAILURE: Replica " << i
                << " shares a"
                   " private key with replica "
                << existingKeyHolder << " under the " << name << " threshold cryptosystem.\n";
      return false;
    }
  }

  std::cout << "The " << name
            << " threshold cryptosystem tested"
               " successfully.\n";
  return true;
}

// Tests all threshold cryptosystems given in the keyfiles by calling
// testThresholdCryptosystem for each of them.
static bool testThresholdKeys(const std::vector<TestReplicaConfig>& configs,
                              std::vector<std::unique_ptr<Cryptosystem>>& cryptoSystems) {
  uint16_t numReplicas = configs.size() - configs.front().numRoReplicas;

  for (uint16_t i = 0; i < numReplicas; ++i) {
    std::cout << "System " << i << "\nshare verification keys:\n";
    for (auto&& sharedKey : cryptoSystems[i]->getSystemVerificationKeys()) std::cout << sharedKey << "\n";
    std::cout << "share secret key:\n"
              << cryptoSystems[i]->getPrivateKey(i + 1) << "\n"
              << "----------------------------------------------------------\n";
  }

  // Compute thresholds.
  uint16_t f = configs.front().fVal;
  uint16_t c = configs.front().cVal;
  uint16_t slowThresh = f * 2 + c + 1;
  uint16_t commitThresh = f * 3 + c + 1;
  uint16_t optThresh = f * 3 + c * 2 + 1;

  std::cout << "Testing threshold cryptographic keys...\n";

  std::vector<IThresholdSigner*> signers;
  std::vector<IThresholdVerifier*> verifiers;

  // same signers for all
  for (uint16_t i = 0; i < numReplicas; ++i) signers.push_back(cryptoSystems[i]->createThresholdSigner());

  for (uint16_t i = 0; i < numReplicas; ++i) verifiers.push_back(cryptoSystems[i]->createThresholdVerifier(slowThresh));
  if (!testThresholdCryptosystem("slow path commit", signers, verifiers, numReplicas, slowThresh)) return false;

  verifiers.clear();
  for (uint16_t i = 0; i < numReplicas; ++i)
    verifiers.push_back(cryptoSystems[i]->createThresholdVerifier(commitThresh));
  if (!testThresholdCryptosystem("commit", signers, verifiers, numReplicas, commitThresh)) return false;

  verifiers.clear();
  for (uint16_t i = 0; i < numReplicas; ++i) verifiers.push_back(cryptoSystems[i]->createThresholdVerifier(optThresh));

  if (!testThresholdCryptosystem("optimistic fast path commit", signers, verifiers, numReplicas, optThresh))
    return false;

  std::cout << "All threshold cryptography tests were successful.\n";

  return true;
}

// Helper function for determining whether --help was given.
static bool containsHelpOption(int argc, char** argv) {
  for (int i = 1; i < argc; ++i) {
    if (std::string(argv[i]) == "--help") {
      return true;
    }
  }
  return false;
}

/**
 * Main function for the TestGeneratedKeys executable. TestGeneratedKeys is
 * intended as a test for the correctness of GenerateConcordKeys, although it
 * can also validate a set of keyfiles that have been manually created or
 * modified as long as they comply with the output keyfile naming convention
 * that GenerateConcordKeys uses.
 *
 * @param argc The number of arguments to this executable, including the name by
 *             which this executable was launched as the first by convention.
 * @param argv Command line arguments to this executable, including the name by
 *             which it was launched as the first argument per convention. This
 *             executable can be run with the --help option for usage
 *             information on what command line arguments are expected.
 *
 * @return 0 if the testing is launched and completed successfully; -1 if the
 *         tests could not be launched (for example, because of issue with the
 *         command line parameters or missing input files) or if the input key
 *         files fail the tests.
 */
int main(int argc, char** argv) {
  try {
    std::string usageMessage =
        "Usage:\n"
        "TestGeneratedKeys \n"
        "  -n Number of regular replicas\n"
        "  -r Number of read-only replicas\n"
        "  -o Output file prefix\n"
        "   --help - this help \n\n"
        "TestGeneratedKeys is intended to test the output of the GenerateConcordKeys utility;\n"
        "TestGeneratedKeys expects to find TOTAL_NUMBER_OF_REPLICAS keyfiles each named KEYFILE_PREFIX<i>,\n"
        " where <i> is an integer in the range [0, TOTAL_NUMBRER_OF_REPLICAS - 1]\n\n";

    if ((argc <= 1) || (containsHelpOption(argc, argv))) {
      std::cout << usageMessage;
      return 0;
    }

    uint16_t numReplicas = 0;
    uint16_t ro = 0;
    std::string outputPrefix;

    for (int i = 1; i < argc; ++i) {
      std::string option(argv[i]);

      if (option == "-n") {
        if (i >= argc - 1) {
          std::cout << "Expected an argument to -n.\n";
          return -1;
        }
        numReplicas = parse<std::uint16_t>(argv[i + 1], "-n");
        ++i;
      } else if (option == "-r") {
        if (i >= argc - 1) {
          std::cout << "Expected an argument to -r.\n";
          return -1;
        }
        ro = parse<std::uint16_t>(argv[i + 1], "-r");
        ++i;
      } else if (option == "-o") {
        if (i >= argc - 1) {
          std::cout << "Expected an argument to -o.\n";
          return -1;
        }
        outputPrefix = argv[i + 1];
        ++i;
      } else {
        std::cout << "Unrecognized command line option: " << option << ".\n";
        return -1;
      }
    }

    if (numReplicas == 0) {
      std::cout << "-n was not provided.\n";
      return -1;
    }
    if (outputPrefix.empty()) {
      std::cout << "No value given for required -o parameter.\n";
      return -1;
    }

    std::cout << "TestGeneratedKeys launched.\n";
    std::cout << "Testing keyfiles for a " << numReplicas
              << "-replica Concord"
                 " deployment...\n";

    std::vector<TestReplicaConfig> configs(numReplicas + ro);
    std::vector<std::unique_ptr<Cryptosystem>> cryptoSystems;

    for (uint16_t i = 0; i < numReplicas + ro; ++i) {
      std::string filename = outputPrefix + std::to_string(i);
      cryptoSystems.push_back(std::unique_ptr<Cryptosystem>(inputReplicaKeyfileMultisig(filename, configs[i])));
    }

    std::cout << "All keyfiles were successfully read.\n";

    std::cout << "Verifying sanity of the cryptographic configurations...\n";
    if (!validateFundamentalFields(configs)) {
      return -1;
    }
    std::cout << "Cryptographic configurations read appear to be sane.\n";
    std::cout << "Testing key functionality and agreement...\n";
    if (!testRSAKeys(configs)) {
      return -1;
    }
    if (!testThresholdKeys(configs, cryptoSystems)) {
      return -1;
    }

    // generate new threshold keypair for cryptosystem 0 and re-check the cryptosystems
    auto keyPair = cryptoSystems[0]->generateNewKeyPair();
    cryptoSystems[0]->updateKeys(keyPair.first, keyPair.second);
    for (uint16_t i = 1; i < numReplicas; ++i) cryptoSystems[i]->updateVerificationKey(keyPair.second, 1);

    if (!testThresholdKeys(configs, cryptoSystems)) {
      return -1;
    }

    std::cout << "Done testing all keys.\n";
    std::cout << "TestGeneratedKeys: SUCCESS.\n";

    return 0;
  } catch (std::exception& e) {
    std::cerr << "Exception: " << e.what() << std::endl;
    return 1;
  }
}
