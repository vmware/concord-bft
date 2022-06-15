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
#include <vector>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wpedantic"
#include <cryptopp/dll.h>
#pragma GCC diagnostic pop

#include "threshsign/ThresholdSignaturesTypes.h"
#include "KeyfileIOUtils.hpp"
#include "util/filesystem.hpp"
// Helper functions and static state to this executable's main function.

static bool containsHelpOption(int argc, char** argv) {
  for (int i = 1; i < argc; ++i) {
    if (std::string(argv[i]) == "--help") {
      return true;
    }
  }
  return false;
}

static CryptoPP::RandomPool sGlobalRandGen;
const unsigned int rsaKeyLength = 2048;

static std::pair<std::string, std::string> generateRsaKey() {
  // Uses CryptoPP implementation of RSA key generation.

  std::pair<std::string, std::string> keyPair;

  CryptoPP::RSAES<CryptoPP::OAEP<CryptoPP::SHA256>>::Decryptor priv(sGlobalRandGen, rsaKeyLength);
  CryptoPP::HexEncoder privEncoder(new CryptoPP::StringSink(keyPair.first));
  priv.AccessMaterial().Save(privEncoder);
  privEncoder.MessageEnd();

  CryptoPP::RSAES<CryptoPP::OAEP<CryptoPP::SHA256>>::Encryptor pub(priv);
  CryptoPP::HexEncoder pubEncoder(new CryptoPP::StringSink(keyPair.second));
  pub.AccessMaterial().Save(pubEncoder);
  pubEncoder.MessageEnd();

  return keyPair;
}

/**
 * Main function for the GenerateConcordKeys executable. Pseudorandomly
 * generates a new set of keys for a Concord deployment with given F and C
 * values and writes them to an output file. The output is formatted using
 * constructs from a subset of YAML to make it both human and machine readable.
 * The output includes an RSA key pair for each of the replicas for general
 * communication and non-threshold cryptographic purposes and the complete key
 * set for the four threshold cryptosystems used by Concord. All
 * per-replica keys are given in lists in which the order corresponds to the
 * replicas' order.
 *
 * @param argc The number of command line arguments to this main function,
 *             including the command this executable was launched with.
 * @param argv Command line arguments to this function, with the first being the
 *             command this executable was launched with as is conventional. A
 *             description of expected and supported command line parameters is
 *             available by runnign the utility with the "--help" option.
 *
 * @return 0; currently this utility will output an error message to the command
 *         line if it exits unsuccessfully due to invalid command-line
 *         parameters, but it will not return an exit code indicating an error.
 */
int main(int argc, char** argv) {
  try {
    std::string usageMessage =
        "Usage:\n"
        "GenerateConcordKeys:\n"
        "  -n Number of regular replicas\n"
        "  -f Number of faulty replicas to tolerate\n"
        "  -r Number of read-only replicas\n"
        "  -o Output file prefix\n"
        "   --help - this help \n\n"
        "The generated keys will be output to a number of files, one per replica.\n"
        "The files will each be named OUTPUT_FILE_PREFIX<i>, where <i> is a sequential ID\n"
        "for the replica to which the file corresponds in the range [0,TOTAL_NUMBER_OF_REPLICAS].\n"
        "Each regular replica file contains all public keys for the cluster, private keys for itself.\n"
        "Each read-only replica contains only RSA public keys for the cluster.\n"
        "Optionally, for regular replica, types of cryptosystems to use can be chosen:\n"
        "  --slow_commit_cryptosys SYSTEM_TYPE PARAMETER\n"
        "  --commit_cryptosys SYSTEM_TYPE PARAMETER\n"
        "  --opptimistic_commit_cryptosys SYSTEM_TYPE PARAMETER\n"
        "Currently, the following cryptosystem types are supported (and take the following as parameters):\n";

    auto& cryptosystemTypes = Cryptosystem::getAvailableCryptosystemTypes();
    for (size_t i = 0; i < cryptosystemTypes.size(); ++i) {
      usageMessage += "  " + cryptosystemTypes[i].first + " (" + cryptosystemTypes[i].second + ")\n";
    }

    usageMessage +=
        "If any of these cryptosystem selections are not made"
        " explictly, a default will\nbe selected.\n\nSpecial options:\n  --help :"
        " display this usage message and exit.\n";

    // Display the usage message and exit if no arguments were given, or if --help
    // was given anywhere.
    if ((argc <= 1) || (containsHelpOption(argc, argv))) {
      std::cout << usageMessage;
      return 0;
    }
    bftEngine::ReplicaConfig& config = bftEngine::ReplicaConfig::instance();
    uint16_t n = 0;
    uint16_t ro = 0;
    std::string outputPrefix;

    std::string defaultSysType = "UninitializedCryptoSystem";
    std::string defaultSubSysType = "UninitializedCryptoSubSystem";

#ifdef USE_RELIC
    defaultSysType = MULTISIG_BLS_SCHEME;
    defaultSubSysType = "BN-P254";
#endif

// Note that if both USE_RELIC and MULTISIG_EDDSA_SCHEME macros are set, the last option is the one which will be taken
#ifdef USE_EDDSA_OPENSSL
    defaultSysType = MULTISIG_EDDSA_SCHEME;
    defaultSubSysType = "ED25519";
#endif

    // These are currently unused
    std::string slowType = defaultSysType;
    std::string slowParam = defaultSubSysType;
    std::string commitType = defaultSysType;
    std::string commitParam = defaultSubSysType;
    std::string optType = defaultSysType;
    std::string optParam = defaultSubSysType;

    for (int i = 1; i < argc; ++i) {
      std::string option(argv[i]);
      if (option == "-f") {
        if (i >= argc - 1) throw std::runtime_error("Expected an argument to -f");
        config.fVal = parse<std::uint16_t>(argv[i++ + 1], "-f");
      } else if (option == "-n") {
        if (i >= argc - 1) throw std::runtime_error("Expected an argument to -n");
        // Note we do not enforce a minimum value for n here; since we require
        // n > 3f and f > 0, lower bounds for n will be handled when we
        // enforce the n > 3f constraint below.
        n = parse<std::uint16_t>(argv[i++ + 1], "-n");
      } else if (option == "-r") {
        if (i >= argc - 1) throw std::runtime_error("Expected an argument to -r");
        ro = parse<std::uint16_t>(argv[i++ + 1], "-r");
      } else if (option == "-o") {
        if (i >= argc - 1) throw std::runtime_error("Expected an argument to -o");
        outputPrefix = argv[i++ + 1];
      } else if (option == "--slow_commit_cryptosys") {
        if (i >= argc - 2) throw std::runtime_error("Expected 2 arguments to --slow_commit_cryptosys");
        slowType = argv[i++ + 1];
        slowParam = argv[i++ + 2];
      } else if (option == "--commit_cryptosys") {
        if (i >= argc - 2) throw std::runtime_error("Expected 2 arguments to --execution_cryptosys");
        commitType = argv[i++ + 1];
        commitParam = argv[i++ + 2];
      } else if (option == "--optimistic_commit_cryptosys") {
        if (i >= argc - 2) throw std::runtime_error("Expected 2 arguments to --execution_cryptosys");
        optType = argv[i++ + 1];
        optParam = argv[i++ + 2];
      } else {
        throw std::runtime_error("Unrecognized command line argument: " + option);
      }
    }

    // Check that required parameters were actually given.
    if (config.fVal == 0) throw std::runtime_error("No value given for required -f parameter");
    if (n == 0) throw std::runtime_error("No value given for required -n parameter");
    if (outputPrefix.empty()) throw std::runtime_error("No value given for required -o parameter");

    // Verify constraints between F and N and compute C.

    // Note we check that N >= 3F + 1 using uint32_ts even though F and N are
    // uint16_ts just in case 3F + 1 overflows a uint16_t.
    uint32_t minN = 3 * (uint32_t)config.fVal + 1;
    if ((uint32_t)n < minN)
      throw std::runtime_error(
          "Due to the design of Byzantine fault tolerance, number of"
          " replicas (-n) must be greater than or equal to (3 * F + 1), where F"
          " is the maximum number of faulty\nreplicas (-f)");

    // We require N - 3F - 1 to be even so C can be an integer.
    if (((n - (3 * config.fVal) - 1) % 2) != 0)
      throw std::runtime_error(
          "For technical reasons stemming from our current"
          " implementation of Byzantine\nfault tolerant consensus, we currently"
          " require that (N - 3F - 1) be even, where\nN is the total number of"
          " replicas (-n) and F is the maximum number of faulty\nreplicas (-f)");

    config.cVal = (n - (3 * config.fVal) - 1) / 2;

    std::vector<std::pair<std::string, std::string>> rsaKeys;
    for (uint16_t i = 0; i < n + ro; ++i) {
      rsaKeys.push_back(generateRsaKey());
      config.publicKeysOfReplicas.insert(std::pair<uint16_t, std::string>(i, rsaKeys[i].second));
    }

    // We want to generate public key for n-out-of-n case
    Cryptosystem cryptoSys(defaultSysType, defaultSubSysType, n, n);
    cryptoSys.generateNewPseudorandomKeys();

    LOG_INFO(GL, "Outputting replica keys to: " << fs::absolute(outputPrefix).parent_path().string());

    // Output the generated keys.
    for (uint16_t i = 0; i < n; ++i) {
      config.replicaId = i;
      config.replicaPrivateKey = rsaKeys[i].first;
      outputReplicaKeyfile(n, ro, config, outputPrefix + std::to_string(i), &cryptoSys);
    }

    for (uint16_t i = n; i < n + ro; ++i) {
      config.isReadOnly = true;
      config.replicaId = i;
      config.replicaPrivateKey = rsaKeys[i].first;
      outputReplicaKeyfile(n, ro, config, outputPrefix + std::to_string(i));
    }
  } catch (std::exception& e) {
    std::cerr << "Exception: " << e.what() << std::endl;
    return 1;
  }
  return 0;
}
