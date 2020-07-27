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
        "  --execution_cryptosys SYSTEM_TYPE PARAMETER\n"
        "  --slow_commit_cryptosys SYSTEM_TYPE PARAMETER\n"
        "  --commit_cryptosys SYSTEM_TYPE PARAMETER\n"
        "  --opptimistic_commit_cryptosys SYSTEM_TYPE PARAMETER\n"
        "Currently, the following cryptosystem types are supported (and take the following as parameters):\n";

    std::vector<std::pair<std::string, std::string>> cryptosystemTypes;
    Cryptosystem::getAvailableCryptosystemTypes(cryptosystemTypes);
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
    bftEngine::ReplicaConfig config;
    uint16_t n = 0;
    uint16_t ro = 0;

    // Note we have declared this stream locally to main and by value, and that,
    // if the output file is successfully opened, we are relying on ofstream's
    // destructor being called implicitly when main returns and this goes out of
    // scope to close the stream.
    std::string outputPrefix;

    std::string execType = MULTISIG_BLS_SCHEME;
    std::string execParam = "BN-P254";
    std::string slowType = MULTISIG_BLS_SCHEME;
    std::string slowParam = "BN-P254";
    std::string commitType = MULTISIG_BLS_SCHEME;
    std::string commitParam = "BN-P254";
    std::string optType = MULTISIG_BLS_SCHEME;
    std::string optParam = "BN-P254";

    // Read input from the command line.
    // Note we ignore argv[0] because that just contains the command that was used
    // to launch this executable by convention.
    for (int i = 1; i < argc; ++i) {
      std::string option(argv[i]);

      if (option == "-f") {
        if (i >= argc - 1) {
          std::cout << "Expected an argument to -f.\n";
          return -1;
        }
        config.fVal = parse<std::uint16_t>(argv[i + 1], "-f");
        ++i;

      } else if (option == "-n") {
        if (i >= argc - 1) {
          std::cout << "Expected an argument to -n.\n";
          return -1;
        }
        // Note we do not enforce a minimum value for n here; since we require
        // n > 3f and f > 0, lower bounds for n will be handled when we
        // enforce the n > 3f constraint below.
        n = parse<std::uint16_t>(argv[i + 1], "-n");
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

      } else if (option == "--execution_cryptosys") {
        if (i >= argc - 2) {
          std::cout << "Expected 2 arguments to --execution_cryptosys.\n";
          return -1;
        }
        execType = argv[i + 1];
        execParam = argv[i + 2];
        i += 2;

      } else if (option == "--slow_commit_cryptosys") {
        if (i >= argc - 2) {
          std::cout << "Expected 2 arguments to --slow_commit_cryptosys.\n";
          return -1;
        }
        slowType = argv[i + 1];
        slowParam = argv[i + 2];
        i += 2;

      } else if (option == "--commit_cryptosys") {
        if (i >= argc - 2) {
          std::cout << "Expected 2 arguments to --execution_cryptosys.\n";
          return -1;
        }
        commitType = argv[i + 1];
        commitParam = argv[i + 2];
        i += 2;

      } else if (option == "--optimistic_commit_cryptosys") {
        if (i >= argc - 2) {
          std::cout << "Expected 2 arguments to --execution_cryptosys.\n";
          return -1;
        }
        optType = argv[i + 1];
        optParam = argv[i + 2];
        i += 2;

      } else {
        std::cout << "Unrecognized command line argument: " << option << "\n";
        return -1;
      }
    }

    // Check that required parameters were actually given.
    if (config.fVal == 0) {
      std::cout << "No value given for required -f parameter.\n";
      return -1;
    }
    if (n == 0) {
      std::cout << "No value given for required -n parameter.\n";
      return -1;
    }
    if (outputPrefix.empty()) {
      std::cout << "No value given for required -o parameter.\n";
      return -1;
    }

    // Verify constraints between F and N and compute C.

    // Note we check that N >= 3F + 1 using uint32_ts even though F and N are
    // uint16_ts just in case 3F + 1 overflows a uint16_t.
    uint32_t minN = 3 * (uint32_t)config.fVal + 1;
    if ((uint32_t)n < minN) {
      std::cout << "Due to the design of Byzantine fault tolerance, number of"
                   " replicas (-n) must be\ngreater than or equal to (3 * F + 1), where F"
                   " is the maximum number of faulty\nreplicas (-f).\n";
      return -1;
    }

    // We require N - 3F - 1 to be even so C can be an integer.
    if (((n - (3 * config.fVal) - 1) % 2) != 0) {
      std::cout << "For technical reasons stemming from our current"
                   " implementation of Byzantine\nfault tolerant consensus, we currently"
                   " require that (N - 3F - 1) be even, where\nN is the total number of"
                   " replicas (-n) and F is the maximum number of faulty\nreplicas (-f).\n";
      return -1;
    }

    config.cVal = (n - (3 * config.fVal) - 1) / 2;

    uint16_t execThresh = config.fVal + 1;
    uint16_t slowThresh = config.fVal * 2 + config.cVal + 1;
    uint16_t commitThresh = config.fVal * 3 + config.cVal + 1;
    uint16_t optThresh = n;

    // Verify cryptosystem selections.
    if (!Cryptosystem::isValidCryptosystemSelection(execType, execParam, n, execThresh)) {
      std::cout << "Invalid selection of cryptosystem for execution cryptosystem"
                   " (with threshold "
                << execThresh << " out of " << n << "): " << execType << " " << execParam << ".\n";
      return -1;
    }
    if (!Cryptosystem::isValidCryptosystemSelection(slowType, slowParam, n, slowThresh)) {
      std::cout << "Invalid selection of cryptosystem for slow path commit"
                   " cryptosystem (with threshold "
                << slowThresh << " out of " << n << "): " << slowType << " " << slowParam << ".\n";
      return -1;
    }
    if (!Cryptosystem::isValidCryptosystemSelection(commitType, commitParam, n, commitThresh)) {
      std::cout << "Invalid selection of cryptosystem for commit cryptosystem"
                   " (with threshold "
                << commitThresh << " out of " << n << "): " << commitType << " " << commitParam << ".\n";
      return -1;
    }
    if (!Cryptosystem::isValidCryptosystemSelection(optType, optParam, n, optThresh)) {
      std::cout << "Invalid selection of cryptosystem for optimistic fast path"
                   " commit cryptosystem (with threshold "
                << optThresh << " out of " << n << "): " << optType << " " << optParam << ".\n";
      return -1;
    }

    std::vector<std::pair<std::string, std::string>> rsaKeys;
    for (uint16_t i = 0; i < n + ro; ++i) {
      rsaKeys.push_back(generateRsaKey());
      config.publicKeysOfReplicas.insert(std::pair<uint16_t, std::string>(i, rsaKeys[i].second));
    }

    Cryptosystem execSys(execType, execParam, n, execThresh);
    Cryptosystem slowSys(slowType, slowParam, n, slowThresh);
    Cryptosystem commitSys(commitType, commitParam, n, commitThresh);
    Cryptosystem optSys(optType, optParam, n, optThresh);

    execSys.generateNewPseudorandomKeys();
    slowSys.generateNewPseudorandomKeys();
    commitSys.generateNewPseudorandomKeys();
    optSys.generateNewPseudorandomKeys();

    // Output the generated keys.
    for (uint16_t i = 0; i < n; ++i) {
      config.replicaId = i;
      config.replicaPrivateKey = rsaKeys[i].first;
      outputReplicaKeyfile(n, ro, config, outputPrefix + std::to_string(i), &execSys, &slowSys, &commitSys, &optSys);
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
