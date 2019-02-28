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

#include <cryptopp/dll.h>

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

  CryptoPP::RSAES<CryptoPP::OAEP<CryptoPP::SHA256>>::Decryptor priv(
      sGlobalRandGen, rsaKeyLength);
  CryptoPP::HexEncoder privEncoder(new CryptoPP::StringSink(keyPair.first));
  priv.DEREncode(privEncoder);
  privEncoder.MessageEnd();

  CryptoPP::RSAES<CryptoPP::OAEP<CryptoPP::SHA256>>::Encryptor pub(priv);
  CryptoPP::HexEncoder pubEncoder(new CryptoPP::StringSink(keyPair.second));
  pub.DEREncode(pubEncoder);
  pubEncoder.MessageEnd();

  return keyPair;
}

static bool parseUInt16(uint16_t& output,
                        const std::string& str,
                        uint16_t min,
                        uint16_t max,
                        const std::string& name) {
  long long unverifiedNum;
  std::string errorMessage = "Invalid value given for " + name + ": " + str +
                             " (expected integer in range [" +
                             std::to_string(min) + ", " + std::to_string(max) +
                             "], inclusive.\n";

  try {
    unverifiedNum = std::stoll(str);
  } catch (std::invalid_argument e) {
    std::cout << errorMessage;
    return false;
  } catch (std::out_of_range e) {
    std::cout << errorMessage;
    return false;
  }

  if ((unverifiedNum < (long long)min) || (unverifiedNum > (long long)max)) {
    std::cout << errorMessage;
    return false;
  } else {
    output = (uint16_t)unverifiedNum;
    return true;
  }
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
  std::string usageMessage =
      "Usage:\n"
      "GenerateConcordKeys -n TOTAL_NUMBER_OF_REPLICAS \\\n"
      "  -f NUMBER_OF_FAULTY_REPLICAS_TO_TOLERATE -o OUTPUT_FILE_PREFIX\n"
      "The generated keys will be output to a number of files, one per replica."
      " The\nfiles will each be named OUTPUT_FILE_PREFIX<i>, where <i> is a"
      " sequential ID for\nthe replica to which the file corresponds in the"
      " range [0,\nTOTAL_NUMBER_OF_REPLICAS]. Each file contains all public"
      " keys for the cluster,\nbut only the private keys for the replica with"
      " the corresponding ID.\n"
      "Optionally, you may also choose what types of cryptosystems to use:\n"
      "  --execution_cryptosys SYSTEM_TYPE PARAMETER\n"
      "  --slow_commit_cryptosys SYSTEM_TYPE PARAMETER\n"
      "  --commit_cryptosys SYSTEM_TYPE PARAMETER\n"
      "  --opptimistic_commit_cryptosys SYSTEM_TYPE PARAMETER\n"
      "Currently, the following cryptosystem types are supported\n"
      "(and take the following as parameters):\n";

  std::vector<std::pair<std::string, std::string>> cryptosystemTypes;
  Cryptosystem::getAvailableCryptosystemTypes(cryptosystemTypes);
  for (size_t i = 0; i < cryptosystemTypes.size(); ++i) {
    usageMessage += "  " + cryptosystemTypes[i].first + " (" +
                    cryptosystemTypes[i].second + ")\n";
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

  uint16_t f;
  uint16_t n;

  // Note we have declared this stream locally to main and by value, and that,
  // if the output file is successfully opened, we are relying on ofstream's
  // destructor being called implicitly when main returns and this goes out of
  // scope to close the stream.
  std::string outputPrefix;

  bool hasF = false;
  bool hasN = false;
  bool hasOutput = false;

  std::string execType = "threshold-bls";
  std::string execParam = "BN-P254";
  std::string slowType = "threshold-bls";
  std::string slowParam = "BN-P254";
  std::string commitType = "threshold-bls";
  std::string commitParam = "BN-P254";
  std::string optType = "multisig-bls";
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
      std::string arg = argv[i + 1];
      if (parseUInt16(f, arg, 1, UINT16_MAX, "-f")) {
        hasF = true;
      } else {
        return -1;
      }
      ++i;

    } else if (option == "-n") {
      if (i >= argc - 1) {
        std::cout << "Expected an argument to -n.\n";
        return -1;
      }
      std::string arg = argv[i + 1];

      // Note we do not enforce a minimum value for n here; since we require
      // n > 3f and f > 0, lower bounds for n will be handled when we
      // enforce the n > 3f constraint below.
      if (parseUInt16(n, arg, 0, UINT16_MAX, "-n")) {
        hasN = true;
      } else {
        return -1;
      }
      ++i;

    } else if (option == "-o") {
      if (i >= argc - 1) {
        std::cout << "Expected an argument to -o.\n";
        return -1;
      }
      outputPrefix = argv[i + 1];
      hasOutput = true;
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
  if (!hasF) {
    std::cout << "No value given for required -f parameter.\n";
    return -1;
  }
  if (!hasN) {
    std::cout << "No value given for required -n parameter.\n";
    return -1;
  }
  if (!hasOutput) {
    std::cout << "No value given for required -o parameter.\n";
    return -1;
  }

  // Verify constraints between F and N and compute C.

  // Note we check that N >= 3F + 1 using uint32_ts even though F and N are
  // uint16_ts just in case 3F + 1 overflows a uint16_t.
  uint32_t minN = 3 * (uint32_t)f + 1;
  if ((uint32_t)n < minN) {
    std::cout << "Due to the design of Byzantine fault tolerance, number of"
                 " replicas (-n) must be\ngreater than or equal to (3 * F + "
                 "1), where F"
                 " is the maximum number of faulty\nreplicas (-f).\n";
    return -1;
  }

  // We require N - 3F - 1 to be even so C can be an integer.
  if (((n - (3 * f) - 1) % 2) != 0) {
    std::cout
        << "For technical reasons stemming from our current"
           " implementation of Byzantine\nfault tolerant consensus, we "
           "currently"
           " require that (N - 3F - 1) be even, where\nN is the total number of"
           " replicas (-n) and F is the maximum number of faulty\nreplicas "
           "(-f).\n";
    return -1;
  }

  uint16_t c = (n - (3 * f) - 1) / 2;

  uint16_t execThresh = f + 1;
  uint16_t slowThresh = f * 2 + c + 1;
  uint16_t commitThresh = f * 3 + c + 1;
  uint16_t optThresh = n;

  // Verify cryptosystem selections.
  if (!Cryptosystem::isValidCryptosystemSelection(
          execType, execParam, n, execThresh)) {
    std::cout << "Invalid selection of cryptosystem for execution cryptosystem"
                 " (with threshold "
              << execThresh << " out of " << n << "): " << execType << " "
              << execParam << ".\n";
    return -1;
  }
  if (!Cryptosystem::isValidCryptosystemSelection(
          slowType, slowParam, n, slowThresh)) {
    std::cout << "Invalid selection of cryptosystem for slow path commit"
                 " cryptosystem (with threshold "
              << slowThresh << " out of " << n << "): " << slowType << " "
              << slowParam << ".\n";
    return -1;
  }
  if (!Cryptosystem::isValidCryptosystemSelection(
          commitType, commitParam, n, commitThresh)) {
    std::cout << "Invalid selection of cryptosystem for commit cryptosystem"
                 " (with threshold "
              << commitThresh << " out of " << n << "): " << commitType << " "
              << commitParam << ".\n";
    return -1;
  }
  if (!Cryptosystem::isValidCryptosystemSelection(
          optType, optParam, n, optThresh)) {
    std::cout << "Invalid selection of cryptosystem for optimistic fast path"
                 " commit cryptosystem (with threshold "
              << optThresh << " out of " << n << "): " << optType << " "
              << optParam << ".\n";
    return -1;
  }

  // Validate that all output files are valid before possibly wasting a
  // significant ammount of time generating keys that cannot be output.
  std::vector<std::ofstream> outputFiles;
  for (uint16_t i = 0; i < n; ++i) {
    outputFiles.push_back(std::ofstream(outputPrefix + std::to_string(i)));
    if (!outputFiles.back().is_open()) {
      std::cout << "Could not open output file " << outputPrefix << i << ".\n";
      return -1;
    }
  }

  std::vector<std::pair<std::string, std::string>> rsaKeys;
  for (uint16_t i = 0; i < n; ++i) {
    rsaKeys.push_back(generateRsaKey());
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
    if (!outputReplicaKeyfile(i,
                              n,
                              f,
                              c,
                              outputFiles[i],
                              outputPrefix + std::to_string(i),
                              rsaKeys,
                              execSys,
                              slowSys,
                              commitSys,
                              optSys)) {
      return -1;
    }
  }

  return 0;
}
