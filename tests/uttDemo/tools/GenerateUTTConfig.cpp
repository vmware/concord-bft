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
#include <sstream>
#include <vector>
#include <string>

#include <string.hpp>

#include <utt/Params.h>
#include <utt/RandSigDKG.h>
#include <utt/RegAuth.h>
#include <utt/Wallet.h>
#include <utt/Coin.h>

#include "app_state.hpp"
#include "utt_config.hpp"

using namespace libutt;
using namespace utt_config;

using Fr = typename libff::default_ec_pp::Fp_type;

// Helper functions and static state to this executable's main function.
// static bool containsHelpOption(int argc, char** argv) {
//   for (int i = 1; i < argc; ++i) {
//     if (std::string(argv[i]) == "--help") {
//       return true;
//     }
//   }
//   return false;
// }

// template <typename T>
// T parse(const std::string& str, const std::string& name) {
//   try {
//     return concord::util::to<T>(str);
//   } catch (std::exception& e) {
//     std::ostringstream oss;
//     oss << "Exception: " << e.what() << " Invalid value  for " << name << ": " << str << " expected range ["
//         << std::numeric_limits<T>::min() << ", " << std::numeric_limits<T>::max() << "]";

//     throw std::runtime_error(oss.str());
//   }
// }

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

    // Display the usage message and exit if no arguments were given, or if --help
    // was given anywhere.
    // if ((argc <= 1) || (containsHelpOption(argc, argv))) {
    //   std::cout << usageMessage;
    //   return 0;
    // }

    uint16_t n = 4;
    uint16_t f = 1;
    std::string clientOutputPrefix = "utt_client_";
    std::string replicaOutputPrefix = "utt_replica_";

    // for (int i = 1; i < argc; ++i) {
    //   std::string option(argv[i]);
    //   if (option == "-f") {
    //     if (i >= argc - 1) throw std::runtime_error("Expected an argument to -f");
    //     f = parse<std::uint16_t>(argv[i++ + 1], "-f");
    //   } else if (option == "-n") {
    //     if (i >= argc - 1) throw std::runtime_error("Expected an argument to -n");
    //     // Note we do not enforce a minimum value for n here; since we require
    //     // n > 3f and f > 0, lower bounds for n will be handled when we
    //     // enforce the n > 3f constraint below.
    //     n = parse<std::uint16_t>(argv[i++ + 1], "-n");
    //   } else if (option == "-r") {
    //     if (i >= argc - 1) throw std::runtime_error("Expected an argument to -r");
    //     ro = parse<std::uint16_t>(argv[i++ + 1], "-r");
    //   } else if (option == "-o") {
    //     if (i >= argc - 1) throw std::runtime_error("Expected an argument to -o");
    //     outputPrefix = argv[i++ + 1];
    //   } else {
    //     throw std::runtime_error("Unrecognized command line argument: " + option);
    //   }
    // }

    // Check that required parameters were actually given.
    if (f == 0) throw std::runtime_error("No value given for required -f parameter");
    if (n == 0) throw std::runtime_error("No value given for required -n parameter");
    // if (outputPrefix.empty()) throw std::runtime_error("No value given for required -o parameter");

    // Verify constraints between F and N and compute C.

    // Note we check that N >= 3F + 1 using uint32_ts even though F and N are
    // uint16_ts just in case 3F + 1 overflows a uint16_t.
    uint32_t minN = 3 * (uint32_t)f + 1;
    if ((uint32_t)n < minN)
      throw std::runtime_error(
          "Due to the design of Byzantine fault tolerance, number of"
          " replicas (-n) must be greater than or equal to (3 * F + 1), where F"
          " is the maximum number of faulty\nreplicas (-f)");

    // Initialize library
    AppState::initUTTLibrary();

    int thresh = f + 1;
    int numClients = 3;

    RandSigDKG dkg = RandSigDKG(thresh, n, Params::NumMessages);
    const auto& bsk = dkg.getSK();
    auto bskShares = dkg.getAllShareSKs();

    Params p = Params::random(dkg.getCK());                             // All replicas
    RegAuthSK rsk = RegAuthSK::random(p.getRegCK(), p.getIbeParams());  // eventually not needed

    std::vector<size_t> normalCoinValues = {100, 100};
    size_t budgetCoinValue = 1000;

    // Keep configs to check deserialization later
    std::vector<UTTClientConfig> clientConfigs;
    std::vector<UTTReplicaConfig> replicaConfigs;
    std::vector<std::string> pids;

    // Create client pids
    for (int i = 0; i < numClients; ++i) pids.emplace_back("user_" + std::to_string(i + 1));

    // Create client configs with a wallet and pre-minted coins
    for (int i = 0; i < numClients; ++i) {
      UTTClientConfig clientCfg;
      clientCfg.pids_ = pids;                                   // Pids
      clientCfg.wallet_.p = p;                                  // The Params
      clientCfg.wallet_.ask = rsk.registerRandomUser(pids[i]);  // The User Secret Key
      clientCfg.wallet_.bpk = bsk.toPK();                       // The Bank Public Key
      clientCfg.wallet_.rpk = rsk.toPK();                       // The Registry Public Key

      // Pre-mint normal coins
      for (size_t val : normalCoinValues) {
        auto sn = Fr::random_element();
        auto val_fr = Fr(static_cast<long>(val));
        Coin c(p.getCoinCK(), p.null, sn, val_fr, Coin::NormalType(), Coin::DoesNotExpire(), clientCfg.wallet_.ask);

        // sign *full* coin commitment using bank's SK
        c.sig = bsk.sign(c.augmentComm());

        clientCfg.wallet_.coins.emplace_back(std::move(c));
      }

      // Pre-mint budget coin
      if (budgetCoinValue > 0) {
        auto sn = Fr::random_element();
        auto val_fr = Fr(static_cast<long>(budgetCoinValue));
        Coin c(
            p.getCoinCK(), p.null, sn, val_fr, Coin::BudgetType(), Coin::SomeExpirationDate(), clientCfg.wallet_.ask);

        // sign *full* coin commitment using bank's SK
        c.sig = bsk.sign(c.augmentComm());

        clientCfg.wallet_.budgetCoin = std::move(c);
      }

      std::ofstream ofs(clientOutputPrefix + std::to_string(i + 1));
      ofs << clientCfg;

      clientConfigs.emplace_back(std::move(clientCfg));
    }

    // Create replica configs
    for (int i = 0; i < n; ++i) {
      UTTReplicaConfig replicaCfg;
      replicaCfg.pids_ = pids;              // Pids
      replicaCfg.p_ = p;                    // The Params
      replicaCfg.rpk_ = rsk.toPK();         // The Registry Public Key
      replicaCfg.bpk_ = bsk.toPK();         // The Bank Public Key
      replicaCfg.bskShare_ = bskShares[i];  // The Bank Secret Key Share

      std::ofstream ofs(replicaOutputPrefix + std::to_string(i));
      ofs << replicaCfg;

      replicaConfigs.emplace_back(std::move(replicaCfg));
    }

    // Check deserialization
    for (int i = 0; i < numClients; ++i) {
      const auto fileName = clientOutputPrefix + std::to_string(i + 1);
      std::ifstream ifs(fileName);
      if (!ifs.is_open()) throw std::runtime_error("Could not open file " + fileName);

      UTTClientConfig cfg;
      ifs >> cfg;

      if (cfg != clientConfigs[i]) throw std::runtime_error("Client config deserialization mismatch: " + fileName);

      std::cout << "Successfully deserialized " << fileName << '\n';
      std::cout << "num coins: " << cfg.wallet_.coins.size() << '\n';
      std::cout << "pids: " << JoinStr(cfg.pids_) << '\n';
    }

    for (int i = 0; i < n; ++i) {
      const auto fileName = replicaOutputPrefix + std::to_string(i);
      std::ifstream ifs(fileName);
      if (!ifs.is_open()) throw std::runtime_error("Could not open file " + fileName);

      UTTReplicaConfig cfg;
      ifs >> cfg;

      if (cfg != replicaConfigs[i]) throw std::runtime_error("Replica config deserialization mismatch: " + fileName);

      std::cout << "Successfully deserialized " << fileName << '\n';
    }

  } catch (std::exception& e) {
    std::cerr << "Exception: " << e.what() << std::endl;
    return 1;
  }
  return 0;
}