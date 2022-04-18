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

#include <config_file_parser.hpp>
#include "utt_blockchain_app.hpp"
#include "utt_config.hpp"

using namespace libutt;
using namespace utt_config;

using Fr = typename libff::default_ec_pp::Fp_type;

int main(int argc, char** argv) {
  try {
    if (argc != 2) {
      std::cout << "Usage: provide a utt configuration file!\n";
      exit(-1);
    }

    logging::initLogger("config/logging.properties");
    auto logger = logging::getLogger("uttdemo.gen-utt-cfg");

    std::string cfgFileName = argv[1];
    concord::util::ConfigFileParser cfgFileParser(logger, cfgFileName);
    if (!cfgFileParser.Parse()) throw std::runtime_error("Failed to parse configuration file: " + cfgFileName);

    // Config values
    std::string walletConfigPrefix;
    std::string replicaConfigPrefix;
    int numReplicas = 0;
    int numFaulty = 0;
    std::string currencyUnit;
    int publicBalance = 0;
    std::vector<size_t> normalCoinValues;
    size_t budgetCoinValue = 0;
    std::vector<std::string> walletPids;

    {  // Wallet config file prefix (Required)
      if (cfgFileParser.Count("wallet_config_prefix") != 1)
        throw std::runtime_error("Must provide single wallet config prefix value!");
      walletConfigPrefix = cfgFileParser.GetNthValue("wallet_config_prefix", 1);
      if (walletConfigPrefix.empty()) throw std::runtime_error("Wallet config prefix is empty!");
    }

    {  // Replica config file prefix (Required)
      if (cfgFileParser.Count("replica_config_prefix") != 1)
        throw std::runtime_error("Must provide single replica config prefix value!");
      replicaConfigPrefix = cfgFileParser.GetNthValue("replica_config_prefix", 1);
      if (walletConfigPrefix.empty()) throw std::runtime_error("Replica config prefix is empty!");
    }

    {  // Number of replicas (Required)
      if (cfgFileParser.Count("num_replicas") != 1) throw std::runtime_error("Must provide number of replicas!");
      auto str = cfgFileParser.GetNthValue("num_replicas", 1);
      numReplicas = std::atoi(str.c_str());
      if (numReplicas < 4) throw std::runtime_error("Number of replicas cannot be less than 4!");
    }

    {  // Number of faulty replicas (Required)
      if (cfgFileParser.Count("num_faulty") != 1) throw std::runtime_error("Must provide number of faulty replicas!");
      auto str = cfgFileParser.GetNthValue("num_faulty", 1);
      numFaulty = std::atoi(str.c_str());
      if (numFaulty < 1) throw std::runtime_error("Number of faulty replicas cannot be less than 1!");
    }

    {  // Unit for displaying the currency values (Optional)
      currencyUnit = cfgFileParser.GetNthValue("currency_unit", 1);
      if (currencyUnit.empty()) currencyUnit = "$";  // Use Default
    }

    {  // Configure initial public balance for each account (Optional)
      auto str = cfgFileParser.GetNthValue("public_balance", 1);
      if (!str.empty()) {
        publicBalance = std::atoi(str.c_str());
        if (publicBalance < 0) throw std::runtime_error("Public balance cannot be negative!");
      }
    }

    {  // List of initial utt coins for each wallet (Optional)
      auto strValues = cfgFileParser.GetValues("utt_coins");
      for (const auto& str : strValues) {
        int value = atoi(str.c_str());
        if (value <= 0) throw std::runtime_error("UTT coin value must be positive!");
        normalCoinValues.emplace_back(static_cast<size_t>(value));
      }
    }

    {  // Initial utt anonymous budget (Required)
      if (cfgFileParser.Count("utt_budget") != 1) throw std::runtime_error("Must provide utt budget value!");
      auto str = cfgFileParser.GetNthValue("utt_budget", 1);
      int value = std::atoi(str.c_str());
      if (value <= 0) throw std::runtime_error{"UTT budget must be positive!"};
      budgetCoinValue = static_cast<size_t>(value);
    }

    {  // List of wallet pids (Required)
      walletPids = cfgFileParser.GetValues("wallet_pids");
      if (walletPids.empty()) throw std::runtime_error("Must provide wallet pids!");

      std::vector<std::string> checkUnique{walletPids};
      std::unique(checkUnique.begin(), checkUnique.end());
      if (walletPids.size() != checkUnique.size()) throw std::runtime_error("Must provide unique pids!");
    }

    int minN = 3 * numFaulty + 1;
    if (numReplicas < minN)
      throw std::runtime_error(
          "Due to the design of Byzantine fault tolerance, number of"
          " replicas must be greater than or equal to (3 * F + 1), where F"
          " is the maximum number of faulty replicas");

    // Initialize library
    UTTBlockchainApp::initUTTLibrary();

    int thresh = numFaulty + 1;
    RandSigDKG dkg = RandSigDKG(thresh, numReplicas, Params::NumMessages);
    const auto& bsk = dkg.getSK();
    auto bskShares = dkg.getAllShareSKs();

    Params p = Params::random(dkg.getCK());                             // All replicas
    RegAuthSK rsk = RegAuthSK::random(p.getRegCK(), p.getIbeParams());  // eventually not needed

    // Keep configs to check deserialization later
    std::vector<UTTClientConfig> clientConfigs;
    std::vector<UTTReplicaConfig> replicaConfigs;

    // Create client configs with a wallet and pre-minted coins
    for (int i = 0; i < (int)walletPids.size(); ++i) {
      UTTClientConfig clientCfg;
      clientCfg.pids_ = walletPids;                                   // Pids
      clientCfg.initPublicBalance_ = publicBalance;                   // Initial public balance
      clientCfg.wallet_.p = p;                                        // The Params
      clientCfg.wallet_.ask = rsk.registerRandomUser(walletPids[i]);  // The User Secret Key
      clientCfg.wallet_.bpk = bsk.toPK();                             // The Bank Public Key
      clientCfg.wallet_.rpk = rsk.toPK();                             // The Registry Public Key

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

      std::ofstream ofs(walletConfigPrefix + std::to_string(i + 1));
      ofs << clientCfg;

      clientConfigs.emplace_back(std::move(clientCfg));
    }

    // Create replica configs
    for (int i = 0; i < numReplicas; ++i) {
      UTTReplicaConfig replicaCfg;
      replicaCfg.pids_ = walletPids;                  // Pids
      replicaCfg.initPublicBalance_ = publicBalance;  // Initial public balance
      replicaCfg.p_ = p;                              // The Params
      replicaCfg.rpk_ = rsk.toPK();                   // The Registry Public Key
      replicaCfg.bpk_ = bsk.toPK();                   // The Bank Public Key
      replicaCfg.bskShare_ = bskShares[i];            // The Bank Secret Key Share

      std::ofstream ofs(replicaConfigPrefix + std::to_string(i));
      ofs << replicaCfg;

      replicaConfigs.emplace_back(std::move(replicaCfg));
    }

    // Check deserialization
    for (int i = 0; i < (int)walletPids.size(); ++i) {
      const auto fileName = walletConfigPrefix + std::to_string(i + 1);
      std::ifstream ifs(fileName);
      if (!ifs.is_open()) throw std::runtime_error("Could not open file " + fileName);

      UTTClientConfig cfg;
      ifs >> cfg;

      if (cfg != clientConfigs[i]) throw std::runtime_error("Client config deserialization mismatch: " + fileName);

      std::cout << "Verified serialization of " << fileName << '\n';
    }

    for (int i = 0; i < numReplicas; ++i) {
      const auto fileName = replicaConfigPrefix + std::to_string(i);
      std::ifstream ifs(fileName);
      if (!ifs.is_open()) throw std::runtime_error("Could not open file " + fileName);

      UTTReplicaConfig cfg;
      ifs >> cfg;

      if (cfg != replicaConfigs[i]) throw std::runtime_error("Replica config deserialization mismatch: " + fileName);

      std::cout << "Verified serialization of " << fileName << '\n';
    }

  } catch (std::exception& e) {
    std::cerr << "Exception: " << e.what() << std::endl;
    exit(-1);
  }
  return 0;
}
