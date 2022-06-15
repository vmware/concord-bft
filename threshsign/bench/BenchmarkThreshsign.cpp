// Concord
//
// Copyright (c) 2018-2022 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the
// LICENSE file.
//

#define PICOBENCH_IMPLEMENT
#define PICOBENCH_STD_FUNCTION_BENCHMARKS

#include <vector>
#include <deque>
#include <cstdlib>
#include <iostream>
#include <chrono>
#include <boost/program_options.hpp>
#include "thread_pool.hpp"

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wconversion"
#pragma GCC diagnostic ignored "-Wsign-conversion"
#pragma GCC diagnostic ignored "-Wsign-compare"
#pragma GCC diagnostic ignored "-Wunused-variable"
#pragma GCC diagnostic ignored "-Wc99-extensions"
#include "picobench.h"
#include "macros.h"
#include "include/threshsign/eddsa/EdDSAMultisigFactory.h"
#include "include/threshsign/eddsa/SingleEdDSASignature.h"
#include "include/threshsign/bls/relic/BlsThresholdFactory.h"
#include "include/threshsign/bls/relic/PublicParametersFactory.h"

static std::unique_ptr<std::vector<uint8_t>> s_data{nullptr};

/**
 * Initializes randomDataBytes random bytes
 * @param randomDataBytes
 */
void initRandomData(size_t randomDataBytes) {
  ConcordAssertEQ(randomDataBytes % sizeof(uint32_t), 0);
  // Single threaded implementation
  s_data = std::make_unique<std::vector<uint8_t>>(randomDataBytes);
  uint32_t* randData = (uint32_t*)s_data->data();
  std::cout << "Initializing " << (randomDataBytes >> 10) << "KB of uniform random data... ";
  std::cout.flush();
  std::srand(20222022);
  for (int i = 0; i < randomDataBytes / sizeof(uint32_t); i++) {
    randData[i] = std::rand();
  }
  std::cout << "Initialization finished" << std::endl;
}

const std::vector<uint8_t>& getRandomData() { return *s_data; }

enum Algorithm { EdDSA = 1, BlsThreshold = 2, BlsMultisig = 3 };

/// Returns an initialized factory which is used to generate signers and verifiers of alg
std::unique_ptr<IThresholdFactory> getFactory(Algorithm alg) {
  if (alg == EdDSA) {
    return std::make_unique<EdDSAMultisigFactory>();
  }
  if (alg == BlsThreshold || alg == BlsMultisig) {
    bool multisig = alg == BlsMultisig;
    return std::make_unique<BLS::Relic::BlsThresholdFactory>(
        BLS::Relic::PublicParametersFactory::getByCurveType("BN-P254"), multisig);
  }

  throw;
}

/// Used because of the interface of picobench
struct BenchmarkParams {
  std::unique_ptr<IThresholdFactory> factory;
  uint64_t messageByteCount;
  int signerCount;
  int threshold;
  uint64_t messageCount;
};

/**
 * A benchmark which measures the time it takes for threshold signers to sign messageCount messages
 * @param s
 */
void signerBenchmark(picobench::state& s) {
  const BenchmarkParams& params = *(BenchmarkParams*)s.user_data();
  auto& [factory, msgByteSize, signerCount, threshold, messageCount] = params;
  const auto& dataToSign = getRandomData();
  auto [signers, _] = factory->newRandomSigners(threshold, signerCount);

  std::vector<std::unique_ptr<char[]>> signatures;
  const auto signatureSize = signers[1]->requiredLengthForSignedData();
  for (int i = 0; i < threshold; i++) {
    signatures.push_back(std::make_unique<char[]>(signatureSize));
  }

  uint64_t signaturesPerformed = 0;
  {
    picobench::scope scope(s);
    for (int msgIdx = 0; msgIdx < s.iterations(); msgIdx++) {
      const auto* messageBytes = &dataToSign[(msgIdx * msgByteSize) % messageCount];
      for (int i = 1; i <= threshold; i++) {
        signers[i]->signData((const char*)messageBytes, msgByteSize, signatures[i - 1].get(), signatureSize);
        signaturesPerformed++;
      }
    }
  }
  s.set_result(signaturesPerformed);
}

/**
 * A benchmark which measures the time it takes for a verifier to verify threshold signatures of messageCount messages
 * @param s
 */
void verifierBenchmark(picobench::state& s) {
  const BenchmarkParams& params = *(BenchmarkParams*)s.user_data();
  auto& [factory, msgByteSize, signerCount, threshold, messageCount] = params;

  if (s.iterations() > messageCount) {
    std::cout << "Warning: The number of verification rounds: " << s.iterations()
              << ", Exceeds the message count: " << messageCount
              << ".\nResults will not include accumulation time for the surplus messages." << std::endl;
  }

  auto& dataToSign = getRandomData();
  auto [signers, verifier] = factory->newRandomSigners(threshold, signerCount);

  const auto signatureSize = signers[1]->requiredLengthForSignedData();
  std::vector<std::vector<std::unique_ptr<char[]>>> allSignatures(threshold);

  for (int msgIdx = 0; msgIdx < std::min((uint64_t)s.iterations(), messageCount); msgIdx++) {
    const auto* msgBytes = &dataToSign[msgIdx * msgByteSize];
    for (int signer = 1; signer <= threshold; signer++) {
      auto& signerSignatures = allSignatures[signer - 1];
      signerSignatures.push_back(std::make_unique<char[]>(signatureSize));
      auto& currentSignature = signerSignatures.back();
      signers[signer]->signData((const char*)msgBytes, msgByteSize, currentSignature.get(), signatureSize);
    }
  }

  std::vector<IThresholdAccumulator*> accumulators(messageCount);
  for (int msgIdx = 0; msgIdx < messageCount; msgIdx++) {
    accumulators[msgIdx] = verifier->newAccumulator(false);
  }

  std::vector<char> fullSignatureBuffer(verifier->requiredLengthForSignedData());
  uint64_t validVerificationCount = 0;
  {
    picobench::scope scope(s);
    for (int iteration = 0; iteration < s.iterations(); iteration++) {
      auto msgIdx = iteration % messageCount;
      auto* msg = &dataToSign[msgIdx * msgByteSize];

      auto& accumulator = *accumulators[msgIdx];
      accumulator.setExpectedDigest((const uint8_t*)msg, msgByteSize);
      if (iteration < messageCount) {
        for (int i = 1; i <= threshold; i++) {
          auto& currentMsgSignature = allSignatures[i - 1][msgIdx];
          accumulator.add(currentMsgSignature.get(), signatureSize);
        }
      }

      auto actualSigBytes = accumulator.getFullSignedData(fullSignatureBuffer.data(), fullSignatureBuffer.size());
      validVerificationCount +=
          verifier->verify((const char*)msg, msgByteSize, fullSignatureBuffer.data(), actualSigBytes);
    }
  }
  s.set_result(validVerificationCount);
}

std::function<void(picobench::state& s)> printInfo(const std::string& name,
                                                   std::function<void(picobench::state& s)> func) {
  return [func, name](picobench::state& s) {
    std::cout << "Running " << name << "...";
    std::cout.flush();
    auto start = std::chrono::high_resolution_clock::now();
    func(s);
    auto end = std::chrono::high_resolution_clock::now();
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
    std::cout << " Done. Result: " << (uint64_t)s.result() << ", Time Elapsed: " << elapsed << "ms" << std::endl;
  };
}

std::string appendBenchmarkInfo(const std::string& title,
                                uint64_t signerCount,
                                uint64_t threshold,
                                uint64_t msgLength) {
  return title + " Signers: " + std::to_string(signerCount) + std::string(", Threshold: ") + std::to_string(threshold) +
         ", Message Bytes: " + std::to_string(msgLength >> 10) + "KB";
}

namespace po = boost::program_options;

int main(int argc, char* argv[]) {
  po::options_description desc;
  bool noSignersBenchmark = true;
  bool noVerifierBenchmark = true;
  // clang-format off
  desc.add_options()
    ("signers", po::value<std::vector<int>>()->required()->multitoken(), "Number of signers, separated by a comma")
    ("thresholds", po::value<std::vector<int>>()->required()->multitoken(), "Signature validation thresholds, separated by a comma")
    ("random", po::value<uint64_t>()->default_value(1 << 20), "Random bytes to be used as messages to sign")
    ("message", po::value<uint64_t>()->default_value(1 << 10), "The byte size of a single message")
    ("no-signer", po::bool_switch(&noSignersBenchmark), "Dont benchmark signers")
    ("no-verifier", po::bool_switch(&noVerifierBenchmark), "Dont benchmark verifier")
  ;
  // clang-format on
  po::variables_map opts;
  po::store(po::command_line_parser(argc, argv).options(desc).allow_unregistered().run(), opts);
  po::notify(opts);

  auto signers = opts["signers"].as<std::vector<int>>();
  auto thresholds = opts["thresholds"].as<std::vector<int>>();

  if (signers.empty()) {
    std::cerr << "There must be at least one signer" << std::endl;
    exit(1);
  }

  if (signers.size() != thresholds.size()) {
    std::cerr << "The number of thresholds cannot differ from the number of parameters passes to the signers option"
              << std::endl;
    exit(1);
  }

  const auto randomByteCount = opts["random"].as<uint64_t>();
  const auto messageByteCount = opts["message"].as<uint64_t>();
  const std::unordered_map<Algorithm, std::string> algToName = {
      {EdDSA, "eddsa"}, {BlsThreshold, "bls-tresh"}, {BlsMultisig, "bls-multisig"}};
  std::unordered_map<std::string, std::function<void(picobench::state & s)>> suiteToFunction;

  if (!noSignersBenchmark) {
    suiteToFunction.emplace("SignerBenchmark", signerBenchmark);
  }

  if (!noVerifierBenchmark) {
    suiteToFunction.emplace("VerifierBenchmark", verifierBenchmark);
  }

  std::vector<std::string> titles;
  std::vector<std::unique_ptr<BenchmarkParams>> params;

  for (int i = 0; i < signers.size(); i++) {
    for (auto [suiteName, _] : suiteToFunction) {
      titles.push_back(appendBenchmarkInfo(suiteName, signers[i], thresholds[i], messageByteCount));
      picobench::global_registry::set_bench_suite(titles.back().c_str());
      for (auto& [alg, algName] : algToName) {
        BenchmarkParams currentParams = {
            .messageByteCount = messageByteCount,
            .factory = getFactory(alg),
            .signerCount = signers[i],
            .threshold = thresholds[i],
            .messageCount = randomByteCount / messageByteCount,
        };
        params.push_back(std::make_unique<BenchmarkParams>(std::move(currentParams)));
        auto& currentBenchmark = picobench::global_registry::new_benchmark(
            algName.c_str(), printInfo(titles.back() + ", Algorithm: " + algName, suiteToFunction.at(suiteName)));
        currentBenchmark.user_data((uintptr_t)params.back().get());
        if (algName == algToName.at(EdDSA)) {
          currentBenchmark.baseline(true);
        }
      }
    }
  }

  initRandomData(randomByteCount);
  constexpr const uint64_t picobenchSeed = 20222022;
  picobench::runner runner;
  runner.set_default_samples(1);

  runner.parse_cmd_line(argc, argv, "-pico");
  return runner.run(picobenchSeed);
}

#pragma GCC diagnostic pop