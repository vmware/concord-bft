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

// #define PICOBENCH_DEBUG
// #define PICOBENCH_IMPLEMENT_WITH_MAIN
#define PICOBENCH_IMPLEMENT
#define PICOBENCH_STD_FUNCTION_BENCHMARKS

#include <vector>
#include <cstdlib>
#include <iostream>
#include <random>

#include "util/thread_pool.hpp"
#include "picobench/picobench.hpp"
#include "crypto/crypto.hpp"
#include "crypto/openssl/EdDSASigner.hpp"
#include "crypto/openssl/EdDSAVerifier.hpp"
#include "util/filesystem.hpp"

namespace concord::benchmark {
using std::string;
using std::unique_ptr;
using concord::crypto::KeyFormat;
using concord::crypto::generateEdDSAKeyPair;
using concord::crypto::openssl::EdDSAPrivateKey;
using concord::crypto::openssl::EdDSAPublicKey;
using concord::crypto::openssl::deserializeKey;

using TestSigner = concord::crypto::openssl::EdDSASigner<EdDSAPrivateKey>;
using TestVerifier = concord::crypto::openssl::EdDSAVerifier<EdDSAPublicKey>;
using SignatureBytes = std::vector<concord::Byte>;

std::default_random_engine generator;

constexpr size_t RANDOM_DATA_SIZE = 1000U;
constexpr uint8_t RANDOM_DATA_ARRAY_SIZE = 100U;
static string randomData[RANDOM_DATA_ARRAY_SIZE];

const auto eddsaKeysPair = generateEdDSAKeyPair();

/**
 * Initializes 'randomData' with random bytes of size 'len'.
 * @param len Length of the random data to be generated.
 */
void generateRandomData(size_t len) {
  for (uint8_t i{0}; i < RANDOM_DATA_ARRAY_SIZE; ++i) {
    randomData[i].reserve(RANDOM_DATA_SIZE);
  }

  std::uniform_int_distribution<int> distribution(0, 0xFF);

  for (uint8_t i{0}; i < RANDOM_DATA_ARRAY_SIZE; ++i) {
    for (size_t j{0}; j < len; ++j) {
      randomData[i][j] = static_cast<char>(distribution(generator));
    }
  }
}

/**
 * A benchmark which measures the time it takes for EdDSA signer to sign a message.
 * @param s
 */
void edDSASignerBenchmark(picobench::state& s) {
  const auto signingKey = deserializeKey<EdDSAPrivateKey>(eddsaKeysPair.first);
  auto signer_ = unique_ptr<concord::crypto::ISigner>(new TestSigner(signingKey.getBytes()));
  size_t expectedSignerSigLen = signer_->signatureLength();
  SignatureBytes sig(expectedSignerSigLen);

  // Sign with EdDSASigner.
  size_t lenRetData;

  uint64_t signaturesPerformed = 0;
  {
    picobench::scope scope(s);

    for (int msgIdx = 0; msgIdx < s.iterations(); msgIdx++) {
      lenRetData = signer_->sign(randomData[msgIdx % RANDOM_DATA_ARRAY_SIZE], sig.data());
      ++signaturesPerformed;
      ConcordAssertEQ(lenRetData, expectedSignerSigLen);
    }
  }
  s.set_result(signaturesPerformed);
}

/**
 * @brief A benchmark which measures the time it takes for EdDSA verifier to verify a signature.
 *
 * @param s
 */
void edDSAVerifierBenchmark(picobench::state& s) {
  const auto signingKey = deserializeKey<EdDSAPrivateKey>(eddsaKeysPair.first);
  auto signer_ = unique_ptr<concord::crypto::ISigner>(new TestSigner(signingKey.getBytes()));
  size_t expectedSignerSigLen = signer_->signatureLength();
  SignatureBytes sig(expectedSignerSigLen);
  const auto verificationKey = deserializeKey<EdDSAPublicKey>(eddsaKeysPair.second);
  auto verifier_ = unique_ptr<TestVerifier>(new TestVerifier(verificationKey.getBytes()));

  // Sign with EdDSASigner.
  const auto offset = (uint8_t)rand() % RANDOM_DATA_ARRAY_SIZE;
  size_t lenRetData = signer_->sign(randomData[offset], sig.data());
  ConcordAssertEQ(lenRetData, expectedSignerSigLen);

  std::string sigAsString(sig.size(), '\x00');
  std::memcpy(sigAsString.data(), sig.data(), sig.size());
  uint64_t signaturesVerified = 0;
  {
    picobench::scope scope(s);

    for (int msgIdx = 0; msgIdx < s.iterations(); msgIdx++) {
      ++signaturesVerified;

      // validate with EdDSAVerifier.
      ConcordAssert(verifier_->verify(randomData[offset], sigAsString));
    }
  }
  s.set_result(signaturesVerified);
}

/**
 * @brief Construct a new PICOBENCH object.
 * Take one of the fastest samples out of 2 samples.
 */
PICOBENCH(edDSASignerBenchmark).label("EdDSA-Signer").samples(2).iterations({1, 10, 100, 1000, 10000});
PICOBENCH(edDSAVerifierBenchmark).label("EdDSA-Verifier").samples(2).iterations({1, 10, 100, 1000, 10000});
}  // namespace concord::benchmark

/**
 * @brief Entry function.
 *
 * @param argc
 * @param argv
 * @return int
 */
int main(int argc, char* argv[]) {
  concord::benchmark::generateRandomData(concord::benchmark::RANDOM_DATA_SIZE);

  constexpr const uint64_t picobenchSeed = 20222022;
  picobench::runner runner;
  runner.set_default_samples(1);

  return runner.run(picobenchSeed);
}
