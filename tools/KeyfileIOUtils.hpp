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
#pragma once
#include <iostream>
#include <string.hpp>
#include <sstream>
#include <exception>
#include "bftengine/ReplicaConfig.hpp"
#include "threshsign/ThresholdSignaturesTypes.h"

/**
 * Output the required generated keys for a given replica to a file. The replica
 * can then read the keys back to fill out its bftEngine::ReplicaConfig& using
 * inputReplicaKeyFile below.
 *
 * @param numReplicas    The total number of replicas in the deployment this
 *                       replica will be in. Note numReplicas must be equal to
 *                       (3 * f + 2 * c + 1).
 *
 * @param numRoReplicas  The total number of read-only replicas in the deployment this
 *                       replica will be in.
 *
 * @param config         Replica configuration
 * @param outputFilename Filename to which output corresponds; this may be used
 *                       in giving more descriptive error messages.
 * @param commonSys      Common Cryptosystem with all keys already generated (regular replica).
 *                       Is null for ro replica.
 */
void outputReplicaKeyfile(uint16_t numReplicas,
                          uint16_t numRoReplicas,
                          bftEngine::ReplicaConfig& config,
                          const std::string& outputFilename,
                          Cryptosystem* commonSys = nullptr);

/**
 * Read in a keyfile for the current replica that was output with
 * outputReplicaKeyfile above and initialize the relevant fields of a
 * bftEninge::ReplicaConfig with the information read. If successful, this
 * function will set the following fileds of the ReplicaConfig: fVAl, CVal,
 * replicaId, publicKeysOfReplicas, replicaPrivateKey,
 * thresholdSignerForExecution, thresholdVerifierForExecution,
 * thresholdSignerForSlowPathCommit, thresholdVerifierForSlowPathCommit,
 * thresholdSignerForCommit, thresholdVerifierForCommit,
 * thresholdSignerForOptimisticCommit, and thresholdVerifierForOptimisticCommit.
 * Other fields of the ReplicaConfig will not be modified by this function. This
 * function will not modify the ReplicaConfig at all if it is unsuccesfull; it
 * will instead print an error message and return false.
 *
 * @param inputFilename The filename to which input corresponds. This may be
 *                      used in giving more specific error messages.
 * @param config        A bftEngine::ReplicaConfig to output the cryptographic
 *                      configuration read to.
 *
 * @return True if the keyfile is successfully read and the read information
 *         output to config, and false otherwise. Reasons for failure may
 *         include I/O issues or invalid formatting or contents of the keyfile.
 */
Cryptosystem* inputReplicaKeyfileMultisig(const std::string& inputFilename, bftEngine::ReplicaConfig& config);

template <typename T>
T parse(const std::string& str, const std::string& name) {
  try {
    return concord::util::to<T>(str);
  } catch (std::exception& e) {
    std::ostringstream oss;
    oss << "Exception: " << e.what() << " Invalid value  for " << name << ": " << str << " expected range ["
        << std::numeric_limits<T>::min() << ", " << std::numeric_limits<T>::max() << "]";

    throw std::runtime_error(oss.str());
  }
}
