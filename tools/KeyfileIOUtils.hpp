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

#include <iostream>

#include "bftengine/ReplicaConfig.hpp"
#include "threshsign/ThresholdSignaturesTypes.h"

/**
 * Output the required generated keys for a given replica to a file. The replica
 * can then read the keys back to fill out its bftEngine::ReplicaConfig& using
 * inputReplicaKeyFile below.
 *
 * @param replicaID      The replica ID for the replica to which this keyfile
 *                       will correspond. Note replica IDs should be in the
 *                       range [0, numReplicas - 1].
 * @param numReplicas    The total number of replicas in the deployment this
 *                       replica will be in. Note numReplicas must be equal to
 *                       (3 * f + 2 * c + 1).
 * @param f              The F parameter to SBFT for this deployment (the number
 *                       of byzantine faulty replicas to tolerate before safety
 *                       and/or correctness may be lost).
 * @param c              C parameter to the SBFT algorithm for this deployment
 *                       (the number of slow, crashed, or unresponsive replicas
 *                       that can be tolerated before falling back on the slow
 *                       path for commit consensus).
 * @param output         Output stream to which to write this keyfile.
 * @param outputFilename Filename to which output corresponds; this may be used
 *                       in giving more descriptive error messages.
 * @param rsaKeys        RSA keys generated for this deployment. It is expected
 *                       that there is one pair of keys per replica, each
 *                       represented as a pair of strings in rsaKeys, ordered by
 *                       replicaID. The private key should be the first string
 *                       in each pair and the public key the second.
 * @param execSys        Cryptosystem for consensus on execution results in this
 *                       deployment, with all keys already generated.
 * @param slowSys        Cryptosystem for consensus on transaction commit order
 *                       in the slow path for this deployment, with all keys
 *                       already generated.
 * @param commitSys      Cryptosystem for consensus on transaction commit order
 *                       in the general path for this deployment, with all keys
 *                       already generated.
 * @param optSys         Cryptosystem for consensus on transaction commit order
 *                       in the optimistic fast path for this deployment, with
 *                       all keys already generated.
 *
 * @return True if the keyfile was successfully output, false otherwise. Reasons
 * *         this function could fail to successfully output the keyfile may
 *         include I/O issues or disagreement of numReplicas with f and c.
 */
bool outputReplicaKeyfile(
    uint16_t replicaID,
    uint16_t numReplicas,
    uint16_t f,
    uint16_t c,
    std::ostream& output,
    const std::string& outputFilename,
    const std::vector<std::pair<std::string, std::string>>& rsaKeys,
    const Cryptosystem& execSys,
    const Cryptosystem& slowSys,
    const Cryptosystem& commitSys,
    const Cryptosystem& optSys);

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
 * @param input         Input source for the keyfile to be read from.
 * @param inputFilename The filename to which input corresponds. This may be
 *                      used in giving more specific error messages.
 * @param config        A bftEngine::ReplicaConfig to output the cryptographic
 *                      configuration read to.
 *
 * @return True if the keyfile is successfully read and the read information
 *         output to config, and false otherwise. Reasons for failure may
 *         include I/O issues or invalid formatting or contents of the keyfile.
 */
bool inputReplicaKeyfile(std::istream& input,
                         const std::string& inputFilename,
                         bftEngine::ReplicaConfig& config);
