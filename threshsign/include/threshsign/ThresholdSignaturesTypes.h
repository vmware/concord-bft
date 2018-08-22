// Concord
//
// Copyright (c) 2018 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the
// LICENSE file.


#pragma once

/**
 * The current implementation sends the share ID (i.e., the signer ID) with the share signature.
 * Here we define its data type so that we can easily support an arbitray number of signers.
 *
 * WARNING: Do not set this to an unsigned type! You will run into C/C++ signed vs unsigned problems
 * (see http://soundsoftware.ac.uk/c-pitfall-unsigned)
 */
typedef int ShareID;
typedef ShareID NumSharesType;

#define MAX_NUM_OF_SHARES 2048

#define MULTISIG_BLS_SCHEME         "multisig-bls"
#define THRESHOLD_BLS_SCHEME        "threshold-bls"
