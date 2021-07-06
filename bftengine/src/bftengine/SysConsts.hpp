// Concord
//
// Copyright (c) 2018 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").  You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#pragma once

#include <chrono>

#include "PrimitiveTypes.hpp"

using namespace bftEngine::impl;

///////////////////////////////////////////////////////////////////////////////
// Timers
///////////////////////////////////////////////////////////////////////////////

constexpr std::chrono::milliseconds timersResolution(20);

///////////////////////////////////////////////////////////////////////////////
// Number of replicas
///////////////////////////////////////////////////////////////////////////////

//#define SUPPORT_210Replicas
//#define SUPPORT_128Replicas

// TODO(GG): we won't need "MaxNumberOfReplicas" as soon as we start working on reconfiguration
#if defined(SUPPORT_210Replicas)
constexpr int MaxNumberOfReplicas = 210;
#elif defined(SUPPORT_128Replicas)
constexpr int MaxNumberOfReplicas = 128;
#else
constexpr int MaxNumberOfReplicas = 64;
#endif

///////////////////////////////////////////////////////////////////////////////
// Work windows and intervals
///////////////////////////////////////////////////////////////////////////////

constexpr uint16_t kWorkWindowSize = 300;

constexpr uint16_t checkpointWindowSize = 150;
static_assert(kWorkWindowSize == 2 * checkpointWindowSize, "kWorkWindowSize != 2 * checkpointWindowSize");

// TODO(GG): check the value in config:
// (maxConcurrentAgreementsByPrimary should be <= maxLegalConcurrentAgreementsByPrimary)
constexpr uint16_t maxLegalConcurrentAgreementsByPrimary = 16;

// Maximum number of fast paths that are simultaneously in progress.
constexpr uint16_t MaxConcurrentFastPaths = 75;
static_assert(kWorkWindowSize > MaxConcurrentFastPaths + checkpointWindowSize,
              "Violation of kWorkWindowSize > MaxConcurrentFastPaths + checkpointWindowSize");
static_assert(maxLegalConcurrentAgreementsByPrimary < MaxConcurrentFastPaths,
              "Violation of maxConcurrentAgreementsByPrimary < MaxConcurrentFastPaths");

///////////////////////////////////////////////////////////////////////////////
// Batching
///////////////////////////////////////////////////////////////////////////////

// The number of requests in the consensus batch is controlled by maxExternalMessageSize.
// This parameter is required to limit a memory allocation for the bitmap.
constexpr uint32_t maxNumOfRequestsInBatch = 4096;
constexpr uint32_t maxPrimaryQueueSize = 2000;

///////////////////////////////////////////////////////////////////////////////
// Requests for missing information
///////////////////////////////////////////////////////////////////////////////

constexpr int minTimeBetweenStatusRequestsMilli = 70;  // TODO(GG): config/dynamicVal ?

constexpr double factorForMinTimeBetweenStatusRequestsMilli = 0.5;  // TODO(GG): explain , use config

constexpr int sendStatusPeriodMilli = 0;  // if 0, this value is taken from config

///////////////////////////////////////////////////////////////////////////////
// Retransmission Logic
///////////////////////////////////////////////////////////////////////////////

constexpr bool retransmissionsLogicEnabled = false;

constexpr int retransmissionsTimerMilli = 40;

///////////////////////////////////////////////////////////////////////////////
// View Change
///////////////////////////////////////////////////////////////////////////////

constexpr int viewChangeTimeoutMilli =
    0;  //  80 * 1000; // 1 * 60 * 1000; // 20 * 1000; // if 0, this value is taken from config

constexpr bool autoIncViewChangeTimer = true;

///////////////////////////////////////////////////////////////////////////////
// Collectors
///////////////////////////////////////////////////////////////////////////////

constexpr bool dynamicCollectorForPartialProofs = true;  // if false, then the primary will be the collector

constexpr bool dynamicCollectorForExecutionProofs = false;  // if false, then the primary will be the collector

///////////////////////////////////////////////////////////////////////////////
// Debug and Tuning of ControllerWithSimpleHistory
///////////////////////////////////////////////////////////////////////////////

constexpr CommitPath ControllerWithSimpleHistory_debugInitialFirstPath =
    CommitPath::OPTIMISTIC_FAST;  // CommitPath::SLOW;

constexpr float ControllerWithSimpleHistory_debugDowngradeFactor = 0.85F;  //  0.0F;

constexpr float ControllerWithSimpleHistory_debugUpgradeFactorForFastOptimisticPath = 0.95F;

constexpr float ControllerWithSimpleHistory_debugUpgradeFactorForFastPath = 0.85F;

constexpr bool ControllerWithSimpleHistory_debugDowngradeEnabled = true;

constexpr bool ControllerWithSimpleHistory_debugUpgradeEnabled = true;

///////////////////////////////////////////////////////////////////////////////
// Persistency
///////////////////////////////////////////////////////////////////////////////

constexpr uint32_t maxSizeOfCombinedSignature = 1024;  // TODO(GG): should be checked

constexpr uint32_t MaxSizeOfPrivateKey = 1024;  // TODO(GG): should be checked
constexpr uint32_t MaxSizeOfPublicKey = 1024;   // TODO(GG): should be checked

static const std::string secFilePrefix = "gen-sec";
