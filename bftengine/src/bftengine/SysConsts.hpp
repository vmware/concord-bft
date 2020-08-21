//Concord
//
//Copyright (c) 2018 VMware, Inc. All Rights Reserved.
//
//This product is licensed to you under the Apache 2.0 license (the "License").  You may not use this product except in compliance with the Apache 2.0 License. 
//
//This product may include a number of subcomponents with separate copyright notices and license terms. Your use of these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE file.


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

constexpr uint16_t maxLegalConcurrentAgreementsByPrimary = 16; // TODO(GG): check the value in config (maxConcurrentAgreementsByPrimary should be <= maxLegalConcurrentAgreementsByPrimary)


// Maximum number of fast paths that are simultaneously in progress.
constexpr uint16_t MaxConcurrentFastPaths = 75;
static_assert(kWorkWindowSize > MaxConcurrentFastPaths + checkpointWindowSize, "Violation of kWorkWindowSize > MaxConcurrentFastPaths + checkpointWindowSize");
static_assert(maxLegalConcurrentAgreementsByPrimary < MaxConcurrentFastPaths, "Violation of maxConcurrentAgreementsByPrimary < MaxConcurrentFastPaths");


///////////////////////////////////////////////////////////////////////////////
// Messages
///////////////////////////////////////////////////////////////////////////////

constexpr uint32_t maxExternalMessageSize = 512 * 1000; // TODO(GG): some message types may need different values  

constexpr uint32_t maxReplyMessageSize = 8 * 1024;

///////////////////////////////////////////////////////////////////////////////
// Batching
///////////////////////////////////////////////////////////////////////////////

constexpr uint32_t maxNumOfRequestsInBatch = 1024;

///////////////////////////////////////////////////////////////////////////////
// Requests for missing information 
///////////////////////////////////////////////////////////////////////////////

constexpr int minTimeBetweenStatusRequestsMilli = 70; // TODO(GG): config/dynamicVal ?

constexpr double factorForMinTimeBetweenStatusRequestsMilli = 0.5; // TODO(GG): explain , use config

constexpr int sendStatusPeriodMilli = 0; // if 0, this value is taken from config

///////////////////////////////////////////////////////////////////////////////
// Rretransmission Logic
///////////////////////////////////////////////////////////////////////////////

constexpr bool retransmissionsLogicEnabled = false;

constexpr int retransmissionsTimerMilli = 40;

///////////////////////////////////////////////////////////////////////////////
// View Change
///////////////////////////////////////////////////////////////////////////////

constexpr bool forceViewChangeProtocolEnabled = false;
constexpr bool forceViewChangeProtocolDisabled = false;
static_assert(!forceViewChangeProtocolEnabled || !forceViewChangeProtocolDisabled, "");

constexpr int viewChangeTimeoutMilli = 0; //  80 * 1000; // 1 * 60 * 1000; // 20 * 1000; // if 0, this value is taken from config


constexpr bool autoIncViewChangeTimer = true;

constexpr bool autoPrimaryUpdateEnabled = false;
constexpr int autoPrimaryUpdateMilli =  40 * 1000; // 3 * 60 * 1000;

///////////////////////////////////////////////////////////////////////////////
// Collectors
///////////////////////////////////////////////////////////////////////////////

constexpr bool dynamicCollectorForPartialProofs = true; // if false, then the primary will be the collector

constexpr bool dynamicCollectorForExecutionProofs = false; // if false, then the primary will be the collector

///////////////////////////////////////////////////////////////////////////////
// State Transfer
///////////////////////////////////////////////////////////////////////////////

constexpr int timeToWaitBeforeStartingStateTransferInMainWindowMilli = 2000; // TODO(GG): move to configuration??

constexpr uint32_t sizeOfReservedPage = 4096;


///////////////////////////////////////////////////////////////////////////////
// Debug and Tuning of ControllerWithSimpleHistory
///////////////////////////////////////////////////////////////////////////////

constexpr CommitPath ControllerWithSimpleHistory_debugInitialFirstPath = CommitPath::OPTIMISTIC_FAST; // CommitPath::SLOW;

constexpr float ControllerWithSimpleHistory_debugDowngradeFactor = 0.85F; //  0.0F; 

constexpr float ControllerWithSimpleHistory_debugUpgradeFactorForFastOptimisticPath = 0.95F;

constexpr float ControllerWithSimpleHistory_debugUpgradeFactorForFastPath = 0.85F;


constexpr bool ControllerWithSimpleHistory_debugDowngradeEnabled = true;


constexpr bool ControllerWithSimpleHistory_debugUpgradeEnabled = true;

///////////////////////////////////////////////////////////////////////////////
// Persistency
///////////////////////////////////////////////////////////////////////////////

constexpr uint32_t maxSizeOfCombinedsignature = 1024; // TODO(GG): should be checked

constexpr bool debugPersistentStorageEnabled = true;
