//Concord
//
//Copyright (c) 2018 VMware, Inc. All Rights Reserved.
//
//This product is licensed to you under the Apache 2.0 license (the "License").  You may not use this product except in compliance with the Apache 2.0 License. 
//
//This product may include a number of subcomponents with separate copyright notices and license terms. Your use of these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE file.

#pragma once

#include <stdint.h>
#include <set>

class IThresholdSigner;
class IThresholdVerifier;
namespace bftEngine
{
	struct ReplicaConfig
	{
		// F value - max number of faulty/malicious replicas. fVal >= 1 
		uint16_t fVal;      

		// C value. cVal >=0
		uint16_t cVal;     
							
		// unique identifier of the replica. 
		// The number of replicas in the system should be N = 3*fVal + 2*cVal + 1
		// In the current version, replicaId should be a number between 0 and  N-1 
		// replicaId should also represent this replica in ICommunication.
		uint16_t replicaId;

		
		// number of objects that represent clients. 
		// numOfClientProxies >= 1
		uint16_t numOfClientProxies; 

		// a time interval in milliseconds. represents how often the replica sends a status report to the other replicas. 
		// statusReportTimerMillisec > 0
		uint16_t statusReportTimerMillisec; 

		// number of consensus operations that can be executed in parallel
		// 1 <= concurrencyLevel <= 30
		uint16_t concurrencyLevel;

		uint16_t maxBatchSize;

		uint16_t commitTimerMillisec;

		// autoViewChangeEnabled=true , if the automatic view change protocol is enabled
		bool autoViewChangeEnabled;

		bool dynamicCollectorEnabled;

		// a time interval in milliseconds. represents the timeout used by the  view change protocol (TODO: add more details)
		uint16_t viewChangeTimerMillisec;

		// public keys of all replicas. map from replica identifier to a public key
		std::set<std::pair<uint16_t, std::string>> publicKeysOfReplicas;

		// private key of the current replica
		std::string replicaPrivateKey;

		// signer and verifier of a threshold signature (for threshold fVal+1 out of N)
		// In the current version, both should be nullptr 
		IThresholdSigner* thresholdSignerForExecution;
		IThresholdVerifier* thresholdVerifierForExecution;

		// signer and verifier of a threshold signature (for threshold N-fVal-cVal out of N)
		IThresholdSigner* thresholdSignerForSlowPathCommit;
		IThresholdVerifier* thresholdVerifierForSlowPathCommit;

		// signer and verifier of a threshold signature (for threshold N-cVal out of N)
		// If cVal==0, then both should be nullptr 
		IThresholdSigner* thresholdSignerForCommit;
		IThresholdVerifier* thresholdVerifierForCommit;

		// signer and verifier of a threshold signature (for threshold N out of N)
		IThresholdSigner* thresholdSignerForOptimisticCommit;
		IThresholdVerifier* thresholdVerifierForOptimisticCommit;
	};
}