//Concord
//
//Copyright (c) 2019 VMware, Inc. All Rights Reserved.
//
//This product is licensed to you under the Apache 2.0 license (the "License").  You may not use this product except in compliance with the Apache 2.0 License. 
//
//This product may include a number of subcomponents with separate copyright notices and license terms. Your use of these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE file.

#pragma once

#include <stdint.h>
#include <set>
#include <string>

#include "ReplicaConfig.hpp"

class IThresholdSigner;
class IThresholdVerifier;

namespace bftEngine {

class ReplicaConfigSingleton {
    public:
	// Note, for initialization, this function is not thread-safe. The
	// caller needs to make sure that only one thread at a time calls this
	// function when a non-nullptr `config` parameter is supplied.
	static ReplicaConfigSingleton& GetInstance(const ReplicaConfig* config=nullptr);

	uint16_t GetFVal() const;
	uint16_t GetCVal() const;
	uint16_t GetReplicaId() const;
	uint16_t GetNumOfClientProxies() const;
	uint16_t GetStatusReportTimerMillisec() const;
	uint16_t GetConcurrencyLevel() const;
	bool GetAutoViewChangeEnabled() const;
	uint16_t GetViewChangeTimerMillisec() const;
	std::set<std::pair<uint16_t, std::string>> GetPublicKeysOfReplicas() const;
	std::string GetReplicaPrivateKey() const;
	IThresholdSigner const* GetThresholdSignerForExecution() const;
	IThresholdVerifier const* GetThresholdVerifierForExecution() const;
	IThresholdSigner const* GetThresholdSignerForSlowPathCommit() const;
	IThresholdVerifier const* GetThresholdVerifierForSlowPathCommit() const;
	IThresholdSigner const* GetThresholdSignerForCommit() const;
	IThresholdVerifier const* GetThresholdVerifierForCommit() const;
	IThresholdSigner const* GetThresholdSignerForOptimisticCommit() const;
	IThresholdVerifier const* GetThresholdVerifierForOptimisticCommit() const;

    private:
	ReplicaConfigSingleton();
	ReplicaConfig config_;
	bool is_initialized_;
};

}  // namespace bftEngine
