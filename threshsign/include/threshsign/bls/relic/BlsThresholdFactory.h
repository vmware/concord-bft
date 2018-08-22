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

#include "threshsign/IThresholdFactory.h"

#include "BlsPublicParameters.h"

namespace BLS {
namespace Relic {

class BlsThresholdKeygenBase;

class BlsThresholdFactory : public IThresholdFactory {
protected:
    BlsPublicParameters params;

public:
    BlsThresholdFactory(const BlsPublicParameters& params);
    virtual ~BlsThresholdFactory() {}

public:
    const BlsPublicParameters& getPublicParameters() const { return params; }

    std::unique_ptr<BlsThresholdKeygenBase> newKeygen(NumSharesType reqSigners, NumSharesType totalSigners) const;

    virtual IThresholdVerifier * newVerifier(NumSharesType reqSigners, NumSharesType totalSigners, const char * publicKeyStr,
    		const std::vector<std::string>& verifKeysStr) const;
    virtual IThresholdSigner * newSigner(ShareID id, const char * secretKeyStr) const;
    virtual std::tuple<std::vector<IThresholdSigner*>, IThresholdVerifier*> newRandomSigners(NumSharesType reqSigners, NumSharesType numSigners) const;
};

} /* namespace Relic */
} /* namespace BLS */
