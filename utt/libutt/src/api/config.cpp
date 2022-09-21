// UTT
//
// Copyright (c) 2020-2022 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the sub-component's license, as noted in the LICENSE
// file.

#include "config.hpp"

#include <utt/Params.h>
#include <utt/RandSigDKG.h>
#include <utt/RandSig.h>
#include <utt/RegAuth.h>
#include <utt/Serialization.h>

#include <UTTParams.hpp>

namespace libutt::api {

struct Configuration::Impl {
  // [TODO-UTT] The secrets need to be encrypted
  // [TODO-UTT] Decide if each participant can be represented by an index or it needs something more like a string id
  std::vector<RandSigShareSK> commitSecrets_;
  std::vector<RegAuthShareSK> registrationSecrets_;
  std::vector<RandSigSharePK> committerVerificationKeyShares_;
  std::vector<RegAuthSharePK> registrationVerificationKeyShares_;
  RandSigPK commitVerificationKey_;
  RegAuthPK registrationVerificationKey_;
  UTTParams params_;
};

Configuration::Configuration() : pImpl_{new Impl{}} {}

Configuration::Configuration(size_t n, size_t t) : Configuration() {
  if (n == 0 || t == 0 || t > n) throw std::runtime_error("Invalid configuration participant size and/or threshold");

  // Generate public parameters and committer (bank) authority secret keys
  auto dkg = libutt::RandSigDKG(t, n, libutt::Params::NumMessages);

  // Generate registration authority secret keys
  auto rsk = libutt::RegAuthSK::generateKeyAndShares(t, n);

  // Pass in the commitment keys to UTTParams
  // This struct matches the expected data structure by UTTParams::create since the method hides the actual type by
  // using a void*
  struct CommitmentKeys {
    libutt::CommKey cck;
    libutt::CommKey rck;
  };
  CommitmentKeys commitmentKeys{dkg.getCK(), rsk.ck_reg};
  pImpl_->params_ = libutt::api::UTTParams::create((void*)(&commitmentKeys));

  // For some reason we need to go back and set the IBE parameters although we might not be using IBE.
  rsk.setIBEParams(pImpl_->params_.getParams().ibe);

  // Public verification keys
  pImpl_->commitVerificationKey_ = dkg.sk.toPK();
  pImpl_->registrationVerificationKey_ = rsk.toPK();

  // [TODO-UTT] Important!!! - These secrets need to be encrypted for each participant
  // before the config is used to deploy a UTT instance
  pImpl_->commitSecrets_ = std::move(dkg.skShares);
  pImpl_->registrationSecrets_ = std::move(rsk.shares);

  // We need to make the public key shares accessible to all participants.
  pImpl_->committerVerificationKeyShares_.reserve(pImpl_->commitSecrets_.size());
  for (size_t i = 0; i < pImpl_->commitSecrets_.size(); ++i) {
    pImpl_->committerVerificationKeyShares_.emplace_back(pImpl_->commitSecrets_[i].toPK());
  }

  pImpl_->registrationVerificationKeyShares_.reserve(pImpl_->registrationSecrets_.size());
  for (size_t i = 0; i < pImpl_->registrationSecrets_.size(); ++i) {
    pImpl_->registrationVerificationKeyShares_.emplace_back(pImpl_->registrationSecrets_[i].toPK());
  }
}

Configuration::~Configuration() = default;

Configuration::Configuration(Configuration&& o) = default;
Configuration& Configuration::operator=(Configuration&& o) = default;

bool Configuration::operator==(const Configuration& o) {
  return pImpl_->params_ == o.pImpl_->params_ && pImpl_->commitSecrets_ == o.pImpl_->commitSecrets_ &&
         pImpl_->registrationSecrets_ == o.pImpl_->registrationSecrets_ &&
         pImpl_->committerVerificationKeyShares_ == o.pImpl_->committerVerificationKeyShares_ &&
         pImpl_->registrationVerificationKeyShares_ == o.pImpl_->registrationVerificationKeyShares_ &&
         pImpl_->commitVerificationKey_ == o.pImpl_->commitVerificationKey_ &&
         pImpl_->registrationVerificationKey_ == o.pImpl_->registrationVerificationKey_;
}

bool Configuration::operator!=(const Configuration& o) { return !operator==(o); }

}  // namespace libutt::api

std::ostream& operator<<(std::ostream& out, const libutt::api::Configuration& config) {
  libutt::serializeVector(out, config.pImpl_->commitSecrets_);
  libutt::serializeVector(out, config.pImpl_->registrationSecrets_);
  libutt::serializeVector(out, config.pImpl_->committerVerificationKeyShares_);
  libutt::serializeVector(out, config.pImpl_->registrationVerificationKeyShares_);
  out << config.pImpl_->commitVerificationKey_ << std::endl;
  out << config.pImpl_->registrationVerificationKey_ << std::endl;
  out << config.pImpl_->params_;
  return out;
}

std::istream& operator>>(std::istream& in, libutt::api::Configuration& config) {
  libutt::deserializeVector(in, config.pImpl_->commitSecrets_);
  libutt::deserializeVector(in, config.pImpl_->registrationSecrets_);
  libutt::deserializeVector(in, config.pImpl_->committerVerificationKeyShares_);
  libutt::deserializeVector(in, config.pImpl_->registrationVerificationKeyShares_);
  in >> config.pImpl_->commitVerificationKey_;
  libff::consume_OUTPUT_NEWLINE(in);
  in >> config.pImpl_->registrationVerificationKey_;
  libff::consume_OUTPUT_NEWLINE(in);
  in >> config.pImpl_->params_;
  return in;
}