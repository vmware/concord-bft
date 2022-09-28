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

struct PublicConfig::Impl {
  UTTParams params_;
  RandSigPK commitVerificationKey_;
  RegAuthPK registrationVerificationKey_;
};

PublicConfig::PublicConfig() : pImpl_{new Impl{}} {}
PublicConfig::~PublicConfig() = default;

PublicConfig::PublicConfig(PublicConfig&& o) = default;
PublicConfig& PublicConfig::operator=(PublicConfig&& o) = default;

bool PublicConfig::operator==(const PublicConfig& o) {
  return pImpl_->params_ == o.pImpl_->params_ && pImpl_->commitVerificationKey_ == o.pImpl_->commitVerificationKey_ &&
         pImpl_->registrationVerificationKey_ == o.pImpl_->registrationVerificationKey_;
}

bool PublicConfig::operator!=(const PublicConfig& o) { return !operator==(o); }

std::string PublicConfig::getCommitVerificationKey() const {
  return libutt::serialize<libutt::RandSigPK>(pImpl_->commitVerificationKey_);
}

std::string PublicConfig::getRegistrationVerificationKey() const {
  return libutt::serialize<libutt::RegAuthPK>(pImpl_->registrationVerificationKey_);
}

const UTTParams& PublicConfig::getParams() const { return pImpl_->params_; }

struct Configuration::Impl {
  // [TODO-UTT] The commit and registration secrets need to be encrypted
  // [TODO-UTT] Decide if each participant can be represented by an index or it needs something more like a string id
  uint16_t n_ = 0;
  uint16_t t_ = 0;
  PublicConfig publicConfig_;
  std::vector<RandSigShareSK> commitSecrets_;
  std::vector<RegAuthShareSK> registrationSecrets_;
  std::vector<RandSigSharePK> committerVerificationKeyShares_;
  std::vector<RegAuthSharePK> registrationVerificationKeyShares_;
};

Configuration::Configuration() : pImpl_{new Impl{}} {}

Configuration::Configuration(uint16_t n, uint16_t t) : Configuration() {
  if (n == 0 || t == 0 || t > n) throw std::runtime_error("Invalid configuration participant size and/or threshold");

  pImpl_->n_ = n;
  pImpl_->t_ = t;

  // Generate public parameters and committer (bank) authority secret keys
  auto dkg = libutt::RandSigDKG(pImpl_->t_, pImpl_->n_, libutt::Params::NumMessages);

  // Generate registration authority secret keys
  auto rsk = libutt::RegAuthSK::generateKeyAndShares(pImpl_->t_, pImpl_->n_);

  // Pass in the commitment keys to UTTParams
  // This struct matches the expected data structure by UTTParams::create since the method hides the actual type by
  // using a void*
  struct CommitmentKeys {
    libutt::CommKey cck;
    libutt::CommKey rck;
  };
  CommitmentKeys commitmentKeys{dkg.getCK(), rsk.ck_reg};
  pImpl_->publicConfig_.pImpl_->params_ = libutt::api::UTTParams::create((void*)(&commitmentKeys));

  // For some reason we need to go back and set the IBE parameters although we might not be using IBE.
  rsk.setIBEParams(pImpl_->publicConfig_.pImpl_->params_.getParams().ibe);

  // Public verification keys
  pImpl_->publicConfig_.pImpl_->commitVerificationKey_ = dkg.sk.toPK();
  pImpl_->publicConfig_.pImpl_->registrationVerificationKey_ = rsk.toPK();

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
  return pImpl_->publicConfig_ == o.pImpl_->publicConfig_ && pImpl_->commitSecrets_ == o.pImpl_->commitSecrets_ &&
         pImpl_->registrationSecrets_ == o.pImpl_->registrationSecrets_ &&
         pImpl_->committerVerificationKeyShares_ == o.pImpl_->committerVerificationKeyShares_ &&
         pImpl_->registrationVerificationKeyShares_ == o.pImpl_->registrationVerificationKeyShares_;
}

bool Configuration::operator!=(const Configuration& o) { return !operator==(o); }

bool Configuration::isValid() const {
  return pImpl_->t_ > 0 && pImpl_->n_ >= pImpl_->t_ && pImpl_->commitSecrets_.size() == pImpl_->n_ &&
         pImpl_->registrationSecrets_.size() == pImpl_->n_ &&
         pImpl_->committerVerificationKeyShares_.size() == pImpl_->n_ &&
         pImpl_->registrationVerificationKeyShares_.size() == pImpl_->n_;
}

uint16_t Configuration::getNumParticipants() const { return pImpl_->n_; }
uint16_t Configuration::getThreshold() const { return pImpl_->t_; }
const PublicConfig& Configuration::getPublicConfig() const { return pImpl_->publicConfig_; }

std::string Configuration::getCommitSecret(uint16_t idx) const {
  return libutt::serialize<libutt::RandSigShareSK>(pImpl_->commitSecrets_.at(idx));
}

std::string Configuration::getRegistrationSecret(uint16_t idx) const {
  return libutt::serialize<libutt::RegAuthShareSK>(pImpl_->registrationSecrets_.at(idx));
}

std::string Configuration::getCommitVerificationKeyShare(uint16_t idx) const {
  return libutt::serialize<libutt::RandSigSharePK>(pImpl_->committerVerificationKeyShares_.at(idx));
}

std::string Configuration::getRegistrationVerificationKeyShare(uint16_t idx) const {
  return libutt::serialize<libutt::RegAuthSharePK>(pImpl_->registrationVerificationKeyShares_.at(idx));
}

}  // namespace libutt::api

std::ostream& operator<<(std::ostream& out, const libutt::api::PublicConfig& config) {
  out << config.pImpl_->params_ << std::endl;
  out << config.pImpl_->commitVerificationKey_ << std::endl;
  out << config.pImpl_->registrationVerificationKey_;
  return out;
}

std::istream& operator>>(std::istream& in, libutt::api::PublicConfig& config) {
  in >> config.pImpl_->params_;
  libff::consume_OUTPUT_NEWLINE(in);
  in >> config.pImpl_->commitVerificationKey_;
  libff::consume_OUTPUT_NEWLINE(in);
  in >> config.pImpl_->registrationVerificationKey_;
  return in;
}

std::ostream& operator<<(std::ostream& out, const libutt::api::Configuration& config) {
  out << config.pImpl_->n_ << std::endl << config.pImpl_->t_ << std::endl;
  out << config.pImpl_->publicConfig_ << std::endl;
  libutt::serializeVector(out, config.pImpl_->commitSecrets_);
  libutt::serializeVector(out, config.pImpl_->registrationSecrets_);
  libutt::serializeVector(out, config.pImpl_->committerVerificationKeyShares_);
  libutt::serializeVector(out, config.pImpl_->registrationVerificationKeyShares_);
  return out;
}

std::istream& operator>>(std::istream& in, libutt::api::Configuration& config) {
  in >> config.pImpl_->n_;
  libff::consume_OUTPUT_NEWLINE(in);
  in >> config.pImpl_->t_;
  libff::consume_OUTPUT_NEWLINE(in);
  in >> config.pImpl_->publicConfig_;
  libff::consume_OUTPUT_NEWLINE(in);
  libutt::deserializeVector(in, config.pImpl_->commitSecrets_);
  libutt::deserializeVector(in, config.pImpl_->registrationSecrets_);
  libutt::deserializeVector(in, config.pImpl_->committerVerificationKeyShares_);
  libutt::deserializeVector(in, config.pImpl_->registrationVerificationKeyShares_);
  return in;
}