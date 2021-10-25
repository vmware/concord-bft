// Concord
//
// Copyright (c) 2019 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include "SourceSelector.hpp"

namespace bftEngine {
namespace bcst {
namespace impl {

bool SourceSelector::hasSource() const { return currentReplica_ != NO_REPLICA && sourceSelectionTimeMilli_ > 0; }

void SourceSelector::setSourceSelectionTime(uint64_t currTimeMilli) {
  LOG_TRACE(logger_, KVLOG(sourceSelectionTimeMilli_, currTimeMilli));
  sourceSelectionTimeMilli_ = currTimeMilli;
}

void SourceSelector::setFetchingTimeStamp(uint64_t currTimeMilli, bool retransmissionOngoing) {
  fetchingTimeStamp_ = currTimeMilli;
  fetchRetransmissionOngoing_ = retransmissionOngoing;
  if (!retransmissionOngoing) fetchRetransmissionCounter_ = 0;
  LOG_TRACE(logger_, KVLOG(fetchingTimeStamp_, fetchRetransmissionOngoing_, fetchRetransmissionCounter_));
}

void SourceSelector::removeCurrentReplica() {
  preferredReplicas_.erase(currentReplica_);
  currentReplica_ = NO_REPLICA;
  receivedValidBlockFromSrc_ = false;
  setSourceSelectionTime(0);
}

void SourceSelector::onReceivedValidBlockFromSource() {
  ConcordAssertNE(currentReplica_, NO_REPLICA);
  if (!receivedValidBlockFromSrc_) {
    receivedValidBlockFromSrc_ = true;
    LOG_INFO(logger_, "Insert source " << currentReplica_ << " into actualSources_");
    actualSources_.push_back(currentReplica_);
  }
}

void SourceSelector::setAllReplicasAsPreferred() { preferredReplicas_ = allOtherReplicas_; }

void SourceSelector::reset() {
  preferredReplicas_.clear();
  currentReplica_ = NO_REPLICA;
  setSourceSelectionTime(0);
  fetchingTimeStamp_ = 0;
  fetchRetransmissionCounter_ = 0;
  fetchRetransmissionOngoing_ = false;
  receivedValidBlockFromSrc_ = false;
  actualSources_.clear();
}

bool SourceSelector::isReset() const {
  return preferredReplicas_.empty() && (currentReplica_ == NO_REPLICA) && (sourceSelectionTimeMilli_ == 0) &&
         (fetchingTimeStamp_ == 0) && (fetchRetransmissionCounter_ == 0) && !fetchRetransmissionOngoing_ &&
         actualSources_.empty() && !receivedValidBlockFromSrc_;
}

bool SourceSelector::retransmissionTimeoutExpired(uint64_t currTimeMilli) const {
  // TODO(GG): TBD - compute dynamically
  // if fetchingTimeStamp_ or fetchingTimeStamp_ are not set - no need to retransmit since destination has never yet
  // transmitted to this source
  if (currentReplica_ == NO_REPLICA) {
    LOG_DEBUG(logger_, "Retransmit - no replica");
    return false;
  }
  if (fetchingTimeStamp_ == 0) {
    LOG_DEBUG(logger_, "Retransmit" << KVLOG(fetchingTimeStamp_));
    return false;
  }
  auto diff = (currTimeMilli - fetchingTimeStamp_);
  if (diff > retransmissionTimeoutMilli_) {
    LOG_DEBUG(logger_, "Retransmit" << KVLOG(diff, currTimeMilli, fetchingTimeStamp_, retransmissionTimeoutMilli_));
    return true;
  }
  return false;
}

uint64_t SourceSelector::timeSinceSourceSelectedMilli(uint64_t currTimeMilli) const {
  return ((currentReplica_ == NO_REPLICA) || (currTimeMilli < sourceSelectionTimeMilli_))
             ? 0
             : (currTimeMilli - sourceSelectionTimeMilli_);
}

// Replace the source.
void SourceSelector::updateSource(uint64_t currTimeMilli, SourceReplaceType replaceSourceType) {
  if ((currentReplica_ != NO_REPLICA) && (replaceSourceType == SourceReplaceType::IMMEDIATE)) {
    preferredReplicas_.erase(currentReplica_);
  }
  if (preferredReplicas_.empty()) {
    preferredReplicas_ = allOtherReplicas_;
  }
  selectSource(currTimeMilli);
}

// Create a list of ids of the form "0, 1, 4"
std::string SourceSelector::preferredReplicasToString() const {
  std::ostringstream oss;
  for (auto it = preferredReplicas_.begin(); it != preferredReplicas_.end(); ++it) {
    if (it == preferredReplicas_.begin()) {
      oss << *it;
    } else {
      oss << ", " << *it;
    }
  }
  return oss.str();
}

SourceReplaceType SourceSelector::shouldReplaceSource(uint64_t currTimeMilli,
                                                      bool badDataFromCurrentSource,
                                                      bool lastInBatch) const {
  if (currentReplica_ == NO_REPLICA) {
    LOG_INFO(logger_, "Should replace source: no source");
    return SourceReplaceType::IMMEDIATE;
  } else if (badDataFromCurrentSource) {
    LOG_INFO(logger_, "Should replace source: bad data from" << KVLOG(currentReplica_));
    return SourceReplaceType::IMMEDIATE;
  } else if (retransmissionTimeoutExpired(currTimeMilli)) {
    if (fetchRetransmissionOngoing_) {
      ++fetchRetransmissionCounter_;
      fetchRetransmissionOngoing_ = false;
      LOG_WARN(logger_,
               "Retransmission timeout expired:" << KVLOG(currentReplica_,
                                                          currTimeMilli,
                                                          fetchingTimeStamp_,
                                                          retransmissionTimeoutMilli_,
                                                          fetchRetransmissionCounter_));
    }
  }

  if (fetchRetransmissionCounter_ >= maxFetchRetransmissions_) {
    LOG_INFO(logger_,
             "Should replace source: retransmission timeout expired: " << KVLOG(currentReplica_,
                                                                                currTimeMilli,
                                                                                fetchingTimeStamp_,
                                                                                retransmissionTimeoutMilli_,
                                                                                fetchRetransmissionCounter_,
                                                                                maxFetchRetransmissions_));
    return SourceReplaceType::IMMEDIATE;
  }
  if (lastInBatch && (sourceReplacementTimeoutMilli_ > 0)) {
    auto dt = timeSinceSourceSelectedMilli(currTimeMilli);
    if (dt > sourceReplacementTimeoutMilli_) {
      LOG_INFO(logger_,
               "Should replace source: source replacement timeout:" << KVLOG(
                   currentReplica_, dt, sourceReplacementTimeoutMilli_));
      return SourceReplaceType::GRACEFUL;
    }
  }
  return SourceReplaceType::DONOT;
}

void SourceSelector::selectSource(uint64_t currTimeMilli) {
  const size_t size = preferredReplicas_.size();
  ConcordAssert(size > 0);

  auto i = preferredReplicas_.begin();
  if (size > 1) {
    // TODO(GG): can be optimized
    unsigned int c = randomGen_() % size;
    while (c > 0) {
      c--;
      i++;
    }
  }
  currentReplica_ = *i;
  setSourceSelectionTime(currTimeMilli);
  fetchRetransmissionOngoing_ = false;
  fetchingTimeStamp_ = 0;
  fetchRetransmissionCounter_ = 0;
  receivedValidBlockFromSrc_ = false;
}
}  // namespace impl
}  // namespace bcst
}  // namespace bftEngine
