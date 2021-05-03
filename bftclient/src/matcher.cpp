// Concord
//
// Copyright (c) 2020 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License"). You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include "matcher.h"

namespace bft::client {

std::optional<Match> Matcher::onReply(UnmatchedReply&& reply) {
  if (!valid(reply)) return std::nullopt;
  if (!config_.include_primary_) reply.metadata.primary = std::nullopt;
  auto key = MatchKey{reply.metadata, std::move(reply.data)};
  if (matches_[key].count(reply.rsi.from)) {
    if (matches_[key][reply.rsi.from] != reply.rsi.data) {
      LOG_ERROR(logger_,
                "Received two different pieces of replica specific information from: " << reply.rsi.from.val
                                                                                       << ". Keeping the new one.");
    }
  }
  matches_[key].insert_or_assign(reply.rsi.from, std::move(reply.rsi.data));

  return match();
}

std::optional<Match> Matcher::match() {
  auto result = std::find_if(matches_.begin(), matches_.end(), [this](const auto& match) {
    return match.second.size() == config_.quorum.wait_for;
  });
  if (result == matches_.end()) return std::nullopt;
  primary_ = result->first.metadata.primary;
  return Match{Reply{result->first.data, std::move(result->second)}, result->first.metadata.primary};
}

bool Matcher::valid(const UnmatchedReply& reply) const {
  if (config_.sequence_number != reply.metadata.seq_num) {
    LOG_WARN(logger_,
             "Received msg with mismatched sequence number. Expected: " << config_.sequence_number
                                                                        << ", got: " << reply.metadata.seq_num);
    return false;
  }

  if (!validSource(reply.rsi.from)) {
    LOG_WARN(logger_, "Received reply from invalid source: " << reply.rsi.from.val);
    return false;
  }

  return true;
}

bool Matcher::validSource(const ReplicaId& source) const {
  const auto& valid_sources = config_.quorum.destinations;
  return std::find(valid_sources.begin(), valid_sources.end(), source) != valid_sources.end();
}

}  // namespace bft::client
