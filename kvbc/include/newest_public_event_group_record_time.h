// Concord
//
// Copyright (c) 2022 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#pragma once

#include "db_interfaces.h"
#include "kvbc_app_filter/kvbc_app_filter.h"

#include <string>

#include <google/protobuf/util/time_util.h>

namespace concord::kvbc {

inline std::string newestPublicEventGroupRecordTime(const IReader& reader) {
  using google::protobuf::util::TimeUtil;

  auto filter = KvbAppFilter{&reader, ""};
  const auto last_public_event_group = filter.getNewestPublicEventGroup();
  if (!last_public_event_group) {
    // No public events - return epoch.
    return TimeUtil::ToString(TimeUtil::GetEpoch());
  }
  return last_public_event_group->record_time;
}

}  // namespace concord::kvbc
