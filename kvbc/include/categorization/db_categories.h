// Concord
//
// Copyright (c) 2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the
// "License").  You may not use this product except in compliance with the
// Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.
#ifndef CONCORD_KVBC_CATEGORIZATION_H_
#define CONCORD_KVBC_CATEGORIZATION_H_

#include <string>

namespace concord::kvbc::categorization {

inline const auto kExecutionProvableCategory = "execution_provable";
inline const auto kExecutionPrivateCategory = "execution_private";
inline const auto kExecutionEventsCategory = "execution_events";
inline const auto kRequestsRecord = "requests_record";
inline const auto kExecutionEventGroupDataCategory = "execution_event_group_data";
inline const auto kExecutionEventGroupTagCategory = "execution_event_group_tag";
inline const auto kExecutionEventGroupLatestCategory = "execution_event_group_latest";

// Concord and Concord-BFT internal category that is used for various kinds of metadata.
// The type of the internal category is VersionedKeyValueCategory.
inline const auto kConcordInternalCategoryId = "concord_internal";
inline const auto kConcordReconfigurationCategoryId = "concord_reconfiguration";

}  // namespace concord::kvbc::categorization

#endif  // CONCORD_KVBC_CATEGORIZATION_H_
