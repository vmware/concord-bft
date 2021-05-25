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

#include <string>

namespace concord::kvbc::categorization {

inline const auto kExecutionProvableCategory = "execution_provable";
inline const auto kExecutionPrivateCategory = "execution_private";
inline const auto kExecutionEventsCategory = "execution_events";

// Concord and Concord-BFT internal category that is used for various kinds of metadata.
// The type of the internal category is VersionedKeyValueCategory.
inline const auto kConcordInternalCategoryId = "concord_internal";

}  // namespace concord::kvbc::categorization
