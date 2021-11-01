// Concord
//
// Copyright (c) 2019-2020 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").  You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the sub-component's license, as noted in the LICENSE
// file.

#pragma once

#include <future>
#include <variant>

#include "messages/FullCommitProofMsg.hpp"
#include "messages/RetranProcResultInternalMsg.hpp"
#include "messages/TickInternalMsg.hpp"
#include "messages/OnTransferringCompleteMsg.hpp"
#include "messages/PrePrepareMsg.hpp"
#include "messages/SignatureInternalMsgs.hpp"
#include "messages/ViewChangeIndicatorInternalMsg.hpp"
#include "messages/PrePrepareCarrierInternalMsg.hpp"
#include "messages/ValidatedMessageCarrierInternalMsg.hpp"
#include "messages/FinishExecutePrePrepareInternalMsg.hpp"
#include "IRequestHandler.hpp"

namespace bftEngine::impl {

struct GetStatus {
  std::string key;
  std::promise<std::string> output;
};
// An InternalMessage is a value type sent from threads to the ReplicaImp over the IncomingMsgsStorage channel.
//
// All internal messages should be included in this variant.
//
// The purpose of using a variant is to explicitly encode the set of internal messages as a closed
// type. We know all the internal messages, and it is a bug if a message outside of this set is ever
// sent or received. By encoding it in a variant, the compiler makes it impossible to even build the
// software if we attempt to send a message that is not included. While this doesn't enforce
// actually being able to handle the message, adding a message and its corresponding handler can now
// be looked for in the same review.
//
// It also provides a nice quick visual inspection of where to look to see which messages there are, and for small
// messages prevents the need to allocate on the heap. Note that the size of a variant is the size of its largest member
// though, so this latter part is a tradeoff between heap allocation performance and memory usage, as a queue of all
// internal messages will become larger if the size of the variant grows.
using InternalMessage = std::variant<FullCommitProofMsg*,
                                     PrePrepareMsg*,

                                     // Prepare signature related
                                     CombinedSigFailedInternalMsg,
                                     CombinedSigSucceededInternalMsg,
                                     VerifyCombinedSigResultInternalMsg,

                                     // Commit signature related
                                     CombinedCommitSigSucceededInternalMsg,
                                     CombinedCommitSigFailedInternalMsg,
                                     VerifyCombinedCommitSigResultInternalMsg,

                                     // Move to next view due to some valid reason
                                     ViewChangeIndicatorInternalMsg,

                                     // Carries PrePrepare message after validation.
                                     PrePrepareCarrierInternalMsg,

                                     // Add Carriers of messages which will encapsulate
                                     // Incoming External messages to Internal message.
                                     CarrierMesssage*,

                                     // Retransmission manager related
                                     RetranProcResultInternalMsg,

                                     // Diagnostics related
                                     GetStatus,

                                     // Concord Cron related
                                     TickInternalMsg,

                                     // post execution defer related
                                     OnTransferringCompletelMsg,

                                     FinishExecutePrePrepareInternalMsg>;

}  // namespace bftEngine::impl
