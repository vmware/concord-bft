// Copyright (c) 2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the sub-component's license, as noted in the LICENSE
// file.

#include "client/concordclient/concord_client.hpp"

using std::unique_ptr;

using bft::client::Msg;
using bft::client::WriteConfig;
using bft::client::ReadConfig;

using client::thin_replica_client::SubscribeRequest;

using opentracing::Span;

namespace client::concordclient {

template <class CallbackT>
void ConcordClient::send(const WriteConfig& config,
                         Msg&& msg,
                         const unique_ptr<Span>& parent_span,
                         CallbackT callback) {}

template <class CallbackT>
void ConcordClient::send(const ReadConfig& config, Msg&& msg, const unique_ptr<Span>& parent_span, CallbackT callback) {
}

template <class CallbackT>
void ConcordClient::subscribe(const SubscribeRequest& request,
                              const unique_ptr<Span>& parent_span,
                              CallbackT callback) {}

void ConcordClient::unsubscribe() {}

}  // namespace client::concordclient
