// Concord
//
// Copyright (c) 2020-2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include "client/thin-replica-client/trs_connection.hpp"

#include <grpcpp/grpcpp.h>
#include <future>
#include "thin_replica.grpc.pb.h"

using com::vmware::concord::thin_replica::Data;
using com::vmware::concord::thin_replica::Hash;
using com::vmware::concord::thin_replica::ReadStateHashRequest;
using com::vmware::concord::thin_replica::ReadStateRequest;
using com::vmware::concord::thin_replica::SubscriptionRequest;
using com::vmware::concord::thin_replica::ThinReplica;

using grpc::ChannelArguments;
using grpc::ClientContext;
using grpc::InsecureChannelCredentials;
using grpc::SslCredentialsOptions;
using grpc::Status;
using grpc_connectivity_state::GRPC_CHANNEL_READY;

using std::future_status;
using std::launch;

using namespace std::chrono_literals;

namespace client::thin_replica_client {

void TrsConnection::createStub() {
  ConcordAssertNE(channel_, nullptr);
  stub_ = ThinReplica::NewStub(channel_);
}

void TrsConnection::createChannel() {
  grpc::ChannelArguments args;
  args.SetMaxReceiveMessageSize(kGrpcMaxInboundMsgSizeInBytes);

  if (config_->use_tls) {
    LOG_INFO(logger_, "TLS for thin replica client is enabled for server: " << address_);

    grpc::SslCredentialsOptions opts = {config_->server_cert, config_->client_key, config_->client_cert};
    channel_ = grpc::CreateCustomChannel(address_, grpc::SslCredentials(opts), args);
  } else {
    LOG_WARN(logger_,
             "TLS for thin replica client is disabled, falling back to "
             "insecure channel");
    channel_ = grpc::CreateCustomChannel(address_, grpc::InsecureChannelCredentials(), args);
  }
}

void TrsConnection::connect(std::unique_ptr<TrsConnectionConfig>& config) {
  if (!channel_) {
    config_ = std::move(config);
    createChannel();
    createStub();
  } else if (!stub_) {
    createStub();
  }
  // Initiate connection
  channel_->GetState(true);
}

bool TrsConnection::isConnected() {
  if (!channel_) {
    return false;
  }
  auto status = channel_->GetState(false);
  LOG_DEBUG(logger_, "gRPC connection status (" << address_ << ") " << status);
  return status == GRPC_CHANNEL_READY;
}

void TrsConnection::disconnect() {
  cancelStateStream();
  cancelDataStream();
  cancelHashStream();
  stub_.reset();
  channel_.reset();
}

TrsConnection::Result TrsConnection::openDataStream(const SubscriptionRequest& request) {
  ConcordAssertNE(stub_, nullptr);
  ConcordAssertEQ(data_stream_, nullptr);
  ConcordAssertEQ(data_context_, nullptr);

  data_context_.reset(new grpc::ClientContext());
  data_context_->AddMetadata("client_id", client_id_);

  auto stream =
      async(launch::async, [this, &request] { return stub_->SubscribeToUpdates(data_context_.get(), request); });
  auto status = stream.wait_for(data_timeout_);
  if (status == future_status::timeout || status == future_status::deferred) {
    data_context_->TryCancel();
    stream.wait();
    data_context_.reset();

    // If SubscribeToUpdates did end up returning a pointer to an allocated
    // stream, make sure it does not get leaked.
    data_stream_ = stream.get();
    data_stream_.reset();

    return Result::kTimeout;
  }

  ConcordAssert(status == future_status::ready);
  data_stream_ = stream.get();
  if (data_stream_) return Result::kSuccess;

  auto grpc_status = data_stream_->Finish();
  if (grpc_status.error_code() == grpc::StatusCode::OUT_OF_RANGE) return Result::kOutOfRange;
  if (grpc_status.error_code() == grpc::StatusCode::NOT_FOUND) return Result::kNotFound;
  data_context_.reset();
  return Result::kFailure;
}

void TrsConnection::cancelDataStream() {
  if (!data_stream_) {
    ConcordAssertEQ(data_context_, nullptr);
    return;
  }
  ConcordAssertNE(data_context_, nullptr);
  data_context_->TryCancel();
  data_context_.reset();
  data_stream_.reset();
}

bool TrsConnection::hasDataStream() { return bool(data_stream_); }

TrsConnection::Result TrsConnection::readData(Data* data) {
  ConcordAssertNE(data_stream_, nullptr);
  ConcordAssertNE(data_context_, nullptr);

  auto result = async(launch::async, [this, data] { return data_stream_->Read(data); });
  auto status = result.wait_for(data_timeout_);
  if (status == future_status::timeout || status == future_status::deferred) {
    data_context_->TryCancel();
    result.wait();
    data_context_.reset();
    data_stream_.reset();
    return Result::kTimeout;
  }

  ConcordAssert(status == future_status::ready);
  if (result.get()) return Result::kSuccess;

  auto grpc_status = data_stream_->Finish();
  if (grpc_status.error_code() == grpc::StatusCode::OUT_OF_RANGE) return Result::kOutOfRange;
  if (grpc_status.error_code() == grpc::StatusCode::NOT_FOUND) return Result::kNotFound;
  return Result::kFailure;
}

TrsConnection::Result TrsConnection::openStateStream(const ReadStateRequest& request) {
  ConcordAssertNE(stub_, nullptr);
  ConcordAssertEQ(state_stream_, nullptr);
  ConcordAssertEQ(state_context_, nullptr);

  state_context_.reset(new grpc::ClientContext());
  state_context_->AddMetadata("client_id", client_id_);

  auto stream = async(launch::async, [this, &request] { return stub_->ReadState(state_context_.get(), request); });
  auto status = stream.wait_for(data_timeout_);
  if (status == future_status::timeout || status == future_status::deferred) {
    state_context_->TryCancel();
    stream.wait();
    state_context_.reset();

    // If ReadState did end up returning a pointer to an allocated stream, make
    // sure it does not get leaked.
    state_stream_ = stream.get();
    state_stream_.reset();

    return Result::kTimeout;
  }

  ConcordAssert(status == future_status::ready);
  state_stream_ = stream.get();
  if (!state_stream_) {
    state_context_.reset();
    return Result::kFailure;
  } else {
    return Result::kSuccess;
  }
}

void TrsConnection::cancelStateStream() {
  if (!state_stream_) {
    ConcordAssertEQ(state_context_, nullptr);
    return;
  }
  ConcordAssertNE(state_context_, nullptr);
  state_context_->TryCancel();
  state_context_.reset();
  state_stream_.reset();
}

TrsConnection::Result TrsConnection::closeStateStream() {
  if (!state_stream_) {
    return Result::kSuccess;
  }
  ConcordAssertNE(state_context_, nullptr);

  // "state" is not an infite data stream and we expect proper termination
  auto result = async(launch::async, [this] { return state_stream_->Finish(); });
  auto status = result.wait_for(data_timeout_);
  if (status == future_status::timeout || status == future_status::deferred) {
    state_context_->TryCancel();
    result.wait();
    state_context_.reset();
    state_stream_.reset();
    return Result::kTimeout;
  }

  ConcordAssert(status == future_status::ready);
  state_context_.reset();
  state_stream_.reset();
  Status finish_reported_status = result.get();
  if (finish_reported_status.ok()) {
    return Result::kSuccess;
  } else {
    LOG_WARN(logger_,
             "Finishing ReadState from " << address_
                                         << " failed with error code: " << finish_reported_status.error_code() << ", \""
                                         << finish_reported_status.error_message() << "\").");
    return Result::kFailure;
  }
}

bool TrsConnection::hasStateStream() { return bool(state_stream_); }

TrsConnection::Result TrsConnection::readState(Data* data) {
  ConcordAssertNE(state_stream_, nullptr);
  ConcordAssertNE(state_context_, nullptr);

  auto result = async(launch::async, [this, data] { return state_stream_->Read(data); });
  auto status = result.wait_for(data_timeout_);
  if (status == future_status::timeout || status == future_status::deferred) {
    state_context_->TryCancel();
    result.wait();
    state_context_.reset();
    state_stream_.reset();
    return Result::kTimeout;
  }

  ConcordAssert(status == future_status::ready);
  return result.get() ? Result::kSuccess : Result::kFailure;
}

TrsConnection::Result TrsConnection::readStateHash(const ReadStateHashRequest& request, Hash* hash) {
  ConcordAssertNE(stub_, nullptr);

  ClientContext context;
  context.AddMetadata("client_id", client_id_);
  auto result =
      async(launch::async, [this, &context, &request, hash] { return stub_->ReadStateHash(&context, request, hash); });
  auto status = result.wait_for(hash_timeout_);
  if (status == future_status::timeout || status == future_status::deferred) {
    context.TryCancel();
    result.wait();
    return Result::kTimeout;
  }

  ConcordAssert(status == future_status::ready);
  Status call_grpc_status = result.get();
  if (!call_grpc_status.ok()) {
    LOG_WARN(logger_,
             "ReadStateHash from " << address_ << " failed with error code: " << call_grpc_status.error_code() << ", \""
                                   << call_grpc_status.error_message() << "\".");
  }

  if (call_grpc_status.ok()) return Result::kSuccess;

  if (call_grpc_status.error_code() == grpc::StatusCode::OUT_OF_RANGE) return Result::kOutOfRange;
  if (call_grpc_status.error_code() == grpc::StatusCode::NOT_FOUND) return Result::kNotFound;
  return Result::kFailure;
}

TrsConnection::Result TrsConnection::openHashStream(SubscriptionRequest& request) {
  ConcordAssertNE(stub_, nullptr);
  ConcordAssertEQ(hash_stream_, nullptr);
  ConcordAssertEQ(hash_context_, nullptr);

  hash_context_.reset(new grpc::ClientContext());
  hash_context_->AddMetadata("client_id", client_id_);

  auto stream =
      async(launch::async, [this, &request] { return stub_->SubscribeToUpdateHashes(hash_context_.get(), request); });
  auto status = stream.wait_for(data_timeout_);
  if (status == future_status::timeout || status == future_status::deferred) {
    hash_context_->TryCancel();
    stream.wait();
    hash_context_.reset();

    // If SubscribeToUpdateHashes did end up returning a pointer to an allocated
    // stream, make sure it does not get leaked.
    hash_stream_ = stream.get();
    hash_stream_.reset();

    return Result::kTimeout;
  }

  ConcordAssert(status == future_status::ready);
  hash_stream_ = stream.get();
  if (hash_stream_) return Result::kSuccess;

  auto grpc_status = hash_stream_->Finish();
  if (grpc_status.error_code() == grpc::StatusCode::OUT_OF_RANGE) return Result::kOutOfRange;
  if (grpc_status.error_code() == grpc::StatusCode::NOT_FOUND) return Result::kNotFound;
  hash_context_.reset();
  return Result::kFailure;
}

void TrsConnection::cancelHashStream() {
  if (!hash_stream_) {
    ConcordAssertEQ(hash_context_, nullptr);
    return;
  }
  ConcordAssertNE(hash_context_, nullptr);
  hash_context_->TryCancel();
  hash_context_.reset();
  hash_stream_.reset();
}

bool TrsConnection::hasHashStream() { return bool(hash_stream_); }

TrsConnection::Result TrsConnection::readHash(Hash* hash) {
  ConcordAssertNE(hash_stream_, nullptr);
  ConcordAssertNE(hash_context_, nullptr);

  auto result = async(launch::async, [this, hash] { return hash_stream_->Read(hash); });
  auto status = result.wait_for(hash_timeout_);
  if (status == future_status::timeout || status == future_status::deferred) {
    hash_context_->TryCancel();
    result.wait();
    hash_context_.reset();
    hash_stream_.reset();
    return Result::kTimeout;
  }

  ConcordAssert(status == future_status::ready);
  if (result.get()) return Result::kSuccess;

  auto grpc_status = hash_stream_->Finish();
  if (grpc_status.error_code() == grpc::StatusCode::OUT_OF_RANGE) return Result::kOutOfRange;
  if (grpc_status.error_code() == grpc::StatusCode::NOT_FOUND) return Result::kNotFound;
  return Result::kFailure;
}

TrsConnection::Result TrsConnection::openStateSnapshotStream(
    const vmware::concord::replicastatesnapshot::StreamSnapshotRequest& request) {
  // TODO: Add implementation
  ConcordAssert("openStateSnapshotStream should not be called. It is unimplemented." && false);
  return Result::kFailure;
}

void TrsConnection::cancelStateSnapshotStream() {
  // TODO: Add implementation
  ConcordAssert("cancelStateSnapshotStream should not be called. It is unimplemented." && false);
  return;
}
bool TrsConnection::hasStateSnapshotStream() {
  // TODO: Add implementation
  ConcordAssert("hasStateSnapshotStream should not be called. It is unimplemented." && false);
  return false;
}
TrsConnection::Result TrsConnection::readStateSnapshot(vmware::concord::replicastatesnapshot::KeyValuePair* key_value) {
  // TODO: Add implementation
  ConcordAssert("readStateSnapshot should not be called. It is unimplemented." && false);
  return Result::kFailure;
}

}  // namespace client::thin_replica_client
