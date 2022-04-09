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

#include "client/thin-replica-client/grpc_connection.hpp"

#include <grpcpp/grpcpp.h>
#include <future>
#include "thin_replica.grpc.pb.h"

using com::vmware::concord::thin_replica::Data;
using com::vmware::concord::thin_replica::Hash;
using com::vmware::concord::thin_replica::ReadStateHashRequest;
using com::vmware::concord::thin_replica::ReadStateRequest;
using com::vmware::concord::thin_replica::SubscriptionRequest;
using com::vmware::concord::thin_replica::ThinReplica;
using vmware::concord::replicastatesnapshot::ReplicaStateSnapshotService;

using grpc::ChannelArguments;
using grpc::ClientContext;
using grpc::InsecureChannelCredentials;
using grpc::SslCredentialsOptions;
using grpc::Status;
using grpc_connectivity_state::GRPC_CHANNEL_READY;

using std::future_status;
using std::launch;

using namespace std::chrono_literals;

namespace client::concordclient {

void GrpcConnection::createTrcStub() {
  ConcordAssertNE(channel_, nullptr);
  trc_stub_ = ThinReplica::NewStub(channel_);
}
void GrpcConnection::createRssStub() {
  ConcordAssertNE(channel_, nullptr);
  rss_stub_ = ReplicaStateSnapshotService::NewStub(channel_);
}

void GrpcConnection::createChannel() {
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

void GrpcConnection::connect(std::unique_ptr<GrpcConnectionConfig>& config) {
  if (!channel_) {
    config_ = std::move(config);
    createChannel();
    createTrcStub();
    createRssStub();
  } else {
    if (!trc_stub_) {
      createTrcStub();
    }
    if (!rss_stub_) {
      createRssStub();
    }
  }
  // Initiate connection
  channel_->GetState(true);
}

bool GrpcConnection::isConnected() {
  ReadLock read_lock(channel_mutex_);
  return isConnectedNoLock();
}

bool GrpcConnection::isConnectedNoLock() {
  if (!channel_) {
    return false;
  }
  auto status = channel_->GetState(false);
  LOG_DEBUG(logger_, "gRPC connection status (" << address_ << ") " << status);
  return status == GRPC_CHANNEL_READY;
}

void GrpcConnection::disconnect() {
  cancelStateStream();
  cancelDataStream();
  cancelHashStream();
  cancelAllStateSnapshotStreams();
  trc_stub_.reset();
  rss_stub_.reset();
  channel_.reset();
}

void GrpcConnection::checkAndReConnect(std::unique_ptr<GrpcConnectionConfig>& config) {
  WriteLock write_lock(channel_mutex_);
  if (!isConnectedNoLock()) {
    disconnect();
    connect(config);
  }
}

GrpcConnection::Result GrpcConnection::openDataStream(const SubscriptionRequest& request) {
  ReadLock read_lock(channel_mutex_);
  ConcordAssertNE(trc_stub_, nullptr);
  ConcordAssertEQ(data_stream_, nullptr);
  ConcordAssertEQ(data_context_, nullptr);

  data_context_.reset(new grpc::ClientContext());
  data_context_->AddMetadata("client_id", client_id_);

  auto stream = async(launch::async, [this, &request] {
    ReadLock read_lock(channel_mutex_);
    return trc_stub_->SubscribeToUpdates(data_context_.get(), request);
  });
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

  ConcordAssertEQ(status, future_status::ready);
  data_stream_ = stream.get();
  if (data_stream_) return Result::kSuccess;

  auto grpc_status = data_stream_->Finish();
  if (grpc_status.error_code() == grpc::StatusCode::OUT_OF_RANGE) return Result::kOutOfRange;
  if (grpc_status.error_code() == grpc::StatusCode::NOT_FOUND) return Result::kNotFound;
  data_context_.reset();
  return Result::kFailure;
}

void GrpcConnection::cancelDataStream() {
  if (!data_stream_) {
    ConcordAssertEQ(data_context_, nullptr);
    return;
  }
  ConcordAssertNE(data_context_, nullptr);
  data_context_->TryCancel();
  data_context_.reset();
  data_stream_.reset();
}

bool GrpcConnection::hasDataStream() { return bool(data_stream_); }

GrpcConnection::Result GrpcConnection::readData(Data* data) {
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

  ConcordAssertEQ(status, future_status::ready);
  if (result.get()) return Result::kSuccess;

  auto grpc_status = data_stream_->Finish();
  if (grpc_status.error_code() == grpc::StatusCode::OUT_OF_RANGE) return Result::kOutOfRange;
  if (grpc_status.error_code() == grpc::StatusCode::NOT_FOUND) return Result::kNotFound;
  return Result::kFailure;
}

GrpcConnection::Result GrpcConnection::openStateStream(const ReadStateRequest& request) {
  ReadLock read_lock(channel_mutex_);
  ConcordAssertNE(trc_stub_, nullptr);
  ConcordAssertEQ(state_stream_, nullptr);
  ConcordAssertEQ(state_context_, nullptr);

  state_context_.reset(new grpc::ClientContext());
  state_context_->AddMetadata("client_id", client_id_);

  auto stream = async(launch::async, [this, &request] {
    ReadLock read_lock(channel_mutex_);
    return trc_stub_->ReadState(state_context_.get(), request);
  });
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

  ConcordAssertEQ(status, future_status::ready);
  state_stream_ = stream.get();
  if (!state_stream_) {
    state_context_.reset();
    return Result::kFailure;
  } else {
    return Result::kSuccess;
  }
}

void GrpcConnection::cancelStateStream() {
  if (!state_stream_) {
    ConcordAssertEQ(state_context_, nullptr);
    return;
  }
  ConcordAssertNE(state_context_, nullptr);
  state_context_->TryCancel();
  state_context_.reset();
  state_stream_.reset();
}

GrpcConnection::Result GrpcConnection::closeStateStream() {
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

  ConcordAssertEQ(status, future_status::ready);
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

bool GrpcConnection::hasStateStream() { return bool(state_stream_); }

GrpcConnection::Result GrpcConnection::readState(Data* data) {
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

  ConcordAssertEQ(status, future_status::ready);
  return result.get() ? Result::kSuccess : Result::kFailure;
}

GrpcConnection::Result GrpcConnection::readStateHash(const ReadStateHashRequest& request, Hash* hash) {
  ReadLock read_lock(channel_mutex_);
  ConcordAssertNE(trc_stub_, nullptr);

  ClientContext context;
  context.AddMetadata("client_id", client_id_);
  auto result = async(launch::async, [this, &context, &request, hash] {
    ReadLock read_lock(channel_mutex_);
    return trc_stub_->ReadStateHash(&context, request, hash);
  });
  auto status = result.wait_for(hash_timeout_);
  if (status == future_status::timeout || status == future_status::deferred) {
    context.TryCancel();
    result.wait();
    return Result::kTimeout;
  }

  ConcordAssertEQ(status, future_status::ready);
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

GrpcConnection::Result GrpcConnection::openHashStream(SubscriptionRequest& request) {
  ReadLock read_lock(channel_mutex_);
  ConcordAssertNE(trc_stub_, nullptr);
  ConcordAssertEQ(hash_stream_, nullptr);
  ConcordAssertEQ(hash_context_, nullptr);

  hash_context_.reset(new grpc::ClientContext());
  hash_context_->AddMetadata("client_id", client_id_);

  auto stream = async(launch::async, [this, &request] {
    ReadLock read_lock(channel_mutex_);
    return trc_stub_->SubscribeToUpdateHashes(hash_context_.get(), request);
  });
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

  ConcordAssertEQ(status, future_status::ready);
  hash_stream_ = stream.get();
  if (hash_stream_) return Result::kSuccess;

  auto grpc_status = hash_stream_->Finish();
  if (grpc_status.error_code() == grpc::StatusCode::OUT_OF_RANGE) return Result::kOutOfRange;
  if (grpc_status.error_code() == grpc::StatusCode::NOT_FOUND) return Result::kNotFound;
  hash_context_.reset();
  return Result::kFailure;
}

void GrpcConnection::cancelHashStream() {
  if (!hash_stream_) {
    ConcordAssertEQ(hash_context_, nullptr);
    return;
  }
  ConcordAssertNE(hash_context_, nullptr);
  hash_context_->TryCancel();
  hash_context_.reset();
  hash_stream_.reset();
}

bool GrpcConnection::hasHashStream() { return bool(hash_stream_); }

GrpcConnection::Result GrpcConnection::readHash(Hash* hash) {
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

  ConcordAssertEQ(status, future_status::ready);
  if (result.get()) return Result::kSuccess;

  auto grpc_status = hash_stream_->Finish();
  if (grpc_status.error_code() == grpc::StatusCode::OUT_OF_RANGE) return Result::kOutOfRange;
  if (grpc_status.error_code() == grpc::StatusCode::NOT_FOUND) return Result::kNotFound;
  return Result::kFailure;
}

GrpcConnection::Result GrpcConnection::openStateSnapshotStream(
    const vmware::concord::replicastatesnapshot::StreamSnapshotRequest& request, RequestId& request_id) {
  ReadLock read_lock(channel_mutex_);
  ConcordAssertNE(rss_stub_, nullptr);
  request_id = current_req_id_.fetch_add(1u);

  std::unique_ptr<grpc::ClientContext> snapshot_context = std::make_unique<grpc::ClientContext>();
  snapshot_context->AddMetadata("client_id", client_id_);

  std::unique_ptr<grpc::ClientReaderInterface<vmware::concord::replicastatesnapshot::StreamSnapshotResponse>>
      snapshot_stream;

  auto stream = async(launch::async, [this, &request, &snapshot_context] {
    ReadLock read_lock(channel_mutex_);
    return rss_stub_->StreamSnapshot(snapshot_context.get(), request);
  });
  auto status = stream.wait_for(data_timeout_);
  if (status == future_status::timeout || status == future_status::deferred) {
    snapshot_context->TryCancel();
    stream.wait();
    snapshot_context.reset();

    // If StreamSnapshot did end up returning a pointer to an allocated
    // stream, make sure it does not get leaked.
    snapshot_stream = stream.get();
    snapshot_stream.reset();
    current_req_id_.fetch_sub(1u);
    request_id = 0u;
    return Result::kTimeout;
  }

  ConcordAssertEQ(status, future_status::ready);
  snapshot_stream = stream.get();
  if (snapshot_stream) {
    WriteLock write_lock(rss_streams_mutex_);
    rss_streams_.emplace(request_id, std::make_pair(std::move(snapshot_context), std::move(snapshot_stream)));
    return Result::kSuccess;
  }

  current_req_id_.fetch_sub(1u);
  request_id = 0u;
  snapshot_stream.reset();
  snapshot_context.reset();
  return Result::kFailure;
}

void GrpcConnection::cancelStateSnapshotStream(RequestId request_id) {
  {
    ReadLock read_lock(rss_streams_mutex_);
    auto it = rss_streams_.find(request_id);
    if (it != rss_streams_.end()) {
      if (!(it->second).second) {
        ConcordAssertEQ((it->second).first, nullptr);
        return;
      }
      ConcordAssertNE((it->second).first, nullptr);
      (it->second).first->TryCancel();
      (it->second).first.reset();
      (it->second).second.reset();
    }
  }
  {
    WriteLock write_lock(rss_streams_mutex_);
    rss_streams_.erase(request_id);
  }
}

void GrpcConnection::cancelAllStateSnapshotStreams() {
  {
    ReadLock read_lock(rss_streams_mutex_);
    for (auto& c : rss_streams_) {
      if (!c.second.second) {
        ConcordAssertEQ(c.second.first, nullptr);
        continue;
      }
      ConcordAssertNE(c.second.first, nullptr);
      c.second.first->TryCancel();
      c.second.first.reset();
      c.second.second.reset();
    }
  }
  {
    WriteLock write_lock(rss_streams_mutex_);
    rss_streams_.clear();
  }
}

bool GrpcConnection::hasStateSnapshotStream(RequestId request_id) {
  ReadLock read_lock(rss_streams_mutex_);
  auto it = rss_streams_.find(request_id);
  if (it != rss_streams_.end()) {
    return bool((it->second).second);
  }
  return false;
}

GrpcConnection::Result GrpcConnection::readStateSnapshot(
    RequestId request_id, vmware::concord::replicastatesnapshot::StreamSnapshotResponse* snapshot_response) {
  auto result = async(launch::async, [this, snapshot_response, request_id] {
    ReadLock read_lock(rss_streams_mutex_);
    return (((this->rss_streams_)[request_id]).second)->Read(snapshot_response);
  });
  auto status = result.wait_for(snapshot_timeout_);

  if (status == future_status::timeout || status == future_status::deferred) {
    {
      ReadLock read_lock(rss_streams_mutex_);
      auto it = rss_streams_.find(request_id);
      if (it != rss_streams_.end()) {
        ((it->second).first)->TryCancel();
        result.wait();
        ((it->second).first).reset();
        ((it->second).second).reset();
      }
    }
    {
      WriteLock write_lock(rss_streams_mutex_);
      rss_streams_.erase(request_id);
    }

    return Result::kTimeout;
  }

  ConcordAssertEQ(status, future_status::ready);
  if (result.get()) {
    return Result::kSuccess;
  }

  {
    ReadLock read_lock(rss_streams_mutex_);
    auto it = rss_streams_.find(request_id);
    if (it != rss_streams_.end()) {
      auto grpc_status = ((it->second).second)->Finish();
      if (grpc_status.error_code() == grpc::StatusCode::OK) return Result::kEndOfStream;
      if (grpc_status.error_code() == grpc::StatusCode::OUT_OF_RANGE) return Result::kOutOfRange;
      if (grpc_status.error_code() == grpc::StatusCode::NOT_FOUND) return Result::kNotFound;
    }
  }

  return Result::kFailure;
}

}  // namespace client::concordclient
