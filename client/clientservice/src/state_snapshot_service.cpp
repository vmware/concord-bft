// Concord
//
// Copyright (c) 2022 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License"). You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include "concord.cmf.hpp"
#include <opentracing/tracer.h>
#include <google/protobuf/util/time_util.h>

#include "assertUtils.hpp"
#include "client/clientservice/state_snapshot_service.hpp"
#include "client/clientservice/event_service.hpp"

using grpc::Status;
using grpc::ServerContext;
using grpc::ServerWriter;

using vmware::concord::client::statesnapshot::v1::GetRecentSnapshotRequest;
using vmware::concord::client::statesnapshot::v1::GetRecentSnapshotResponse;
using vmware::concord::client::statesnapshot::v1::StreamSnapshotRequest;
using vmware::concord::client::statesnapshot::v1::StreamSnapshotResponse;
using vmware::concord::client::statesnapshot::v1::ReadAsOfRequest;
using vmware::concord::client::statesnapshot::v1::ReadAsOfResponse;

using bft::client::ReadConfig;
using bft::client::WriteConfig;
using bft::client::RequestConfig;
using concord::messages::StateSnapshotRequest;
using concord::messages::StateSnapshotResponse;
using concord::messages::SignedPublicStateHashRequest;
using concord::messages::SignedPublicStateHashResponse;
using concord::messages::SnapshotResponseStatus;
using concord::messages::ReconfigurationRequest;
using concord::messages::ReconfigurationResponse;
using concord::messages::StateSnapshotReadAsOfRequest;
using concord::messages::StateSnapshotReadAsOfResponse;

using concord::client::concordclient::SnapshotKVPair;
using concord::client::concordclient::StreamUpdateQueue;
using concord::client::concordclient::UpdateNotFound;
using concord::client::concordclient::OutOfRangeSubscriptionRequest;
using concord::client::concordclient::StreamUnavailable;
using concord::client::concordclient::InternalError;
using concord::client::concordclient::EndOfStream;

namespace concord::client::clientservice {

static concord::util::SHA3_256::Digest singleHash(const std::string& key) {
  return concord::util::SHA3_256{}.digest(key.data(), key.size());
}

static void nextHash(const std::string& key, const std::string& value, concord::util::SHA3_256::Digest& prev_hash) {
  auto hasher = concord::util::SHA3_256{};
  hasher.init();
  hasher.update(prev_hash.data(), prev_hash.size());
  const auto key_hash = singleHash(key);
  hasher.update(key_hash.data(), key_hash.size());
  hasher.update(value.data(), value.size());
  prev_hash = hasher.finish();
}

template <typename ResponseT>
struct ResponseType {
  std::variant<ResponseT, std::vector<std::unique_ptr<ResponseT>>> response;
};

template <typename ResponseT>
static void getResponseSetStatus(concord::client::concordclient::SendResult&& send_result,
                                 ResponseType<ResponseT>& response,
                                 grpc::Status& return_status,
                                 const std::string& correlation_id,
                                 bool is_rsi,
                                 const std::string& log_str) {
  auto logger = logging::getLogger(log_str);
  if (!std::holds_alternative<bft::client::Reply>(send_result)) {
    switch (std::get<uint32_t>(send_result)) {
      case (static_cast<uint32_t>(bftEngine::OperationResult::INVALID_REQUEST)):
        LOG_INFO(logger, "Request failed with INVALID_ARGUMENT error for cid=" << correlation_id);
        return_status = grpc::Status(grpc::StatusCode::UNKNOWN, "Invalid argument");
        break;
      case (static_cast<uint32_t>(bftEngine::OperationResult::NOT_READY)):
        LOG_INFO(logger, "Request failed with NOT_READY error for cid=" << correlation_id);
        return_status = grpc::Status(grpc::StatusCode::UNAVAILABLE, "No clients connected to the replicas");
        break;
      case (static_cast<uint32_t>(bftEngine::OperationResult::TIMEOUT)):
        LOG_INFO(logger, "Request failed with TIMEOUT error for cid=" << correlation_id);
        return_status = grpc::Status(grpc::StatusCode::UNAVAILABLE, "Timeout");
        break;
      case (static_cast<uint32_t>(bftEngine::OperationResult::EXEC_DATA_TOO_LARGE)):
        LOG_INFO(logger, "Request failed with EXEC_DATA_TOO_LARGE error for cid=" << correlation_id);
        return_status = grpc::Status(grpc::StatusCode::UNKNOWN, "Execution data too large");
        break;
      case (static_cast<uint32_t>(bftEngine::OperationResult::EXEC_DATA_EMPTY)):
        LOG_INFO(logger, "Request failed with EXEC_DATA_EMPTY error for cid=" << correlation_id);
        return_status = grpc::Status(grpc::StatusCode::UNKNOWN, "Execution data is empty");
        break;
      case (static_cast<uint32_t>(bftEngine::OperationResult::CONFLICT_DETECTED)):
        LOG_INFO(logger, "Request failed with CONFLICT_DETECTED error for cid=" << correlation_id);
        return_status = grpc::Status(grpc::StatusCode::UNAVAILABLE, "Aborted");
        break;
      case (static_cast<uint32_t>(bftEngine::OperationResult::OVERLOADED)):
        LOG_INFO(logger, "Request failed with OVERLOADED error for cid=" << correlation_id);
        return_status = grpc::Status(grpc::StatusCode::UNAVAILABLE, "All clients occupied");
        break;
      case (static_cast<uint32_t>(bftEngine::OperationResult::UNKNOWN)):
        LOG_INFO(logger, "Request failed with UNKNOWN error for cid=" << correlation_id);
        return_status = grpc::Status(grpc::StatusCode::UNKNOWN, "Failure due to unknown reason");
        break;
      case (static_cast<uint32_t>(bftEngine::OperationResult::INTERNAL_ERROR)):
        LOG_INFO(logger, "Request failed with INTERNAL error for cid=" << correlation_id);
        return_status = grpc::Status(grpc::StatusCode::UNKNOWN, "Failure due to some internal error");
        break;
      default:
        LOG_INFO(logger, "Request failed with INTERNAL error for cid=" << correlation_id);
        return_status = grpc::Status(grpc::StatusCode::UNKNOWN, "Internal error");
        break;
    }
    return;
  }
  return_status = grpc::Status::OK;
  auto reply = std::get<bft::client::Reply>(send_result);
  if (is_rsi) {
    ReconfigurationResponse res;
    concord::messages::deserialize(reply.matched_data, res);
    response.response.template emplace<ResponseT>(std::get<ResponseT>(res.response));
  } else {
    std::vector<std::unique_ptr<ResponseT>> responses;
    for (const auto& r : reply.rsi) {
      ReconfigurationResponse res;
      concord::messages::deserialize(r.second, res);
      responses.emplace_back(std::make_unique<ResponseT>(std::get<ResponseT>(res.response)));
    }
    response.response.template emplace<std::vector<std::unique_ptr<ResponseT>>>(std::move(responses));
  }
}

Status StateSnapshotServiceImpl::GetRecentSnapshot(ServerContext* context,
                                                   const GetRecentSnapshotRequest* proto_request,
                                                   GetRecentSnapshotResponse* response) {
  std::chrono::milliseconds timeout = 5s;  // This is default timeout
  if (context != nullptr) {
    auto time_left =
        gpr_time_to_millis(gpr_time_sub(gpr_now(context->raw_deadline().clock_type), context->raw_deadline()));
    std::chrono::milliseconds curr_timeout{time_left};
    if (curr_timeout.count() > 0) {
      timeout = curr_timeout;
    }
  }

  WriteConfig write_config{RequestConfig{false, 0, 1024 * 1024, timeout, "snapshotreq", "", false, true},
                           bft::client::LinearizableQuorum{}};
  StateSnapshotResponse snapshot_response;
  grpc::Status return_status;
  auto callback =
      [&snapshot_response, &return_status, &write_config](concord::client::concordclient::SendResult&& send_result) {
        ResponseType<StateSnapshotResponse> reponse;
        getResponseSetStatus(std::move(send_result),
                             reponse,
                             return_status,
                             write_config.request.correlation_id,
                             false,
                             "concord.client.clientservice.state_snapshot_service.GetRecentSnapshot.callback");
        snapshot_response = std::get<StateSnapshotResponse>(reponse.response);
      };

  ReconfigurationRequest rreq;
  StateSnapshotRequest cmd;
  cmd.checkpoint_kv_count = 0;
  cmd.participant_id = client_->getTRId();
  rreq.command = cmd;
  bft::client::Msg message;
  concord::messages::serialize(message, rreq);
  client_->send(write_config, std::move(message), callback);
  if (return_status.ok() && snapshot_response.data.has_value()) {
    response->set_snapshot_id(snapshot_response.data->snapshot_id);
    response->set_event_group_id(snapshot_response.data->event_group_id);
    response->set_key_value_count_estimate(snapshot_response.data->key_value_count_estimate);
    auto* ledger_time = response->mutable_ledger_time();
    *ledger_time = google::protobuf::util::TimeUtil::GetEpoch();
  }
  return return_status;
}

Status StateSnapshotServiceImpl::StreamSnapshot(ServerContext* context,
                                                const StreamSnapshotRequest* proto_request,
                                                ServerWriter<StreamSnapshotResponse>* stream) {
  ConcordAssertNE(proto_request, nullptr);
  concord::client::concordclient::StreamSnapshotRequest request;
  request.snapshot_id = proto_request->snapshot_id();
  if (proto_request->has_last_received_key()) {
    request.last_received_key = proto_request->last_received_key();
  }
  std::shared_ptr<StreamUpdateQueue> update_queue = std::make_shared<StreamUpdateQueue>();

  client_->readStream(request, update_queue);

  Status status = grpc::Status(grpc::StatusCode::UNAVAILABLE, "Service not available");
  bool is_end_of_stream = false;
  auto accumulated_hash = singleHash(std::string{});
  while (!context->IsCancelled()) {
    StreamSnapshotResponse response;
    std::unique_ptr<SnapshotKVPair> update;
    try {
      update = update_queue->pop();
    } catch (const UpdateNotFound& e) {
      status = grpc::Status(grpc::StatusCode::NOT_FOUND, e.what());
      break;
    } catch (const OutOfRangeSubscriptionRequest& e) {
      status = grpc::Status(grpc::StatusCode::UNAVAILABLE, e.what());
      break;
    } catch (const InternalError& e) {
      status = grpc::Status(grpc::StatusCode::UNKNOWN, e.what());
      break;
    } catch (const StreamUnavailable& e) {
      status = grpc::Status(grpc::StatusCode::UNAVAILABLE, e.what());
      break;
    } catch (const EndOfStream& e) {
      is_end_of_stream = true;
      status = grpc::Status(grpc::StatusCode::OK, "All good");
      break;
    } catch (...) {
      break;
    }

    if (!update) {
      status = grpc::Status(grpc::StatusCode::UNAVAILABLE, "Stream ended abruptly");
      break;
    }

    auto kvpair = response.mutable_key_value();
    kvpair->set_key(update->key);
    kvpair->set_value(update->val);
    stream->Write(response);
    nextHash(update->key, update->val, accumulated_hash);
  }

  if (is_end_of_stream && status.ok()) {
    std::chrono::milliseconds timeout = 5s;  // This is default timeout
    if (context != nullptr) {
      auto time_left =
          gpr_time_to_millis(gpr_time_sub(gpr_now(context->raw_deadline().clock_type), context->raw_deadline()));
      std::chrono::milliseconds curr_timeout{time_left};
      if (curr_timeout.count() > 0) {
        timeout = curr_timeout;
      }
    }
    isHashValid(proto_request->snapshot_id(), accumulated_hash, timeout, status);
  }

  return status;
}

void StateSnapshotServiceImpl::isHashValid(uint64_t snapshot_id,
                                           const concord::util::SHA3_256::Digest& final_hash,
                                           const std::chrono::milliseconds& timeout,
                                           Status& return_status) const {
  ReadConfig read_config{RequestConfig{false, 0, 1024 * 1024, timeout, "signedHashReq", "", false, true},
                         bft::client::ByzantineSafeQuorum{}};
  std::vector<std::unique_ptr<SignedPublicStateHashResponse>> signed_hash_responses;
  auto callback =
      [&signed_hash_responses, &return_status, &read_config](concord::client::concordclient::SendResult&& send_result) {
        ResponseType<SignedPublicStateHashResponse> reponse;
        getResponseSetStatus(std::move(send_result),
                             reponse,
                             return_status,
                             read_config.request.correlation_id,
                             true,
                             "concord.client.clientservice.state_snapshot_service.signedhash.callback");
        signed_hash_responses =
            std::get<std::vector<std::unique_ptr<SignedPublicStateHashResponse>>>(std::move(reponse.response));
      };

  ReconfigurationRequest rreq;
  SignedPublicStateHashRequest cmd;
  cmd.snapshot_id = snapshot_id;
  cmd.participant_id = client_->getTRId();
  rreq.command = cmd;
  bft::client::Msg message;
  concord::messages::serialize(message, rreq);
  client_->send(read_config, std::move(message), callback);
  for (const auto& res : signed_hash_responses) {
    if (return_status.ok()) {
      switch (res->status) {
        case SnapshotResponseStatus::InternalError:
          return_status = grpc::Status(grpc::StatusCode::UNKNOWN, "Internal error");
          break;
        case SnapshotResponseStatus::SnapshotNonExistent:
          return_status = grpc::Status(grpc::StatusCode::UNAVAILABLE, "Snapshot doesn't exist");
          break;
        case SnapshotResponseStatus::SnapshotPending:
          return_status = grpc::Status(grpc::StatusCode::UNAVAILABLE, "Snapshot not ready");
          break;
        case SnapshotResponseStatus::Success:
          if ((res->data).snapshot_id != snapshot_id) {
            return_status = grpc::Status(grpc::StatusCode::FAILED_PRECONDITION, "Hash mismatch from replicas");
          }
          if ((res->data).hash != final_hash) {
            return_status = grpc::Status(grpc::StatusCode::FAILED_PRECONDITION, "Hash mismatch from replicas");
          }
          break;
        default:
          ConcordAssert(false);
          break;
      }
    } else {
      break;  // Break the for loop abruptly
    }
  }
}

Status StateSnapshotServiceImpl::ReadAsOf(ServerContext* context,
                                          const ReadAsOfRequest* proto_request,
                                          ReadAsOfResponse* response) {
  std::chrono::milliseconds timeout = 5s;  // This is default timeout
  if (context != nullptr) {
    auto time_left =
        gpr_time_to_millis(gpr_time_sub(gpr_now(context->raw_deadline().clock_type), context->raw_deadline()));
    std::chrono::milliseconds curr_timeout{time_left};
    if (curr_timeout.count() > 0) {
      timeout = curr_timeout;
    }
  }

  ReadConfig read_config{RequestConfig{false, 0, 1024 * 1024, timeout, "readasofreq", "", false, true},
                         bft::client::ByzantineSafeQuorum{}};

  std::vector<std::unique_ptr<StateSnapshotReadAsOfResponse>> read_as_of_responses;
  grpc::Status return_status;
  auto callback =
      [&read_as_of_responses, &return_status, &read_config](concord::client::concordclient::SendResult&& send_result) {
        ResponseType<StateSnapshotReadAsOfResponse> reponse;
        getResponseSetStatus(std::move(send_result),
                             reponse,
                             return_status,
                             read_config.request.correlation_id,
                             true,
                             "concord.client.clientservice.state_snapshot_service.signedhash.callback");
        read_as_of_responses =
            std::get<std::vector<std::unique_ptr<StateSnapshotReadAsOfResponse>>>(std::move(reponse.response));
      };

  ConcordAssertNE(proto_request, nullptr);
  ReconfigurationRequest rreq;
  StateSnapshotReadAsOfRequest cmd;
  cmd.snapshot_id = proto_request->snapshot_id();
  cmd.participant_id = client_->getTRId();
  for (int i = 0; i < proto_request->keys_size(); ++i) {
    cmd.keys.push_back(proto_request->keys(i));
  }
  rreq.command = cmd;
  bft::client::Msg message;
  concord::messages::serialize(message, rreq);
  client_->send(read_config, std::move(message), callback);
  for (const auto& res : read_as_of_responses) {
    if (return_status.ok()) {
      switch (res->status) {
        case SnapshotResponseStatus::InternalError:
          return_status = grpc::Status(grpc::StatusCode::UNKNOWN, "Internal error");
          break;
        case SnapshotResponseStatus::SnapshotNonExistent:
          return_status = grpc::Status(grpc::StatusCode::UNAVAILABLE, "Snapshot doesn't exist");
          break;
        case SnapshotResponseStatus::SnapshotPending:
          return_status = grpc::Status(grpc::StatusCode::UNAVAILABLE, "Snapshot not ready");
          break;
        case SnapshotResponseStatus::Success: {
          auto rv_size = res->values.size();
          auto pr_size = proto_request->keys_size();
          if (rv_size != static_cast<decltype(rv_size)>(pr_size)) {
            return_status = grpc::Status(grpc::StatusCode::UNKNOWN, "Internal error");
          } else {
            bool set_values = (response->values_size() == 0);
            size_t pos = 0;
            for (const auto& v : res->values) {
              if (set_values) {
                auto* resp_val = response->add_values();
                if (v.has_value()) {
                  resp_val->set_value(v.value());
                }
              } else {
                if (v.has_value()) {
                  if (v.value() != response->values(pos).value()) {
                    return_status =
                        grpc::Status(grpc::StatusCode::UNKNOWN, "Got different values from different replicas");
                  }
                } else {
                  if ((response->values(pos)).has_value()) {
                    return_status =
                        grpc::Status(grpc::StatusCode::UNKNOWN, "Got different values from different replicas");
                  }
                }
                pos++;
              }
            }
          }
        } break;
        default:
          ConcordAssert(false);
          break;
      }
    } else {
      break;  // Break the for loop.
    }
  }
  return return_status;
}

}  // namespace concord::client::clientservice
