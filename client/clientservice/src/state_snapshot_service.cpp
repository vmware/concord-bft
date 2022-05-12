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

#include <condition_variable>
#include <opentracing/tracer.h>
#include <google/protobuf/util/time_util.h>

#include "assertUtils.hpp"
#include "client/clientservice/state_snapshot_service.hpp"
#include "client/concordclient/snapshot_update.hpp"

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
using concord::messages::ReconfigurationErrorMsg;
using concord::messages::StateSnapshotReadAsOfRequest;
using concord::messages::StateSnapshotReadAsOfResponse;

using concord::client::concordclient::SnapshotKVPair;
using concord::client::concordclient::SnapshotQueue;
using concord::client::concordclient::BasicSnapshotQueue;
using concord::client::concordclient::UpdateNotFound;
using concord::client::concordclient::OutOfRangeSubscriptionRequest;
using concord::client::concordclient::StreamUnavailable;
using concord::client::concordclient::InternalError;
using concord::client::concordclient::EndOfStream;
using concord::client::concordclient::RequestOverload;

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

// If is_rsi is true then the response is in the rsi vector else the matched data will be used.
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
      case (static_cast<uint32_t>(bftEngine::OperationResult::EXEC_ENGINE_REJECT_ERROR)):
        LOG_INFO(logger, "Request failed with EXEC_ENGINE_REJECT_ERROR error for cid=" << correlation_id);
        return_status = grpc::Status(grpc::StatusCode::UNKNOWN, "Aborted");
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
  try {
    if (is_rsi) {
      std::vector<std::unique_ptr<ResponseT>> responses;
      for (const auto& r : reply.rsi) {
        ReconfigurationResponse res;
        concord::messages::deserialize(r.second, res);
        if (res.success) {
          if (std::holds_alternative<ResponseT>(res.response)) {
            responses.emplace_back(std::make_unique<ResponseT>(std::get<ResponseT>(res.response)));
          } else {
            LOG_INFO(logger, "Request failed from replica id : " << r.first.val);
            return_status = grpc::Status(grpc::StatusCode::UNKNOWN, "Failure due to some internal error");
            return;
          }
        } else {
          if (std::holds_alternative<ReconfigurationErrorMsg>(res.response)) {
            LOG_WARN(logger,
                     "Request failed from replica id : " << r.first.val << " with message : "
                                                         << std::get<ReconfigurationErrorMsg>(res.response).error_msg
                                                         << " for response result : " << reply.result);
          }
          LOG_WARN(logger,
                   "Request failed in RSI with INTERNAL error for cid="
                       << correlation_id << " replica sent variant of index : " << res.response.index()
                       << " for response result : " << reply.result);
          return_status = grpc::Status(grpc::StatusCode::UNKNOWN, "Failure due to some internal error");
          return;
        }
      }
      response.response.template emplace<std::vector<std::unique_ptr<ResponseT>>>(std::move(responses));
    } else {
      ReconfigurationResponse res;
      concord::messages::deserialize(reply.matched_data, res);
      if (res.success) {
        if (std::holds_alternative<ResponseT>(res.response)) {
          response.response.template emplace<ResponseT>(std::get<ResponseT>(res.response));
        } else {
          LOG_INFO(logger, "Request failed from replicas");
          return_status = grpc::Status(grpc::StatusCode::UNKNOWN, "Failure due to some internal error");
          return;
        }
      } else {
        if (std::holds_alternative<ReconfigurationErrorMsg>(res.response)) {
          LOG_WARN(logger,
                   "Request failed from replicas with message : "
                       << std::get<ReconfigurationErrorMsg>(res.response).error_msg
                       << " for response result : " << reply.result);
          return_status = grpc::Status(grpc::StatusCode::UNKNOWN, "Failure due to some internal error");
        }
        LOG_WARN(logger,
                 "Response failed with INTERNAL error for cid=" << correlation_id << " replica sent variant of index : "
                                                                << res.response.index()
                                                                << " for response result : " << reply.result);
        return_status = grpc::Status(grpc::StatusCode::UNKNOWN, "Failure due to some internal error");
        return;
      }
    }
  } catch (std::exception& e) {
    LOG_FATAL(logger,
              "Response parsing failed with parse error for cid=" << correlation_id << " exception = " << e.what()
                                                                  << " for response result : " << reply.result);
    ConcordAssert(false);
  }
}

static std::shared_ptr<bftEngine::RequestCallBack> getCallbackLambda(const bftEngine::RequestCallBack& callback) {
  return std::make_shared<bftEngine::RequestCallBack>(callback);
}

std::chrono::milliseconds StateSnapshotServiceImpl::setTimeoutFromDeadline(ServerContext* context) {
  std::chrono::milliseconds timeout = 120s;  // This is default timeout
  if (context != nullptr) {
    auto end_time = context->deadline();
    auto curr_timeout =
        std::chrono::duration_cast<std::chrono::milliseconds>(end_time - std::chrono::system_clock::now());
    if ((curr_timeout.count() > 0) && (curr_timeout.count() <= MAX_TIMEOUT_MS)) {
      timeout = curr_timeout;
    } else {
      LOG_WARN(logger_,
               "Extreme deadline set by client of client service "
                   << KVLOG(curr_timeout.count(), end_time.time_since_epoch().count()));
    }
  }
  return timeout;
}

Status StateSnapshotServiceImpl::GetRecentSnapshot(ServerContext* context,
                                                   const GetRecentSnapshotRequest* proto_request,
                                                   GetRecentSnapshotResponse* response) {
  std::chrono::milliseconds timeout = setTimeoutFromDeadline(context);
  LOG_INFO(logger_, "Received a GetRecentSnapshotRequest with timeout : " << timeout.count() << "ms");

  auto write_config = std::shared_ptr<WriteConfig>(
      new WriteConfig{RequestConfig{false, 0, 1024 * 1024, timeout, "snapshotreq", "", false, true},
                      bft::client::LinearizableQuorum{}});
  auto snapshot_response = std::make_shared<StateSnapshotResponse>();
  auto return_status = std::make_shared<grpc::Status>(grpc::StatusCode::UNAVAILABLE, "Timeout");
  auto wait_for_me = std::make_shared<std::condition_variable>();
  auto reply_available = std::make_shared<bool>(false);
  auto callback = getCallbackLambda([snapshot_response, return_status, write_config, wait_for_me, reply_available](
                                        concord::client::concordclient::SendResult&& send_result) {
    ResponseType<StateSnapshotResponse> reponse;
    getResponseSetStatus(std::move(send_result),
                         reponse,
                         *return_status,
                         (write_config->request).correlation_id,
                         false,
                         "concord.client.clientservice.state_snapshot_service.getrecentsnapshot.callback");
    if (return_status->ok()) {
      *snapshot_response = std::get<StateSnapshotResponse>(reponse.response);
    }
    *reply_available = true;
    wait_for_me->notify_one();
  });

  ReconfigurationRequest rreq;
  StateSnapshotRequest cmd;
  cmd.checkpoint_kv_count = 0;
  cmd.participant_id = client_->getSubscriptionId();
  rreq.command = cmd;
  bft::client::Msg message;
  concord::messages::serialize(message, rreq);
  client_->send(*write_config, std::move(message), *callback);
  bool response_available = false;
  {
    std::mutex mtx;
    std::unique_lock<std::mutex> lck(mtx);
    response_available = wait_for_me->wait_for(lck, timeout, [reply_available]() { return *reply_available; });
  }

  if (!response_available) {
    clearAllPrevDoneCallbacksAndAdd(reply_available, callback);
    return grpc::Status(grpc::StatusCode::UNAVAILABLE, "Timeout");
  }

  if (return_status->ok() && snapshot_response->data.has_value()) {
    response->set_snapshot_id(snapshot_response->data->snapshot_id);
    switch (snapshot_response->data->blockchain_height_type) {
      case concord::messages::BlockchainHeightType::EventGroupId:
        response->set_event_group_id(snapshot_response->data->blockchain_height);
        break;
      case concord::messages::BlockchainHeightType::BlockId:
        response->set_block_id(snapshot_response->data->blockchain_height);
        break;
    }
    response->set_key_value_count_estimate(snapshot_response->data->key_value_count_estimate);
    auto* ledger_time = response->mutable_ledger_time();
    if (!google::protobuf::util::TimeUtil::FromString(snapshot_response->data->last_application_transaction_time,
                                                      ledger_time)) {
      *ledger_time = google::protobuf::util::TimeUtil::GetEpoch();
    }
  }
  return *return_status;
}

Status StateSnapshotServiceImpl::StreamSnapshot(ServerContext* context,
                                                const StreamSnapshotRequest* proto_request,
                                                ServerWriter<StreamSnapshotResponse>* stream) {
  ConcordAssertNE(proto_request, nullptr);
  LOG_INFO(logger_,
           "Received a StreamSnapshotRequest with snapshot id : "
               << proto_request->snapshot_id() << " with last received key "
               << (proto_request->has_last_received_key() ? proto_request->last_received_key() : "EMPTY"));
  concord::client::concordclient::StateSnapshotRequest request;
  request.snapshot_id = proto_request->snapshot_id();
  if (proto_request->has_last_received_key()) {
    request.last_received_key = proto_request->last_received_key();
  }
  std::shared_ptr<SnapshotQueue> update_queue = std::make_shared<BasicSnapshotQueue>();

  client_->getSnapshot(request, update_queue);

  Status status = grpc::Status(grpc::StatusCode::UNAVAILABLE, "Service not available");
  bool is_end_of_stream = false;
  auto accumulated_hash = singleHash(std::string{});
  // Wait for the update from other thread for 500 ms before checking the context
  auto pop_timeout = 500ms;
  while (!context->IsCancelled()) {
    StreamSnapshotResponse response;
    std::unique_ptr<SnapshotKVPair> update;
    try {
      update = update_queue->popTill(pop_timeout);
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
    } catch (const RequestOverload& e) {
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
      continue;
    }

    auto kvpair = response.mutable_key_value();
    kvpair->set_key(update->key);
    kvpair->set_value(update->val);
    stream->Write(response);
    nextHash(update->key, update->val, accumulated_hash);
  }

  if (is_end_of_stream && status.ok()) {
    std::chrono::milliseconds timeout = setTimeoutFromDeadline(context);
    isHashValid(proto_request->snapshot_id(), accumulated_hash, timeout, status);
  }

  return status;
}

void StateSnapshotServiceImpl::isHashValid(uint64_t snapshot_id,
                                           const concord::util::SHA3_256::Digest& final_hash,
                                           const std::chrono::milliseconds& timeout,
                                           Status& return_status) {
  auto read_config = std::shared_ptr<ReadConfig>(
      new ReadConfig{RequestConfig{false, 0, 1024 * 1024, timeout, "signedHashReq", "", false, true},
                     bft::client::ByzantineSafeQuorum{}});
  auto wait_for_me = std::make_shared<std::condition_variable>();
  auto reply_available = std::make_shared<bool>(false);
  auto hash_valid_return_status = std::make_shared<grpc::Status>(grpc::StatusCode::UNAVAILABLE, "Timeout");
  auto signed_hash_responses = std::make_shared<std::vector<std::unique_ptr<SignedPublicStateHashResponse>>>();
  auto callback =
      getCallbackLambda([signed_hash_responses, hash_valid_return_status, read_config, wait_for_me, reply_available](
                            concord::client::concordclient::SendResult&& send_result) {
        ResponseType<SignedPublicStateHashResponse> reponse;
        getResponseSetStatus(std::move(send_result),
                             reponse,
                             *hash_valid_return_status,
                             (read_config->request).correlation_id,
                             true,
                             "concord.client.clientservice.state_snapshot_service.signedhash.callback");
        if (hash_valid_return_status->ok()) {
          *signed_hash_responses =
              std::get<std::vector<std::unique_ptr<SignedPublicStateHashResponse>>>(std::move(reponse.response));
        }
        *reply_available = true;
        wait_for_me->notify_one();
      });

  ReconfigurationRequest rreq;
  SignedPublicStateHashRequest cmd;
  cmd.snapshot_id = snapshot_id;
  cmd.participant_id = client_->getSubscriptionId();
  rreq.command = cmd;
  bft::client::Msg message;
  concord::messages::serialize(message, rreq);
  client_->send(*read_config, std::move(message), *callback);

  bool response_available = false;
  {
    std::mutex mtx;
    std::unique_lock<std::mutex> lck(mtx);
    response_available = wait_for_me->wait_for(lck, timeout, [reply_available]() { return *reply_available; });
  }

  if (!response_available) {
    clearAllPrevDoneCallbacksAndAdd(reply_available, callback);
    return_status = grpc::Status(grpc::StatusCode::UNAVAILABLE, "Timeout");
    return;
  }

  if (response_available) {
    return_status = *hash_valid_return_status;
  }

  for (const auto& res : *signed_hash_responses) {
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
            return_status = grpc::Status(grpc::StatusCode::FAILED_PRECONDITION, "snapshot id mismatch from replicas");
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

void StateSnapshotServiceImpl::compareWithRsiAndSetReadAsOfResponse(
    const std::unique_ptr<StateSnapshotReadAsOfResponse>& bft_readasof_response,
    const ReadAsOfRequest* const proto_request,
    ReadAsOfResponse*& response,
    grpc::Status& return_status) const {
  auto rv_size = bft_readasof_response->values.size();
  auto pr_size = proto_request->keys_size();
  if (rv_size != static_cast<decltype(rv_size)>(pr_size)) {
    return_status = grpc::Status(grpc::StatusCode::UNKNOWN, "Internal error");
    return;
  }
  // If the value is empty then indicate that we have to assign the first value.
  bool have_to_set_value = (response->values_size() == 0);
  size_t pos = 0;
  // Set value from the first replica in rsi and compare values from other replicas.
  for (const auto& v : bft_readasof_response->values) {
    if (have_to_set_value) {
      auto* resp_val = response->add_values();
      if (v.has_value()) {
        resp_val->set_value(v.value());
      }
    } else {
      // This is the case of comparing the values from rsi.
      if (v.has_value()) {
        // If the value is available then chech the actual value
        if (v.value() != response->values(pos).value()) {
          return_status = grpc::Status(grpc::StatusCode::UNKNOWN, "Got different values from different replicas");
        }
      } else {
        // If the value is not available the it should be set for the response aswell.
        if ((response->values(pos)).has_value()) {
          return_status = grpc::Status(grpc::StatusCode::UNKNOWN, "Got different values from different replicas");
        }
      }
      pos++;
    }
  }
}

Status StateSnapshotServiceImpl::ReadAsOf(ServerContext* context,
                                          const ReadAsOfRequest* proto_request,
                                          ReadAsOfResponse* response) {
  std::chrono::milliseconds timeout = setTimeoutFromDeadline(context);
  LOG_INFO(logger_, "Received a ReadAsOfRequest with timeout : " << timeout.count() << "ms");

  auto read_config = std::shared_ptr<ReadConfig>(
      new ReadConfig{RequestConfig{false, 0, 1024 * 1024, timeout, "readasofreq", "", false, true},
                     bft::client::ByzantineSafeQuorum{}});
  auto signed_hash_responses = std::make_shared<std::vector<std::unique_ptr<SignedPublicStateHashResponse>>>();
  auto read_as_of_responses = std::make_shared<std::vector<std::unique_ptr<StateSnapshotReadAsOfResponse>>>();
  auto wait_for_me = std::make_shared<std::condition_variable>();
  auto reply_available = std::make_shared<bool>(false);
  auto return_status = std::make_shared<grpc::Status>(grpc::StatusCode::UNAVAILABLE, "Timeout");
  auto callback = getCallbackLambda([read_as_of_responses, return_status, read_config, wait_for_me, reply_available](
                                        concord::client::concordclient::SendResult&& send_result) {
    ResponseType<StateSnapshotReadAsOfResponse> reponse;
    getResponseSetStatus(std::move(send_result),
                         reponse,
                         *return_status,
                         (read_config->request).correlation_id,
                         true,
                         "concord.client.clientservice.state_snapshot_service.readasof.callback");
    if (return_status->ok()) {
      *read_as_of_responses =
          std::get<std::vector<std::unique_ptr<StateSnapshotReadAsOfResponse>>>(std::move(reponse.response));
    }
    *reply_available = true;
    wait_for_me->notify_one();
  });

  ConcordAssertNE(proto_request, nullptr);
  ReconfigurationRequest rreq;
  StateSnapshotReadAsOfRequest cmd;
  cmd.snapshot_id = proto_request->snapshot_id();
  cmd.participant_id = client_->getSubscriptionId();
  for (int i = 0; i < proto_request->keys_size(); ++i) {
    cmd.keys.push_back(proto_request->keys(i));
  }
  rreq.command = cmd;
  bft::client::Msg message;
  concord::messages::serialize(message, rreq);
  client_->send(*read_config, std::move(message), *callback);
  bool response_available = false;
  {
    std::mutex mtx;
    std::unique_lock<std::mutex> lck(mtx);
    response_available = wait_for_me->wait_for(lck, timeout, [reply_available]() { return *reply_available; });
  }

  if (!response_available) {
    clearAllPrevDoneCallbacksAndAdd(reply_available, callback);
    return grpc::Status(grpc::StatusCode::UNAVAILABLE, "Timeout");
  }

  for (const auto& res : *read_as_of_responses) {
    if (!return_status->ok()) {
      break;
    }

    switch (res->status) {
      case SnapshotResponseStatus::InternalError:
        *return_status = grpc::Status(grpc::StatusCode::UNKNOWN, "Internal error");
        break;
      case SnapshotResponseStatus::SnapshotNonExistent:
        *return_status = grpc::Status(grpc::StatusCode::UNAVAILABLE, "Snapshot doesn't exist");
        break;
      case SnapshotResponseStatus::SnapshotPending:
        *return_status = grpc::Status(grpc::StatusCode::UNAVAILABLE, "Snapshot not ready");
        break;
      case SnapshotResponseStatus::Success:
        compareWithRsiAndSetReadAsOfResponse(res, proto_request, response, *return_status);
        break;
      default:
        ConcordAssert(false);
        break;
    }
  }
  return *return_status;
}

void StateSnapshotServiceImpl::clearAllPrevDoneCallbacksAndAdd(std::shared_ptr<bool> condition,
                                                               std::shared_ptr<bftEngine::RequestCallBack> callback) {
  std::unique_lock<std::mutex> cleanup_lck(cleanup_mutex_);
  bool got_value = false;
  do {
    got_value = false;
    for (auto it = callbacks_for_cleanup_.begin(); it != callbacks_for_cleanup_.end(); ++it) {
      if (*(it->first)) {
        // We can delete this callback
        got_value = true;
        callbacks_for_cleanup_.erase(it);
        break;
      }
    }
  } while (got_value);
  callbacks_for_cleanup_.emplace(condition, callback);
}

}  // namespace concord::client::clientservice
