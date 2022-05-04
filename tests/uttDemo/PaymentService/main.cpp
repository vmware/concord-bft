// Concord
//
// Copyright (c) 2018-2022 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include <iostream>
#include <fstream>
#include <memory>
#include <mutex>
#include <condition_variable>

#include "SharedTypes.hpp"
#include <config_file_parser.hpp>
#include "config/test_comm_config.hpp"
#include "config/test_parameters.hpp"
#include "communication/CommFactory.hpp"
#include "bftclient/config.h"
#include "bftclient/bft_client.h"
#include "bftclient/seq_num_generator.h"
#include "utt_messages.cmf.hpp"

using namespace bftEngine;
using namespace bft::communication;
using std::string;
using namespace utt::messages;
using namespace bft::client;

/////////////////////////////////////////////////////////////////////////////////////////////////////
struct PaymentServiceParams {
  uint16_t paymentProcessorId_ = 0;
  uint16_t numOfFaulty_ = 0;
  std::string configFileName_;
};

/////////////////////////////////////////////////////////////////////////////////////////////////////
struct WalletRequest {
  UTTRequest req_;
  NodeNum sender_ = 0;
};

/////////////////////////////////////////////////////////////////////////////////////////////////////
class PaymentServiceCommunicator : public IReceiver {
 public:
  PaymentServiceCommunicator(logging::Logger& logger, uint16_t paymentServiceId, const std::string& cfgFileName)
      : logger_{logger} {
    // [TODO-UTT] Support for other communcation modes like TCP/TLS (if needed)

    if (cfgFileName.empty()) throw std::runtime_error("Network config filename empty!");

    concord::util::ConfigFileParser cfgFileParser(logger_, cfgFileName);
    if (!cfgFileParser.Parse()) throw std::runtime_error("Failed to parse configuration file: " + cfgFileName);

    // Load payment service listen address
    auto paymentServiceAddr = cfgFileParser.GetNthValue("payment_services_config", paymentServiceId);
    if (paymentServiceAddr.empty())
      throw std::runtime_error("No payment service address for id " + std::to_string(paymentServiceId));

    std::string listenHost;
    uint16_t listenPort = 0;
    {
      std::stringstream ss(std::move(paymentServiceAddr));
      std::getline(ss, listenHost, ':');
      ss >> listenPort;

      if (listenHost.empty()) throw std::runtime_error("Empty wallet address!");
      if (listenPort == 0) throw std::runtime_error("Invalid wallet port!");
    }

    LOG_INFO(logger_, "PaymentService listening addr: " << listenHost << " : " << listenPort);

    // Load wallet network addresses
    // Map from payment service id to wallet ids
    // 1 -> {1, 2, 3}
    // 2 -> {4, 5, 6}
    // 3 -> {7, 8, 9}
    std::unordered_map<NodeNum, NodeInfo> nodes;
    for (int i = 0; i < 3; ++i) {
      const NodeNum walletId = (paymentServiceId - 1) * 3 + i + 1;
      auto walletAddr = cfgFileParser.GetNthValue("wallets_config", walletId);
      if (walletAddr.empty()) throw std::runtime_error("No wallet address for id " + std::to_string(walletId));

      std::string walletHost;
      uint16_t walletPort = 0;
      {
        std::stringstream ss(std::move(walletAddr));
        std::getline(ss, walletHost, ':');
        ss >> walletPort;

        if (walletHost.empty()) throw std::runtime_error("Empty wallet address!");
        if (walletPort == 0) throw std::runtime_error("Invalid wallet port!");
      }

      LOG_INFO(logger_, "Serving Wallet with id " << walletId << " at addr: " << walletHost << " : " << walletPort);

      nodes.emplace(walletId, NodeInfo{walletHost, walletPort, false});
    }

    int32_t msgMaxSize = 128 * 1024;  // 128 kB -- Same as TestCommConfig
    NodeNum selfId = 0;               // The payment service is always node 0 with respect to the wallets

    PlainUdpConfig conf(listenHost, listenPort, msgMaxSize, nodes, selfId);

    comm_.reset(CommFactory::create(conf));
    if (!comm_) throw std::runtime_error("Failed to create PaymentService communication!");

    comm_->setReceiver(selfId, this);
    comm_->start();
  }

  ~PaymentServiceCommunicator() { comm_->stop(); }

  // Invoked when a new message is received
  // Notice that the memory pointed by message may be freed immediately
  // after the execution of this method.
  void onNewMessage(NodeNum sourceNode, const char* const message, size_t messageLength, NodeNum endpointNum) override {
    // This is called from the listening thread of the communication
    LOG_INFO(logger_, "onNewMessage from: " << sourceNode << " msgLen: " << messageLength);

    // Deserialize the received UTTRquest from a wallet
    WalletRequest req;
    req.sender_ = sourceNode;

    auto begin = reinterpret_cast<const uint8_t*>(message);
    auto end = begin + messageLength;
    deserialize(begin, end, req.req_);

    // Push the request on the queue
    // Note that wallets wait for replies before sending new requests
    // so the queue cannot be overwhelmed under normal operation
    {
      std::lock_guard lg{mut_};
      requests_.emplace(std::move(req));
    }
    condVar_.notify_one();  // Notify getRequest
  }

  // Invoked when the known status of a connection is changed.
  // For each NodeNum, this method will never be concurrently
  // executed by two different threads.
  void onConnectionStatusChanged(NodeNum node, ConnectionStatus newStatus) override {
    // Not applicable to UDP
    LOG_INFO(logger_, "onConnectionStatusChanged from: " << node << " newStatus: " << (int)newStatus);
  }

  std::optional<WalletRequest> getRequest() {
    std::optional<WalletRequest> req;

    // Get or wait for the next request
    {
      std::unique_lock<std::mutex> lk{mut_};

      condVar_.wait(lk, [&]() { return !requests_.empty(); });

      req = std::move(requests_.front());
      requests_.pop();
    }

    return req;
  }

  void sendReply(NodeNum receiver, std::vector<uint8_t>&& reply) { comm_->send(receiver, std::move(reply)); }

 private:
  logging::Logger& logger_;
  std::unique_ptr<ICommunication> comm_;
  std::queue<WalletRequest> requests_;
  std::mutex mut_;
  std::condition_variable condVar_;
};

/////////////////////////////////////////////////////////////////////////////////////////////////////
uint64_t nextSeqNum() {
  static SeqNumberGenerator gen{ClientId{0}};  // ClientId used just for logging
  return gen.unique();
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
PaymentServiceParams setupParams(int argc, char** argv) {
  PaymentServiceParams params;
  char argTempBuffer[PATH_MAX + 10];
  int o = 0;
  while ((o = getopt(argc, argv, "i:f:c:p:n:")) != EOF) {
    switch (o) {
      case 'i': {
        strncpy(argTempBuffer, optarg, sizeof(argTempBuffer) - 1);
        argTempBuffer[sizeof(argTempBuffer) - 1] = 0;
        string idStr = argTempBuffer;
        int tempId = std::stoi(idStr);
        if (tempId >= 0 && tempId < UINT16_MAX) params.paymentProcessorId_ = (uint16_t)tempId;
      } break;

      case 'f': {
        strncpy(argTempBuffer, optarg, sizeof(argTempBuffer) - 1);
        argTempBuffer[sizeof(argTempBuffer) - 1] = 0;
        string fStr = argTempBuffer;
        int tempfVal = std::stoi(fStr);
        if (tempfVal >= 1 && tempfVal < UINT16_MAX) params.numOfFaulty_ = (uint16_t)tempfVal;
      } break;

      case 'n': {
        strncpy(argTempBuffer, optarg, sizeof(argTempBuffer) - 1);
        argTempBuffer[sizeof(argTempBuffer) - 1] = 0;
        params.configFileName_ = argTempBuffer;
      } break;

      default:
        break;
    }
  }
  return params;
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
auto logger = logging::getLogger("uttdemo.payment-service");

/////////////////////////////////////////////////////////////////////////////////////////////////////
ICommunication* setupCommunicationParams(ClientParams& cp) {
  TestCommConfig testCommConfig(logger);
  uint16_t numOfReplicas = cp.get_numOfReplicas();
#ifdef USE_COMM_PLAIN_TCP
  PlainTcpConfig conf =
      testCommConfig.GetTCPConfig(false, cp.clientId, cp.numOfClients, numOfReplicas, cp.configFileName);
#elif USE_COMM_TLS_TCP
  TlsTcpConfig conf =
      testCommConfig.GetTlsTCPConfig(false, cp.clientId, cp.numOfClients, numOfReplicas, cp.configFileName);
#else
  PlainUdpConfig conf =
      testCommConfig.GetUDPConfig(false, cp.clientId, cp.numOfClients, numOfReplicas, cp.configFileName);
#endif

  return CommFactory::create(conf);
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
bft::client::Reply sendUTTRequest(Client& client, const UTTRequest& msg) {
  if (std::holds_alternative<TxRequest>(msg.request)) {
    // Send write

    // Ensure we only wait for F+1 replies (ByzantineSafeQuorum)
    WriteConfig writeConf{RequestConfig{false, nextSeqNum()}, ByzantineSafeQuorum{}};

    Msg reqBytes;
    serialize(reqBytes, msg);
    return client.send(writeConf, std::move(reqBytes));  // Sync send

  } else if (std::holds_alternative<GetLastBlockRequest>(msg.request) ||
             std::holds_alternative<GetBlockDataRequest>(msg.request)) {
    // Send read

    ReadConfig readConf{RequestConfig{false, nextSeqNum()}, LinearizableQuorum{}};

    Msg reqBytes;
    serialize(reqBytes, msg);
    return client.send(readConf, std::move(reqBytes));  // Sync send
  }

  throw std::runtime_error("Unhandled UTTRequest type!");
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
int main(int argc, char** argv) {
  logging::initLogger("config/logging.properties");

  PaymentServiceParams params = setupParams(argc, argv);

  if (params.paymentProcessorId_ == 0 || params.numOfFaulty_ == 0) {
    std::cout << "Wrong usage! Required parameters: " << argv[0] << " -f <numFaulty> -i <id>";
    exit(-1);
  }

  ClientParams bftClientParams;
  bftClientParams.numOfFaulty = params.numOfFaulty_;
  bftClientParams.numOfReplicas = 3 * params.numOfFaulty_ + 1;
  bftClientParams.configFileName = params.configFileName_;

  // Map payment processor id to bft client id
  bftClientParams.clientId = bftClientParams.numOfReplicas + (params.paymentProcessorId_ - 1);

  // Create the bft client communication
  SharedCommPtr bftClientComm = SharedCommPtr(setupCommunicationParams(bftClientParams));
  if (!bftClientComm) {
    LOG_FATAL(logger, "Failed to create bft client communication!");
    exit(-1);
  }

  // Create the bft client
  ClientConfig bftClientConfig;
  bftClientConfig.f_val = bftClientParams.numOfFaulty;
  for (uint16_t i = 0; i < bftClientParams.numOfReplicas; ++i) bftClientConfig.all_replicas.emplace(ReplicaId{i});

  bftClientConfig.id = ClientId{bftClientParams.clientId};

  Client client(bftClientComm, bftClientConfig);

  try {
    // Create the payment service communicator
    // - receives requests from wallets
    // - forwards the requests to the bft client
    // - sends the bft reply back to the wallet

    LOG_INFO(logger, "Starting PaymentService " << params.paymentProcessorId_);

    PaymentServiceCommunicator comm(logger, params.paymentProcessorId_, params.configFileName_);

    // Process wallet requests synchronously
    while (auto req = comm.getRequest()) {
      ConcordAssert(req->sender_ != 0);

      BftReply bftReply;

      try {
        auto reply = sendUTTRequest(client, req->req_);
        bftReply.result = reply.result;
        bftReply.matched_data = std::move(reply.matched_data);
        for (auto& kvp : reply.rsi) bftReply.rsi.emplace(kvp.first.val, std::move(kvp.second));
        const auto primaryReplicaId = client.primary();
        if (primaryReplicaId) bftReply.primary = primaryReplicaId->val;

      } catch (const bft::client::TimeoutException& e) {
        bftReply.result = static_cast<uint32_t>(OperationResult::TIMEOUT);
      }

      std::vector<uint8_t> replyBytes;
      serialize(replyBytes, bftReply);

      comm.sendReply(req->sender_, std::move(replyBytes));
    }

  } catch (std::exception& e) {
    LOG_FATAL(logger, "Exception: " << e.what());
    exit(-1);
  }
}
