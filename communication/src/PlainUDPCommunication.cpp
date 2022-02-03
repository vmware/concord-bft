// Concord
//
// Copyright (c) 2018-2022 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the
// LICENSE file.

#include "assertUtils.hpp"
#include "Logger.hpp"
#include "communication/CommDefs.hpp"

#include "errnoString.hpp"

#include <iostream>
#include <cstddef>
#include <cassert>
#include <cstring>
#include <unordered_map>
#include <atomic>
#include <mutex>
#include <thread>
#include <functional>
#include <poll.h>

using namespace std;

namespace bft::communication {

struct NodeAddressResolveResult {
  NodeNum nodeId;
  bool wasFound;
  string key;
};

class PlainUDPCommunication::PlainUdpImpl {
 public:
  // Initializes a new UDPCommunication layer that will listen on the given listenPort.
  PlainUdpImpl(const PlainUdpConfig &config)
      : maxMsgSize_{config.bufferLength_},
        udpListenPort_{config.listenPort_},
        endpoints_{config.nodes_},
        statusCallback_{config.statusCallback_},
        selfId_{config.selfId_} {
    ConcordAssert((config.listenPort_ > 0) && "Port should not be negative!");
    ConcordAssert((config.nodes_.size() > 0) && "No communication endpoints_ specified!");
    LOG_DEBUG(
        logger_,
        "Node " << config.selfId_ << ", listen IP: " << config.listenHost_ << ", listen port: " << config.listenPort_);
    for (auto next = config.nodes_.begin(); next != config.nodes_.end(); next++) {
      auto key = create_key(next->second.host, next->second.port);
      addr2nodes_[key] = next->first;
      LOG_DEBUG(logger_, "Node " << config.selfId_ << ", got peer: " << key);
      if (statusCallback_ && next->second.isReplica) {
        PeerConnectivityStatus pcs{};
        pcs.peerId = next->first;
        pcs.peerHost = next->second.host;
        pcs.peerPort = next->second.port;
        pcs.statusType = StatusType::Started;
        statusCallback_(pcs);
      }
      Addr ad;
      memset((char *)&ad, 0, sizeof(ad));
      ad.sin_family = AF_INET;
      ad.sin_addr.s_addr = inet_addr(next->second.host.c_str());
      ad.sin_port = htons(next->second.port);
      nodes2addresses_.insert({next->first, ad});
    }
    LOG_DEBUG(logger_, "Starting UDP communication. Port = %" << udpListenPort_);
    LOG_DEBUG(logger_, "#endpoints = " << nodes2addresses_.size());
    udpSockFd_ = 0;
  }

  int getMaxMessageSize() { return maxMsgSize_; }

  int Start() {
    int error = 0;
    Addr sAddr;

    if (!receiverRef_) {
      LOG_DEBUG(logger_, "Cannot Start(): Receiver not set");
      return -1;
    }

    std::lock_guard<std::mutex> guard(runningLock_);
    if (running_) {
      LOG_DEBUG(logger_, "Cannot Start(): already running!");
      return -1;
    }

    bufferForIncomingMessages_ = (char *)std::malloc(maxMsgSize_);

    // Initialize socket.
    udpSockFd_ = socket(AF_INET, SOCK_DGRAM, 0);
    if (udpSockFd_ < 0) {
      LOG_FATAL(logger_, "Failed to initialize socket, error: " << strerror(errno));
      std::terminate();
    }

    // Name the socket.
    sAddr.sin_family = AF_INET;
    sAddr.sin_addr.s_addr = htonl(INADDR_ANY);
    sAddr.sin_port = htons(udpListenPort_);

    // Bind the socket.
    error = ::bind(udpSockFd_, (struct sockaddr *)&sAddr, sizeof(Addr));
    if (error < 0) {
      LOG_FATAL(logger_,
                "Error while binding: IP=" << sAddr.sin_addr.s_addr << ", Port=" << sAddr.sin_port
                                           << ", errno=" << concordUtils::errnoString(errno));
      ConcordAssert(false && "Failure occurred while binding the socket!");
      std::terminate();
    }
    running_ = true;
    startRecvThread();
    return 0;
  }

  int Stop() {
    std::lock_guard<std::mutex> guard(runningLock_);
    if (!running_) {
      return -1;
    }
    running_ = false;
    // Stopping the receiving thread happens as the last step because it relies on the 'running' flag.
    stopRecvThread();
    std::free(bufferForIncomingMessages_);
    bufferForIncomingMessages_ = nullptr;
    return 0;
  }

  bool isRunning() const { return running_; }

  void setReceiver(NodeNum &receiverNum, IReceiver *pRcv) { receiverRef_ = pRcv; }

  ConnectionStatus getCurrentConnectionStatus(const NodeNum &node) {
    if (isRunning()) return ConnectionStatus::Connected;
    return ConnectionStatus::Disconnected;
  }

  int sendAsyncMessage(const NodeNum &destNode, std::shared_ptr<std::vector<uint8_t>> msg) {
    ssize_t error = 0;
    if (msg->size() > MAX_UDP_PAYLOAD_SIZE) {
      LOG_ERROR(logger_, "Error, exceeded UDP payload size limit, message length: " << std::to_string(msg->size()));
      return -1;
    }

    if (!running_) throw std::runtime_error("The communication layer is not running!");
    const Addr *to = &nodes2addresses_[destNode];
    ConcordAssert((to != NULL) && "The destination endpoint does not exist!");
    ConcordAssert((msg->size() > 0) && "The message length must be positive!");
    LOG_DEBUG(logger_,
              " Sending " << msg->size() << " bytes to " << destNode << " (" << inet_ntoa(to->sin_addr) << ":"
                          << ntohs(to->sin_port));
    error = sendto(udpSockFd_, msg->data(), msg->size(), 0, (struct sockaddr *)to, sizeof(Addr));
    if (error < 0) {
      // -1 return value means underlying socket error.
      LOG_INFO(logger_, "Error while sending: " << concordUtils::errnoString(errno));
    } else if (error < (ssize_t)msg->size()) {
      // Message was partially sent. Unclear why this would happen, perhaps due to oversized messages (?).
      LOG_INFO(logger_, "Sent %d out of %d bytes!");
    }

    if (error == (ssize_t)msg->size()) {
      if (statusCallback_) {
        PeerConnectivityStatus pcs{};
        pcs.peerId = selfId_;
        pcs.statusType = StatusType::MessageSent;
        // pcs.statusTime = we don't set it since it is set by the aggregator
        // in the upcoming version timestamps should be reviewed
        statusCallback_(pcs);
      }
    }
    return 0;
  }

  void startRecvThread() {
    LOG_DEBUG(logger_, "Starting the receiving thread..");
    recvThreadRef_.reset(new std::thread(std::bind(&PlainUdpImpl::recvThreadRoutine, this)));
  }

  NodeAddressResolveResult addrToNodeId(Addr netAddress) {
    auto key = create_key(netAddress);
    auto res = addr2nodes_.find(key);
    if (res == addr2nodes_.end()) {
      // IG: if we don't know the sender we just ignore this message and
      // continue.
      LOG_WARN(logger_, "Unknown sender, address: " << key);
      return NodeAddressResolveResult({0, false, key});
    }
    LOG_DEBUG(logger_, "Sender resolved, ID: " << res->second << " address: " << key);
    return NodeAddressResolveResult({res->second, true, key});
  }

  void stopRecvThread() { recvThreadRef_->join(); }

  void recvThreadRoutine() {
    ConcordAssert((udpSockFd_ != 0) && "Unable to start receiving: socket not define!");
    ConcordAssert((receiverRef_ != 0) && "Unable to start receiving: receiver not defined!");

    // The main receive loop.
    Addr fromAddress;
    socklen_t fromAddressLength = sizeof(fromAddress);
    int mLen = 0;
    int timeout = 5000;  // In milliseconds.
    int iRes = 0;

    pollfd fds;  // Handle only one file descriptor.
    fds.fd = udpSockFd_;
    fds.events = POLLIN;  // Register for data read.

    do {
      mLen = 0;
      iRes = poll(&fds, 1, timeout);
      if (0 < iRes) {  // Event(s) reported.
        mLen = recvfrom(
            udpSockFd_, bufferForIncomingMessages_, maxMsgSize_, 0, (sockaddr *)&fromAddress, &fromAddressLength);
      } else if (0 > iRes) {  // Error.
        LOG_ERROR(logger_, "Poll failed. " << std::strerror(errno));
        continue;
      } else {  // Timeout
        LOG_DEBUG(logger_, "Poll timeout occurred. timeout = " << timeout << " milli seconds.");
        continue;
      }
      if (!running_) break;
      LOG_DEBUG(logger_, "Node " << selfId_ << ": recvfrom returned " << mLen << " bytes");

      if (mLen < 0) {
        LOG_DEBUG(logger_, "Node " << selfId_ << ": Error in recvfrom(): " << mLen);
        continue;
      } else if (!mLen) {
        // Probably, Stop() set 'running' to false and shut down the socket. Or, maybe, we received an actual
        // zero-length UDP datagram, but we never send those.)
        LOG_DEBUG(logger_, "Node " << selfId_ << ": Received empty message (shutting down?)");
        continue;
      }

      auto resolveNode = addrToNodeId(fromAddress);
      if (!resolveNode.wasFound) {
        LOG_DEBUG(logger_, "Sender not found, address: " << resolveNode.key);
        continue;
      }

      auto sendingNode = resolveNode.nodeId;
      if (receiverRef_ != NULL) {
        LOG_DEBUG(logger_, "Node " << selfId_ << ": Calling onNewMessage, msg from: " << sendingNode);
        receiverRef_->onNewMessage(sendingNode, bufferForIncomingMessages_, mLen);
      } else {
        LOG_ERROR(logger_, "Node " << selfId_ << ": receiver is NULL");
      }

      bool isReplica = check_replica(sendingNode);
      if (statusCallback_ && isReplica) {
        PeerConnectivityStatus pcs{};
        pcs.peerId = sendingNode;

        char str[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &(fromAddress.sin_addr), str, INET_ADDRSTRLEN);
        pcs.peerHost = string(str);

        pcs.peerPort = ntohs(fromAddress.sin_port);
        pcs.statusType = StatusType::MessageReceived;

        // pcs.statusTime = we dont set it since it is set by the aggregator
        // in the upcoming version timestamps should be reviewed
        statusCallback_(pcs);
      }
    } while (running_);

    // Since neither WinSock nor Linux sockets manual doesn't specify explicitly that there is no need to call shutdown
    // on UDP socket,we call it to be sure that the send/receive is disable when we call close(). It is safe since when
    // shutdown() is called on UDP socket, it should (in the worst case) to return ENOTCONN that just should be ignored.
    shutdown(udpSockFd_, SHUT_RDWR);
    close(udpSockFd_);
    udpSockFd_ = 0;
  }

 private:
  bool check_replica(NodeNum node) {
    auto it = endpoints_.find(node);
    if (it == endpoints_.end()) {
      return false;
    }
    return it->second.isReplica;
  }

  string create_key(const string &ip, uint16_t port) {
    auto key = ip + ":" + to_string(port);
    return key;
  }

  string create_key(Addr a) { return create_key(inet_ntoa(a.sin_addr), ntohs(a.sin_port)); }

 private:
  // reversed map for getting node id by ip
  unordered_map<std::string, NodeNum> addr2nodes_;

  // pre defined address map
  unordered_map<NodeNum, Addr> nodes2addresses_;

  size_t maxMsgSize_;

  // The underlying socket we use to send & receive.
  int32_t udpSockFd_;

  // Reference to the receiving thread.
  std::unique_ptr<std::thread> recvThreadRef_;

  // The port we're listening on for incoming datagrams.
  uint16_t udpListenPort_;

  // The list of all nodes we're communicating with.
  std::unordered_map<NodeNum, NodeInfo> endpoints_;

  // Prevent multiple Start() invocations, i.e., multiple recvThread.
  std::mutex runningLock_;

  // Reference to an IReceiver where we dispatch any received messages.
  IReceiver *receiverRef_ = nullptr;

  char *bufferForIncomingMessages_ = nullptr;

  UPDATE_CONNECTIVITY_FN statusCallback_ = nullptr;

  NodeNum selfId_;

  // Max UDP packet bytes can be sent, without headers.
  static constexpr uint16_t MAX_UDP_PAYLOAD_SIZE = 65535 - 20 - 8;

  // Flag to indicate whether the current communication layer still runs.
  std::atomic<bool> running_{false};

  logging::Logger logger_ = logging::getLogger("plain-udp");
};

PlainUDPCommunication::~PlainUDPCommunication() {
  if (ptrImpl_) delete ptrImpl_;
}

PlainUDPCommunication::PlainUDPCommunication(const PlainUdpConfig &config) { ptrImpl_ = new PlainUdpImpl(config); }

PlainUDPCommunication *PlainUDPCommunication::create(const PlainUdpConfig &config) {
  return new PlainUDPCommunication(config);
}

int PlainUDPCommunication::getMaxMessageSize() { return ptrImpl_->getMaxMessageSize(); }

int PlainUDPCommunication::start() { return ptrImpl_->Start(); }

int PlainUDPCommunication::stop() {
  if (!ptrImpl_) return 0;

  auto res = ptrImpl_->Stop();
  return res;
}

bool PlainUDPCommunication::isRunning() const { return ptrImpl_->isRunning(); }

ConnectionStatus PlainUDPCommunication::getCurrentConnectionStatus(NodeNum node) {
  return ptrImpl_->getCurrentConnectionStatus(node);
}

int PlainUDPCommunication::send(NodeNum destNode, std::vector<uint8_t> &&msg, NodeNum endpointNum) {
  auto m = std::make_shared<std::vector<uint8_t>>(std::move(msg));
  return ptrImpl_->sendAsyncMessage(destNode, m);
}

std::set<NodeNum> PlainUDPCommunication::send(std::set<NodeNum> dests,
                                              std::vector<uint8_t> &&msg,
                                              NodeNum endpointNum) {
  std::set<NodeNum> failed_nodes;
  auto m = std::make_shared<std::vector<uint8_t>>(std::move(msg));
  for (auto &d : dests) {
    if (ptrImpl_->sendAsyncMessage(d, m) != 0) {
      failed_nodes.insert(d);
    }
  }
  return failed_nodes;
}

void PlainUDPCommunication::setReceiver(NodeNum receiverNum, IReceiver *receiver) {
  ptrImpl_->setReceiver(receiverNum, receiver);
}

}  // namespace bft::communication
