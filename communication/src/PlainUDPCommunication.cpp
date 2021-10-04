// Concord
//
// Copyright (c) 2018-2020 VMware, Inc. All Rights Reserved.
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
 private:
  /* reversed map for getting node id by ip */
  unordered_map<std::string, NodeNum> addr2nodes;

  /* pre defined address map */
  unordered_map<NodeNum, Addr> nodes2addresses;

  size_t maxMsgSize;

  /** The underlying socket we use to send & receive. */
  int32_t udpSockFd;

  /** Reference to the receiving thread. */
  std::unique_ptr<std::thread> recvThreadRef;

  /* The port we're listening on for incoming datagrams. */
  uint16_t udpListenPort;

  /* The list of all nodes we're communicating with. */
  std::unordered_map<NodeNum, NodeInfo> endpoints;

  /** Prevent multiple Start() invocations, i.e., multiple recvThread. */
  std::mutex runningLock;

  /** Reference to an IReceiver where we dispatch any received messages. */
  IReceiver *receiverRef = nullptr;

  char *bufferForIncomingMessages = nullptr;

  UPDATE_CONNECTIVITY_FN statusCallback = nullptr;

  NodeNum selfId;

  // Max UDP packet bytes can be sent, without headers.
  static constexpr uint16_t MAX_UDP_PAYLOAD_SIZE = 65535 - 20 - 8;

  /** Flag to indicate whether the current communication layer still runs. */
  std::atomic<bool> running{false};

  logging::Logger _logger = logging::getLogger("plain-udp");

  bool check_replica(NodeNum node) {
    auto it = endpoints.find(node);
    if (it == endpoints.end()) {
      return false;
    }

    return it->second.isReplica;
  }

  string create_key(const string &ip, uint16_t port) {
    auto key = ip + ":" + to_string(port);
    return key;
  }

  string create_key(Addr a) { return create_key(inet_ntoa(a.sin_addr), ntohs(a.sin_port)); }

 public:
  /**
   * Initializes a new UDPCommunication layer that will listen on the given
   * listenPort.
   */
  PlainUdpImpl(const PlainUdpConfig &config)
      : maxMsgSize{config.bufferLength},
        udpListenPort{config.listenPort},
        endpoints{config.nodes},
        statusCallback{config.statusCallback},
        selfId{config.selfId} {
    ConcordAssert((config.listenPort > 0) && "Port should not be negative!");
    ConcordAssert((config.nodes.size() > 0) && "No communication endpoints specified!");

    LOG_DEBUG(
        _logger,
        "Node " << config.selfId << ", listen IP: " << config.listenHost << ", listen port: " << config.listenPort);

    for (auto next = config.nodes.begin(); next != config.nodes.end(); next++) {
      auto key = create_key(next->second.host, next->second.port);
      addr2nodes[key] = next->first;

      LOG_DEBUG(_logger, "Node " << config.selfId << ", got peer: " << key);

      if (statusCallback && next->second.isReplica) {
        PeerConnectivityStatus pcs{};
        pcs.peerId = next->first;
        pcs.peerHost = next->second.host;
        pcs.peerPort = next->second.port;
        pcs.statusType = StatusType::Started;
        statusCallback(pcs);
      }

      Addr ad;
      memset((char *)&ad, 0, sizeof(ad));
      ad.sin_family = AF_INET;
      ad.sin_addr.s_addr = inet_addr(next->second.host.c_str());
      ad.sin_port = htons(next->second.port);
      nodes2addresses.insert({next->first, ad});
    }

    LOG_DEBUG(_logger, "Starting UDP communication. Port = %" << udpListenPort);
    LOG_DEBUG(_logger, "#endpoints = " << nodes2addresses.size());

    udpSockFd = 0;
  }

  int getMaxMessageSize() { return maxMsgSize; }

  int Start() {
    int error = 0;
    Addr sAddr;

    if (!receiverRef) {
      LOG_DEBUG(_logger, "Cannot Start(): Receiver not set");
      return -1;
    }

    std::lock_guard<std::mutex> guard(runningLock);

    if (running) {
      LOG_DEBUG(_logger, "Cannot Start(): already running!");
      return -1;
    }

    bufferForIncomingMessages = (char *)std::malloc(maxMsgSize);

    // Initialize socket.
    udpSockFd = socket(AF_INET, SOCK_DGRAM, 0);

    if (udpSockFd < 0) {
      LOG_FATAL(_logger, "Failed to initialize socket, error: " << strerror(errno));
      std::terminate();
    }

    // Name the socket.
    sAddr.sin_family = AF_INET;
    sAddr.sin_addr.s_addr = htonl(INADDR_ANY);
    sAddr.sin_port = htons(udpListenPort);

    // Bind the socket.
    error = ::bind(udpSockFd, (struct sockaddr *)&sAddr, sizeof(Addr));
    if (error < 0) {
      LOG_FATAL(_logger,
                "Error while binding: IP=" << sAddr.sin_addr.s_addr << ", Port=" << sAddr.sin_port
                                           << ", errno=" << concordUtils::errnoString(errno));
      ConcordAssert(false && "Failure occurred while binding the socket!");
      std::terminate();
    }

#ifdef WIN32
    {
      BOOL tmpBuf = FALSE;
      DWORD bytesReturned = 0;
      WSAIoctl(udpSockFd, _WSAIOW(IOC_VENDOR, 12), &tmpBuf, sizeof(tmpBuf), NULL, 0, &bytesReturned, NULL, NULL);
    }
#endif

    running = true;
    startRecvThread();

    return 0;
  }

  int Stop() {
    std::lock_guard<std::mutex> guard(runningLock);
    if (!running) {
      return -1;
    }

    running = false;

    /** Stopping the receiving thread happens as the last step because it
     * relies on the 'running' flag. */
    stopRecvThread();

    std::free(bufferForIncomingMessages);
    bufferForIncomingMessages = nullptr;

    return 0;
  }

  bool isRunning() const { return running; }

  void setReceiver(NodeNum &receiverNum, IReceiver *pRcv) { receiverRef = pRcv; }

  ConnectionStatus getCurrentConnectionStatus(const NodeNum &node) {
    if (isRunning()) return ConnectionStatus::Connected;
    return ConnectionStatus::Disconnected;
  }

  int sendAsyncMessage(const NodeNum &destNode, std::shared_ptr<std::vector<uint8_t>> msg) {
    ssize_t error = 0;

    if (msg->size() > MAX_UDP_PAYLOAD_SIZE) {
      LOG_ERROR(_logger, "Error, exceeded UDP payload size limit, message length: " << std::to_string(msg->size()));
      return -1;
    }

    if (!running) throw std::runtime_error("The communication layer is not running!");

    const Addr *to = &nodes2addresses[destNode];

    ConcordAssert((to != NULL) && "The destination endpoint does not exist!");
    ConcordAssert((msg->size() > 0) && "The message length must be positive!");

    LOG_DEBUG(_logger,
              " Sending " << msg->size() << " bytes to " << destNode << " (" << inet_ntoa(to->sin_addr) << ":"
                          << ntohs(to->sin_port));

    error = sendto(udpSockFd, msg->data(), msg->size(), 0, (struct sockaddr *)to, sizeof(Addr));

    if (error < 0) {
      /** -1 return value means underlying socket error. */
      LOG_INFO(_logger, "Error while sending: " << concordUtils::errnoString(errno));
    } else if (error < (ssize_t)msg->size()) {
      /** Mesage was partially sent. Unclear why this would happen, perhaps
       * due to oversized messages (?). */
      LOG_INFO(_logger, "Sent %d out of %d bytes!");
    }

    if (error == (ssize_t)msg->size()) {
      if (statusCallback) {
        PeerConnectivityStatus pcs{};
        pcs.peerId = selfId;
        pcs.statusType = StatusType::MessageSent;

        // pcs.statusTime = we dont set it since it is set by the aggregator
        // in the upcoming version timestamps should be reviewed
        statusCallback(pcs);
      }
    }

    return 0;
  }

  void startRecvThread() {
    LOG_DEBUG(_logger, "Starting the receiving thread..");
    recvThreadRef.reset(new std::thread(std::bind(&PlainUdpImpl::recvThreadRoutine, this)));
  }

  NodeAddressResolveResult addrToNodeId(Addr netAddress) {
    auto key = create_key(netAddress);
    auto res = addr2nodes.find(key);
    if (res == addr2nodes.end()) {
      // IG: if we don't know the sender we just ignore this message and
      // continue.
      LOG_ERROR(_logger, "Unknown sender, address: " << key);
      return NodeAddressResolveResult({0, false, key});
    }

    LOG_DEBUG(_logger, "Sender resolved, ID: " << res->second << " address: " << key);
    return NodeAddressResolveResult({res->second, true, key});
  }

  void stopRecvThread() {
    //    LOG_ERROR(_logger,"Stopping the receiving thread..");
    recvThreadRef->join();
    //    LOG_ERROR(_logger,"Stopping the receiving thread..");
  }

  void recvThreadRoutine() {
    ConcordAssert((udpSockFd != 0) && "Unable to start receiving: socket not define!");
    ConcordAssert((receiverRef != 0) && "Unable to start receiving: receiver not defined!");

    /** The main receive loop. */
    Addr fromAddress;
#ifdef _WIN32
    int fromAddressLength = sizeof(fromAddress);
#else
    socklen_t fromAddressLength = sizeof(fromAddress);
#endif
    int mLen = 0;
    int timeout = 5000;  // In milliseconds.
    int iRes = 0;

    pollfd fds;  // Handle only one file descriptor.
    fds.fd = udpSockFd;
    fds.events = POLLIN;  // Register for data read.

    do {
      mLen = 0;
      iRes = poll(&fds, 1, timeout);
      if (0 < iRes) {  // Event(s) reported.
        mLen =
            recvfrom(udpSockFd, bufferForIncomingMessages, maxMsgSize, 0, (sockaddr *)&fromAddress, &fromAddressLength);
      } else if (0 > iRes) {  // Error.
        LOG_ERROR(_logger, "Poll failed. " << std::strerror(errno));
        continue;
      } else {  // Timeout
        LOG_DEBUG(_logger, "Poll timeout occurred. timeout = " << timeout << " milli seconds.");
        continue;
      }

      if (!running) break;
      LOG_DEBUG(_logger, "Node " << selfId << ": recvfrom returned " << mLen << " bytes");

      if (mLen < 0) {
        LOG_DEBUG(_logger, "Node " << selfId << ": Error in recvfrom(): " << mLen);
        continue;
      } else if (!mLen) {
        // Probably, Stop() set 'running' to false and shut down the
        // socket.  (Or, maybe, we received an actual zero-length UDP
        // datagram, but we never send those.)
        LOG_DEBUG(_logger, "Node " << selfId << ": Received empty message (shutting down?)");
        continue;
      }

      auto resolveNode = addrToNodeId(fromAddress);
      if (!resolveNode.wasFound) {
        LOG_DEBUG(_logger, "Sender not found, address: " << resolveNode.key);
        continue;
      }

      auto sendingNode = resolveNode.nodeId;
      if (receiverRef != NULL) {
        LOG_DEBUG(_logger, "Node " << selfId << ": Calling onNewMessage, msg from: " << sendingNode);
        receiverRef->onNewMessage(sendingNode, bufferForIncomingMessages, mLen);
      } else {
        LOG_ERROR(_logger, "Node " << selfId << ": receiver is NULL");
      }

      bool isReplica = check_replica(sendingNode);
      if (statusCallback && isReplica) {
        PeerConnectivityStatus pcs{};
        pcs.peerId = sendingNode;

        char str[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &(fromAddress.sin_addr), str, INET_ADDRSTRLEN);
        pcs.peerHost = string(str);

        pcs.peerPort = ntohs(fromAddress.sin_port);
        pcs.statusType = StatusType::MessageReceived;

        // pcs.statusTime = we dont set it since it is set by the aggregator
        // in the upcoming version timestamps should be reviewed
        statusCallback(pcs);
      }

    } while (running);

    // since neither WinSock nor Linux sockets manual
    // doesnt specify explicitly that there is no need to call shutdown on
    // UDP socket,we call it to be sure that the send/receive is disable when
    // we call close(). its safe since when shutdown() is called on UDP
    // socket, it should (in the worst case) to return ENOTCONN that just
    // should be ignored.
#ifdef _WIN32
    shutdown(udpSockFd, SD_BOTH);
#else
    shutdown(udpSockFd, SHUT_RDWR);
#endif

    close(udpSockFd);
    udpSockFd = 0;
  }
};

PlainUDPCommunication::~PlainUDPCommunication() {
  if (_ptrImpl) delete _ptrImpl;
}

PlainUDPCommunication::PlainUDPCommunication(const PlainUdpConfig &config) { _ptrImpl = new PlainUdpImpl(config); }

PlainUDPCommunication *PlainUDPCommunication::create(const PlainUdpConfig &config) {
  return new PlainUDPCommunication(config);
}

int PlainUDPCommunication::getMaxMessageSize() { return _ptrImpl->getMaxMessageSize(); }

int PlainUDPCommunication::start() { return _ptrImpl->Start(); }

int PlainUDPCommunication::stop() {
  if (!_ptrImpl) return 0;

  auto res = _ptrImpl->Stop();
  return res;
}

bool PlainUDPCommunication::isRunning() const { return _ptrImpl->isRunning(); }

ConnectionStatus PlainUDPCommunication::getCurrentConnectionStatus(NodeNum node) {
  return _ptrImpl->getCurrentConnectionStatus(node);
}

int PlainUDPCommunication::send(NodeNum destNode, std::vector<uint8_t> &&msg) {
  auto m = std::make_shared<std::vector<uint8_t>>(std::move(msg));
  return _ptrImpl->sendAsyncMessage(destNode, m);
}

std::set<NodeNum> PlainUDPCommunication::send(std::set<NodeNum> dests, std::vector<uint8_t> &&msg) {
  std::set<NodeNum> failed_nodes;
  auto m = std::make_shared<std::vector<uint8_t>>(std::move(msg));
  for (auto &d : dests) {
    if (_ptrImpl->sendAsyncMessage(d, m) != 0) {
      failed_nodes.insert(d);
    }
  }
  return failed_nodes;
}

void PlainUDPCommunication::setReceiver(NodeNum receiverNum, IReceiver *receiver) {
  _ptrImpl->setReceiver(receiverNum, receiver);
}

}  // namespace bft::communication
