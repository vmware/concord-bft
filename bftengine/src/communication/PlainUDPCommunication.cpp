// Concord
//
// Copyright (c) 2018 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the
// LICENSE file.

#include <iostream>
#include <stddef.h>
#include <stdio.h>
#include <cassert>
#include <sstream>
#include <cstring>
#include <unordered_map>
#include "CommDefs.hpp"
#include "Logger.hpp"
#include <atomic>
#include <mutex>
#include <thread>
#include <functional>

#define Assert(cond, txtMsg) assert(cond && (txtMsg))

#if defined(_WIN32)
#define CLOSESOCKET(x) closesocket((x));
#else
#define CLOSESOCKET(x) close((x));
#endif

#if defined(_WIN32)
#pragma warning(disable:4996) // TODO(GG+SG): should be removed!! (see also _CRT_SECURE_NO_WARNINGS)
#endif

using namespace std;
using namespace bftEngine;

struct NodeAddressResolveResult
{
  NodeNum nodeId;
  bool wasFound;
  string key;
};

class PlainUDPCommunication::PlainUdpImpl {
 private:
  /* reversed map for getting node id by ip */
  unordered_map<std::string, NodeNum> addr2nodes;

  /* pre defined adress map */
  unordered_map<NodeNum, Addr> nodes2adresses;

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

  /** Flag to indicate whether the current communication layer still runs. */
  std::atomic<bool> running;

  concordlogger::Logger _logger = concordlogger::Log::getLogger("plain-udp");

  bool check_replica(NodeNum node)
  {
    auto it = endpoints.find(node);
    if (it == endpoints.end()) {
      return false;
    }

    return it->second.isReplica;
  }

  string
  create_key(string ip, uint16_t port) {
    auto key = ip + ":" + to_string(port);
    return key;
  }

  string
  create_key(Addr a) {
    return create_key(inet_ntoa(a.sin_addr), ntohs(a.sin_port));
  }

 public:
  /**
  * Initializes a new UDPCommunication layer that will listen on the given
  * listenPort.
  */
  PlainUdpImpl(const PlainUdpConfig &config)
      : maxMsgSize{config.bufferLength},
        udpListenPort{config.listenPort},
        endpoints{std::move(config.nodes)},
        statusCallback{config.statusCallback},
        selfId{config.selfId},
        running{false} {
    Assert(config.listenPort > 0, "Port should not be negative!");
    Assert(config.nodes.size() > 0, "No communication endpoints specified!");

    LOG_DEBUG(_logger, "Node " << config.selfId <<
        ", listen IP: " << config.listenIp <<
        ", listen port: " << config.listenPort);

    for (auto next = config.nodes.begin();
         next != config.nodes.end();
         next++) {
      auto key = create_key(next->second.ip, next->second.port);
      addr2nodes[key] = next->first;

      LOG_DEBUG(_logger, "Node " << config.selfId <<
          ", got peer: " << key);

      if (statusCallback && next->second.isReplica) {
        PeerConnectivityStatus pcs{};
        pcs.peerId = next->first;
        pcs.peerIp = next->second.ip;
        pcs.peerPort = next->second.port;
        pcs.statusType = StatusType::Started;
        statusCallback(pcs);
      }

      Addr ad;
      memset((char *) &ad, 0, sizeof(ad));
      ad.sin_family = AF_INET;
      ad.sin_addr.s_addr = inet_addr(next->second.ip.c_str());
      ad.sin_port = htons(next->second.port);
      nodes2adresses.insert({next->first, ad});
    }

    LOG_DEBUG(_logger, "Starting UDP communication. Port = %" << udpListenPort);
    LOG_DEBUG(_logger, "#endpoints = " << nodes2adresses.size());

    udpSockFd = 0;
  }

  int
  getMaxMessageSize() {
    return maxMsgSize;
  }

  int
  Start() {
    int error = 0;
    Addr sAddr;

    if (!receiverRef) {
      LOG_DEBUG(_logger, "Cannot Start(): Receiver not set");
      return -1;
    }

    std::lock_guard<std::mutex> guard(runningLock);

    if (running == true) {
      LOG_DEBUG(_logger, "Cannot Start(): already running!");
      return -1;
    }

    bufferForIncomingMessages = (char *) std::malloc(maxMsgSize);

    // Initialize socket.
    udpSockFd = socket(AF_INET, SOCK_DGRAM, 0);

    /*
    uint64_t buf_size = 40*1024*1024;  //20 MB
    setsockopt(udpSockFd, SOL_SOCKET, SO_RCVBUF, &buf_size, sizeof(buf_size));
    setsockopt(udpSockFd, SOL_SOCKET, SO_SNDBUF, &buf_size, sizeof(buf_size));
    */

    // Name the socket.
    sAddr.sin_family = AF_INET;
    sAddr.sin_addr.s_addr = htonl(INADDR_ANY);
    sAddr.sin_port = htons(udpListenPort);

    // Bind the socket.
    error = ::bind(udpSockFd, (struct sockaddr *) &sAddr, sizeof(Addr));
    if (error < 0) {
      LOG_FATAL(_logger, "Error while binding: IP=" << sAddr.sin_addr.s_addr <<
                         ", Port=" << sAddr.sin_port <<
                         ", errno="<< strerror(errno));
      Assert(false, "Failure occurred while binding the socket!");
      exit(1); // TODO(GG): not really ..... change this !
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

  int
  Stop() {
	  std::lock_guard<std::mutex> guard(runningLock);
    if (running == false) {
      LOG_DEBUG(_logger, "Cannot Stop(): not running!");
      return -1;
    }

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
    CLOSESOCKET(udpSockFd);

    running = false;

    /** Stopping the receiving thread happens as the last step because it
      * relies on the 'running' flag. */
    stopRecvThread();

    std::free(bufferForIncomingMessages);
    udpSockFd = 0;

    return 0;
  }

  bool
  isRunning() const {
    return running;
  }

  void
  setReceiver(NodeNum &receiverNum, IReceiver *pRcv) {
    receiverRef = pRcv;
  }

  ConnectionStatus
  getCurrentConnectionStatus(const NodeNum &node) const {
    return ConnectionStatus::Unknown;
  }

  int
  sendAsyncMessage(const NodeNum &destNode,
                   const char *const message,
                   const size_t &messageLength) {
    int error = 0;

    Assert(running == true, "The communication layer is not running!");

    const Addr *to = &nodes2adresses[destNode];

    Assert(to != NULL, "The destination endpoint does not exist!");
    Assert(messageLength > 0, "The message length must be positive!");
    Assert(message != NULL, "No message provided!");


    error = sendto(udpSockFd, message, messageLength, 0,
                   (struct sockaddr *) to, sizeof(Addr));
    
    LOG_DEBUG(_logger, "Sending " << messageLength
                        << " bytes to "
                        << destNode << " (" << inet_ntoa(to->sin_addr) << ":"
                        << ntohs(to->sin_port) << ") error:" << error);
    if (error < 0) {
      /** -1 return value means underlying socket error. */
      string err = strerror(errno);
      LOG_DEBUG(_logger, "Error while sending: " << strerror(errno));
      Assert(false, "Failure occurred while sending!");
      /** Fail-fast. */
    } else if (error < (int) messageLength) {
      /** Mesage was partially sent. Unclear why this would happen, perhaps
        * due to oversized messages (?). */
      LOG_DEBUG(_logger, "Sent %d out of %d bytes!");
      Assert(false, "Send error occurred!");    /** Fail-fast. */
    }

    if (error == (ssize_t) messageLength) {
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

  void
  startRecvThread() {
    LOG_DEBUG(_logger, "Starting the receiving thread..");
    recvThreadRef.reset(new std::thread(std::bind(&PlainUdpImpl::recvThreadRoutine, this)));
  }

  NodeAddressResolveResult
  addrToNodeId(Addr netAdress) {
    auto key = create_key(netAdress);
    auto res = addr2nodes.find(key);
    if (res == addr2nodes.end()) {
      // IG: if we don't know the sender we just ignore this message and
      // continue.
      LOG_ERROR(_logger, "Unknown sender, address: " << key);
      return NodeAddressResolveResult({0, false, key});
    }

    LOG_DEBUG(_logger, "Sender resolved, ID: " << res->second << "address: " << key);
    return NodeAddressResolveResult({res->second, true, key});
  }

  void
  stopRecvThread() {
    //LOG_DEBUG("Stopping the receiving thread..");
    recvThreadRef->join();
    //LOG_DEBUG("Stopping the receiving thread..");
  }

  void
  recvThreadRoutine() {
    Assert(udpSockFd != 0,
           "Unable to start receiving: socket not define!");
    Assert(receiverRef != 0,
           "Unable to start receiving: receiver not defined!");

    /** The main receive loop. */
    Addr fromAdress;
#ifdef _WIN32
    int fromAdressLength = sizeof(fromAdress);
#else
    socklen_t fromAdressLength = sizeof(fromAdress);
#endif
    int mLen = 0;
    do {
      mLen = recvfrom(udpSockFd,
                      bufferForIncomingMessages,
                      maxMsgSize,
                      0,
                      (sockaddr *) &fromAdress,
                      &fromAdressLength);

      LOG_DEBUG(_logger, "recvfrom returned " << mLen << " bytes");
     
      if (mLen < 0) {
        LOG_DEBUG(_logger, "Error in recvfrom(): " << mLen);
        continue;
      } else if (!mLen) {
        // Probably, Stop() set 'running' to false and shut down the
        // socket.  (Or, maybe, we received an actual zero-length UDP
        // datagram, but we never send those.)
        LOG_DEBUG(_logger, "Received empty message (shutting down?)");
        continue;
      }

      auto resolveNode = addrToNodeId(fromAdress);
      if(!resolveNode.wasFound) {
        LOG_DEBUG(_logger, "Sender not found, adress: " << resolveNode.key);
        continue;
      }

      auto sendingNode = resolveNode.nodeId;
      if (receiverRef != NULL) {
        LOG_DEBUG(_logger, "Calling onNewMessage, msg from: " << sendingNode);
        receiverRef->onNewMessage(sendingNode,
                                  bufferForIncomingMessages,
                                  mLen);
      } else {
        LOG_ERROR(_logger, "receiver is NULL");
      }

      bool isReplica = check_replica(sendingNode);
      if (statusCallback && isReplica) {
        PeerConnectivityStatus pcs{};
        pcs.peerId = sendingNode;

        char str[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &(fromAdress.sin_addr), str, INET_ADDRSTRLEN);
        pcs.peerIp = string(str);

        pcs.peerPort = ntohs(fromAdress.sin_port);
        pcs.statusType = StatusType::MessageReceived;

        // pcs.statusTime = we dont set it since it is set by the aggregator
        // in the upcoming version timestamps should be reviewed
        statusCallback(pcs);
      }

    } while (running);
  }
};

PlainUDPCommunication::~PlainUDPCommunication() {
  if(_ptrImpl)
    delete _ptrImpl;
}

PlainUDPCommunication::PlainUDPCommunication(const PlainUdpConfig &config) {
  _ptrImpl = new PlainUdpImpl(config);
}

PlainUDPCommunication *PlainUDPCommunication::create(const PlainUdpConfig &config) {
  return new PlainUDPCommunication(config);
}

int PlainUDPCommunication::getMaxMessageSize() {
  return _ptrImpl->getMaxMessageSize();
}

int PlainUDPCommunication::Start() {
  return _ptrImpl->Start();
}

int PlainUDPCommunication::Stop() {
  if (!_ptrImpl)
    return 0;

  auto res = _ptrImpl->Stop();
  return res;
}

bool PlainUDPCommunication::isRunning() const {
  return _ptrImpl->isRunning();
}

ConnectionStatus
PlainUDPCommunication::getCurrentConnectionStatus(const NodeNum node) const {
  return _ptrImpl->getCurrentConnectionStatus(node);
}

int
PlainUDPCommunication::sendAsyncMessage(const NodeNum destNode,
                                        const char *const message,
                                        const size_t messageLength) {
  return _ptrImpl->sendAsyncMessage(destNode, message, messageLength);
}

void
PlainUDPCommunication::setReceiver(NodeNum receiverNum, IReceiver *receiver) {
  _ptrImpl->setReceiver(receiverNum, receiver);
}
