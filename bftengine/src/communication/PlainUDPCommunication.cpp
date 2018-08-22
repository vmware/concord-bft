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
#include "CommImpl.hpp"
#include "Threading.h"

#define Assert(cond, txtMsg) assert(cond && (txtMsg))

#define LOG_DEBUG(txt) ;

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

enum UDPMessageType : int8_t
{
   DataMessage = 1,
   ServiceMessage
};

class PlainUDPCommunication::PlainUdpImpl
{
private:
   /* reversed map for getting node id by ip */
   unordered_map<std::string, NodeNum> addr2nodes;

   size_t maxMsgSize;

   /** The underlying socket we use to send & receive. */
   int32_t udpSockFd;

   /** Reference to the receiving thread. */
   Thread recvThreadRef;

   /* The port we're listening on for incoming datagrams. */
   uint16_t udpListenPort;

   /* The list of all nodes we're communicating with. */
   std::unordered_map<NodeNum, Addr> nodes2adresses;

   /** Flag to indicate whether the current communication layer still runs. */
   bool running;

   /** Prevent multiple Start() invocations, i.e., multiple recvThread. */
   Mutex runningLock;

   /** Reference to an IReceiver where we dispatch any received messages. */
   IReceiver *receiverRef = nullptr;

   char *bufferForIncomingMessages;

   string
   create_key(string ip, uint16_t port)
   {
      auto key = ip + ":" + to_string(port);
      return key;
   }

   string
   create_key(Addr a)
   {
      return create_key(inet_ntoa(a.sin_addr), ntohs(a.sin_port));
   }

public:
   /**
   * Initializes a new UDPCommunication layer that will listen on the given
   * listenPort.
   */
   PlainUdpImpl(const PlainUdpConfig &config)
      :  maxMsgSize {config.bufferLength},
         udpListenPort {config.listenPort}
   {
	  Assert(config.listenPort > 0, "Port should not be negative!");
	  Assert(config.nodes.size() > 0, "No communication endpoints "
                                         "specified!");
      addr2nodes = unordered_map<string, NodeNum>();
      for (auto next = config.nodes.begin();
           next != config.nodes.end();
           next++) {
         string ip = std::get<0>(next->second);
         auto port = std::get<1>(next->second);
         auto key = create_key(ip, port);
         addr2nodes[key] = next->first;

         Addr ad;
         memset((char*)&ad, 0, sizeof(ad));
         ad.sin_family = AF_INET;
         ad.sin_addr.s_addr = inet_addr(ip.c_str());
         ad.sin_port = htons(port);
         nodes2adresses.insert({next->first, ad});
      }

      LOG_DEBUG("Starting UDP communication. Port = %" << udpListenPort);
      LOG_DEBUG("#endpoints = " << nodes2adresses.size());

      bufferForIncomingMessages = (char*)std::malloc(maxMsgSize);

      udpSockFd = 0;
      running = false;
      init(&runningLock);
   }

   int
   getMaxMessageSize()
   {
      return maxMsgSize;
   }

   int
   Start()
   {
      int error = 0;
      Addr sAddr;

      if (!receiverRef) {
         LOG_DEBUG("Cannot Start(): Receiver not set");
         return -1;
      }

      mutexLock(&runningLock);

      if (running == true) {
         LOG_DEBUG("Cannot Start(): already running!");
         mutexUnlock(&runningLock);
         return -1;
      }

      // Initialize socket.
      udpSockFd = socket(AF_INET, SOCK_DGRAM, 0);

      // Name the socket.
      sAddr.sin_family = AF_INET;
      sAddr.sin_addr.s_addr = htonl(INADDR_ANY);
      sAddr.sin_port = htons(udpListenPort);

      // Bind the socket.
      error = ::bind(udpSockFd, (struct sockaddr *) &sAddr, sizeof(Addr));
      if (error < 0) {
         LOG_DEBUG("Error while binding: " << strerror(errno));
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

      startRecvThread();

      running = true;
      mutexUnlock(&runningLock);
      return 0;
   }

   int
   Stop()
   {
      mutexLock(&runningLock);
      if (running == false) {
         LOG_DEBUG("Cannot Stop(): not running!");
         mutexUnlock(&runningLock);
         return -1;
      }

#if defined(_WIN32)
	  shutdown(udpSockFd, SD_BOTH);
#else
	  shutdown(udpSockFd, SHUT_RDWR);
#endif
      running = false;
      mutexUnlock(&runningLock);

      /** Stopping the receiving thread happens as the last step because it
        * relies on the 'running' flag. */
      stopRecvThread();

      CLOSESOCKET(udpSockFd);
      std::free(bufferForIncomingMessages);
      udpSockFd = 0;

      return 0;
   }

   bool
   isRunning() const
   {
      return running;
   }

   void
   setReceiver(NodeNum &receiverNum, IReceiver *pRcv)
   {
      receiverRef = pRcv;
   }

   ConnectionStatus
   getCurrentConnectionStatus(const NodeNum &node) const
   {
      return ConnectionStatus::Unknown;
   }

   int
   sendAsyncMessage(const NodeNum &destNode,
                    const char *const message,
                    const size_t &messageLength)
   {
      int error = 0;

	  Assert(running == true, "The communication layer is not running!");

     const Addr *to = &nodes2adresses[destNode];

	  Assert(to != NULL, "The destination endpoint does not exist!");
	  Assert(messageLength > 0, "The message length must be positive!");
	  Assert(message != NULL, "No message provided!");

      LOG_DEBUG(" Sending " << messageLength
                << " bytes to "
                << destNode << " (" << inet_ntoa(to->sin_addr) << ":"
                << ntohs(to->sin_port));

      error = sendto(udpSockFd, message, messageLength, 0,
                     (struct sockaddr *) to, sizeof(Addr));

      if (error < 0) {
         /** -1 return value means underlying socket error. */
         string err = strerror(errno);
         LOG_DEBUG("Error while sending: " << strerror(errno));
		 Assert(false, "Failure occurred while sending!");     /** Fail-fast. */
      } else if (error < (int) messageLength) {
         /** Mesage was partially sent. Unclear why this would happen, perhaps
           * due to oversized messages (?). */
         LOG_DEBUG("Sent %d out of %d bytes!");
		 Assert(false, "Send error occurred!");    /** Fail-fast. */
      }

      return 0;
   }

   void
   startRecvThread()
   {
      LOG_DEBUG("Starting the receiving thread..");
      createThread(&recvThreadRef,
                   &PlainUdpImpl::recvRoutineWrapper,
                   (void *) this);
   }

   NodeNum
   addrToNodeId(Addr netAdress)
   {
      auto key = create_key(netAdress);
      auto res = addr2nodes.find(key);
      if (res == addr2nodes.end())
         return 0;//TBD, (IG): to change to macro of default NodeNum

      return res->second;
   }

   void
   stopRecvThread()
   {
      //LOG_DEBUG("Stopping the receiving thread..");
      threadJoin(recvThreadRef);
      //LOG_DEBUG("Stopping the receiving thread..");
   }

#if defined(_WIN32)
   static DWORD WINAPI
   recvRoutineWrapper(LPVOID instance)
#else

   static void *
   recvRoutineWrapper(void *instance)
#endif
   {
      auto inst = reinterpret_cast<PlainUdpImpl*>(instance);
      inst->recvThreadRoutine();
	  return 0;
   }

   void
   recvThreadRoutine()
   {
      bool sRunning = true;       /** Indicates whether we're still running */

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

         //LOG_DEBUG("recvfrom returned " << mLen << " bytes");

         if (mLen < 0) {
            LOG_DEBUG("Error in recvfrom(): " << mLen);
            continue;
         }

         auto sendingNode = addrToNodeId(fromAdress);
         if (mLen > 0 && (receiverRef != NULL)) {
            receiverRef->onNewMessage(sendingNode,
                                      bufferForIncomingMessages,
                                      mLen);
         }

         /** Make sure we're still running. */
         sRunning = isRunning();
      } while (sRunning == true);
   }
};

PlainUDPCommunication::~PlainUDPCommunication()
{
   _ptrImpl->Stop();
}

PlainUDPCommunication::PlainUDPCommunication(PlainUdpConfig &config)
{
   _ptrImpl = new PlainUdpImpl(config);
}

PlainUDPCommunication *PlainUDPCommunication::create(PlainUdpConfig &config)
{
   return new PlainUDPCommunication(config);
}

int PlainUDPCommunication::getMaxMessageSize()
{
   return _ptrImpl->getMaxMessageSize();
}

int PlainUDPCommunication::Start()
{
   return _ptrImpl->Start();
}

int PlainUDPCommunication::Stop()
{
   if(!_ptrImpl)
      return 0;

   auto res = _ptrImpl->Stop();
   delete _ptrImpl;
   return res;
}

bool PlainUDPCommunication::isRunning() const
{
   return _ptrImpl->isRunning();
}

ConnectionStatus
PlainUDPCommunication::getCurrentConnectionStatus(const NodeNum node) const
{
   return _ptrImpl->getCurrentConnectionStatus(node);
}

int
PlainUDPCommunication::sendAsyncMessage( const NodeNum destNode,
                                         const char *const message,
                                         const size_t messageLength)
{
   return _ptrImpl->sendAsyncMessage(destNode, message, messageLength);
}

void
PlainUDPCommunication::setReceiver(NodeNum receiverNum, IReceiver *receiver)
{
   _ptrImpl->setReceiver(receiverNum, receiver);
}
