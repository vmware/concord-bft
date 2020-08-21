//Concord
//
//Copyright (c) 2018 VMware, Inc. All Rights Reserved.
//
//This product is licensed to you under the Apache 2.0 license (the "License").  You may not use this product except in compliance with the Apache 2.0 License. 
//
//This product may include a number of subcomponents with separate copyright notices and license terms. Your use of these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE file.

#include <queue>
#include <thread>
#include <mutex>
#include <cmath>
#include <condition_variable>

#include "ClientMsgs.hpp"
#include "SimpleClient.hpp"
#include "assertUtils.hpp"
#include "TimeUtils.hpp"
#include "MessageBase.hpp"
#include "ClientRequestMsg.hpp"
#include "ClientReplyMsg.hpp"
#include "MsgsCertificate.hpp"
#include "DynamicUpperLimitWithSimpleFilter2.hpp"
#include "Logger.hpp"
#include "ArchipelagoTimeManager.hpp"

namespace bftEngine
{
    namespace impl
    {
        class ArchipelagoSimpleClientImp : public SimpleClient, public IReceiver
        {
        public:
            ArchipelagoSimpleClientImp(ICommunication* communication, uint16_t clientId,
                uint16_t fVal, uint16_t cVal, SimpleClientParams &p);

            // SimpleClient methods

            virtual ~ArchipelagoSimpleClientImp() override;

            virtual int sendRequest(bool isReadOnly, const char* request, uint32_t lengthOfRequest, uint64_t reqSeqNum, uint64_t timeoutMilli, uint32_t lengthOfReplyBuffer, char* replyBuffer, uint32_t& actualReplyLength) override;

            virtual int sendRequestToResetSeqNum() override;

            virtual int sendRequestToReadLatestSeqNum(uint64_t timeoutMilli, uint64_t& outLatestReqSeqNum) override;

            // IReceiver methods

            virtual void onNewMessage(const NodeNum sourceNode,
                const char* const message, const size_t messageLength) override;

            virtual void onConnectionStatusChanged(const NodeNum node, const ConnectionStatus newStatus) override;

            // used by  MsgsCertificate
            static bool equivalent(ClientReplyMsg *r1, ClientReplyMsg* r2) 
            {
                if (r1->reqSeqNum() != r2->reqSeqNum()) return false;

                if (r1->currentPrimaryId() != r2->currentPrimaryId()) return false;

                if (r1->replyLength() != r2->replyLength()) return false;

                char* p1 = r1->replyBuf();
                char* p2 = r2->replyBuf();

                if (memcmp(p1, p2, r1->replyLength()) != 0) return false;

                return true;
            }

        protected:
            static const uint32_t maxLegalMsgSize = 64 * 1024; // TODO(GG): ???
            static const uint16_t timersResolutionMilli = 50;

            const uint16_t _clientId;
            const uint16_t _fVal;
            const uint16_t _cVal;
            const std::set<uint16_t> _replicas;
            ICommunication* const _communication;

            MsgsCertificate<ClientReplyMsg, false, false, true, ArchipelagoSimpleClientImp> replysCertificate;

            std::mutex _lock; // protects _msgQueue and pendingRequest
            std::condition_variable _condVar;

            queue<MessageBase*> _msgQueue;
            ClientRequestMsg* pendingRequest = nullptr;

            Time timeOfLastTransmission = MinTime;
            uint16_t numberOfTransmissions = 0;

            bool _primaryReplicaIsKnown = false;
            uint16_t _knownPrimaryReplica;
            
            DynamicUpperLimitWithSimpleFilter<uint64_t> limitOfExpectedOperationTime;

            // configuration params
            uint16_t clientSendsRequestToAllReplicasFirstThresh;
            uint16_t clientSendsRequestToAllReplicasPeriodThresh;
            uint16_t clientPeriodicResetThresh;

            ClientGetTimeStampMsg* timestampMsg;
            ClientGetTimeStampMsg dummy;
            int dummyiters;
            std::unordered_map<ReplicaId, ClientSignedTimeStampMsg*> timestampsFromReplicas;
            bool timestampCollected = false;
            const uint16_t numRequired;

            void sendPendingRequest();

            void onMessageFromReplica(MessageBase* msg);
            void onRetransmission();


            void reset();

            void onMessage(ClientReplyMsg* msg);
            void onMessage(ClientSignedTimeStampMsg* msg);
        };

        void ArchipelagoSimpleClientImp::onMessageFromReplica(MessageBase* msg)
        {
            if (msg->type() == MsgCode::Reply) {
                ClientReplyMsg* replyMsg = nullptr;
                if (!ClientReplyMsg::ToActualMsgType(_clientId, msg, replyMsg))
                {
                    delete msg;
                    return;
                }
                onMessage(replyMsg);
            } else if (msg->type() == MsgCode::ClientSignedTimeStamp) {
                ClientSignedTimeStampMsg* replyMsg = nullptr;
                if (!ClientSignedTimeStampMsg::ToActualMsgType(_clientId, msg, replyMsg))
                {
                    delete msg;
                    return;
                }
                onMessage(replyMsg);
            } else {
                LOG_INFO_F(GL, "Client receive error msg type %d!", (int)msg->type());
            }
        }

        void ArchipelagoSimpleClientImp::onMessage(ClientReplyMsg* msg)
        {
             LOG_INFO_F(GL, "Client %d received ClientReplyMsg with seqNum=%"
            PRIu64
            " sender=%d  size=%d  primaryId=%d hash=%" PRIu64 "",
                _clientId, msg->reqSeqNum(), msg->senderId(), msg->size(), (int)msg->currentPrimaryId(), msg->debugHash());


            if (msg->reqSeqNum() != pendingRequest->requestSeqNum())
            {
                delete msg;
                return;
            }

            replysCertificate.addMsg(msg, msg->senderId());

            if (replysCertificate.isInconsistent())
            {
                // TODO(GG): print .....
                replysCertificate.resetAndFree();
                for (auto&& m : timestampsFromReplicas) delete m.second;
                timestampsFromReplicas.clear();
                timestampCollected = false;
            }
        }

        void ArchipelagoSimpleClientImp::onMessage(ClientSignedTimeStampMsg* msg)
        {
            LOG_INFO_F(GL, "Client %d received SignedTimeStamp with sender=%d size=%d time=%" PRIu64 "",
                _clientId, msg->senderId(), (int)msg->size(), msg->timeStamp());

            if (timestampsFromReplicas.count(msg->senderId()) > 0) {
                delete msg;
                return;
            }
            timestampsFromReplicas[msg->senderId()] = msg;
            if (!timestampCollected && timestampsFromReplicas.size() >= numRequired) {
                CombinedTimeStampMsg t(_clientId, (timestampMsg->digestOfRequests()).content(), DIGEST_SIZE);
                for (auto& item : timestampsFromReplicas) {
                    t.addPartialMsg(item.second);
                }
                t.computeMeanTimeStamp();
                pendingRequest->setCombinedTimestamp(&t);
                timestampCollected = true;
                LOG_INFO_F(GL, "Request(reqLength=%d size=%d totalSize=%d timestampSize=%d timeStamp=%" PRIu64 ")", (int)pendingRequest->requestLength(), 
                  (int)pendingRequest->size(), (int)pendingRequest->totalSize(), (int)t.endLocationOfLastVerifiedTimeStamp(), t.timeStamp());
            }
        }
        
        void ArchipelagoSimpleClientImp::onRetransmission()
        {
            sendPendingRequest();
        }



        // in this version we assume that the set of replicas is 0,1,2,...,numberOfReplicas (TODO(GG): should be changed to support full dynamic reconfiguration)
        static std::set<ReplicaId> generateSetOfReplicas_helpFunc(const int16_t numberOfReplicas)
        {
            std::set<ReplicaId> retVal;
            for (int16_t i = 0; i < numberOfReplicas; i++)
                retVal.insert(i);
            return retVal;
        }

        ArchipelagoSimpleClientImp::ArchipelagoSimpleClientImp(ICommunication* communication,uint16_t clientId, uint16_t fVal, uint16_t cVal,SimpleClientParams &p) :
            _clientId{ clientId },
            _fVal{ fVal },
            _cVal{ cVal },
            _replicas{ generateSetOfReplicas_helpFunc(3 * fVal + 2 * cVal + 1) },
            _communication{ communication },
            replysCertificate(3 * fVal + 2 * cVal + 1, fVal, 2 * fVal + cVal + 1, clientId),
            limitOfExpectedOperationTime(p.clientInitialRetryTimeoutMilli, 2,
                    p.clientMaxRetryTimeoutMilli, p.clientMinRetryTimeoutMilli,
                    4, 32, 2, 2), // 32, 1000, 2, 2
            clientSendsRequestToAllReplicasFirstThresh{p.clientSendsRequestToAllReplicasFirstThresh},
            clientSendsRequestToAllReplicasPeriodThresh{p.clientSendsRequestToAllReplicasPeriodThresh},
            clientPeriodicResetThresh{p.clientPeriodicResetThresh},
            timestampMsg{ new ClientGetTimeStampMsg(clientId) },
            numRequired{ static_cast<uint16_t>(2 * fVal + cVal + 1) }
        {
                Assert(_fVal >= 1);
                //Assert(!_communication->isRunning());

                pendingRequest = nullptr;

                timeOfLastTransmission = MinTime;

                numberOfTransmissions = 0;

                _primaryReplicaIsKnown = false;
                _knownPrimaryReplica = 0;

                _communication->setReceiver(_clientId, this);
        }

        ArchipelagoSimpleClientImp::~ArchipelagoSimpleClientImp() 
        {
            Assert(replysCertificate.isEmpty());
            Assert(_msgQueue.empty());
            Assert(pendingRequest == nullptr);
            Assert(timeOfLastTransmission == MinTime);
            Assert(numberOfTransmissions == 0);

            delete timestampMsg;
            for (auto&& m : timestampsFromReplicas) delete m.second;
            timestampsFromReplicas.clear();
        }

        int ArchipelagoSimpleClientImp::sendRequest(bool isReadOnly, const char* request, uint32_t lengthOfRequest, uint64_t reqSeqNum, uint64_t timeoutMilli, uint32_t lengthOfReplyBuffer, char* replyBuffer, uint32_t& actualReplyLength)
        {            
            // TODO(GG): check params ...
            LOG_INFO_F(GL, "Client %d - sends request %" PRIu64 " (isRO=%d, "
                            "request "
                            "size=%zu, retransmissionMilli=%d) ",
                            _clientId, reqSeqNum, (int)isReadOnly, (size_t)lengthOfRequest,  (int)limitOfExpectedOperationTime.upperLimit());

            if (!_communication->isRunning())
            {
                _communication->Start(); // TODO(GG): patch ................ change
            }

            Assert(replysCertificate.isEmpty());
            Assert(_msgQueue.empty());
            Assert(pendingRequest == nullptr);
            Assert(timeOfLastTransmission == MinTime);
            Assert(numberOfTransmissions == 0);
            
            dummyiters = (int)(lengthOfRequest >> 4) - 1;
            DigestUtil::compute(request, lengthOfRequest, (timestampMsg->digestOfRequests()).content(), sizeof(DIGEST_SIZE));

            //static const std::chrono::milliseconds timersRes(timersResolutionMilli);
            static const std::chrono::milliseconds timersRes(1);

            const Time beginTime = getMonotonicTime();

            ClientRequestMsg* reqMsg = new ClientRequestMsg(_clientId, isReadOnly, reqSeqNum, lengthOfRequest, request, true);
            pendingRequest = reqMsg;
            
            sendPendingRequest();

            bool requestTimeout = false;
            bool requestCommitted = false;
            
            while (true)
            {
                queue<MessageBase*> newMsgs;
                {
                    std::unique_lock<std::mutex> mlock(_lock);
                    _condVar.wait_for(mlock, timersRes);
                    _msgQueue.swap(newMsgs);
                }
                
                while (!newMsgs.empty())
                {
                    if (replysCertificate.isComplete())
                    {
                        delete newMsgs.front();
                    }
                    else
                    {
                        MessageBase* msg = newMsgs.front();
                        onMessageFromReplica(msg);
                    }
                    newMsgs.pop();
                }

                if (replysCertificate.isComplete())
                {
                    requestCommitted = true;
                    break;
                }

                const Time currTime = getMonotonicTime();

                if (timeoutMilli != INFINITE_TIMEOUT && (uint64_t)absDifference(beginTime, currTime) > timeoutMilli)
                {
                    requestTimeout = true;
                    break;
                }

                if (((uint64_t)absDifference(timeOfLastTransmission, currTime))/1000 > limitOfExpectedOperationTime.upperLimit())
                {
                    onRetransmission();
                }
            }



            if (requestCommitted)
            {
                Assert(replysCertificate.isComplete());

                uint64_t durationMilli = ((uint64_t)absDifference(getMonotonicTime(), beginTime)) / 1000;
                limitOfExpectedOperationTime.add(durationMilli);

                LOG_INFO_F(GL, "Client %d - request %" PRIu64 " has committed "
                                          "(isRO=%d, request size=%zu,  retransmissionMilli=%d,  durationMilli=%d) ",
                    _clientId, reqSeqNum, (int)isReadOnly, (size_t)lengthOfRequest,  (int)limitOfExpectedOperationTime.upperLimit(), (int)durationMilli);

                ClientReplyMsg* correctReply = replysCertificate.bestCorrectMsg();

                _primaryReplicaIsKnown = true;
                _knownPrimaryReplica = correctReply->currentPrimaryId();

                if (correctReply->replyLength() <= lengthOfReplyBuffer)
                {
                    memcpy(replyBuffer, correctReply->replyBuf(), correctReply->replyLength());
                    actualReplyLength = correctReply->replyLength();
                    reset();
                    return 0;
                }
                else
                {
                    reset();
                    return (-2);
                }                
            }
            else if (requestTimeout)
            {
                //Logger::printInfo("Client %d - request %" PRIu64 " - timeout");

                if (timeoutMilli >= limitOfExpectedOperationTime.upperLimit())
                {
                    _primaryReplicaIsKnown = false;
                    limitOfExpectedOperationTime.add(timeoutMilli);
                }

                reset();
                return (-1);
            }

            Assert(false);
            return 0;
        }

        int ArchipelagoSimpleClientImp::sendRequestToResetSeqNum()
        {
            Assert(false); // not implemented yet
            return 0;
        }

        void ArchipelagoSimpleClientImp::reset()
        {
            replysCertificate.resetAndFree();
            for (auto&& m : timestampsFromReplicas) delete m.second;
            timestampsFromReplicas.clear();
            timestampCollected = false;

            queue<MessageBase*> newMsgs;
            {
                std::unique_lock<std::mutex> mlock(_lock);
                _msgQueue.swap(newMsgs);

                delete pendingRequest;
                pendingRequest = nullptr;
            }

            while (!newMsgs.empty())
            {
                delete newMsgs.front();
                newMsgs.pop();
            }

            timeOfLastTransmission = MinTime;
            numberOfTransmissions = 0;
        }

        int ArchipelagoSimpleClientImp::sendRequestToReadLatestSeqNum(uint64_t timeoutMilli, uint64_t& outLatestReqSeqNum)
        {
            Assert(false); // not implemented yet
            return 0;
        }

        void ArchipelagoSimpleClientImp::onNewMessage(const NodeNum sourceNode,
                                           const char* const message, const size_t messageLength)
        {
            // check source
            int16_t senderId = (int16_t)sourceNode;
            if (_replicas.count(senderId) == 0) return;

            // check length
            if (messageLength > maxLegalMsgSize) return;
            if (messageLength < sizeof(MessageBase::Header)) return;

            //MessageBase::Header* msgHeader = (MessageBase::Header*)message;

            // check type
            // if (msgHeader->msgType != REPLY_MSG_TYPE) return;

            std::unique_lock<std::mutex> mlock(_lock);
            {
                if (pendingRequest == nullptr) return;

                // create msg object
                MessageBase::Header* msgBody = (MessageBase::Header*)std::malloc(messageLength);
                memcpy(msgBody, message, messageLength);
                MessageBase* pMsg = new MessageBase(senderId, msgBody, messageLength, true);

                _msgQueue.push(pMsg); // TODO(GG): handle overflow
                _condVar.notify_one();
            }
        }

        void ArchipelagoSimpleClientImp::onConnectionStatusChanged(const NodeNum node, const ConnectionStatus newStatus)
        {
        }

        void ArchipelagoSimpleClientImp::sendPendingRequest()
        {
            Assert(pendingRequest != nullptr)

            timeOfLastTransmission = getMonotonicTime();
            numberOfTransmissions++;

            const bool resetReplies = (numberOfTransmissions % clientPeriodicResetThresh == 1);

            LOG_INFO_F(GL,"Client %d - sends request %" PRIu64 " "
                                                           "(isRO=%d, "
                                                   "request "
                                          "size=%zu, "
                " retransmissionMilli=%d, numberOfTransmissions=%d, resetReplies=%d)",
                _clientId, pendingRequest->requestSeqNum(), (int)pendingRequest->isReadOnly(), (size_t)pendingRequest->size(),
                (int)limitOfExpectedOperationTime.upperLimit(), (int)numberOfTransmissions, (int)resetReplies);


            if (resetReplies)
            {
                replysCertificate.resetAndFree();
                for (auto&& m : timestampsFromReplicas) delete m.second;
                timestampsFromReplicas.clear();
                timestampCollected = false;

                for (int i = 0; i < dummyiters; i++) {
                    for (uint16_t r : _replicas)
                    {
                        _communication->sendAsyncMessage(r, dummy.body(), dummy.size());
                    }
                }

                for (uint16_t r : _replicas)
                {
                    _communication->sendAsyncMessage(r, timestampMsg->body(), timestampMsg->size());
                }

                static const std::chrono::milliseconds timersRes(1);
                while (true)
                {
                    queue<MessageBase*> newMsgs;
                    {
                        std::unique_lock<std::mutex> mlock(_lock);
                        _condVar.wait_for(mlock, timersRes);
                        _msgQueue.swap(newMsgs);
                    }
                    
                    while (!newMsgs.empty())
                    {
                        if (timestampCollected)
                        {
                            delete newMsgs.front();
                        }
                        else
                        {
                            MessageBase* msg = newMsgs.front();
                            onMessageFromReplica(msg);
                        }
                        newMsgs.pop();
                    }

                    if (timestampCollected) break;

                    const Time currTime = getMonotonicTime();
                    if (((uint64_t)absDifference(timeOfLastTransmission, currTime))/1000 > limitOfExpectedOperationTime.upperLimit() / 2)
                    {
                        for (uint16_t r : _replicas)
                        {
                            _communication->sendAsyncMessage(r, timestampMsg->body(), timestampMsg->size());
                        }
                    }
                }
            }

            for (uint16_t r : _replicas)
            {
                // int stat = 
                _communication->sendAsyncMessage(r, pendingRequest->body(), pendingRequest->size());
                // TODO(GG): handle errors (print and/or ....)
            }
        }
    }
}

namespace bftEngine
{
    SimpleClient* SimpleClient::createArchipelagoSimpleClient(
            ICommunication* communication,
            uint16_t clientId,
            uint16_t fVal,
            uint16_t cVal,
            SimpleClientParams p)
    {
        return new impl::ArchipelagoSimpleClientImp(communication, clientId, fVal, cVal, p);
    }
}