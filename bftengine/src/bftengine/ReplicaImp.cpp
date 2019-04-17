//Concord
//
//Copyright (c) 2018 VMware, Inc. All Rights Reserved.
//
//This product is licensed to you under the Apache 2.0 license (the "License").  You may not use this product except in compliance with the Apache 2.0 License. 
//
//This product may include a number of subcomponents with separate copyright notices and license terms. Your use of these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE file.


#include "ReplicaImp.hpp"
#include "assertUtils.hpp"
#include "ClientRequestMsg.hpp"
#include "PrePrepareMsg.hpp"
#include "CheckpointMsg.hpp"
#include "ClientReplyMsg.hpp"
#include "Logger.hpp"
#include "PartialExecProofMsg.hpp"
#include "FullExecProofMsg.hpp"
#include "StartSlowCommitMsg.hpp"
#include "ControllerWithSimpleHistory.hpp"
#include "ReqMissingDataMsg.hpp"
#include "SimpleAckMsg.hpp"
#include "ViewChangeMsg.hpp"
#include "NewViewMsg.hpp"
#include "PartialCommitProofMsg.hpp"
#include "FullCommitProofMsg.hpp"
#include "StateTransferMsg.hpp"
#include "DebugStatistics.hpp"
#include "ReplicaStatusMsg.hpp"
#include "NullStateTransfer.hpp"


namespace bftEngine
{
	namespace impl
	{
		// Handler functions for replica's timers

		static void viewChangeTimerHandlerFunc(Time t, void* p)
		{
			InternalReplicaApi* r = (InternalReplicaApi*)p;
			Assert(r != nullptr);
			Timer& timer = r->getViewChangeTimer();
			r->onViewsChangeTimer(t, timer);
			timer.start(); // restart timer
		}

		//
		static void stateTranTimerHandlerFunc(Time t, void* p)
		{
			InternalReplicaApi* r = (InternalReplicaApi*)p;
			Assert(r != nullptr);
			Timer& timer = r->getStateTranTimer();
			r->onStateTranTimer(t, timer);
			timer.start(); // restart timer
		}

		static void retransmissionsTimerHandlerFunc(Time t, void* p)
		{
			Assert(retransmissionsLogicEnabled);

			InternalReplicaApi* r = (InternalReplicaApi*)p;
			Assert(r != nullptr);
			Timer& timer = r->getRetransmissionsTimer();
			r->onRetransmissionsTimer(t, timer);

			timer.start(); // restart timer
		}

		static void statusTimerHandlerFunc(Time t, void* p)
		{
			InternalReplicaApi* r = (InternalReplicaApi*)p;
			Assert(r != nullptr);
			Timer& timer = r->getStatusTimer();
			r->onStatusReportTimer(t, timer);

			timer.start(); // restart timer
		}

		static void slowPathTimerHandlerFunc(Time t, void* p)
		{
			InternalReplicaApi* r = (InternalReplicaApi*)p;
			Assert(r != nullptr);
			Timer& timer = r->getSlowPathTimer();
			r->onSlowPathTimer(t, timer);

			timer.start(); // restart timer
		}

		static void infoRequestHandlerFunc(Time t, void* p)
		{
			InternalReplicaApi* r = (InternalReplicaApi*)p;
			Assert(r != nullptr);
			Timer& timer = r->getInfoRequestTimer();
			r->onInfoRequestTimer(t, timer);

			timer.start(); // restart timer
		}

		static void debugStatHandlerFunc(Time t, void* p)
		{
			InternalReplicaApi* r = (InternalReplicaApi*)p;
			Assert(r != nullptr);
			Timer& timer = r->getDebugStatTimer();
			r->onDebugStatTimer(t, timer);

			timer.start(); // restart timer
		}


		std::unordered_map<uint16_t, PtrToMetaMsgHandler> ReplicaImp::createMapOfMetaMsgHandlers()
		{
			std::unordered_map<uint16_t, PtrToMetaMsgHandler> r;

			r[MsgCode::Checkpoint] = &ReplicaImp::metaMessageHandler_IgnoreWhenCollectingState<CheckpointMsg>;
			r[MsgCode::CommitPartial] = &ReplicaImp::metaMessageHandler_IgnoreWhenCollectingState<CommitPartialMsg>;
			r[MsgCode::CommitFull] = &ReplicaImp::metaMessageHandler_IgnoreWhenCollectingState<CommitFullMsg>;
			r[MsgCode::FullCommitProof] = &ReplicaImp::metaMessageHandler_IgnoreWhenCollectingState<FullCommitProofMsg>;
			r[MsgCode::NewView] = &ReplicaImp::metaMessageHandler_IgnoreWhenCollectingState<NewViewMsg>;
			r[MsgCode::PrePrepare] = &ReplicaImp::metaMessageHandler_IgnoreWhenCollectingState<PrePrepareMsg>;
			r[MsgCode::PartialCommitProof] = &ReplicaImp::metaMessageHandler_IgnoreWhenCollectingState<PartialCommitProofMsg>;
			r[MsgCode::PartialExecProof] = &ReplicaImp::metaMessageHandler_IgnoreWhenCollectingState<PartialExecProofMsg>;
			r[MsgCode::PreparePartial] = &ReplicaImp::metaMessageHandler_IgnoreWhenCollectingState<PreparePartialMsg>;
			r[MsgCode::PrepareFull] = &ReplicaImp::metaMessageHandler_IgnoreWhenCollectingState<PrepareFullMsg>;
			r[MsgCode::ReqMissingData] = &ReplicaImp::metaMessageHandler_IgnoreWhenCollectingState<ReqMissingDataMsg>;
			r[MsgCode::SimpleAckMsg] = &ReplicaImp::metaMessageHandler_IgnoreWhenCollectingState<SimpleAckMsg>;
			r[MsgCode::StartSlowCommit] = &ReplicaImp::metaMessageHandler_IgnoreWhenCollectingState<StartSlowCommitMsg>;
			r[MsgCode::ViewChange] = &ReplicaImp::metaMessageHandler_IgnoreWhenCollectingState<ViewChangeMsg>;
			r[MsgCode::Request] = &ReplicaImp::metaMessageHandler_IgnoreWhenCollectingState<ClientRequestMsg>;
			r[MsgCode::ReplicaStatus] = &ReplicaImp::metaMessageHandler_IgnoreWhenCollectingState<ReplicaStatusMsg>;

			r[MsgCode::StateTransfer] = &ReplicaImp::metaMessageHandler<StateTransferMsg>;


			return r;
		}


		template <typename T> void ReplicaImp::metaMessageHandler(MessageBase* msg)
		{
			T* validMsg = nullptr;
			bool isValid = T::ToActualMsgType(*repsInfo, msg, validMsg);

			if (isValid)
			{
				onMessage(validMsg);
			}
			else
			{
				onReportAboutInvalidMessage(msg);
				delete msg;
				return;
			}	
		}
 
		template <typename T> void ReplicaImp::metaMessageHandler_IgnoreWhenCollectingState(MessageBase* msg)
		{
			if (stateTransfer->isCollectingState())
			{
				delete msg;
			}
			else
			{
				metaMessageHandler<T>(msg);
			}
		}

		void ReplicaImp::onReportAboutInvalidMessage(MessageBase* msg)
		{
			LOG_WARN_F(GL, "Node %d received invalid message from Node %d (type==%d)", (int)myReplicaId, (int)msg->senderId(), (int)msg->type());

			// TODO(GG): logic that deals with invalid messages (e.g., a node that sends invalid messages may have a problem (old version,bug,malicious,...)).
		}



		void ReplicaImp::send(MessageBase* m, NodeIdType dest)
		{
			// debug code begin 

			if (m->type() == MsgCode::Checkpoint)
			{
				CheckpointMsg* c = (CheckpointMsg*)m;
				if (c->digestOfState().isZero()) LOG_WARN_F(GL, "Debug: checkpoint digest is zero");
			}


			// debug code end



			sendRaw(m->body(), dest, m->type(), m->size());
		}

		void ReplicaImp::sendToAllOtherReplicas(MessageBase *m)
		{
			for (ReplicaId dest : repsInfo->idsOfPeerReplicas())
				sendRaw(m->body(), dest, m->type(), m->size());
		}


		void ReplicaImp::sendRaw(char* m, NodeIdType dest, uint16_t type, MsgSize size)
		{
			int errorCode = 0;

			if (dest == ALL_OTHER_REPLICAS)
			{
				for (ReplicaId d : repsInfo->idsOfPeerReplicas())
					sendRaw(m, d, type, size);
				return;
			}

		#ifdef DEBUG_STATISTICS
			DebugStatistics::onSendExMessage(type);
		#endif

			errorCode = communication->sendAsyncMessage(dest, m, size);

			if (errorCode != 0)
			{
				LOG_ERROR_F(GL, "In ReplicaImp::sendRaw - communication->sendAsyncMessage returned error %d for message type %d", errorCode, (int)type);
			}
		}


		void ReplicaImp::recvMsg(void*& item, bool& external)
		{
			bool newMsg = false;
			while (!newMsg)
			{
				newMsg = incomingMsgsStorage.pop(item, external, timersResolution);

				if (!newMsg) timersScheduler.evaluate(); // TODO(GG): make sure that we don't check timers too often (i.e. much faster than timersResolution)
			}
		}

		ReplicaImp::MsgReceiver::MsgReceiver(IncomingMsgsStorage& queue)
			:  incomingMsgs{ queue }
		{
		}


		void ReplicaImp::MsgReceiver::onNewMessage(
			const NodeNum sourceNode,
			const char* const message,
			const size_t messageLength)
		{
			if (messageLength > maxExternalMessageSize) return;
			if (messageLength < sizeof(MessageBase::Header)) return;

			MessageBase::Header* msgBody = (MessageBase::Header*)std::malloc(messageLength);
			memcpy(msgBody, message, messageLength);

			NodeIdType n = (uint16_t)sourceNode; // TODO(GG): make sure that this casting is okay

			MessageBase* pMsg = new MessageBase(n, msgBody, messageLength, true);

			// TODO(GG): TBD: do we want to verify messages in this thread (communication) ?

			incomingMsgs.pushExternalMsg(pMsg);
		}


		void ReplicaImp::MsgReceiver::onConnectionStatusChanged(const NodeNum node, const ConnectionStatus newStatus)
		{
		}


		void ReplicaImp::onMessage(ClientRequestMsg *m)
		{
			const NodeIdType senderId = m->senderId();
			const NodeIdType clientId = m->clientProxyId();
			const bool readOnly = m->isReadOnly();
			const ReqId reqSeqNum = m->requestSeqNum();

			LOG_INFO_F(GL, "Node %d received ClientRequestMsg (clientId=%d reqSeqNum=%" PRIu64 ", readOnly=%d) from Node %d", myReplicaId, clientId, reqSeqNum, readOnly ? 1 : 0, senderId);

			if (stateTransfer->isCollectingState())
			{
				LOG_INFO_F(GL, "ClientRequestMsg is ignored becuase this replica is collecting missing state from the other replicas");
				delete m;
				return;
			}

			// check message validity
			const bool invalidClient = !clientsManager->isValidClient(clientId);
			const bool sentFromReplicaToNonPrimary = repsInfo->isIdOfReplica(senderId) && !isCurrentPrimary();

			if (invalidClient || sentFromReplicaToNonPrimary) // TODO(GG): more conditions (make sure that a request of client A cannot be generated by client B!=A)
			{
				LOG_INFO_F(GL, "ClientRequestMsg is invalid");
				onReportAboutInvalidMessage(m);
				delete m;
				return;
			}

			if (readOnly)
			{
				executeReadOnlyRequest(m);
				delete m;
				return;
			}

			if (!currentViewIsActive())
			{
				LOG_INFO_F(GL, "ClientRequestMsg is ignored becuase current view is inactive");
				delete m;
				return;
			}

			const ReqId seqNumberOfLastReply = clientsManager->seqNumberOfLastReplyToClient(clientId);

			if (seqNumberOfLastReply < reqSeqNum)
			{
				if (isCurrentPrimary())
				{
					if (clientsManager->noPendingAndRequestCanBecomePending(clientId, reqSeqNum) && (requestsQueueOfPrimary.size() < 700)) // TODO(GG): use config/parameter
					{
						requestsQueueOfPrimary.push(m);
						tryToSendPrePrepareMsg(true);
						return;
					}
					else
					{
						LOG_INFO_F(GL, "ClientRequestMsg is ignored becuase: request is old, OR primary is current working on a request from the same client, OR queue contains too many requests");
					}
				}
				else // not the current primary
				{
					if (clientsManager->noPendingAndRequestCanBecomePending(clientId, reqSeqNum))
					{
						clientsManager->addPendingRequest(clientId, reqSeqNum);

						send(m, currentPrimary()); // TODO(GG): add a mechanism that retransmits (otherwise we may start unnecessary view-change )

						LOG_INFO_F(GL, "Sending ClientRequestMsg to current primary");
					}
					else
					{
						LOG_INFO_F(GL, "ClientRequestMsg is ignored becuase request is old or replica has another pending request from the same client");
					}
				}
			}
			else if (seqNumberOfLastReply == reqSeqNum)
			{
				LOG_INFO_F(GL, "ClientRequestMsg has already been executed - retransmit reply to client");

				ClientReplyMsg* repMsg = clientsManager->allocateMsgWithLatestReply(clientId, currentPrimary());

				send(repMsg, clientId);

				delete repMsg;
			}
			else
			{
				LOG_INFO_F(GL, "ClientRequestMsg is ignored becuase request is old");
			}


			delete m;
		}

		void ReplicaImp::tryToSendPrePrepareMsg(bool batchingLogic)
		{
			Assert(isCurrentPrimary() && currentViewIsActive());

			if (primaryLastUsedSeqNum + 1 > lastStableSeqNum + kWorkWindowSize) return;

			if (primaryLastUsedSeqNum + 1 > lastExecutedSeqNum + maxConcurrentAgreementsByPrimary) return; // TODO(GG): should also be checked by the non-primary replicas

			if (requestsQueueOfPrimary.empty()) return;

			// remove irrelevant requests from the head of the requestsQueueOfPrimary (and update requestsInQueue)
			ClientRequestMsg* first = requestsQueueOfPrimary.front();
			while (first != nullptr && !clientsManager->noPendingAndRequestCanBecomePending(first->clientProxyId(), first->requestSeqNum()))
			{
				delete first;
				requestsQueueOfPrimary.pop();
				first = (!requestsQueueOfPrimary.empty() ? requestsQueueOfPrimary.front() : nullptr);
			}

			const size_t requestsInQueue = requestsQueueOfPrimary.size();

			if (requestsInQueue == 0) return;

			Assert(primaryLastUsedSeqNum >= lastExecutedSeqNum);

			uint64_t concurrentDiff = ((primaryLastUsedSeqNum + 1) - lastExecutedSeqNum);
			uint64_t minBatchSize = 1;

			// update maxNumberOfPendingRequestsInRecentHistory (if needed)
			if (requestsInQueue > maxNumberOfPendingRequestsInRecentHistory)  maxNumberOfPendingRequestsInRecentHistory = requestsInQueue;

			// TODO(GG): the batching logic should be part of the configuration - TBD.
			if (batchingLogic && (concurrentDiff >= 2))
			{
				minBatchSize = concurrentDiff * batchingFactor;

				const size_t maxReasonableMinBatchSize = 350; // TODO(GG): use param from configuration

				if (minBatchSize > maxReasonableMinBatchSize) minBatchSize = maxReasonableMinBatchSize;
			}

			if (requestsInQueue < minBatchSize) return;

			primaryLastUsedSeqNum++;

			// update batchingFactor
			if ((primaryLastUsedSeqNum % kWorkWindowSize) == 0) // TODO(GG): do we want to update batchingFactor when the view is changed
			{
				const size_t aa = 4; // TODO(GG): read from configuration
				batchingFactor = (maxNumberOfPendingRequestsInRecentHistory / aa);
				if (batchingFactor < 1) batchingFactor = 1;
				maxNumberOfPendingRequestsInRecentHistory = 0;
			}

			Assert(primaryLastUsedSeqNum <= lastExecutedSeqNum + MaxConcurrentFastPaths); // because maxConcurrentAgreementsByPrimary <  MaxConcurrentFastPaths

			CommitPath firstPath = controller->getCurrentFirstPath();

			Assert((cVal != 0) || (firstPath != CommitPath::FAST_WITH_THRESHOLD)); // assert: (cVal==0) --> (firstPath != CommitPath::FAST_WITH_THRESHOLD) 

			controller->onSendingPrePrepare(primaryLastUsedSeqNum, firstPath);

			PrePrepareMsg *pp = new PrePrepareMsg(myReplicaId, curView, primaryLastUsedSeqNum, firstPath, false);

			ClientRequestMsg* nextRequest = requestsQueueOfPrimary.front();
			while (nextRequest != nullptr && nextRequest->size() <= pp->remainingSizeForRequests())
			{
				if (clientsManager->noPendingAndRequestCanBecomePending(nextRequest->clientProxyId(), nextRequest->requestSeqNum()))
				{
					pp->addRequest(nextRequest->body(), nextRequest->size());
					clientsManager->addPendingRequest(nextRequest->clientProxyId(), nextRequest->requestSeqNum());
				}
				delete nextRequest;
				requestsQueueOfPrimary.pop();
				nextRequest = (requestsQueueOfPrimary.size() > 0 ? requestsQueueOfPrimary.front() : nullptr);
			}

			pp->finishAddingRequests();

			Assert(pp->numberOfRequests() > 0);

			LOG_INFO_F(GL, "Sending PrePrepareMsg (seqNumber=%" PRId64 ", requests=%d, size=%d",
				pp->seqNumber(), (int)pp->numberOfRequests(), (int)requestsQueueOfPrimary.size());

			for (ReplicaId x : repsInfo->idsOfPeerReplicas())
			{
				sendRetransmittableMsgToReplica(pp, x, primaryLastUsedSeqNum);
			}

			SeqNumInfo& seqNumInfo = mainLog->get(primaryLastUsedSeqNum);
			seqNumInfo.addSelfMsg(pp);

			if (firstPath == CommitPath::SLOW)
			{
				seqNumInfo.startSlowPath();
                                metric_slow_path_count_.Get().Inc();
				sendPreparePartial(seqNumInfo);
			}
			else
			{
				sendPartialProof(seqNumInfo);
			}
		}


		template <typename T>
		bool ReplicaImp::relevantMsgForActiveView(const T* msg)
		{
			const SeqNum  msgSeqNum = msg->seqNumber();
			const ViewNum msgViewNum = msg->viewNumber();

			if (currentViewIsActive() &&
				(msgViewNum == curView) &&
				(msgSeqNum > strictLowerBoundOfSeqNums) &&
				(mainLog->insideActiveWindow(msgSeqNum)))
			{
				Assert(msgSeqNum >  lastStableSeqNum);
				Assert(msgSeqNum <= lastStableSeqNum + kWorkWindowSize);

				return true;
			}
			else
			{
				const bool myReplicaMayBeBehind = (curView < msgViewNum) || (msgSeqNum > mainLog->currentActiveWindow().second);
				if (myReplicaMayBeBehind)
				{
					onReportAboutAdvancedReplica(msg->senderId(), msgSeqNum, msgViewNum);
				}
				else
				{
					const bool msgReplicaMayBeBehind = (curView > msgViewNum) || (msgSeqNum + kWorkWindowSize < mainLog->currentActiveWindow().first);

					if (msgReplicaMayBeBehind) onReportAboutLateReplica(msg->senderId(), msgSeqNum, msgViewNum);
				}

				return false;
			}
		}


		void ReplicaImp::onMessage(PrePrepareMsg* msg)
		{
			const SeqNum msgSeqNum = msg->seqNumber();

			LOG_INFO_F(GL, "Node %d received PrePrepareMsg from node %d for seqNumber %" PRId64 " (size=%d)",
				(int)myReplicaId, (int)msg->senderId(), msgSeqNum, (int)msg->size());

			if (!currentViewIsActive() && viewsManager->waitingForMsgs() && msgSeqNum > lastStableSeqNum)
			{
				Assert(!msg->isNull()); // we should never send (and never accept) null PrePrepare message

				if (viewsManager->addPotentiallyMissingPP(msg, lastStableSeqNum))
				{
					LOG_INFO_F(GL, "Node %d adds PrePrepareMsg for seqNumber %" PRId64 " to viewsManager", (int)myReplicaId, msgSeqNum);
					tryToEnterView();
				}
				else
				{
					LOG_INFO_F(GL, "Node %d does not add PrePrepareMsg for seqNumber %" PRId64 " to viewsManager", (int)myReplicaId, msgSeqNum);
				}

				return; // TODO(GG): memory deallocation is confusing .....
			}

			bool msgAdded = false;

			if (relevantMsgForActiveView(msg) && (msg->senderId() == currentPrimary()))
			{
				sendAckIfNeeded(msg, msg->senderId(), msgSeqNum);

				SeqNumInfo& seqNumInfo = mainLog->get(msgSeqNum);

				if (seqNumInfo.addMsg(msg))
				{
					msgAdded = true;

					if (msg->firstPath() != CommitPath::SLOW && !seqNumInfo.slowPathStarted()) // TODO(GG): make sure we correctly handle a situation where StartSlowCommitMsg is handled before PrePrepareMsg
					{
						sendPartialProof(seqNumInfo);
					}
					else
					{
						seqNumInfo.startSlowPath();
                                                metric_slow_path_count_.Get().Inc();
						sendPreparePartial(seqNumInfo); ;
					}
				}
			}

			if (!msgAdded) delete msg;
		}


		void ReplicaImp::tryToStartSlowPaths()
		{
			if (!isCurrentPrimary() || stateTransfer->isCollectingState() || !currentViewIsActive())
				return; // TODO(GG): consider to stop the related timer when this method is not needed (to avoid useless invocations)

			const SeqNum minSeqNum = lastExecutedSeqNum + 1;

			if (minSeqNum > lastStableSeqNum + kWorkWindowSize)
			{
				LOG_INFO_F(GL, "Replica::tryToStartSlowPaths() : minSeqNum > lastStableSeqNum + kWorkWindowSize");
				return;
			}

			const SeqNum maxSeqNum = primaryLastUsedSeqNum;

			Assert(maxSeqNum <= lastStableSeqNum + kWorkWindowSize);
			Assert(minSeqNum <= maxSeqNum + 1);

			if (minSeqNum > maxSeqNum)
				return;

			sendCheckpointIfNeeded(); // TODO(GG): TBD - do we want it here ?

			const Time currTime = getMonotonicTime();

			for (SeqNum i = minSeqNum; i <= maxSeqNum; i++)
			{
				SeqNumInfo& seqNumInfo = mainLog->get(i);

				if (seqNumInfo.partialProofs().hasFullProof() || // already has a full proof
					seqNumInfo.slowPathStarted() || // slow path has already  started 
					seqNumInfo.partialProofs().getSelfPartialCommitProof() == nullptr || // did not start a fast path
					(!seqNumInfo.hasPrePrepareMsg())
					)
					continue; // slow path is not needed 

				const Time timeOfPartProof = seqNumInfo.partialProofs().getTimeOfSelfPartialProof();

				if (absDifference(currTime, timeOfPartProof) < controller->timeToStartSlowPathMilli() * 1000)
					break; // don't try the next seq numbers

				LOG_INFO_F(GL, "Primary initiates slow path for seqNum=%" PRId64 " (currTime=%lld timeOfPartProof=%lld", i, currTime, timeOfPartProof);

				controller->onStartingSlowCommit(i);

				seqNumInfo.startSlowPath();
                                metric_slow_path_count_.Get().Inc();

				// send StartSlowCommitMsg to all replicas

				StartSlowCommitMsg* startSlow = new StartSlowCommitMsg(myReplicaId, curView, i);

				for (ReplicaId x : repsInfo->idsOfPeerReplicas())
				{
					sendRetransmittableMsgToReplica(startSlow, x, i);
				}

				delete startSlow;

				sendPreparePartial(seqNumInfo);
			}
		}



		void ReplicaImp::tryToAskForMissingInfo()
		{
			if (!currentViewIsActive() || stateTransfer->isCollectingState()) return;

			Assert(maxSeqNumTransferredFromPrevViews <= lastStableSeqNum + kWorkWindowSize);

			const bool recentViewChange = (maxSeqNumTransferredFromPrevViews > lastStableSeqNum);

			SeqNum minSeqNum = 0;
			SeqNum maxSeqNum = 0;

			if (!recentViewChange)
			{
				const int16_t searchWindow = 4; // TODO(GG): TBD - read from configuration
				minSeqNum = lastExecutedSeqNum + 1;
				maxSeqNum = std::min(minSeqNum + searchWindow - 1, lastStableSeqNum + kWorkWindowSize);
			}
			else
			{
				const int16_t searchWindow = 32; // TODO(GG): TBD - read from configuration
				minSeqNum = lastStableSeqNum + 1;
				while (minSeqNum <= lastStableSeqNum + kWorkWindowSize)
				{
					SeqNumInfo& seqNumInfo = mainLog->get(minSeqNum);
					if (!seqNumInfo.isCommitted__gg()) break;
					minSeqNum++;
				}
				maxSeqNum = std::min(minSeqNum + searchWindow - 1, lastStableSeqNum + kWorkWindowSize);
			}

			if (minSeqNum > lastStableSeqNum + kWorkWindowSize) return;

			const Time curTime = getMonotonicTime();

			SeqNum lastRelatedSeqNum = 0;

			// TODO(GG): improve/optimize the following loops

			for (SeqNum i = minSeqNum; i <= maxSeqNum; i++)
			{
				Assert(mainLog->insideActiveWindow(i));

				const SeqNumInfo& seqNumInfo = mainLog->get(i);

				Time t = seqNumInfo.getTimeOfFisrtRelevantInfoFromPrimary();

				const Time lastInfoRequest = seqNumInfo.getTimeOfLastInfoRequest();

				if ((t < lastInfoRequest)) t = lastInfoRequest;

				if (t != MinTime && (t < curTime))
				{
					int64_t diffMilli = absDifference(curTime, t) / 1000;
					if (diffMilli >= dynamicUpperLimitOfRounds->upperLimit()) lastRelatedSeqNum = i;
				}
			}

			for (SeqNum i = minSeqNum; i <= lastRelatedSeqNum; i++)
			{
				if (!recentViewChange)
					tryToSendReqMissingDataMsg(i);
				else
					tryToSendReqMissingDataMsg(i, true, currentPrimary());
			}
		}



		void ReplicaImp::onMessage(StartSlowCommitMsg* msg)
		{
			const SeqNum msgSeqNum = msg->seqNumber();

			LOG_INFO_F(GL, "Node %d received StartSlowCommitMsg for seqNumber %" PRId64 "", myReplicaId, msgSeqNum);

			if (relevantMsgForActiveView(msg))
			{
				sendAckIfNeeded(msg, currentPrimary(), msgSeqNum);

				SeqNumInfo& seqNumInfo = mainLog->get(msgSeqNum);

				if (!seqNumInfo.slowPathStarted() && !seqNumInfo.isPrepared())
				{
					LOG_INFO_F(GL, "Node %d starts slow path for seqNumber %" PRId64 "", myReplicaId, msgSeqNum);

					seqNumInfo.startSlowPath();
                                        metric_slow_path_count_.Get().Inc();

					if (seqNumInfo.hasPrePrepareMsg() == false)
						tryToSendReqMissingDataMsg(msgSeqNum);
					else
						sendPreparePartial(seqNumInfo);
				}
			}

			delete msg;
		}


		void ReplicaImp::sendPartialProof(SeqNumInfo& seqNumInfo)
		{
			PartialProofsSet& partialProofs = seqNumInfo.partialProofs();

			if (!seqNumInfo.hasPrePrepareMsg()) return;

			PrePrepareMsg* pp = seqNumInfo.getPrePrepareMsg();
			Digest& ppDigest = pp->digestOfRequests();
			const SeqNum seqNum = pp->seqNumber();

			if (!partialProofs.hasFullProof())
			{
				// send PartialCommitProofMsg to all collectors

				PartialCommitProofMsg* part = partialProofs.getSelfPartialCommitProof();

				if (part == nullptr)
				{
					IThresholdSigner* commitSigner = nullptr;
					CommitPath commitPath = pp->firstPath();

					Assert((cVal != 0) || (commitPath != CommitPath::FAST_WITH_THRESHOLD));

					if ((commitPath == CommitPath::FAST_WITH_THRESHOLD) && (cVal>0))
						commitSigner = thresholdSignerForCommit;
					else
						commitSigner = thresholdSignerForOptimisticCommit;

					Digest tmpDigest;
					Digest::calcCombination(ppDigest, curView, seqNum, tmpDigest);

					part = new PartialCommitProofMsg(myReplicaId, curView, seqNum, commitPath, tmpDigest, commitSigner);
					partialProofs.addSelfMsgAndPPDigest(part, tmpDigest);
				}

				partialProofs.setTimeOfSelfPartialProof(getMonotonicTime());

				// send PartialCommitProofMsg (only if, from my point of view, at most MaxConcurrentFastPaths are in progress)
				if (seqNum <= lastExecutedSeqNum + MaxConcurrentFastPaths)
				{
					// TODO(GG): improve the following code (use iterators instead of a simple array)
					int8_t numOfRouters = 0;
					ReplicaId routersArray[2];

					repsInfo->getCollectorsForPartialProofs(curView, seqNum, &numOfRouters, routersArray);

					for (int i = 0; i < numOfRouters; i++)
					{
						ReplicaId router = routersArray[i];
						if (router != myReplicaId)
						{
							sendRetransmittableMsgToReplica(part, router, seqNum);
						}
					}
				}
			}

			if (partialProofs.hasFullProof())
			{
				commitAndSendFullCommitProof(seqNum, seqNumInfo, partialProofs);
			}

		}


		void ReplicaImp::sendPreparePartial(SeqNumInfo& seqNumInfo)
		{
			Assert(currentViewIsActive());

			if (seqNumInfo.getSelfPreparePartialMsg() == nullptr && seqNumInfo.hasPrePrepareMsg() && !seqNumInfo.isPrepared())
			{
				PrePrepareMsg* pp = seqNumInfo.getPrePrepareMsg();

				Assert(pp != nullptr);

				LOG_INFO_F(GL, "Sending PreparePartialMsg for seqNumber %" PRId64 "", pp->seqNumber());

				PreparePartialMsg* p = PreparePartialMsg::create(curView, pp->seqNumber(), myReplicaId, pp->digestOfRequests(), thresholdSignerForSlowPathCommit);
				seqNumInfo.addSelfMsg(p);

				if (!isCurrentPrimary()) sendRetransmittableMsgToReplica(p, currentPrimary(), pp->seqNumber());
			}
		}


		void ReplicaImp::sendCommitPartial(const SeqNum s)
		{
			Assert(currentViewIsActive());
			Assert(mainLog->insideActiveWindow(s));

			SeqNumInfo& seqNumInfo = mainLog->get(s);
			PrePrepareMsg* pp = seqNumInfo.getPrePrepareMsg();

			Assert(seqNumInfo.isPrepared());
			Assert(pp != nullptr);
			Assert(pp->seqNumber() == s);

			if (seqNumInfo.committedOrHasCommitPartialFromReplica(myReplicaId))
				return; // not needed 

			LOG_INFO_F(GL, "Sending CommitPartialMsg for seqNumber %" PRId64 "", s);

			Digest d;
			Digest::digestOfDigest(pp->digestOfRequests(), d);

			CommitPartialMsg* c = CommitPartialMsg::create(curView, s, myReplicaId, d, thresholdSignerForSlowPathCommit);
			seqNumInfo.addSelfCommitPartialMsgAndDigest(c, d);

			if (!isCurrentPrimary()) sendRetransmittableMsgToReplica(c, currentPrimary(), s);
		}


		void ReplicaImp::onMessage(PartialCommitProofMsg* msg)
		{
			const SeqNum msgSeqNum = msg->seqNumber();
			const SeqNum msgView = msg->viewNumber();
			const NodeIdType msgSender = msg->senderId();

			Assert(repsInfo->isIdOfPeerReplica(msgSender));
			Assert(repsInfo->isCollectorForPartialProofs(msgView, msgSeqNum));

			LOG_INFO_F(GL, "Node %d received PartialCommitProofMsg (size=%d) from node %d for seqNumber %" PRId64 "",
				(int)myReplicaId, (int)msg->size(), (int)msgSender, msgSeqNum);

			if (relevantMsgForActiveView(msg))
			{
				sendAckIfNeeded(msg, msgSender, msgSeqNum);

				if (msgSeqNum > lastExecutedSeqNum)
				{
					SeqNumInfo& seqNumInfo = mainLog->get(msgSeqNum);
					PartialProofsSet& pps = seqNumInfo.partialProofs();

					if (pps.addMsg(msg))
					{
						// GG: The following block is not needed (pps.hasFullProof() will always be false) because we create the combined signature in another thread. TODO(GG): verify
						if (pps.hasFullProof()) 
						{
							Assert(seqNumInfo.hasPrePrepareMsg());
							commitAndSendFullCommitProof(msgSeqNum, seqNumInfo, pps);
						}

						return;
					}
				}
			}

			delete msg;
			return;
		}




		void ReplicaImp::onInternalMsg(FullCommitProofMsg* msg)
		{
			if ((stateTransfer->isCollectingState()) || 
				(!currentViewIsActive()) || 
				(curView != msg->viewNumber()) || 
				(!mainLog->insideActiveWindow(msg->seqNumber())))
			{
				delete msg;
				return;
			}

			sendToAllOtherReplicas(msg);

			onMessage(msg);
		}

		void ReplicaImp::onMessage(FullCommitProofMsg* msg)
		{

			LOG_INFO_F(GL, "Node %d received FullCommitProofMsg message for seqNumber %" PRId64 "",
				(int)myReplicaId, (int)msg->seqNumber());

			const SeqNum msgSeqNum = msg->seqNumber();

			if (relevantMsgForActiveView(msg))
			{
				SeqNumInfo& seqNumInfo = mainLog->get(msgSeqNum);
				PartialProofsSet& pps = seqNumInfo.partialProofs();

				if (!pps.hasFullProof() && pps.addMsg(msg)) // TODO(GG): consider to verify the signature in another thread
				{
					Assert(seqNumInfo.hasPrePrepareMsg());

					commitFullCommitProof(msgSeqNum, seqNumInfo);

					return;
				}
			}

			delete msg;
			return;
		}



		void ReplicaImp::onMessage(PreparePartialMsg* msg)
		{
			const SeqNum msgSeqNum = msg->seqNumber();
			const ReplicaId msgSender = msg->senderId();

			bool msgAdded = false;

			if (relevantMsgForActiveView(msg))
			{
				Assert(isCurrentPrimary());
		
				sendAckIfNeeded(msg, msgSender, msgSeqNum);
		
				LOG_INFO_F(GL, "Node %d received PreparePartialMsg from node %d for seqNumber %" PRId64 "", (int)myReplicaId, (int)msgSender, msgSeqNum);
		
				controller->onMessage(msg);
		
				SeqNumInfo& seqNumInfo = mainLog->get(msgSeqNum);
		
				FullCommitProofMsg* fcp = seqNumInfo.partialProofs().getFullProof();
		
				CommitFullMsg* commitFull = seqNumInfo.getValidCommitFullMsg();
		
				PrepareFullMsg* preFull = seqNumInfo.getValidPrepareFullMsg();
		
				if (fcp != nullptr)
				{
					send(fcp, msgSender);
				}
				else if (commitFull != nullptr)
				{
					send(commitFull, msgSender);
				}
				else if (preFull != nullptr)
				{
					send(preFull, msgSender);
				}
				else
				{
					msgAdded = seqNumInfo.addMsg(msg);
				}
			}

			if (!msgAdded)
			{
				LOG_INFO_F(GL, "Node %d ignored the PreparePartialMsg from node %d (seqNumber %" PRId64 ")", (int)myReplicaId, (int)msgSender, msgSeqNum);
				delete msg;
			}
		}


		void ReplicaImp::onMessage(CommitPartialMsg* msg)
		{
			const SeqNum msgSeqNum = msg->seqNumber();
			const ReplicaId msgSender = msg->senderId();

			bool msgAdded = false;

			if (relevantMsgForActiveView(msg))
			{
				Assert(isCurrentPrimary());
		
				sendAckIfNeeded(msg, msgSender, msgSeqNum);
		
				LOG_INFO_F(GL, "Node %d received CommitPartialMsg from node %d for seqNumber %" PRId64 "", (int)myReplicaId, (int)msgSender, msgSeqNum);
		
				SeqNumInfo& seqNumInfo = mainLog->get(msgSeqNum);
		
				FullCommitProofMsg* fcp = seqNumInfo.partialProofs().getFullProof();
		
				CommitFullMsg* commitFull = seqNumInfo.getValidCommitFullMsg();
		
				if (fcp != nullptr)
				{
					send(fcp, msgSender);
				}
				else if (commitFull != nullptr)
				{
					send(commitFull, msgSender);
				}
				else
				{
					msgAdded = seqNumInfo.addMsg(msg);
				}
			}

			if (!msgAdded)
			{
				LOG_INFO_F(GL, "Node %d ignored the CommitPartialMsg from node %d (seqNumber %" PRId64 ")", (int)myReplicaId, (int)msgSender, msgSeqNum);
				delete msg;
			}
		}

		void ReplicaImp::onMessage(PrepareFullMsg* msg)
		{
			const SeqNum msgSeqNum = msg->seqNumber();
			const ReplicaId msgSender = msg->senderId();

			bool msgAdded = false;

			if (relevantMsgForActiveView(msg))
			{
				sendAckIfNeeded(msg, msgSender, msgSeqNum);

				LOG_INFO_F(GL, "Node %d received PrepareFullMsg for seqNumber %" PRId64 "", (int)myReplicaId, msgSeqNum);

				SeqNumInfo& seqNumInfo = mainLog->get(msgSeqNum);

				FullCommitProofMsg* fcp = seqNumInfo.partialProofs().getFullProof();

				CommitFullMsg* commitFull = seqNumInfo.getValidCommitFullMsg();

				PrepareFullMsg* preFull = seqNumInfo.getValidPrepareFullMsg();

				if (fcp != nullptr)
				{
					send(fcp, msgSender);
				}
				else if (commitFull != nullptr)
				{
					send(commitFull, msgSender);
				}
				else if (preFull != nullptr)
				{
					// nop
				}
				else
				{
					msgAdded = seqNumInfo.addMsg(msg);
				}
			}

			if (!msgAdded)
			{
				LOG_INFO_F(GL, "Node %d ignored the PrepareFullMsg from node %d (seqNumber %" PRId64 ")", (int)myReplicaId, (int)msgSender, msgSeqNum);
				delete msg;
			}
		}


		void ReplicaImp::onMessage(CommitFullMsg* msg)
		{
			const SeqNum msgSeqNum = msg->seqNumber();
			const ReplicaId msgSender = msg->senderId();

			bool msgAdded = false;

			if (relevantMsgForActiveView(msg))
			{
				sendAckIfNeeded(msg, msgSender, msgSeqNum);

				LOG_INFO_F(GL, "Node %d received CommitFullMsg for seqNumber %" PRId64 "", (int)myReplicaId, msgSeqNum);

				SeqNumInfo& seqNumInfo = mainLog->get(msgSeqNum);

				FullCommitProofMsg* fcp = seqNumInfo.partialProofs().getFullProof();

				CommitFullMsg* commitFull = seqNumInfo.getValidCommitFullMsg();

				if (fcp != nullptr)
				{
					send(fcp, msgSender); // TODO(GG): do we really want to send this message ? (msgSender already has a CommitFullMsg for the same seq number)
				}
				else if (commitFull != nullptr)
				{
					// nop
				}
				else
				{
					msgAdded = seqNumInfo.addMsg(msg);
				}
			}

			if (!msgAdded)
			{
				LOG_INFO_F(GL, "Node %d ignored the CommitFullMsg from node %d (seqNumber %" PRId64 ")", (int)myReplicaId, (int)msgSender, msgSeqNum);
				delete msg;
			}
		}



		void ReplicaImp::onPrepareCombinedSigFailed(SeqNum seqNumber, ViewNum  v, const std::set<uint16_t>& replicasWithBadSigs)
		{
			LOG_INFO_F(GL, "Node %d - onPrepareCombinedSigFailed seqNumber=%" PRId64 " view=%" PRId64 "", (int)myReplicaId, seqNumber, v);

			if ((stateTransfer->isCollectingState()) || 
				(!currentViewIsActive()) || 
				(curView != v) || 
				(!mainLog->insideActiveWindow(seqNumber)))
			{
				LOG_INFO_F(GL, "Node %d - onPrepareCombinedSigFailed - seqNumber=%" PRId64 " view=%" PRId64 " are not relevant",
					(int)myReplicaId, seqNumber, v);

				return;
			}

			SeqNumInfo& seqNumInfo = mainLog->get(seqNumber);

			seqNumInfo.onCompletionOfPrepareSignaturesProcessing(seqNumber, v, replicasWithBadSigs);

			//TODO(GG): add logic that handles bad replicas ...
		}


		void ReplicaImp::onPrepareCombinedSigSucceeded(SeqNum seqNumber, ViewNum  v, const char* combinedSig, uint16_t combinedSigLen)
		{
			LOG_INFO_F(GL, "Node %d - onPrepareCombinedSigSucceeded seqNumber=%" PRId64 " view=%" PRId64 "", (int)myReplicaId, seqNumber, v);

			if ((stateTransfer->isCollectingState()) || 
				(!currentViewIsActive()) || 
				(curView != v) || 
				(!mainLog->insideActiveWindow(seqNumber)))
			{
				LOG_INFO_F(GL, "Node %d - onPrepareCombinedSigSucceeded - seqNumber=%" PRId64 " view=%" PRId64 " are not relevant",
					(int)myReplicaId, seqNumber, v);

				return;
			}

			SeqNumInfo& seqNumInfo = mainLog->get(seqNumber);

			seqNumInfo.onCompletionOfPrepareSignaturesProcessing(seqNumber, v, combinedSig, combinedSigLen);

			FullCommitProofMsg* fcp = seqNumInfo.partialProofs().getFullProof();

			PrepareFullMsg* preFull = seqNumInfo.getValidPrepareFullMsg();

			Assert(preFull != nullptr);

			if (fcp != nullptr)  return;// don't send if we already have FullCommitProofMsg

			for (ReplicaId x : repsInfo->idsOfPeerReplicas())
				sendRetransmittableMsgToReplica(preFull, x, seqNumber);

			Assert(seqNumInfo.isPrepared());

			sendCommitPartial(seqNumber);
		}

		void ReplicaImp::onPrepareVerifyCombinedSigResult(SeqNum seqNumber, ViewNum  v, bool isValid)
		{
			LOG_INFO_F(GL, "Node %d - onPrepareVerifyCombinedSigResult seqNumber=%" PRId64 " view=%" PRId64 "", (int)myReplicaId, seqNumber, v);

			if ((stateTransfer->isCollectingState()) || 
				(!currentViewIsActive()) || 
				(curView != v) || 
				(!mainLog->insideActiveWindow(seqNumber)))
			{
				LOG_INFO_F(GL, "Node %d - onPrepareVerifyCombinedSigResult - seqNumber=%" PRId64 " view=%" PRId64 " are not relevant",
					(int)myReplicaId, seqNumber, v);

				return;
			}

			SeqNumInfo& seqNumInfo = mainLog->get(seqNumber);

			seqNumInfo.onCompletionOfCombinedPrepareSigVerification(seqNumber, v, isValid);

			if (!isValid) return; // TODO(GG): we should do something about the replica that sent this invalid message 

			FullCommitProofMsg* fcp = seqNumInfo.partialProofs().getFullProof();

			if (fcp != nullptr)  return;// don't send if we already have FullCommitProofMsg

			Assert(seqNumInfo.isPrepared());

			sendCommitPartial(seqNumber);
		}


		void ReplicaImp::onCommitCombinedSigFailed(SeqNum seqNumber, ViewNum  v, const std::set<uint16_t>& replicasWithBadSigs)
		{
			LOG_INFO_F(GL, "Node %d - onCommitCombinedSigFailed seqNumber=%" PRId64 " view=%" PRId64 "", (int)myReplicaId, seqNumber, v);

			if ((stateTransfer->isCollectingState()) || 
				(!currentViewIsActive()) || 
				(curView != v) || 
				(!mainLog->insideActiveWindow(seqNumber)))
			{
				LOG_INFO_F(GL, "Node %d - onCommitCombinedSigFailed - seqNumber=%" PRId64 " view=%" PRId64 " are not relevant",
					(int)myReplicaId, seqNumber, v);

				return;
			}

			SeqNumInfo& seqNumInfo = mainLog->get(seqNumber);

			seqNumInfo.onCompletionOfCommitSignaturesProcessing(seqNumber, v, replicasWithBadSigs);

			//TODO(GG): add logic that handles bad replicas ...
		}

		void ReplicaImp::onCommitCombinedSigSucceeded(SeqNum seqNumber, ViewNum  v, const char* combinedSig, uint16_t combinedSigLen)
		{
			LOG_INFO_F(GL, "Node %d - onCommitCombinedSigSucceeded seqNumber=%" PRId64 " view=%" PRId64 "", (int)myReplicaId, seqNumber, v);

			if ((stateTransfer->isCollectingState()) || 
				(!currentViewIsActive()) || 
				(curView != v) || 
				(!mainLog->insideActiveWindow(seqNumber)))
			{
				LOG_INFO_F(GL, "Node %d - onCommitCombinedSigSucceeded - seqNumber=%" PRId64 " view=%" PRId64 " are not relevant",
					(int)myReplicaId, seqNumber, v);

				return;
			}

			SeqNumInfo& seqNumInfo = mainLog->get(seqNumber);

			seqNumInfo.onCompletionOfCommitSignaturesProcessing(seqNumber, v, combinedSig, combinedSigLen);

			FullCommitProofMsg* fcp = seqNumInfo.partialProofs().getFullProof();
			CommitFullMsg* commitFull = seqNumInfo.getValidCommitFullMsg();

			Assert(commitFull != nullptr);

			if (fcp != nullptr)  return;// ignore if we already have FullCommitProofMsg

			for (ReplicaId x : repsInfo->idsOfPeerReplicas())
				sendRetransmittableMsgToReplica(commitFull, x, seqNumber);

			Assert(seqNumInfo.isCommitted__gg());

			bool askForMissingInfoAboutCommittedItems = (seqNumber > lastExecutedSeqNum + maxConcurrentAgreementsByPrimary);

			executeReadWriteRequests(askForMissingInfoAboutCommittedItems);
		}


		void ReplicaImp::onCommitVerifyCombinedSigResult(SeqNum seqNumber, ViewNum  v, bool isValid)
		{
			LOG_INFO_F(GL, "Node %d - onCommitVerifyCombinedSigResult seqNumber=%" PRId64 " view=%" PRId64 "", myReplicaId, seqNumber, v);

			if ((stateTransfer->isCollectingState()) || 
				(!currentViewIsActive()) || 
				(curView != v) || 
				(!mainLog->insideActiveWindow(seqNumber)))
			{
				LOG_INFO_F(GL, "Node %d - onCommitVerifyCombinedSigResult - seqNumber=%" PRId64 " view=%" PRId64 " are not relevant",
					(int)myReplicaId, seqNumber, v);

				return;
			}

			SeqNumInfo& seqNumInfo = mainLog->get(seqNumber);

			seqNumInfo.onCompletionOfCombinedCommitSigVerification(seqNumber, v, isValid);

			if (!isValid) return; // TODO(GG): we should do something about the replica that sent this invalid message 

			Assert(seqNumInfo.isCommitted__gg());

			bool askForMissingInfoAboutCommittedItems = (seqNumber > lastExecutedSeqNum + maxConcurrentAgreementsByPrimary);

			executeReadWriteRequests(askForMissingInfoAboutCommittedItems);
		}




		void ReplicaImp::onMessage(CheckpointMsg* msg)
		{
			const ReplicaId msgSenderId = msg->senderId();
			const SeqNum msgSeqNum = msg->seqNumber();
			const Digest msgDigest = msg->digestOfState();
			const bool msgIsStable = msg->isStableState();

			LOG_INFO_F(GL, "Node %d received Checkpoint message from node %d for seqNumber %" PRId64 " (size=%d, stable=%s, digestPrefix=%d)",
				(int)myReplicaId, (int)msgSenderId, msgSeqNum, (int)msg->size(), (int)msgIsStable ? "true" : "false", *((int*)(&msgDigest)));

			if ((msgSeqNum > lastStableSeqNum) && (msgSeqNum <= lastStableSeqNum + kWorkWindowSize))
			{
				Assert(mainLog->insideActiveWindow(msgSeqNum));
				CheckpointInfo& checkInfo = checkpointsLog->get(msgSeqNum);
				bool msgAdded = checkInfo.addCheckpointMsg(msg, msg->senderId());

				if (msgAdded) LOG_INFO_F(GL, "Node %d added Checkpoint message from node %d for seqNumber %" PRId64 "",
					(int)myReplicaId, (int)msgSenderId, msgSeqNum);

				if (checkInfo.isCheckpointCertificateComplete())
				{
					Assert(checkInfo.selfCheckpointMsg() != nullptr);
					onSeqNumIsStable(msgSeqNum);

					return;
				}
			}
			else
			{
				delete msg;
			}

			bool askForStateTransfer = false;

			if (msgIsStable && msgSeqNum > lastExecutedSeqNum)
			{
				auto pos = tableOfStableCheckpoints.find(msgSenderId);
				if (pos == tableOfStableCheckpoints.end() || pos->second->seqNumber() < msgSeqNum)
				{
					if (pos != tableOfStableCheckpoints.end()) delete pos->second;
					CheckpointMsg* x = new CheckpointMsg(msgSenderId, msgSeqNum, msgDigest, msgIsStable);
					tableOfStableCheckpoints[msgSenderId] = x;

					LOG_INFO_F(GL, "Node %d added stable Checkpoint message to tableOfStableCheckpoints (message from node %d for seqNumber %" PRId64 ")", (int)myReplicaId, (int)msgSenderId, msgSeqNum);

					if ((uint16_t)tableOfStableCheckpoints.size() >= fVal + 1)
					{
						uint16_t numRelevant = 0;
						uint16_t numRelevantAboveWindow = 0;
						auto tableItrator = tableOfStableCheckpoints.begin();
						while (tableItrator != tableOfStableCheckpoints.end())
						{
							if (tableItrator->second->seqNumber() <= lastExecutedSeqNum)
							{
								delete tableItrator->second;
								tableItrator = tableOfStableCheckpoints.erase(tableItrator);
							}
							else
							{
								numRelevant++;
								if (tableItrator->second->seqNumber() > lastStableSeqNum + kWorkWindowSize)
									numRelevantAboveWindow++;
								tableItrator++;
							}
						}
						Assert(numRelevant == tableOfStableCheckpoints.size());

						LOG_INFO_F(GL, "numRelevant=%d    numRelevantAboveWindow=%d", (int)numRelevant, (int)numRelevantAboveWindow);

						if (numRelevantAboveWindow >= fVal + 1)
						{
							askForStateTransfer = true;
						}
						else if (numRelevant >= fVal + 1)
						{
							Time timeOfLastCommit = MinTime;
							if (mainLog->insideActiveWindow(lastExecutedSeqNum)) timeOfLastCommit = mainLog->get(lastExecutedSeqNum).lastUpdateTimeOfCommitMsgs();

							if (absDifference(getMonotonicTime(), timeOfLastCommit)  > (1000 * timeToWaitBeforeStartingStateTransferInMainWindowMilli)) askForStateTransfer = true;
						}
					}
				}
			}

			if (askForStateTransfer)
			{
				LOG_INFO_F(GL, "call to startCollectingState()");
				stateTransfer->startCollectingState();
			}
			else if (msgSeqNum > lastStableSeqNum + kWorkWindowSize)
			{
				onReportAboutAdvancedReplica(msgSenderId, msgSeqNum);
			}
			else if (msgSeqNum + kWorkWindowSize < lastStableSeqNum)
			{
				onReportAboutLateReplica(msgSenderId, msgSeqNum);
			}
		}


		bool ReplicaImp::handledByRetransmissionsManager(const ReplicaId sourceReplica, const ReplicaId destReplica,
			const ReplicaId primaryReplica, const SeqNum seqNum, const uint16_t msgType)
		{
			Assert(retransmissionsLogicEnabled);

			if (sourceReplica == destReplica) return false;

			const bool sourcePrimary = (sourceReplica == primaryReplica);

			if (sourcePrimary && ((msgType == MsgCode::PrePrepare) || (msgType == MsgCode::StartSlowCommit)))
				return true;

			const bool dstPrimary = (destReplica == primaryReplica);

			if (dstPrimary && ((msgType == MsgCode::PreparePartial) || (msgType == MsgCode::CommitPartial)))
				return true;

			//  TODO(GG): do we want to use acks for FullCommitProofMsg ?

			if (msgType == MsgCode::PartialCommitProof)
			{
				const bool destIsCollector = repsInfo->getCollectorsForPartialProofs(destReplica, curView, seqNum, nullptr, nullptr);
				if (destIsCollector) return true;
			}

			return false;
		}


		void ReplicaImp::sendAckIfNeeded(MessageBase* msg, const NodeIdType sourceNode, const SeqNum seqNum)
		{
			if (!retransmissionsLogicEnabled) return;

			if (!repsInfo->isIdOfPeerReplica(sourceNode)) return;

			if (handledByRetransmissionsManager(sourceNode, myReplicaId, currentPrimary(), seqNum, msg->type()))
			{
				SimpleAckMsg* ackMsg = new SimpleAckMsg(seqNum, curView, myReplicaId, msg->type());

				send(ackMsg, sourceNode);

				delete ackMsg;
			}
		}


		void ReplicaImp::sendRetransmittableMsgToReplica(MessageBase* msg, ReplicaId destReplica, SeqNum s, bool ignorePreviousAcks)
		{
			send(msg, destReplica);

			if (!retransmissionsLogicEnabled) return;

			if (handledByRetransmissionsManager(myReplicaId, destReplica, currentPrimary(), s, msg->type()))
				retransmissionsManager->onSend(destReplica, s, msg->type(), ignorePreviousAcks);
		}



		void ReplicaImp::onRetransmissionsTimer(Time cTime, Timer& timer)
		{
			Assert(retransmissionsLogicEnabled);

			retransmissionsManager->tryToStartProcessing();
		}



		void ReplicaImp::onRetransmissionsProcessingResults(SeqNum relatedLastStableSeqNum, const ViewNum relatedViewNumber,
			const std::forward_list<RetSuggestion>* const suggestedRetransmissions)
		{
			Assert(retransmissionsLogicEnabled);

			if (stateTransfer->isCollectingState() || (relatedViewNumber != curView) || (!currentViewIsActive())) return;
			if (relatedLastStableSeqNum + kWorkWindowSize <= lastStableSeqNum) return;

			const uint16_t myId = myReplicaId;
			const uint16_t primaryId = currentPrimary();

			for (const RetSuggestion& s : *suggestedRetransmissions)
			{
				if ((s.msgSeqNum <= lastStableSeqNum) || (s.msgSeqNum > lastStableSeqNum + kWorkWindowSize)) continue;

				Assert(s.replicaId != myId);

				Assert(handledByRetransmissionsManager(myId, s.replicaId, primaryId, s.msgSeqNum, s.msgType));

				switch (s.msgType) {
				case MsgCode::PrePrepare:
				{
					SeqNumInfo& seqNumInfo = mainLog->get(s.msgSeqNum);
					PrePrepareMsg* msgToSend = seqNumInfo.getSelfPrePrepareMsg();
					Assert(msgToSend != nullptr);
					sendRetransmittableMsgToReplica(msgToSend, s.replicaId, s.msgSeqNum);
					LOG_INFO_F(GL, "Replica %d retransmits to replica %d PrePrepareMsg with seqNumber %" PRId64 "", (int)myId, (int)s.replicaId, s.msgSeqNum);
				}
				break;
				case MsgCode::PartialCommitProof:
				{
					SeqNumInfo& seqNumInfo = mainLog->get(s.msgSeqNum);
					PartialCommitProofMsg* msgToSend = seqNumInfo.partialProofs().getSelfPartialCommitProof();
					Assert(msgToSend != nullptr);
					sendRetransmittableMsgToReplica(msgToSend, s.replicaId, s.msgSeqNum);
					LOG_INFO_F(GL, "Replica %d retransmits to replica %d PartialCommitProofMsg with seqNumber %" PRId64 "", (int)myId, (int)s.replicaId, s.msgSeqNum);
				}
				break;
				/*  TODO(GG): do we want to use acks for FullCommitProofMsg ?
				*/
				case MsgCode::StartSlowCommit:
				{
					StartSlowCommitMsg* msgToSend = new StartSlowCommitMsg(myId, curView, s.msgSeqNum);
					sendRetransmittableMsgToReplica(msgToSend, s.replicaId, s.msgSeqNum);
					delete msgToSend;
					LOG_INFO_F(GL, "Replica %d retransmits to replica %d StartSlowCommitMsg with seqNumber %" PRId64 "", (int)myId, (int)s.replicaId, s.msgSeqNum);
				}
				break;
				case MsgCode::PreparePartial:
				{
					SeqNumInfo& seqNumInfo = mainLog->get(s.msgSeqNum);
					PreparePartialMsg* msgToSend = seqNumInfo.getSelfPreparePartialMsg();
					Assert(msgToSend != nullptr);
					sendRetransmittableMsgToReplica(msgToSend, s.replicaId, s.msgSeqNum);
					LOG_INFO_F(GL, "Replica %d retransmits to replica %d PreparePartialMsg with seqNumber %" PRId64 "", (int)myId, (int)s.replicaId, s.msgSeqNum);
				}
				break;
				case MsgCode::PrepareFull:
				{
					SeqNumInfo& seqNumInfo = mainLog->get(s.msgSeqNum);
					PrepareFullMsg* msgToSend = seqNumInfo.getValidPrepareFullMsg();
					Assert(msgToSend != nullptr);
					sendRetransmittableMsgToReplica(msgToSend, s.replicaId, s.msgSeqNum);
					LOG_INFO_F(GL, "Replica %d retransmits to replica %d PrepareFullMsg with seqNumber %" PRId64 "", (int)myId, (int)s.replicaId, s.msgSeqNum);
				}
				break;

				case MsgCode::CommitPartial:
				{
					SeqNumInfo& seqNumInfo = mainLog->get(s.msgSeqNum);
					CommitPartialMsg* msgToSend = seqNumInfo.getSelfCommitPartialMsg();
					Assert(msgToSend != nullptr);
					sendRetransmittableMsgToReplica(msgToSend, s.replicaId, s.msgSeqNum);
					LOG_INFO_F(GL, "Replica %d retransmits to replica %d CommitPartialMsg with seqNumber %" PRId64 "", (int)myId, (int)s.replicaId, s.msgSeqNum);
				}
				break;

				case MsgCode::CommitFull:
				{
					SeqNumInfo& seqNumInfo = mainLog->get(s.msgSeqNum);
					CommitFullMsg* msgToSend = seqNumInfo.getValidCommitFullMsg();
					Assert(msgToSend != nullptr);
					sendRetransmittableMsgToReplica(msgToSend, s.replicaId, s.msgSeqNum);
					LOG_INFO_F(GL, "Replica %d retransmits to replica %d CommitFullMsg with seqNumber %" PRId64 "", (int)myId, (int)s.replicaId, s.msgSeqNum);
				}
				break;

				default:
					Assert(false);
				}
			}
		}



		void ReplicaImp::onMessage(ReplicaStatusMsg* msg)
		{
			// TODO(GG): we need filter for msgs (to avoid denial of service attack) + avoid sending messages at a high rate.
			// TODO(GG): for some communication modules/protocols, we can also utilize information about connection/disconnection. 

			const ReplicaId msgSenderId = msg->senderId();
			const SeqNum msgLastStable = msg->getLastStableSeqNum();
			const ViewNum msgViewNum = msg->getViewNumber();
			Assert(msgLastStable % checkpointWindowSize == 0);

			LOG_INFO_F(GL, "Node %d received ReplicaStatusMsg from node %d", (int)myReplicaId, (int)msgSenderId);

			/////////////////////////////////////////////////////////////////////////
			// Checkpoints
			/////////////////////////////////////////////////////////////////////////

			if (lastStableSeqNum > msgLastStable + kWorkWindowSize)
			{
				CheckpointMsg* checkMsg = checkpointsLog->get(lastStableSeqNum).selfCheckpointMsg();

				if (checkMsg == nullptr || !checkMsg->isStableState())
				{
					// TODO(GG): warning	
				}
				else
				{
					send(checkMsg, msgSenderId);
				}

				delete msg;
				return;
			}
			else if (msgLastStable > lastStableSeqNum + kWorkWindowSize)
			{
				tryToSendStatusReport(); // ask for help
			}
			else
			{
				// Send checkpoints that may be useful for msgSenderId
				const SeqNum beginRange = std::max(checkpointsLog->currentActiveWindow().first, msgLastStable + checkpointWindowSize);
				const SeqNum endRange = std::min(checkpointsLog->currentActiveWindow().second, msgLastStable + kWorkWindowSize);

				Assert(beginRange % checkpointWindowSize == 0);

				if (beginRange <= endRange)
				{
					Assert(endRange - beginRange <= kWorkWindowSize);

					for (SeqNum i = beginRange; i <= endRange; i = i + checkpointWindowSize)
					{
						CheckpointMsg* checkMsg = checkpointsLog->get(i).selfCheckpointMsg();
						if (checkMsg != nullptr) send(checkMsg, msgSenderId);
					}
				}
			}


			/////////////////////////////////////////////////////////////////////////
			// msgSenderId in older view
			/////////////////////////////////////////////////////////////////////////

			if (msgViewNum < curView)
			{
				ViewChangeMsg* myVC = viewsManager->getMyLatestViewChangeMsg();
				Assert(myVC != nullptr); // becuase curView>0
				send(myVC, msgSenderId);
			}

			/////////////////////////////////////////////////////////////////////////
			// msgSenderId needes information to enter view curView
			/////////////////////////////////////////////////////////////////////////

			else if ((msgViewNum == curView) && (!msg->currentViewIsActive()))
			{
				if (isCurrentPrimary() || (repsInfo->primaryOfView(curView) == msgSenderId)) // if the primary is involved
				{
					if (!isCurrentPrimary()) // I am not the primary of curView
					{
						// send ViewChangeMsg
						if (msg->hasListOfMissingViewChangeMsgForViewChange() && msg->isMissingViewChangeMsgForViewChange(myReplicaId))
						{
							ViewChangeMsg* myVC = viewsManager->getMyLatestViewChangeMsg();
							Assert(myVC != nullptr);
							send(myVC, msgSenderId);
						}
					}
					else // I am the primary of curView
					{
						// send NewViewMsg 
						if (!msg->currentViewHasNewViewMessage() && viewsManager->viewIsActive(curView))
						{
							NewViewMsg* nv = viewsManager->getMyNewViewMsgForCurrentView();
							Assert(nv != nullptr);
							send(nv, msgSenderId);
						}

						// send ViewChangeMsg 
						if (msg->hasListOfMissingViewChangeMsgForViewChange() && msg->isMissingViewChangeMsgForViewChange(myReplicaId))
						{
							ViewChangeMsg* myVC = viewsManager->getMyLatestViewChangeMsg();
							Assert(myVC != nullptr);
							send(myVC, msgSenderId);
						}
						// TODO(GG): send all VC msgs that can help making progress (needed because the original senders may not send the ViewChangeMsg msgs used by the primary)

						// TODO(GG): if viewsManager->viewIsActive(curView), we can send only the VC msgs which are really needed for curView (see in ViewsManager)
					}

					if (viewsManager->viewIsActive(curView))
					{
						if (msg->hasListOfMissingPrePrepareMsgForViewChange())
						{
							for (SeqNum i = msgLastStable + 1; i <= msgLastStable + kWorkWindowSize; i++)
							{
								if (mainLog->insideActiveWindow(i) && msg->isMissingPrePrepareMsgForViewChange(i))
								{
									PrePrepareMsg* prePrepareMsg = mainLog->get(i).getPrePrepareMsg();
									if (prePrepareMsg != nullptr) send(prePrepareMsg, msgSenderId);
								}
							}
						}
					}
					else // if I am also not in curView --- In this case we take messages from viewsManager 
					{
						if (msg->hasListOfMissingPrePrepareMsgForViewChange())
						{
							for (SeqNum i = msgLastStable + 1; i <= msgLastStable + kWorkWindowSize; i++)
							{
								if (msg->isMissingPrePrepareMsgForViewChange(i))
								{
									PrePrepareMsg* prePrepareMsg = viewsManager->getPrePrepare(i); // TODO(GG): we can avoid sending misleading message by using the digest of the expected pre prepare message
									if (prePrepareMsg != nullptr) send(prePrepareMsg, msgSenderId);
								}
							}
						}
					}
				}
			}

			/////////////////////////////////////////////////////////////////////////
			// msgSenderId is also in view curView
			/////////////////////////////////////////////////////////////////////////

			else if ((msgViewNum == curView) && msg->currentViewIsActive())
			{
				if (isCurrentPrimary())
				{
					if (viewsManager->viewIsActive(curView))
					{
						SeqNum beginRange = std::max(lastStableSeqNum + 1, msg->getLastExecutedSeqNum() + 1); // Notice that after a view change, we don't have to pass the PrePrepare messages from the previous view. TODO(GG): verify
						SeqNum endRange = std::min(lastStableSeqNum + kWorkWindowSize, msgLastStable + kWorkWindowSize);

						for (SeqNum i = beginRange; i <= endRange; i++)
						{
							if (msg->isPrePrepareInActiveWindow(i)) continue;

							PrePrepareMsg *prePrepareMsg = mainLog->get(i).getSelfPrePrepareMsg();
							if (prePrepareMsg != 0) send(prePrepareMsg, msgSenderId);
						}


					}
					else
					{
						tryToSendStatusReport();
					}
				}
				else
				{
					// TODO(GG): TBD
				}
			}

			/////////////////////////////////////////////////////////////////////////
			// msgSenderId is in a newer view curView
			/////////////////////////////////////////////////////////////////////////
			else
			{
				Assert(msgViewNum > curView);
				tryToSendStatusReport();
			}

			delete msg;
		}


		void ReplicaImp::tryToSendStatusReport()
		{
			// TODO(GG): in some cases, we can limit the amount of such messages by using information about connection/disconnection (from the communication module)
			// TODO(GG): explain that the current "Status Report" approch is relatively simple (it can be more efficient and sophisticated).

			const Time currentTime = getMonotonicTime();

			const int64_t milliSinceLastTime = (int64_t)(absDifference(currentTime, lastTimeThisReplicaSentStatusReportMsgToAllPeerReplicas) / 1000);

			if (milliSinceLastTime < minTimeBetweenStatusRequestsMilli)	return;

			const int64_t dynamicMinTimeBetweenStatusRequestsMilli =
				(int64_t)((double)dynamicUpperLimitOfRounds->upperLimit() * factorForMinTimeBetweenStatusRequestsMilli);

			if (milliSinceLastTime <  dynamicMinTimeBetweenStatusRequestsMilli) return;

			lastTimeThisReplicaSentStatusReportMsgToAllPeerReplicas = currentTime;  // TODO(GG): handle restart/pause !! (restart/pause may affect time measurements...)

			const bool viewIsActive = currentViewIsActive();
			const bool hasNewChangeMsg = viewsManager->hasNewViewMessage(curView);
			const bool listOfPPInActiveWindow = viewIsActive;
			const bool listOfMissingVCMsg = !viewIsActive && !viewsManager->viewIsPending(curView);
			const bool listOfMissingPPMsg = !viewIsActive && viewsManager->viewIsPending(curView);

			ReplicaStatusMsg msg(myReplicaId, curView, lastStableSeqNum, lastExecutedSeqNum,
				viewIsActive, hasNewChangeMsg, listOfPPInActiveWindow, listOfMissingVCMsg, listOfMissingPPMsg);

			if (listOfPPInActiveWindow)
			{
				const SeqNum start = lastStableSeqNum + 1;
				const SeqNum end = lastStableSeqNum + kWorkWindowSize;

				for (SeqNum i = start; i <= end; i++)
				{
					if (mainLog->get(i).hasPrePrepareMsg())
						msg.setPrePrepareInActiveWindow(i);
				}

			}
			if (listOfMissingVCMsg)
			{
				for (ReplicaId i : repsInfo->idsOfPeerReplicas())
				{
					if (!viewsManager->hasViewChangeMessageForFutureView(i))
						msg.setMissingViewChangeMsgForViewChange(i);
				}
			}
			else if (listOfMissingPPMsg)
			{
				std::vector<SeqNum> missPP;
				if (viewsManager->getNumbersOfMissingPP(lastStableSeqNum, &missPP))
				{
					for (SeqNum i : missPP)
					{
						Assert((i > lastStableSeqNum) && (i <= lastStableSeqNum + kWorkWindowSize));
						msg.setMissingPrePrepareMsgForViewChange(i);
					}
				}
			}

			sendToAllOtherReplicas(&msg);
		}


		void ReplicaImp::onMessage(ViewChangeMsg* msg)
		{
			if (!viewChangeProtocolEnabled) { delete msg; return; }

			const ReplicaId generatedReplicaId = msg->idOfGeneratedReplica(); // Notice that generatedReplicaId may be != msg->senderId()
			Assert(generatedReplicaId != myReplicaId);

			LOG_INFO_F(GL, "Node %d received ViewChangeMsg (generatedReplicaId=%d, newView=%" PRId64 ", lastStable=%" PRId64 ", numberOfElements=%d).",
				(int)myReplicaId, (int)generatedReplicaId, msg->newView(), msg->lastStable(), (int)msg->numberOfElements());

			bool msgAdded = viewsManager->add(msg);

			LOG_INFO_F(GL, "ViewChangeMsg add=%d", (int)msgAdded);

			if (!msgAdded) return;

			// if the current primary wants to leave view
			if (generatedReplicaId == currentPrimary() && msg->newView() > curView)
			{
				LOG_INFO_F(GL, "Primary asks to leave view (primary Id=%d , view=%" PRId64 ")", (int)generatedReplicaId, curView);
				MoveToHigherView(curView + 1);
			}

			ViewNum maxKnownCorrectView = 0;
			ViewNum maxKnownAgreedView = 0;
			viewsManager->computeCorrectRelevantViewNumbers(&maxKnownCorrectView, &maxKnownAgreedView);
			LOG_INFO_F(GL, "maxKnownCorrectView=%" PRId64 ", maxKnownAgreedView=%" PRId64 "", maxKnownCorrectView, maxKnownAgreedView);

			if (maxKnownCorrectView > curView)
			{
				// we have at least f+1 view-changes with view number >= maxKnownCorrectView
				MoveToHigherView(maxKnownCorrectView);

				// update maxKnownCorrectView and maxKnownAgreedView			// TODO(GG): consider to optimize (this part is not always needed)
				viewsManager->computeCorrectRelevantViewNumbers(&maxKnownCorrectView, &maxKnownAgreedView);
				LOG_INFO_F(GL, "maxKnownCorrectView=%" PRId64 ", maxKnownAgreedView=%" PRId64 "", maxKnownCorrectView, maxKnownAgreedView);
			}

			if (viewsManager->viewIsActive(curView)) return; // return, if we are still in the previous view

			if (maxKnownAgreedView != curView) return; // return, if we can't move to the new view yet

													   // Replica now has at least 2f+2c+1 ViewChangeMsg messages with view  >= curView

			if (lastAgreedView < curView)
			{
				lastAgreedView = curView;
                                metric_last_agreed_view_.Get().Set(lastAgreedView);
				timeOfLastAgreedView = getMonotonicTime();
			}

			tryToEnterView();
		}


		void ReplicaImp::onMessage(NewViewMsg* msg)
		{
			if (!viewChangeProtocolEnabled) { delete msg; return; }

			const ReplicaId senderId = msg->senderId();

			Assert(senderId != myReplicaId);  // should be verified in ViewChangeMsg	

			LOG_INFO_F(GL, "Node %d received NewViewMsg message (senderId=%d, newView=%" PRId64 ")",
				(int)myReplicaId, (int)senderId, msg->newView());

			bool added = viewsManager->add(msg);

			LOG_INFO_F(GL, "NewViewMsg add=%d", (int)added);

			if (!added)	return;

			if (viewsManager->viewIsActive(curView)) return; // return, if we are still in the previous view

			tryToEnterView();
		}


		void ReplicaImp::MoveToHigherView(ViewNum nextView)
		{
			Assert(viewChangeProtocolEnabled);

			Assert(curView < nextView);

			const bool wasInPrevViewNumber = viewsManager->viewIsActive(curView);

			LOG_INFO_F(GL, "**************** In MoveToHigherView (curView=%" PRId64 ", nextView=%" PRId64 ", wasInPrevViewNumber=%d)",
				curView, nextView, (int)wasInPrevViewNumber);

			ViewChangeMsg* pVC = nullptr;

			if (!wasInPrevViewNumber)
			{
				pVC = viewsManager->getMyLatestViewChangeMsg();
				Assert(pVC != nullptr);
				pVC->setNewViewNumber(nextView);
			}
			else
			{

				std::vector<ViewsManager::PrevViewInfo> prevViewInfo;
				for (SeqNum i = lastStableSeqNum + 1; i <= lastStableSeqNum + kWorkWindowSize; i++)
				{
					SeqNumInfo& seqNumInfo = mainLog->get(i);

					if (seqNumInfo.getPrePrepareMsg() != nullptr)
					{
						ViewsManager::PrevViewInfo x;

						seqNumInfo.getAndReset(x.prePrepare, x.prepareFull);
						x.hasAllRequests = true;

						Assert(x.prePrepare != nullptr);
						Assert(x.prePrepare->viewNumber() == curView);
						Assert(x.prepareFull == nullptr || x.hasAllRequests); // (x.prepareFull!=nullptr) ==> (x.hasAllRequests==true)
						Assert(x.prepareFull == nullptr || x.prepareFull->viewNumber() == curView); // (x.prepareFull!=nullptr) ==> (x.prepareFull->viewNumber() == curView)

						prevViewInfo.push_back(x);
					}
					else
					{
						seqNumInfo.resetAndFree();
					}
				}

				pVC = viewsManager->exitFromCurrentView(lastStableSeqNum, lastExecutedSeqNum, prevViewInfo);

				Assert(pVC != nullptr);
				pVC->setNewViewNumber(nextView);
			}

			curView = nextView;
                        metric_view_.Get().Set(nextView);

			LOG_INFO_F(GL, "Sending view change message: new view=%" PRId64 ", wasInPrevViewNumber=%d, new primary=%d, lastExecutedSeqNum=%" PRId64 ", lastStableSeqNum=%" PRId64 "",
				curView, (int)wasInPrevViewNumber, (int)currentPrimary(), lastExecutedSeqNum, lastStableSeqNum);

			pVC->finalizeMessage(*repsInfo);

			sendToAllOtherReplicas(pVC);
		}

		void ReplicaImp::GotoNextView()
		{
			// at this point we don't have f+1 ViewChangeMsg messages with view >= curView 

			MoveToHigherView(curView + 1);

			// at this point we don't have enough ViewChangeMsg messages (2f+2c+1) to enter to the new view (because 2f+2c+1 > f+1)
		}

		bool ReplicaImp::tryToEnterView()
		{
			Assert(!currentViewIsActive());

			std::vector<PrePrepareMsg*> prePreparesForNewView;


			LOG_INFO_F(GL, "**************** Calling to viewsManager->tryToEnterView(curView=%" PRId64 ", lastStableSeqNum=%" PRId64 ", lastExecutedSeqNum=%" PRId64 ")",
				curView, lastStableSeqNum, lastExecutedSeqNum);

			bool succ = viewsManager->tryToEnterView(curView, lastStableSeqNum, lastExecutedSeqNum, &prePreparesForNewView);

			if (succ)
				onNewView(prePreparesForNewView);
			else
				tryToSendStatusReport();

			return succ;
		}

		void ReplicaImp::onNewView(const std::vector<PrePrepareMsg*>& prePreparesForNewView)
		{
			SeqNum firstPPSeq = 0;	SeqNum lastPPSeq = 0;

			if (!prePreparesForNewView.empty()) {
				firstPPSeq = prePreparesForNewView.front()->seqNumber();
				lastPPSeq = prePreparesForNewView.back()->seqNumber();
			}

			LOG_INFO_F(GL, "**************** In onNewView curView=%" PRId64 " (num of PPs=%d, first safe seq=%" PRId64 ", last safe seq=%" PRId64 ", lastStableSeqNum=%" PRId64 ", lastExecutedSeqNum=%" PRId64 ", stableLowerBoundWhenEnteredToView= %" PRId64 ")",
				curView, prePreparesForNewView.size(), firstPPSeq, lastPPSeq,
				lastStableSeqNum, lastExecutedSeqNum, viewsManager->stableLowerBoundWhenEnteredToView());

			Assert(viewsManager->viewIsActive(curView));
			Assert(lastStableSeqNum >= viewsManager->stableLowerBoundWhenEnteredToView());
			Assert(lastExecutedSeqNum >= lastStableSeqNum); // we moved to the new state, only after synchronizing the state

			timeOfLastViewEntrance = getMonotonicTime(); // TODO(GG): handle restart/pause

			if (repsInfo->primaryOfView(curView) == myReplicaId)
			{
				NewViewMsg* nv = viewsManager->getMyNewViewMsgForCurrentView();

				nv->finalizeMessage(*repsInfo);

				Assert(nv->newView() == curView);

				sendToAllOtherReplicas(nv);
			}

			if (prePreparesForNewView.empty())
			{
				primaryLastUsedSeqNum = lastStableSeqNum;
				strictLowerBoundOfSeqNums = lastStableSeqNum;
				maxSeqNumTransferredFromPrevViews = lastStableSeqNum;
			}
			else
			{
				primaryLastUsedSeqNum = lastPPSeq;
				strictLowerBoundOfSeqNums = firstPPSeq - 1;
				maxSeqNumTransferredFromPrevViews = lastPPSeq;
			}

			const bool primaryIsMe = (myReplicaId == repsInfo->primaryOfView(curView));

			for (size_t i = 0; i < prePreparesForNewView.size(); i++)
			{
				PrePrepareMsg* pp = prePreparesForNewView[i];
				Assert(pp->seqNumber() >= firstPPSeq);
				Assert(pp->seqNumber() <= lastPPSeq);
				Assert(pp->firstPath() == CommitPath::SLOW); // TODO(GG): don't we want to use the fast path?
				SeqNumInfo& seqNumInfo = mainLog->get(pp->seqNumber());

				if (primaryIsMe)
					seqNumInfo.addSelfMsg(pp);
				else
					seqNumInfo.addMsg(pp);

				seqNumInfo.startSlowPath();
                                metric_slow_path_count_.Get().Inc();
				sendPreparePartial(seqNumInfo);                    
			}

			clientsManager->clearAllPendingRequests();

			// clear requestsQueueOfPrimary
			while (!requestsQueueOfPrimary.empty())
			{
				delete requestsQueueOfPrimary.front();
				requestsQueueOfPrimary.pop();
			}

			LOG_INFO_F(GL, "**************** Start working in view %" PRId64 "", curView);

			controller->onNewView(curView, primaryLastUsedSeqNum);
		}

		void ReplicaImp::sendCheckpointIfNeeded()
		{
			if (stateTransfer->isCollectingState() || !currentViewIsActive()) return;

			const SeqNum lastCheckpointNumber = (lastExecutedSeqNum / checkpointWindowSize) * checkpointWindowSize;

			if (lastCheckpointNumber == 0) return;

			Assert(checkpointsLog->insideActiveWindow(lastCheckpointNumber));

			CheckpointInfo& checkInfo = checkpointsLog->get(lastCheckpointNumber);
			CheckpointMsg* checkpointMessage = checkInfo.selfCheckpointMsg();

			if (!checkpointMessage)
			{
				LOG_INFO_F(GL, "My Checkpoint message is missing"); // TODO(GG): TBD
				return;
			}

			//LOG_INFO_F(GL, "Debug - sendCheckpointIfNeeded - 1");

			if (checkInfo.checkpointSentAllOrApproved()) return;

			//LOG_INFO_F(GL, "Debug - sendCheckpointIfNeeded - 2");

			if (subtract(getMonotonicTime(), checkInfo.selfExecutionTime()) >= 3 * 1000 * 1000) // TODO(GG): 3 seconds, should be in configuration
			{
				checkInfo.setCheckpointSentAllOrApproved();
				sendToAllOtherReplicas(checkpointMessage);
				return;
			}

			//LOG_INFO_F(GL, "Debug - sendCheckpointIfNeeded - 3");


			const SeqNum refSeqNumberForCheckpoint = lastCheckpointNumber + MaxConcurrentFastPaths;

			if (lastExecutedSeqNum < refSeqNumberForCheckpoint) return;

			//LOG_INFO_F(GL, "Debug - sendCheckpointIfNeeded - 4");


			if (mainLog->insideActiveWindow(lastExecutedSeqNum)) // TODO(GG): condition is needed ?
			{
				SeqNumInfo& seqNumInfo = mainLog->get(lastExecutedSeqNum);

				//LOG_INFO_F(GL, "Debug - sendCheckpointIfNeeded - 5");


				if (seqNumInfo.partialProofs().hasFullProof())
				{

					//LOG_INFO_F(GL, "Debug - sendCheckpointIfNeeded - 6");

					checkInfo.tryToMarkCheckpointCertificateCompleted();

					Assert(checkInfo.isCheckpointCertificateComplete());

					onSeqNumIsStable(lastCheckpointNumber);

					checkInfo.setCheckpointSentAllOrApproved();

					return;
				}
			}

			//LOG_INFO_F(GL, "Debug - sendCheckpointIfNeeded - 7");
			
			checkInfo.setCheckpointSentAllOrApproved();
			sendToAllOtherReplicas(checkpointMessage);
		}

		void ReplicaImp::onTransferringCompleteImp(SeqNum newStateCheckpoint)
		{
			bool askAnotherStateTransfer = false;
			
			Assert(newStateCheckpoint % checkpointWindowSize == 0);

			LOG_INFO_F(GL, "onTransferringCompleteImp with newStateCheckpoint==%" PRId64 "", newStateCheckpoint);

			timeOfLastStateSynch = getMonotonicTime(); // TODO(GG): handle restart/pause

			if (newStateCheckpoint > lastExecutedSeqNum)
			{
//				const SeqNum prevLastExecutedSeqNum = lastExecutedSeqNum;

				lastExecutedSeqNum = newStateCheckpoint;
                                metric_last_executed_seq_num_.Get().Set(lastExecutedSeqNum);

				clientsManager->loadInfoFromReservedPages();

				if (newStateCheckpoint > lastStableSeqNum + kWorkWindowSize)
				{
					const SeqNum refPoint = newStateCheckpoint - kWorkWindowSize;
					const bool withRefCheckpoint = (checkpointsLog->insideActiveWindow(refPoint) && (checkpointsLog->get(refPoint).selfCheckpointMsg() != nullptr));

					if (withRefCheckpoint)
						onSeqNumIsStable(refPoint);
					else
						onSeqNumIsStableWithoutRefCheckpoint(refPoint);
				}

				Digest digestOfNewState;
				const uint64_t checkpointNum = newStateCheckpoint / checkpointWindowSize;
				stateTransfer->getDigestOfCheckpoint(checkpointNum, sizeof(Digest), (char*)&digestOfNewState);
				CheckpointMsg* checkpointMsg = new CheckpointMsg(myReplicaId, newStateCheckpoint, digestOfNewState, false);
				CheckpointInfo& checkpointInfo = checkpointsLog->get(newStateCheckpoint);
				checkpointInfo.addCheckpointMsg(checkpointMsg, myReplicaId);
				checkpointInfo.setCheckpointSentAllOrApproved();
				sendToAllOtherReplicas(checkpointMsg);
			}
			else
			{
				LOG_ERROR_F(GL, "Debug Warning: executing onTransferringCompleteImp(newStateCheckpoint) where newStateCheckpoint <= lastExecutedSeqNum");
			}

			if ((uint16_t)tableOfStableCheckpoints.size() >= fVal + 1)
			{
				uint16_t numOfStableCheckpoints = 0;
				auto tableItrator = tableOfStableCheckpoints.begin();
				while (tableItrator != tableOfStableCheckpoints.end())
				{
					if (tableItrator->second->seqNumber() >= newStateCheckpoint)
						numOfStableCheckpoints++;

					if (tableItrator->second->seqNumber() <= lastExecutedSeqNum)
					{
						delete tableItrator->second;
						tableItrator = tableOfStableCheckpoints.erase(tableItrator);
					}
					else
					{
						tableItrator++;
					}
				}
				if (numOfStableCheckpoints >= fVal + 1)
					onSeqNumIsStable(newStateCheckpoint);
				
				if ((uint16_t)tableOfStableCheckpoints.size() >= fVal + 1)
					askAnotherStateTransfer = true;
			}

			if (newStateCheckpoint > primaryLastUsedSeqNum)
				primaryLastUsedSeqNum = newStateCheckpoint;

			if (currentViewIsActive() && !stateTransfer->isCollectingState())
			{
				executeReadWriteRequests();

				if (isCurrentPrimary() && !requestsQueueOfPrimary.empty())
					tryToSendPrePrepareMsg();
			}
			
			if(askAnotherStateTransfer)
			{
				LOG_INFO_F(GL, "call to startCollectingState()");
				stateTransfer->startCollectingState();
			}
		}


		void ReplicaImp::onSeqNumIsStableWithoutRefCheckpoint(SeqNum newStableSeqNum)
		{
			Assert(newStableSeqNum % checkpointWindowSize == 0);

			if (newStableSeqNum <= lastStableSeqNum) return;

			LOG_INFO_F(GL, "onSeqNumIsStableWithoutRefCheckpoint: lastStableSeqNum is now == %" PRId64 "", newStableSeqNum);

			lastStableSeqNum = newStableSeqNum;
                        metric_last_stable_seq_num__.Get().Set(lastStableSeqNum);

			if (lastStableSeqNum > strictLowerBoundOfSeqNums)
				strictLowerBoundOfSeqNums = lastStableSeqNum;

			if (lastStableSeqNum > primaryLastUsedSeqNum)
				primaryLastUsedSeqNum = lastStableSeqNum;

			mainLog->advanceActiveWindow(lastStableSeqNum + 1);

			checkpointsLog->advanceActiveWindow(lastStableSeqNum);

			const uint64_t checkpointNum = lastStableSeqNum / checkpointWindowSize;
			stateTransfer->markCheckpointAsStable(checkpointNum);
		}

		void ReplicaImp::onSeqNumIsStable(SeqNum newStableSeqNum)
		{
			Assert(newStableSeqNum % checkpointWindowSize == 0);

			if (newStableSeqNum <= lastStableSeqNum) return;

			LOG_INFO_F(GL, "onSeqNumIsStable: lastStableSeqNum is now == %" PRId64 "", newStableSeqNum);

			lastStableSeqNum = newStableSeqNum;
                        metric_last_stable_seq_num__.Get().Set(lastStableSeqNum);

			if (lastStableSeqNum > strictLowerBoundOfSeqNums)
				strictLowerBoundOfSeqNums = lastStableSeqNum;

			if (lastStableSeqNum > lastExecutedSeqNum)
			{
				lastExecutedSeqNum = lastStableSeqNum;
                                metric_last_executed_seq_num_.Get().Set(lastExecutedSeqNum);

				clientsManager->loadInfoFromReservedPages();
			}

			if (lastStableSeqNum > primaryLastUsedSeqNum)
				primaryLastUsedSeqNum = lastStableSeqNum;

			mainLog->advanceActiveWindow(lastStableSeqNum + 1);

			checkpointsLog->advanceActiveWindow(lastStableSeqNum);

			const uint64_t checkpointNum = lastStableSeqNum / checkpointWindowSize;
			stateTransfer->markCheckpointAsStable(checkpointNum);

			CheckpointInfo& checkpointInfo = checkpointsLog->get(lastStableSeqNum);
			CheckpointMsg* checkpointMsg = checkpointInfo.selfCheckpointMsg();

			if (checkpointMsg == nullptr)
			{
				Digest digestOfState;
				const uint64_t checkpointNum = lastStableSeqNum / checkpointWindowSize;
				stateTransfer->getDigestOfCheckpoint(checkpointNum, sizeof(Digest), (char*)&digestOfState);
				checkpointMsg = new CheckpointMsg(myReplicaId, lastStableSeqNum, digestOfState, true);
				checkpointInfo.addCheckpointMsg(checkpointMsg, myReplicaId);
			}
			else
			{
				checkpointMsg->setStateAsStable();
			}

			if (!checkpointInfo.isCheckpointCertificateComplete()) checkpointInfo.tryToMarkCheckpointCertificateCompleted();
			Assert(checkpointInfo.isCheckpointCertificateComplete());

			if (currentViewIsActive() && !stateTransfer->isCollectingState()) // TODO(GG): TBD
			{
				if (currentPrimary() == myReplicaId)
					tryToSendPrePrepareMsg();
			}
		}

		void ReplicaImp::tryToSendReqMissingDataMsg(SeqNum seqNumber, bool slowPathOnly, uint16_t destReplicaId)
		{
			if ((!currentViewIsActive()) ||
				(seqNumber <= strictLowerBoundOfSeqNums) ||
				(!mainLog->insideActiveWindow(seqNumber)) ||
				(!mainLog->insideActiveWindow(seqNumber)))
			{
				return;
			}

			SeqNumInfo& seqNumInfo = mainLog->get(seqNumber);
			PartialProofsSet& partialProofsSet = seqNumInfo.partialProofs();

			const Time curTime = getMonotonicTime();

			{
				Time t = seqNumInfo.getTimeOfFisrtRelevantInfoFromPrimary();
				const Time lastInfoRequest = seqNumInfo.getTimeOfLastInfoRequest();

				if ((t < lastInfoRequest)) t = lastInfoRequest;

				if (t == MinTime && (t < curTime))
				{
					int64_t diffMilli = subtract(curTime, t) / 1000;
					if (diffMilli < dynamicUpperLimitOfRounds->upperLimit() / 4) // TODO(GG): config
						return;
				}
			}

			seqNumInfo.setTimeOfLastInfoRequest(curTime);

			LOG_INFO_F(GL, "Node %d trys to request missing data for seqNumber %" PRId64 "", (int)myReplicaId, seqNumber);

			ReqMissingDataMsg reqData(myReplicaId, curView, seqNumber);

			const bool routerForPartialProofs = repsInfo->isCollectorForPartialProofs(curView, seqNumber);

			const bool routerForPartialPrepare = (currentPrimary() == myReplicaId);

			const bool routerForPartialCommit = (currentPrimary() == myReplicaId);

			const bool missingPrePrepare = (seqNumInfo.getPrePrepareMsg() == nullptr);
			const bool missingBigRequests = (!missingPrePrepare) && (!seqNumInfo.hasPrePrepareMsg());

			ReplicaId firstRepId = 0;
			ReplicaId lastRepId = numOfReplicas - 1;
			if (destReplicaId != ALL_OTHER_REPLICAS)
			{
				firstRepId = destReplicaId;
				lastRepId = destReplicaId;
			}

			for (ReplicaId destRep = firstRepId; destRep <= lastRepId; destRep++)
			{
				if (destRep == myReplicaId) continue; // don't send to myself

				const bool destIsPrimary = (currentPrimary() == destRep);

				const bool missingPartialPrepare = (routerForPartialPrepare && (!seqNumInfo.preparedOrHasPreparePartialFromReplica(destRep)));

				const bool missingFullPrepare = !seqNumInfo.preparedOrHasPreparePartialFromReplica(destRep);

				const bool missingPartialCommit = (routerForPartialCommit && (!seqNumInfo.committedOrHasCommitPartialFromReplica(destRep)));

				const bool missingFullCommit = !seqNumInfo.committedOrHasCommitPartialFromReplica(destRep);

				const bool missingPartialProof = !slowPathOnly &&
					routerForPartialProofs &&
					!partialProofsSet.hasFullProof() &&
					!partialProofsSet.hasPartialProofFromReplica(destRep);

				const bool missingFullProof = !slowPathOnly && !partialProofsSet.hasFullProof();

				bool sendNeeded = missingPartialProof || missingPartialPrepare || missingFullPrepare || missingPartialCommit || missingFullCommit || missingFullProof;

				if (destIsPrimary && !sendNeeded) sendNeeded = missingBigRequests || missingPrePrepare;

				if (!sendNeeded) continue;

				reqData.resetFlags();

				if (destIsPrimary && missingPrePrepare) reqData.setPrePrepareIsMissing();

				if (missingPartialProof) reqData.setPartialProofIsMissing();
				if (missingPartialPrepare)      reqData.setPartialPrepareIsMissing();
				if (missingFullPrepare)      reqData.setFullPrepareIsMissing();
				if (missingPartialCommit)       reqData.setPartialCommitIsMissing();
				if (missingFullCommit)       reqData.setFullCommitIsMissing();
				if (missingFullProof)       reqData.setFullCommitProofIsMissing();

				const bool slowPathStarted = seqNumInfo.slowPathStarted();

				if (slowPathStarted) reqData.setSlowPathHasStarted();

				LOG_INFO_F(GL, "Node %d sends ReqMissingDataMsg to %d - seqNumber %" PRId64 " , flags=%X",
					myReplicaId, destRep, seqNumber, (unsigned int)reqData.getFlags());

				send(&reqData, destRep);
			}
		}

		void ReplicaImp::onMessage(ReqMissingDataMsg* msg)
		{
			const SeqNum msgSeqNum = msg->seqNumber();
			const ReplicaId msgSender = msg->senderId();

			LOG_INFO_F(GL, "Node %d received ReqMissingDataMsg message from Node %d - seqNumber %" PRId64 " , flags=%X",
				(int)myReplicaId, (int)msgSender, msgSeqNum, (unsigned int)msg->getFlags());

			if ((currentViewIsActive()) && (msgSeqNum > strictLowerBoundOfSeqNums) &&
				(mainLog->insideActiveWindow(msgSeqNum)) && (mainLog->insideActiveWindow(msgSeqNum)))
			{
				SeqNumInfo& seqNumInfo = mainLog->get(msgSeqNum);

				if (myReplicaId == currentPrimary())
				{
					PrePrepareMsg* pp = seqNumInfo.getSelfPrePrepareMsg();
					if (msg->getPrePrepareIsMissing())
					{
						if (pp != 0)
							send(pp, msgSender);
					}

					if (seqNumInfo.slowPathStarted() && !msg->getSlowPathHasStarted())
					{
						StartSlowCommitMsg startSlowMsg(myReplicaId, curView, msgSeqNum);
						send(&startSlowMsg, msgSender);
					}
				}

				if (msg->getPartialProofIsMissing())
				{
					// TODO(GG): consider not to send if msgSender is not a collector 

					PartialCommitProofMsg* pcf = seqNumInfo.partialProofs().getSelfPartialCommitProof();

					if (pcf != nullptr) send(pcf, msgSender);
				}

				if (msg->getPartialPrepareIsMissing() && (currentPrimary() == msgSender))
				{
					PreparePartialMsg* pr = seqNumInfo.getSelfPreparePartialMsg();

					if (pr != nullptr) send(pr, msgSender);
				}

				if (msg->getFullPrepareIsMissing())
				{
					PrepareFullMsg* pf = seqNumInfo.getValidPrepareFullMsg();

					if (pf != nullptr) send(pf, msgSender);
				}


				if (msg->getPartialCommitIsMissing() && (currentPrimary() == msgSender))
				{
					CommitPartialMsg* c = mainLog->get(msgSeqNum).getSelfCommitPartialMsg();
					if (c != nullptr) send(c, msgSender);
				}

				if (msg->getFullCommitIsMissing())
				{
					CommitFullMsg* c = mainLog->get(msgSeqNum).getValidCommitFullMsg();
					if (c != nullptr) send(c, msgSender);
				}

				if (msg->getFullCommitProofIsMissing() && seqNumInfo.partialProofs().hasFullProof())
				{
					FullCommitProofMsg* fcp = seqNumInfo.partialProofs().getFullProof();
					send(fcp, msgSender);
				}
			}
			else
			{
				LOG_INFO_F(GL, "Node %d ignores the ReqMissingDataMsg message from Node %d", (int)myReplicaId, (int)msgSender);
			}

			delete msg;
		}


		void ReplicaImp::onViewsChangeTimer(Time cTime, Timer& timer) // TODO(GG): review/update logic
		{
			Assert(viewChangeProtocolEnabled);

			if (stateTransfer->isCollectingState()) return;

			const Time currTime = getMonotonicTime();

			//////////////////////////////////////////////////////////////////////////////
			// 
			//////////////////////////////////////////////////////////////////////////////

			if (autoPrimaryUpdateEnabled && currentViewIsActive())
			{
				const uint64_t timeout = (isCurrentPrimary() ? (autoPrimaryUpdateMilli) : (autoPrimaryUpdateMilli + viewChangeTimeoutMilli));

				const uint64_t diffMilli = absDifference(currTime, timeOfLastViewEntrance) / 1000;

				if (diffMilli > timeout) {

					LOG_INFO_F(GL, "**************** Node %d initiates automatic view change in view %" PRId64 " (%" PRIu64  " milli seconds after start working in the previous view)", myReplicaId, curView, diffMilli);

					GotoNextView();
					return;
				}
			}

			//////////////////////////////////////////////////////////////////////////////
			// 
			//////////////////////////////////////////////////////////////////////////////

			uint64_t viewChangeTimeout = viewChangeTimerMilli;
			if (autoIncViewChangeTimer && ((lastViewThatTransferredSeqNumbersFullyExecuted + 1) < curView))
			{
				uint64_t factor = (curView - lastViewThatTransferredSeqNumbersFullyExecuted);
				viewChangeTimeout = viewChangeTimeout * factor; // TODO(GG): review logic here
			}

			if (currentViewIsActive())
			{
				if (isCurrentPrimary()) return;

				const Time timeOfEarliestPendingRequest = clientsManager->timeOfEarliestPendingRequest();

				const bool hasPendingRequest = (timeOfEarliestPendingRequest != MaxTime);

				if (!hasPendingRequest) return;

				const uint64_t diffMilli1 = absDifference(currTime, timeOfLastStateSynch) / 1000;
				const uint64_t diffMilli2 = absDifference(currTime, timeOfLastViewEntrance) / 1000;
				const uint64_t diffMilli3 = absDifference(currTime, timeOfEarliestPendingRequest) / 1000;

				if ((diffMilli1 > viewChangeTimeout) &&
					(diffMilli2 > viewChangeTimeout) &&
					(diffMilli3 > viewChangeTimeout))
				{
					LOG_INFO_F(GL, "**************** Node %d asks to leave view %" PRId64 " (%" PRIu64  " milli seconds after receiving a client request)", myReplicaId, curView, diffMilli3);

					GotoNextView();
					return;
				}
			}
			else // not currentViewIsActive()
			{
				if (lastAgreedView != curView) return;
				if(repsInfo->primaryOfView(lastAgreedView) == myReplicaId) return;

				const Time currTime = getMonotonicTime();
				const uint64_t diffMilli1 = absDifference(currTime, timeOfLastStateSynch) / 1000;
				const uint64_t diffMilli2 = absDifference(currTime, timeOfLastAgreedView) / 1000;

				if ((diffMilli1 > viewChangeTimeout) &&
					(diffMilli2 > viewChangeTimeout))
				{
					LOG_INFO_F(GL, "**************** Node %d asks to jump to view %" PRId64 " (%" PRIu64  " milli seconds after receiving 2f+2c+1 view change msgs for view %" PRId64 ")", myReplicaId, curView, diffMilli2);

					GotoNextView();
					return;
				}
			}
		}

		void ReplicaImp::onStateTranTimer(Time cTime, Timer& timer)
		{
			stateTransfer->onTimer(); 
		}
		

		void ReplicaImp::onStatusReportTimer(Time cTime, Timer& timer)
		{
			tryToSendStatusReport();

		#ifdef DEBUG_MEMORY_MSG
			MessageBase::printLiveMessages();
		#endif
		}

		void ReplicaImp::onSlowPathTimer(Time cTime, Timer& timer)
		{
			tryToStartSlowPaths();

			uint16_t newPeriod = (uint16_t)(controller->slowPathsTimerMilli());

			timer.changePeriodMilli(newPeriod);
		}

		void ReplicaImp::onInfoRequestTimer(Time cTime, Timer& timer)
		{
			tryToAskForMissingInfo();

			uint16_t newPeriod = (uint16_t)(dynamicUpperLimitOfRounds->upperLimit() / 2);

			timer.changePeriodMilli(newPeriod);
		}

		void ReplicaImp::onDebugStatTimer(Time cTime, Timer& timer)
		{
			DebugStatistics::onCycleCheck();
		}


		void  ReplicaImp::commitFullCommitProof(SeqNum seqNum, SeqNumInfo& seqNumInfo)
		{
			seqNumInfo.forceComplete();

			const bool askForMissingInfoAboutCommittedItems = (seqNum > lastExecutedSeqNum + maxConcurrentAgreementsByPrimary); // TODO(GG): check this logic

			executeReadWriteRequests(askForMissingInfoAboutCommittedItems);
		}

		void  ReplicaImp::commitAndSendFullCommitProof(SeqNum seqNum, SeqNumInfo& seqNumInfo, PartialProofsSet& partialProofs)
		{
			FullCommitProofMsg* fcp = partialProofs.getFullProof();

			sendToAllOtherReplicas(fcp);

			commitFullCommitProof(seqNum, seqNumInfo);
		}



		void ReplicaImp::onMessage(SimpleAckMsg* msg)
		{
			if (retransmissionsLogicEnabled)
			{
				uint16_t relatedMsgType = (uint16_t)msg->ackData(); // TODO(GG): does this make sense ?

				LOG_INFO_F(GL, "Node %d received SimpleAckMsg message from node %d for seqNumber %" PRId64 " and type %d", myReplicaId, msg->senderId(), msg->seqNumber(), (int)relatedMsgType);

				retransmissionsManager->onAck(msg->senderId(), msg->seqNumber(), relatedMsgType);
			}
			else
			{
				// TODO(GG): print warning ?
			}

			delete msg;
		}

		void ReplicaImp::onMessage(StateTransferMsg* m)
		{
			size_t h = sizeof(MessageBase::Header);
			stateTransfer->handleStateTransferMessage(m->body() + h, m->size() - h, m->senderId());
		}




		void ReplicaImp::freeStateTransferMsg(char* m)
		{
			// This method may be called by external threads
			char* p = (m - sizeof(MessageBase::Header));
			std::free(p);
		}



		void ReplicaImp::sendStateTransferMessage(char* m, uint32_t size, uint16_t replicaId)
		{
			// This method may be called by external threads
			// TODO(GG): if this method is invoked by an external thread, then send an "internal message" to the replica's main thread 

			if (mainThread.get_id() == std::this_thread::get_id())
			{
				MessageBase* p = new MessageBase(myReplicaId, MsgCode::StateTransfer, size + sizeof(MessageBase::Header));
				char* x = p->body() + sizeof(MessageBase::Header);
				memcpy(x, m, size);
				send(p, replicaId);
				delete p;
			}
			else
			{
				//TODO(GG): implement
				Assert(false);
			}
		}


		void ReplicaImp::onTransferringComplete(int64_t checkpointNumberOfNewState)
		{
			// This method may be called by external threads
			// TODO(GG): if this method is invoked by an external thread, then send an "internal message" to the replica's main thread 
			if (mainThread.get_id() == std::this_thread::get_id())
			{
				onTransferringCompleteImp(checkpointNumberOfNewState * checkpointWindowSize);
			}
			else
			{
				//TODO(GG): implement
				Assert(false);
			}

		}


		void ReplicaImp::changeStateTransferTimerPeriod(uint32_t timerPeriodMilli)
		{
			// This method may be called by external threads
			// TODO(GG): if this method is invoked by an external thread, then send an "internal message" to the replica's main thread 


			if (mainThread.get_id() == std::this_thread::get_id())
			{
				stateTranTimer->changePeriodMilli((uint16_t)timerPeriodMilli); // TODO: check the casting here
			}
			else
			{
				//TODO(GG): implement
				Assert(false);
			}

		}


		void ReplicaImp::onMerkleExecSignature(ViewNum v, SeqNum s, uint16_t signatureLength, const char* signature)
		{
			Assert(false);
			// TODO(GG): use code from previous drafts
		}



		void ReplicaImp::onMessage(PartialExecProofMsg* m)
		{
			Assert(false);
			// TODO(GG): use code from previous drafts
		}


		void ReplicaImp::onReportAboutAdvancedReplica(ReplicaId reportedReplica, SeqNum seqNum, ViewNum viewNum)
		{
			// TODO(GG): simple implementation - should be improved
			tryToSendStatusReport();
		}

		void ReplicaImp::onReportAboutLateReplica(ReplicaId reportedReplica, SeqNum seqNum, ViewNum viewNum)
		{
			// TODO(GG): simple implementation - should be improved
			tryToSendStatusReport();
		}

		void ReplicaImp::onReportAboutAdvancedReplica(ReplicaId reportedReplica, SeqNum seqNum)
		{
			// TODO(GG): simple implementation - should be improved
			tryToSendStatusReport();
		}

		void ReplicaImp::onReportAboutLateReplica(ReplicaId reportedReplica, SeqNum seqNum)
		{
			// TODO(GG): simple implementation - should be improved
			tryToSendStatusReport();
		}



		ReplicaImp::ReplicaImp(const ReplicaConfig& config, RequestsHandler* requestsHandler,
			IStateTransfer* stateTransferr, ICommunication* communication)
			: 
			myReplicaId{ config.replicaId },
			fVal{ config.fVal },
			cVal{ config.cVal },
			numOfReplicas{ (uint16_t)(3 * config.fVal + 2 * config.cVal + 1) },
			numOfClientProxies{ config.numOfClientProxies },
			viewChangeProtocolEnabled{( (!forceViewChangeProtocolEnabled && !forceViewChangeProtocolDisabled) ? config.autoViewChangeEnabled : forceViewChangeProtocolEnabled)},
			supportDirectProofs{ false },
			metaMsgHandlers{ createMapOfMetaMsgHandlers() },
			incomingMsgsStorage{ 20000 }, // TODO(GG): use configuration
			msgReceiver{ nullptr },
			communication{ communication },
			mainThread(),
			mainThreadStarted(false),
			mainThreadShouldStop(false),
			internalThreadPool{ 8 }, // TODO(GG): use configuration
			retransmissionsManager{ nullptr },
			controller{ nullptr },
			repsInfo{ nullptr },
			sigManager{ nullptr },
			viewsManager{ nullptr },
			maxConcurrentAgreementsByPrimary{ 0 },
			curView{ 0 },
			primaryLastUsedSeqNum{ 0 },
			lastStableSeqNum{ 0 },
			lastExecutedSeqNum{ 0 },
			strictLowerBoundOfSeqNums{ 0 },
			maxSeqNumTransferredFromPrevViews{ 0 },
			mainLog{ nullptr },
			checkpointsLog{ nullptr },
			clientsManager{ nullptr },
			replyBuffer{ (char*)std::malloc(maxReplyMessageSize - sizeof(ClientReplyMsgHeader)) },
			stateTransfer{ (stateTransferr!=nullptr ? stateTransferr : new NullStateTransfer()) },
			maxNumberOfPendingRequestsInRecentHistory{ 0 },
			batchingFactor{ 1 },
			userRequestsHandler{ requestsHandler },
			thresholdSignerForExecution{ config.thresholdSignerForExecution },
			thresholdVerifierForExecution{ config.thresholdVerifierForExecution },
			thresholdSignerForSlowPathCommit{ config.thresholdSignerForSlowPathCommit },
			thresholdVerifierForSlowPathCommit{ config.thresholdVerifierForSlowPathCommit },
			thresholdSignerForCommit{ config.thresholdSignerForCommit },
			thresholdVerifierForCommit{ config.thresholdVerifierForCommit },
			thresholdSignerForOptimisticCommit{ config.thresholdSignerForOptimisticCommit },
			thresholdVerifierForOptimisticCommit{ config.thresholdVerifierForOptimisticCommit },
			dynamicUpperLimitOfRounds{ nullptr },
			lastViewThatTransferredSeqNumbersFullyExecuted{ MinTime },
			lastTimeThisReplicaSentStatusReportMsgToAllPeerReplicas{ MinTime },
			timeOfLastStateSynch{ getMonotonicTime() }, // TODO(GG): TBD
			timeOfLastViewEntrance{ getMonotonicTime() }, // TODO(GG): TBD
			lastAgreedView{ 0 },
			timeOfLastAgreedView{ getMonotonicTime() }, // TODO(GG): TBD
			stateTranTimer{ nullptr },
			retranTimer{ nullptr },
			slowPathTimer{ nullptr },
			infoReqTimer{ nullptr },
			statusReportTimer{ nullptr },
			viewChangeTimer{ nullptr },
			debugStatTimer{ nullptr },
			viewChangeTimerMilli{ 0 },
			startSyncEvent{false},
                        metricsComponent_{concordMetrics::Component("replica",
                            std::make_shared<concordMetrics::Aggregator>())},
                        metric_view_{
                          metricsComponent_.RegisterGauge("view", curView)},
                        metric_last_stable_seq_num__{
                          metricsComponent_.RegisterGauge("lastStableSeqNum",
                                                           lastStableSeqNum)},
                        metric_last_executed_seq_num_{
                          metricsComponent_.RegisterGauge("lastExecutedSeqNum",
                                                           lastExecutedSeqNum)},
                        metric_last_agreed_view_{
                          metricsComponent_.RegisterGauge("lastAgreedView",
                                                           lastAgreedView)},
                        metric_slow_path_count_{
                          metricsComponent_.RegisterCounter("slowPathCount",
                                                            0)},
                        metric_received_msgs_{
                          metricsComponent_.RegisterCounter("receivedMsgs", 0)}

		{
			Assert(myReplicaId < numOfReplicas);
			// TODO(GG): more asserts on params !!!!!!!!!!!

//			initAllocator();
			DebugStatistics::initDebugStatisticsData();

			//CryptographyWrapper::init(); // TODO(GG): here ???

			std::set<SigManager::PublicKeyDesc> replicasSigPublicKeys;

			for (auto e : config.publicKeysOfReplicas)
			{
				SigManager::PublicKeyDesc keyDesc = { e.first, e.second };
				replicasSigPublicKeys.insert(keyDesc);
			}			

			sigManager = new SigManager(myReplicaId, numOfReplicas + numOfClientProxies, config.replicaPrivateKey, replicasSigPublicKeys);

			msgReceiver = new MsgReceiver(incomingMsgsStorage);

			communication->setReceiver(myReplicaId, msgReceiver);
			int comStatus = communication->Start();
			Assert(comStatus == 0);

			std::set<NodeIdType> clientsSet;
			for (uint16_t i = numOfReplicas; i < numOfReplicas + numOfClientProxies; i++) clientsSet.insert(i);

			clientsManager = new ClientsManager(myReplicaId, clientsSet, sizeOfReservedPage);

			stateTransfer->init(kWorkWindowSize / checkpointWindowSize + 1, clientsManager->numberOfRequiredReservedPages(), sizeOfReservedPage);
			clientsManager->init(stateTransfer);

			clientsManager->clearReservedPages();

			int statusReportTimerMilli = (sendStatusPeriodMilli > 0) ? sendStatusPeriodMilli : config.statusReportTimerMillisec;;
			Assert(statusReportTimerMilli > 0);

			viewChangeTimerMilli = (viewChangeTimeoutMilli > 0) ? viewChangeTimeoutMilli : config.viewChangeTimerMillisec;
			Assert(viewChangeTimerMilli > 0);

			int concurrencyLevel = config.concurrencyLevel;
			Assert(concurrencyLevel>0);
			Assert(concurrencyLevel < MaxConcurrentFastPaths);

			fprintf(stderr, "\nConcurrency Level: %d\n", concurrencyLevel); // TODO(GG): all configuration should be displayed

			Assert(concurrencyLevel <= maxLegalConcurrentAgreementsByPrimary);
			maxConcurrentAgreementsByPrimary = (uint16_t)concurrencyLevel;

			// TODO(GG): use config ...
			dynamicUpperLimitOfRounds = new DynamicUpperLimitWithSimpleFilter<int64_t>(400, 2, 2500, 70, 32, 1000, 2, 2);

			repsInfo = new ReplicasInfo(myReplicaId, *sigManager, numOfReplicas, fVal, cVal, dynamicCollectorForPartialProofs, dynamicCollectorForExecutionProofs);

			mainLog = new SequenceWithActiveWindow<kWorkWindowSize, 1, SeqNum, SeqNumInfo, SeqNumInfo>(1, (InternalReplicaApi*)this);

			checkpointsLog = new SequenceWithActiveWindow<kWorkWindowSize + checkpointWindowSize, checkpointWindowSize, SeqNum, CheckpointInfo, CheckpointInfo>(0, (InternalReplicaApi*)this);

			// create controller . TODO(GG): do we want to pass the controller as a parameter ?
                        controller = new ControllerWithSimpleHistory(cVal, fVal, myReplicaId, curView, primaryLastUsedSeqNum, metricsComponent_);

			statusReportTimer = new Timer(timersScheduler, (uint16_t)statusReportTimerMilli, statusTimerHandlerFunc, (InternalReplicaApi*)this);

			if (viewChangeProtocolEnabled)
			{
				int t = viewChangeTimerMilli;
				if (autoPrimaryUpdateEnabled && t > autoPrimaryUpdateMilli) t = autoPrimaryUpdateMilli;

				viewChangeTimer = new Timer(timersScheduler, (t / 2), viewChangeTimerHandlerFunc, (InternalReplicaApi*)this); // TODO(GG): what should be the time period here? . TODO(GG):Consider to split to 2 different timers
			}
			else
				viewChangeTimer = nullptr;

			stateTranTimer = new Timer(timersScheduler, 5 * 1000, stateTranTimerHandlerFunc, (InternalReplicaApi*)this);

			if (retransmissionsLogicEnabled)
				retranTimer = new Timer(timersScheduler, retransmissionsTimerMilli, retransmissionsTimerHandlerFunc, (InternalReplicaApi*)this);
			else
				retranTimer = nullptr;


			const int slowPathsTimerPeriod = controller->timeToStartSlowPathMilli();

			slowPathTimer = new Timer(timersScheduler, (uint16_t)slowPathsTimerPeriod, slowPathTimerHandlerFunc, (InternalReplicaApi*)this);

			infoReqTimer = new Timer(timersScheduler, (uint16_t)(dynamicUpperLimitOfRounds->upperLimit() / 2), infoRequestHandlerFunc, (InternalReplicaApi*)this);

		#ifdef DEBUG_STATISTICS
			debugStatTimer = new Timer(timersScheduler, (uint16_t)(DEBUG_STAT_PERIOD_SECONDS * 1000), debugStatHandlerFunc, (InternalReplicaApi*)this);
		#endif

			viewsManager = new ViewsManager(repsInfo, thresholdVerifierForSlowPathCommit);

			if (retransmissionsLogicEnabled)
				retransmissionsManager = new RetransmissionsManager(this, &internalThreadPool, &incomingMsgsStorage, kWorkWindowSize, 0);
			else
				retransmissionsManager = nullptr;

			internalThreadPool.start();
		}


		ReplicaImp::~ReplicaImp()
		{
			// TODO(GG): rewrite this method !!!!!!!! (notice that the order may be important here ). 
			// TODO(GG): don't delete objects that are passed as params (TBD)

			internalThreadPool.stop();
			delete thresholdSignerForCommit;
			delete thresholdVerifierForCommit;
			delete thresholdSignerForExecution;
			delete thresholdVerifierForExecution;

			//delete stateTransfer;

			delete viewsManager;

			delete controller;

			delete dynamicUpperLimitOfRounds;

			delete mainLog;

			delete checkpointsLog;


			DebugStatistics::freeDebugStatisticsData();
//			freeAllocator();

			delete msgReceiver;
			delete communication;
		}

		void ReplicaImp::start()
		{
			Assert(!mainThreadStarted);
			Assert(!mainThreadShouldStop);
			mainThreadStarted = true;

			std::thread mThread([this] {processMessages(); });
			mainThread.swap(mThread);
			startSyncEvent.set();
		}

		void ReplicaImp::stop()
		{
			communication->Stop();

			StopInternalMsg* stopMsg = new StopInternalMsg(this);
			incomingMsgsStorage.pushInternalMsg(stopMsg);

			mainThread.join();

			Assert(mainThreadShouldStop);
			
			mainThreadShouldStop = false;
			mainThreadStarted = false;
		}

		bool ReplicaImp::isRunning() const
		{
			return mainThreadStarted;
		}

		SeqNum ReplicaImp::getLastExecutedSequenceNum() const
		{
			return lastExecutedSeqNum;
		}

		ReplicaImp::StopInternalMsg::StopInternalMsg(ReplicaImp* myReplica)
		{
			replica = myReplica;
		}

		void ReplicaImp::StopInternalMsg::handle()
		{
			replica->mainThreadShouldStop = true;
		}

		void ReplicaImp::processMessages()
		{
			// TODO(GG): change this method to support "restart" ("start" after "stop")

			startSyncEvent.wait_one();

			stateTransfer->startRunning(this);
			clientsManager->clearReservedPages(); // TODO(GG): TBD

			stateTranTimer->start();
			if (retransmissionsLogicEnabled) retranTimer->start();
			slowPathTimer->start();
			infoReqTimer->start();
			statusReportTimer->start();
			if (viewChangeProtocolEnabled) viewChangeTimer->start();
		#ifdef DEBUG_STATISTICS
			debugStatTimer->start();
		#endif

			fprintf(stderr, "Running");

			while (!mainThreadShouldStop)
			{
				void* absMsg = nullptr;
				bool externalMsg = false;

				recvMsg(absMsg, externalMsg); // wait for a message
                                metric_received_msgs_.Get().Inc();
				if (!externalMsg) // if internal message
				{
					// TODO(GG): clean
					InternalMessage* inMsg = (InternalMessage*)absMsg;
					inMsg->handle();
					delete inMsg;
					continue;
				}

				MessageBase* m = (MessageBase*)absMsg;

		#ifdef DEBUG_STATISTICS
				DebugStatistics::onReceivedExMessage(m->type());
		#endif

				auto g = metaMsgHandlers.find(m->type());
				if (g != metaMsgHandlers.end())
				{
					PtrToMetaMsgHandler ptrMetaHandler = g->second;
					(this->*ptrMetaHandler)(m);
				}
				else
				{
					LOG_WARN_F(GL, "Unknown message");
					delete m;
				}
			}
		}


		void ReplicaImp::executeReadOnlyRequest(ClientRequestMsg* request) 
		{
			Assert(request->isReadOnly());
			Assert(!stateTransfer->isCollectingState());

			ClientReplyMsg reply(currentPrimary(), request->requestSeqNum(), myReplicaId);

			uint16_t clientId = request->clientProxyId();
	
			int error = 0;

			uint32_t actualReplyLength = 0;

			if (!supportDirectProofs)
			{
				error = userRequestsHandler->execute(
				    clientId, lastExecutedSeqNum, true, request->requestLength(),
				    request->requestBuf(), reply.maxReplyLength(),
				    reply.replyBuf(), actualReplyLength);
			}
			else
			{
				// TODO(GG): use code from previous drafts
				Assert(false);
			}

			// TODO(GG): TBD - how do we want to support empty replies? (actualReplyLength==0)

			if (!error && actualReplyLength > 0)
			{
				reply.setReplyLength(actualReplyLength);
				send(&reply, clientId);
			}

		#ifdef DEBUG_STATISTICS
			DebugStatistics::onRequestCompleted(true);
		#endif
		}



		void ReplicaImp::executeRequestsInPrePrepareMsg(PrePrepareMsg* ppMsg)
		{
			Assert(!stateTransfer->isCollectingState() && currentViewIsActive());
			Assert(ppMsg != nullptr);
			Assert(ppMsg->viewNumber() == curView);

			RequestsIterator reqIter(ppMsg);
			char* requestBody = nullptr;
			while (reqIter.getAndGoToNext(requestBody)) 
			{
				ClientRequestMsg req((ClientRequestMsgHeader*)requestBody);
				NodeIdType clientId = req.clientProxyId();

				const bool validClient = clientsManager->isValidClient(clientId);
				if (!validClient)
				{
					// TODO(GG): warning?
					continue;
				}
	
				if (clientsManager->seqNumberOfLastReplyToClient(clientId) >= req.requestSeqNum())
				{	
					ClientReplyMsg* replyMsg = clientsManager->allocateMsgWithLatestReply(clientId, currentPrimary());
					send(replyMsg, clientId);
					delete replyMsg;
	
					// TODO(GG): warning?
					continue;
				}


				uint32_t actualReplyLength = 0;
				int error = userRequestsHandler->execute(
						clientId, lastExecutedSeqNum + 1, req.isReadOnly(),
						req.requestLength(), req.requestBuf(),
						maxReplyMessageSize - sizeof(ClientReplyMsgHeader),
						replyBuffer, actualReplyLength);

				Assert(error == 0); // TODO(GG): TBD
			
				Assert(actualReplyLength > 0); // TODO(GG): TBD - how do we want to support empty replies? (actualReplyLength==0)

				ClientReplyMsg* replyMsg = clientsManager->allocateNewReplyMsgAndWriteToStorage(clientId, req.requestSeqNum(), currentPrimary(), replyBuffer, actualReplyLength);
	
				if (!supportDirectProofs && actualReplyLength != 0)
				{
					send(replyMsg, clientId);
				}
	
				delete replyMsg;

				clientsManager->removePendingRequestOfClient(clientId);
			}

			if ((lastExecutedSeqNum + 1) % checkpointWindowSize == 0) 
			{
				const uint64_t checkpointNum = (lastExecutedSeqNum + 1) / checkpointWindowSize;
				stateTransfer->createCheckpointOfCurrentState(checkpointNum);
			}

			lastExecutedSeqNum = lastExecutedSeqNum + 1;
                        metric_last_executed_seq_num_.Get().Set(lastExecutedSeqNum);

			LOG_INFO_F(GL, "\nReplica - executeRequestsInPrePrepareMsg() - lastExecutedSeqNum==%" PRId64 "", lastExecutedSeqNum);

			controller->onNewSeqNumberExecution(lastExecutedSeqNum);




			// TODO(GG): clean the following logic




			if (lastViewThatTransferredSeqNumbersFullyExecuted < curView && (lastExecutedSeqNum >= maxSeqNumTransferredFromPrevViews))
				lastViewThatTransferredSeqNumbersFullyExecuted = curView;

			{ // update dynamicUpperLimitOfRounds
				const SeqNumInfo& seqNumInfo = mainLog->get(lastExecutedSeqNum);
				const Time firstInfo = seqNumInfo.getTimeOfFisrtRelevantInfoFromPrimary();
				const Time currTime = getMonotonicTime();
				if ((firstInfo < currTime)) {
					const int64_t durationMilli = (subtract(currTime, firstInfo) / 1000);
					dynamicUpperLimitOfRounds->add(durationMilli);
				}
			}

			if (supportDirectProofs)
			{
				Assert(false);
				// TODO(GG): use code from previous drafts
			}

			if (lastExecutedSeqNum % checkpointWindowSize == 0)
			{
				Digest checkDigest;
				const uint64_t checkpointNum = lastExecutedSeqNum / checkpointWindowSize;
				stateTransfer->getDigestOfCheckpoint(checkpointNum, sizeof(Digest), (char*)&checkDigest);
				CheckpointMsg* checkMsg = new CheckpointMsg(myReplicaId, lastExecutedSeqNum, checkDigest, false);
				CheckpointInfo& checkInfo = checkpointsLog->get(lastExecutedSeqNum);
				checkInfo.addCheckpointMsg(checkMsg, myReplicaId);
				if (checkInfo.isCheckpointCertificateComplete())
				{
					onSeqNumIsStable(lastExecutedSeqNum);
				}
				checkInfo.setSelfExecutionTime(getMonotonicTime());
			}

			sendCheckpointIfNeeded();
	
		#ifdef DEBUG_STATISTICS
			DebugStatistics::onRequestCompleted(false);
		#endif
		}



		void ReplicaImp::executeReadWriteRequests(const bool requestMissingInfo) 
		{
			Assert(!stateTransfer->isCollectingState() && currentViewIsActive());
			Assert(lastExecutedSeqNum >= lastStableSeqNum);
	
			LOG_INFO_F(GL, "Calling to executeReadWriteRequests(requestMissingInfo=%d)",(int)requestMissingInfo);

			while (lastExecutedSeqNum < lastStableSeqNum + kWorkWindowSize)
			{
				SeqNumInfo& seqNumInfo = mainLog->get(lastExecutedSeqNum + 1);
		
				PrePrepareMsg* prePrepareMsg = seqNumInfo.getPrePrepareMsg();
		
				const bool ready = (prePrepareMsg != nullptr) && (seqNumInfo.isCommitted__gg());
		
				if (requestMissingInfo && !ready)
				{
					LOG_INFO_F(GL, "executeReadWriteRequests - Asking for missing information about %" PRId64 "", lastExecutedSeqNum + 1);
		
					tryToSendReqMissingDataMsg(lastExecutedSeqNum + 1);
				}
		
				if (!ready) break;

				Assert(prePrepareMsg->seqNumber() == lastExecutedSeqNum + 1);
				Assert(prePrepareMsg->viewNumber() == curView); // TODO(GG): TBD

				executeRequestsInPrePrepareMsg(prePrepareMsg);
			}	
	
			if (isCurrentPrimary() && requestsQueueOfPrimary.size() > 0)
				tryToSendPrePrepareMsg(true);
		}

                void ReplicaImp::SetAggregator(
                    std::shared_ptr<concordMetrics::Aggregator> aggregator) {
                  metricsComponent_.SetAggregator(aggregator);
                }


// TODO(GG): the timer for state transfer !!!!

// TODO(GG): !!!! view changes and retransmissionsLogic --- check ....

}
}
