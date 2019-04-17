//Concord
//
//Copyright (c) 2018 VMware, Inc. All Rights Reserved.
//
//This product is licensed to you under the Apache 2.0 license (the "License").  You may not use this product except in compliance with the Apache 2.0 License. 
//
//This product may include a number of subcomponents with separate copyright notices and license terms. Your use of these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE file.

#pragma once

#include "PrimitiveTypes.hpp"

#include "CollectorOfThresholdSignatures.hpp"

#include "SeqNumInfo.hpp"

#include "Digest.hpp"
#include "Crypto.hpp"
#include "DebugStatistics.hpp"
#include "SimpleThreadPool.hpp"
#include "ControllerBase.hpp"
#include "CheckpointMsg.hpp"
#include "RetransmissionsManager.hpp"
#include "DynamicUpperLimitWithSimpleFilter2.hpp"
#include "ViewsManager.hpp"
#include "ReplicasInfo.hpp"
#include "MsgsCertificate.hpp"
#include "InternalReplicaApi.hpp"
#include "IStateTransfer.hpp"
#include "ClientsManager.hpp"
#include "CheckpointInfo.hpp"
#include "ICommunication.hpp"
#include "Replica.hpp"
#include "Threading.h"
#include "Metrics.hpp"

#include <thread>




namespace bftEngine
{
	namespace impl
	{
		class ClientRequestMsg;
		class ClientReplyMsg;
		class PrePrepareMsg;
		class CheckpointMsg;
		class ViewChangeMsg;
		class NewViewMsg;
		class ClientReplyMsg;
		class StartSlowCommitMsg;
		class ReqMissingDataMsg;
		class PreparePartialMsg;
		class PrepareFullMsg;
		class SimpleAckMsg;
		class StateTransferMsg;
		class ReplicaStatusMsg;

		class ReplicaImp;

		using PtrToMetaMsgHandler = void (ReplicaImp::*)(MessageBase* msg);


		class ReplicaImp : public InternalReplicaApi, public IReplicaForStateTransfer
		{
		protected:
			// system params
			const ReplicaId myReplicaId;
			const uint16_t fVal;
			const uint16_t cVal;
			const uint16_t numOfReplicas;
			const uint16_t numOfClientProxies;
			const bool viewChangeProtocolEnabled;
			const bool supportDirectProofs; // TODO(GG): add support

			// pointers to message handlers
			const std::unordered_map<uint16_t, PtrToMetaMsgHandler> metaMsgHandlers;

			// communication
			class MsgReceiver; // forward declaration
			IncomingMsgsStorage incomingMsgsStorage;
			MsgReceiver* msgReceiver;
			ICommunication* communication;

			// main thread of the this replica
			std::thread mainThread;
			bool mainThreadStarted;
			bool mainThreadShouldStop;

			// thread pool of this replica
			SimpleThreadPool internalThreadPool; // TODO(GG): !!!! rename

			// retransmissions manager (can be disabled)
			RetransmissionsManager* retransmissionsManager = nullptr;


			// scheduler
			SimpleOperationsScheduler timersScheduler;

			// controller
			ControllerBase* controller;

			// general information about the replicas
			ReplicasInfo* repsInfo;

			// digital signatures
			SigManager* sigManager;

			// view change logic
			ViewsManager* viewsManager = nullptr;

			//
			uint16_t maxConcurrentAgreementsByPrimary;

			// the current view of this replica
			ViewNum curView;

			// the last SeqNum used to generate a new SeqNum (valid when this replica is the primary) 
			SeqNum primaryLastUsedSeqNum;

			// the latest stable SeqNum known to this replica
			SeqNum lastStableSeqNum;

			// last SeqNum executed  by this replica (or its affect was transferred to this replica)
			SeqNum lastExecutedSeqNum;

			//
			SeqNum strictLowerBoundOfSeqNums;

			// 
			SeqNum maxSeqNumTransferredFromPrevViews;

			// requests queue (used by the primary)
			std::queue<ClientRequestMsg*> requestsQueueOfPrimary; // only used by the primary

			// bounded log used to store information about SeqNums in the range (lastStableSeqNum,lastStableSeqNum + kWorkWindowSize]
			SequenceWithActiveWindow<kWorkWindowSize, 1, SeqNum, SeqNumInfo, SeqNumInfo>* mainLog;

			// bounded log used to store information about checkpoints in the range [lastStableSeqNum,lastStableSeqNum + kWorkWindowSize]
			SequenceWithActiveWindow<kWorkWindowSize + checkpointWindowSize, checkpointWindowSize, SeqNum, CheckpointInfo, CheckpointInfo>* checkpointsLog;


			// last known stable checkpoint of each peer replica. 
			// We sometimes delete checkpoints before lastExecutedSeqNum
			std::map<ReplicaId, CheckpointMsg*> tableOfStableCheckpoints;

			// managing information about the clients
			ClientsManager* clientsManager = nullptr;

			// buffer used to store replies
			char* replyBuffer;

			// pointer to a state transfer module
			bftEngine::IStateTransfer* stateTransfer = nullptr;


			// variables that are used to heuristically compute the 'optimal' batch size
			size_t maxNumberOfPendingRequestsInRecentHistory;
			size_t batchingFactor;

			RequestsHandler* const userRequestsHandler;

			// Threshold signatures
			IThresholdSigner* thresholdSignerForExecution;
			IThresholdVerifier* thresholdVerifierForExecution;

			IThresholdSigner* thresholdSignerForSlowPathCommit;
			IThresholdVerifier* thresholdVerifierForSlowPathCommit;

			IThresholdSigner* thresholdSignerForCommit;
			IThresholdVerifier* thresholdVerifierForCommit;

			IThresholdSigner* thresholdSignerForOptimisticCommit;
			IThresholdVerifier* thresholdVerifierForOptimisticCommit;

			// used to dynamically estimate a upper bound for consensus rounds 
			DynamicUpperLimitWithSimpleFilter<int64_t>* dynamicUpperLimitOfRounds;

			// 
			ViewNum lastViewThatTransferredSeqNumbersFullyExecuted;

			// 
			Time lastTimeThisReplicaSentStatusReportMsgToAllPeerReplicas;
			Time timeOfLastStateSynch;   // last time the replica received a new state (via the state transfer mechanism)
			Time timeOfLastViewEntrance; // last time the replica entered to a new view

			//
			ViewNum lastAgreedView;         // latest view number v such that the replica received 2f+2c+1 ViewChangeMsg messages with view >= v
			Time timeOfLastAgreedView;      // last time we changed lastAgreedView

			// timers
			Timer* stateTranTimer;
			Timer* retranTimer;
			Timer* slowPathTimer;
			Timer* infoReqTimer;
			Timer* statusReportTimer;
			Timer* viewChangeTimer;
			Timer* debugStatTimer = nullptr;

			int viewChangeTimerMilli;

			class MsgReceiver : public IReceiver
			{
			public:
				MsgReceiver(IncomingMsgsStorage& queue);

				virtual ~MsgReceiver() {};

				virtual void onNewMessage(const NodeNum sourceNode,
					const char* const message, const size_t messageLength) override;

				virtual void onConnectionStatusChanged(const NodeNum node, const ConnectionStatus newStatus) override;

			private:
				
				IncomingMsgsStorage& incomingMsgs;
			};


			class StopInternalMsg : public InternalMessage
			{
			public:
				StopInternalMsg(ReplicaImp* myReplica);
				virtual void handle();
			protected:
				ReplicaImp* replica;
			};

			friend class StopInternalMsg;

			// this event is signalled iff the start() method has completed
			// and the process_message() method has not start working yet
			SimpleAutoResetEvent startSyncEvent;

                        //******** METRICS ************************************
                        concordMetrics::Component metricsComponent_;

                        typedef concordMetrics::Component::Handle<
                          concordMetrics::Gauge> GaugeHandle;
                        typedef concordMetrics::Component::Handle<
                          concordMetrics::Status> StatusHandle;
                        typedef concordMetrics::Component::Handle<
                          concordMetrics::Counter> CounterHandle;

                        GaugeHandle metric_view_;
                        GaugeHandle metric_last_stable_seq_num__;
                        GaugeHandle metric_last_executed_seq_num_;
                        GaugeHandle metric_last_agreed_view_;

                        CounterHandle metric_slow_path_count_;
                        CounterHandle metric_received_msgs_;

                        //*****************************************************


		public:


			ReplicaImp(const ReplicaConfig&, RequestsHandler* requestsHandler,
				IStateTransfer* stateTransfer, ICommunication* communication);
			virtual ~ReplicaImp();

			void start();
			void stop(); // TODO(GG): support "restart"
			bool isRunning() const;
			SeqNum getLastExecutedSequenceNum() const;



			void recvMsg(void*& item, bool& external);

			void processMessages();

			// IReplicaForStateTransfer
			virtual void freeStateTransferMsg(char* m) override;
			virtual void sendStateTransferMessage(char* m, uint32_t size, uint16_t replicaId) override;
			virtual void onTransferringComplete(int64_t checkpointNumberOfNewState) override;
			virtual void changeStateTransferTimerPeriod(uint32_t timerPeriodMilli) override;

			// InternalReplicaApi
			virtual void onInternalMsg(FullCommitProofMsg* m) override;
			virtual void onMerkleExecSignature(ViewNum v, SeqNum s, uint16_t signatureLength, const char* signature) override;

		protected:

			static std::unordered_map<uint16_t, PtrToMetaMsgHandler> createMapOfMetaMsgHandlers();

			template <typename T> void metaMessageHandler(MessageBase* msg);
			template <typename T> void metaMessageHandler_IgnoreWhenCollectingState(MessageBase* msg); // TODO(GG): rename 

			ReplicaId currentPrimary() const { return repsInfo->primaryOfView(curView); }
			bool isCurrentPrimary() const { return (currentPrimary() == myReplicaId); }

			static const uint16_t ALL_OTHER_REPLICAS = UINT16_MAX;
			void send(MessageBase* m, NodeIdType dest);
			void sendToAllOtherReplicas(MessageBase *m);
			void sendRaw(char* m, NodeIdType dest, uint16_t type, MsgSize size);

			bool tryToEnterView();
			void onNewView(const std::vector<PrePrepareMsg*>& prePreparesForNewView);
			void MoveToHigherView(ViewNum nextView); // also sends the ViewChangeMsg message
			void GotoNextView();

			void tryToSendStatusReport();
			void tryToSendReqMissingDataMsg(SeqNum seqNumber, bool slowPathOnly = false, uint16_t destReplicaId = ALL_OTHER_REPLICAS);

#ifdef DEBUG_STATISTICS
			friend class DebugStatistics;
#endif

			void onMessage(ClientRequestMsg*);
			void onMessage(PrePrepareMsg*);
			void onMessage(StartSlowCommitMsg*);
			void onMessage(PartialCommitProofMsg*);
			void onMessage(FullCommitProofMsg*);
			void onMessage(PartialExecProofMsg*);
			void onMessage(PreparePartialMsg*);
			void onMessage(CommitPartialMsg*);
			void onMessage(PrepareFullMsg*);
			void onMessage(CommitFullMsg*);
			void onMessage(CheckpointMsg*);
			void onMessage(ViewChangeMsg*);
			void onMessage(NewViewMsg*);
			void onMessage(ReqMissingDataMsg*);
			void onMessage(SimpleAckMsg*);
			void onMessage(ReplicaStatusMsg*);
			void onMessage(StateTransferMsg*);

			bool handledByRetransmissionsManager(const ReplicaId sourceReplica, const ReplicaId destReplica, const ReplicaId primaryReplica, const SeqNum seqNum, const uint16_t msgType);

			void sendRetransmittableMsgToReplica(MessageBase *m, ReplicaId destReplica, SeqNum s, bool ignorePreviousAcks = false);
			void sendAckIfNeeded(MessageBase* msg, const NodeIdType sourceNode, const SeqNum seqNum);

			void tryToSendPrePrepareMsg(bool batchingLogic = false);

			void sendPartialProof(SeqNumInfo&);

			void tryToStartSlowPaths();

			void tryToAskForMissingInfo();

			void sendPreparePartial(SeqNumInfo&);
			void sendCommitPartial(SeqNum);				// TODO(GG): the argument should be a ref to SeqNumInfo

			void executeReadOnlyRequest(ClientRequestMsg *m);

			void executeReadWriteRequests(const bool requestMissingInfo = false);

			void executeRequestsInPrePrepareMsg(PrePrepareMsg *pp);

			void onSeqNumIsStable(SeqNum);

			void onSeqNumIsStableWithoutRefCheckpoint(SeqNum);

			void onTransferringCompleteImp(SeqNum);

			bool currentViewIsActive() const
			{
				return (viewsManager->viewIsActive(curView));
			}

			template <typename T> bool relevantMsgForActiveView(const T* msg);

			void onReportAboutAdvancedReplica(ReplicaId reportedReplica, SeqNum seqNum, ViewNum viewNum);
			void onReportAboutLateReplica(ReplicaId reportedReplica, SeqNum seqNum, ViewNum viewNum);
			void onReportAboutAdvancedReplica(ReplicaId reportedReplica, SeqNum seqNum);
			void onReportAboutLateReplica(ReplicaId reportedReplica, SeqNum seqNum);

			void onReportAboutInvalidMessage(MessageBase* msg);

			void sendCheckpointIfNeeded();

			void  commitFullCommitProof(SeqNum seqNum, SeqNumInfo& seqNumInfo);
			void commitAndSendFullCommitProof(SeqNum seqNum, SeqNumInfo& seqNumInfo, PartialProofsSet& partialProofs);

			virtual IncomingMsgsStorage& getIncomingMsgsStorage() override
			{
				return incomingMsgsStorage;
			}

			virtual SimpleThreadPool& getInternalThreadPool() override
			{
				return internalThreadPool;
			}

			virtual IThresholdVerifier* getThresholdVerifierForExecution() override
			{
				return thresholdVerifierForExecution;
			}

			virtual IThresholdVerifier* getThresholdVerifierForSlowPathCommit() override
			{
				return thresholdVerifierForSlowPathCommit;
			}

			virtual IThresholdVerifier* getThresholdVerifierForCommit() override
			{
				return thresholdVerifierForCommit;
			}

			virtual IThresholdVerifier* getThresholdVerifierForOptimisticCommit() override
			{
				return thresholdVerifierForOptimisticCommit;
			}

			virtual const ReplicasInfo& getReplicasInfo() override
			{
				return (*repsInfo);
			}

			virtual Timer& getViewChangeTimer() override
			{
				return *viewChangeTimer;
			}

			virtual Timer& getStateTranTimer() override
			{
				return *stateTranTimer;
			}

			virtual Timer& getRetransmissionsTimer() override
			{
				return *retranTimer;
			}

			virtual Timer& getStatusTimer() override
			{
				return *statusReportTimer;
			}

			virtual Timer& getSlowPathTimer() override
			{
				return *slowPathTimer;
			}

			virtual Timer& getInfoRequestTimer() override
			{
				return *infoReqTimer;
			}

			virtual Timer& getDebugStatTimer() override
			{
				return *debugStatTimer;
			}



			virtual void onViewsChangeTimer(Time cTime, Timer& timer) override;
			virtual void onStateTranTimer(Time cTime, Timer& timer) override;
			virtual void onRetransmissionsTimer(Time cTime, Timer& timer) override;
			virtual void onStatusReportTimer(Time cTime, Timer& timer) override;
			virtual void onSlowPathTimer(Time cTime, Timer& timer) override;
			virtual void onInfoRequestTimer(Time cTime, Timer& timer) override;
			virtual void onDebugStatTimer(Time cTime, Timer& timer) override;

			// handlers for internal messages

			virtual void onPrepareCombinedSigFailed(SeqNum seqNumber, ViewNum  view, const std::set<uint16_t>& replicasWithBadSigs) override;
			virtual void onPrepareCombinedSigSucceeded(SeqNum seqNumber, ViewNum  view, const char* combinedSig, uint16_t combinedSigLen) override;
			virtual void onPrepareVerifyCombinedSigResult(SeqNum seqNumber, ViewNum  view, bool isValid) override;

			virtual void onCommitCombinedSigFailed(SeqNum seqNumber, ViewNum  view, const std::set<uint16_t>& replicasWithBadSigs) override;
			virtual void onCommitCombinedSigSucceeded(SeqNum seqNumber, ViewNum  view, const char* combinedSig, uint16_t combinedSigLen) override;
			virtual void onCommitVerifyCombinedSigResult(SeqNum seqNumber, ViewNum  view, bool isValid) override;

			virtual void onRetransmissionsProcessingResults(SeqNum relatedLastStableSeqNum, const ViewNum relatedViewNumber,
				const std::forward_list<RetSuggestion>* const suggestedRetransmissions) override;  // TODO(GG): use generic iterators 

                        void SetAggregator(std::shared_ptr<concordMetrics::Aggregator> aggregator);

		};



	}
}


