// Concord
//
// Copyright (c) 2018 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#if defined(_WIN32)		 // TODO(GG): remove
#include <windows.h>
#endif


#include "ReplicaLoader.hpp"
#include "PersistentStorage.hpp"
#include "ReplicaImp.hpp"
#include "ViewsManager.hpp"
#include "FullCommitProofMsg.hpp"


# define Verify(expr, errorCode) {                                          \
    Assert(expr);                                                    \
    if((expr) != true) {                                                    \
        return errorCode;                                                   \
    }                                                                       \
}

#define Succ ReplicaLoader::ErrorCode::Success
#define InconsistentErr ReplicaLoader::ErrorCode::InconsistentData
#define NoDataErr ReplicaLoader::ErrorCode::NoDataInStorage

namespace bftEngine {
namespace impl {

// most code of ReplicaLoader is encapsulated in this file
namespace {

ReplicaLoader::ErrorCode checkReplicaConfig(const LoadedReplicaData& ld)
{
	const ReplicaConfig& c = ld.repConfig;
	Verify(c.fVal >= 1, InconsistentErr);
	Verify(c.cVal >= 0, InconsistentErr);
	
	uint16_t numOfReplicas = 3 * c.fVal + 2 * c.cVal + 1;

	Verify(numOfReplicas <= MaxNumberOfReplicas, InconsistentErr);

	Verify(c.replicaId >= 0 && c.replicaId < numOfReplicas, InconsistentErr);

	Verify(c.numOfClientProxies >= 1, InconsistentErr); // TODO(GG): TBD - do we want maximum number of client proxies?

	Verify(c.statusReportTimerMillisec > 0, InconsistentErr); // TODO(GG): TBD - do we want maximum for statusReportTimerMillisec?

	Verify(c.concurrencyLevel >= 1 && c.concurrencyLevel <= (checkpointWindowSize/5), InconsistentErr);

	std::set<uint16_t> repIDs;
	for (std::pair<uint16_t, std::string> v : c.publicKeysOfReplicas)	{	
		Verify(v.first >= 0 && v.first < numOfReplicas, InconsistentErr);
		Verify(!v.second.empty(), InconsistentErr); // TODO(GG): make sure that the key is valid
		repIDs.insert(v.first);
	}
	Verify(repIDs.size() == numOfReplicas, InconsistentErr);

	Verify(!c.replicaPrivateKey.empty(), InconsistentErr); // TODO(GG): make sure that the key is valid

//	Verify(c.thresholdSignerForExecution == nullptr, InconsistentErr);
//	Verify(c.thresholdVerifierForExecution == nullptr, InconsistentErr); 

	Verify(c.thresholdSignerForSlowPathCommit != nullptr, InconsistentErr);
	Verify(c.thresholdVerifierForSlowPathCommit != nullptr, InconsistentErr);

	if (c.cVal == 0) {
//		Verify(c.thresholdSignerForCommit == nullptr, InconsistentErr);
//		Verify(c.thresholdVerifierForCommit == nullptr, InconsistentErr);
	} else {
		Verify(c.thresholdSignerForCommit != nullptr, InconsistentErr);
		Verify(c.thresholdVerifierForCommit != nullptr, InconsistentErr);
	}

	Verify(c.thresholdSignerForOptimisticCommit != nullptr, InconsistentErr);
	Verify(c.thresholdVerifierForOptimisticCommit != nullptr, InconsistentErr);

	// TODO: make sure that the verifiers and signers are valid

	return Succ;
}

ReplicaLoader::ErrorCode loadConfig(PersistentStorage* p, LoadedReplicaData& ld) {
	Assert(p != nullptr);

	Verify(p->hasReplicaConfig(), NoDataErr);

	ld.repConfig = p->getReplicaConfig();

	ReplicaLoader::ErrorCode stat = checkReplicaConfig(ld);

	Verify((stat == Succ), stat);

	std::set<SigManager::PublicKeyDesc> replicasSigPublicKeys;

	for (auto e : ld.repConfig.publicKeysOfReplicas)
	{
		SigManager::PublicKeyDesc keyDesc = { e.first, e.second };
		replicasSigPublicKeys.insert(keyDesc);
	}			

	uint16_t numOfReplicas = (uint16_t)(3 * ld.repConfig.fVal + 2 * ld.repConfig.cVal + 1);

	ld.sigManager = new SigManager(ld.repConfig.replicaId, numOfReplicas + ld.repConfig.numOfClientProxies, ld.repConfig.replicaPrivateKey, replicasSigPublicKeys);

	ld.repsInfo = new ReplicasInfo(ld.repConfig.replicaId, *ld.sigManager, numOfReplicas, 
		ld.repConfig.fVal, ld.repConfig.cVal, dynamicCollectorForPartialProofs, dynamicCollectorForExecutionProofs);

	return Succ;
}

ReplicaLoader::ErrorCode checkViewDesc(
	const DescriptorOfLastExitFromView* exitDesc,
	const DescriptorOfLastNewView* newDesc)
{
	// TODO: check consistency of descriptors  
	return Succ;
}

ReplicaLoader::ErrorCode loadViewInfo(PersistentStorage* p, LoadedReplicaData& ld) {
	Assert(p != nullptr);
	Assert(ld.repsInfo != nullptr)
	Assert(ld.repConfig.thresholdVerifierForSlowPathCommit != nullptr);
	Assert(ld.viewsManager == nullptr);

	DescriptorOfLastExitFromView descriptorOfLastExitFromView;
	DescriptorOfLastNewView descriptorOfLastNewView;

	const bool hasDescLastExitFromView = p->hasDescriptorOfLastExitFromView();
	const bool hasDescOfLastNewView = p->hasDescriptorOfLastNewView();

	if (hasDescLastExitFromView)
		descriptorOfLastExitFromView = p->getAndAllocateDescriptorOfLastExitFromView();

	if (hasDescOfLastNewView)
		descriptorOfLastNewView = p->getAndAllocateDescriptorOfLastNewView();

	ReplicaLoader::ErrorCode stat = checkViewDesc(
		hasDescLastExitFromView ? &descriptorOfLastExitFromView : nullptr,
		hasDescOfLastNewView ? &descriptorOfLastNewView : nullptr);

	Verify((stat == Succ), stat);

	ViewNum initialViewNum = 0;
	bool isInView = false;
	ViewsManager* viewsManager = nullptr;
	if (!hasDescLastExitFromView && !hasDescOfLastNewView) {

		initialViewNum = 0;
		isInView = true;
		viewsManager = ViewsManager::createInsideViewZero(
			ld.repsInfo,
			ld.repConfig.thresholdVerifierForSlowPathCommit);

		Assert(viewsManager->latestActiveView() == 0);
		Assert(viewsManager->viewIsActive(0));

		ld.maxSeqNumTransferredFromPrevViews = 0;
	}
	else if (hasDescLastExitFromView && !hasDescOfLastNewView) {

		Verify((descriptorOfLastExitFromView.view == 0), InconsistentErr);

		initialViewNum = 0;
		isInView = false;
		viewsManager = ViewsManager::createOutsideView(
			ld.repsInfo,
			ld.repConfig.thresholdVerifierForSlowPathCommit,
			descriptorOfLastExitFromView.view,
			descriptorOfLastExitFromView.lastStable,
			descriptorOfLastExitFromView.lastExecuted,
			descriptorOfLastExitFromView.stableLowerBoundWhenEnteredToView,
			descriptorOfLastExitFromView.myViewChangeMsg,
			descriptorOfLastExitFromView.elements);

		Verify((viewsManager->latestActiveView() == 0), InconsistentErr);
		Verify((!viewsManager->viewIsActive(0)), InconsistentErr);

		ld.maxSeqNumTransferredFromPrevViews = 0;
	}
	else if (hasDescLastExitFromView && hasDescOfLastNewView &&
		(descriptorOfLastExitFromView.view == descriptorOfLastNewView.view)) {

		Verify((descriptorOfLastExitFromView.view >= 1), InconsistentErr);

		initialViewNum = descriptorOfLastExitFromView.view;
		isInView = false;
		viewsManager = ViewsManager::createOutsideView(
			ld.repsInfo,
			ld.repConfig.thresholdVerifierForSlowPathCommit,
			descriptorOfLastExitFromView.view,
			descriptorOfLastExitFromView.lastStable,
			descriptorOfLastExitFromView.lastExecuted,
			descriptorOfLastExitFromView.stableLowerBoundWhenEnteredToView,
			descriptorOfLastExitFromView.myViewChangeMsg,
			descriptorOfLastExitFromView.elements);

		Verify((viewsManager->latestActiveView() == descriptorOfLastExitFromView.view), InconsistentErr);
		Verify((!viewsManager->viewIsActive(descriptorOfLastExitFromView.view)), InconsistentErr);

		ld.maxSeqNumTransferredFromPrevViews = descriptorOfLastNewView.maxSeqNumTransferredFromPrevViews;
	}
	else if (hasDescLastExitFromView && hasDescOfLastNewView &&
		(descriptorOfLastExitFromView.view < descriptorOfLastNewView.view)) {

		Verify((descriptorOfLastExitFromView.view >= 0), InconsistentErr);
		Verify((descriptorOfLastNewView.view >= 1), InconsistentErr);

		initialViewNum = descriptorOfLastNewView.view;
		isInView = true;
		viewsManager = ViewsManager::createInsideView(
			ld.repsInfo,
			ld.repConfig.thresholdVerifierForSlowPathCommit,
			descriptorOfLastNewView.view,
			descriptorOfLastNewView.stableLowerBoundWhenEnteredToView,
			descriptorOfLastNewView.newViewMsg,
			descriptorOfLastNewView.myViewChangeMsg,
			descriptorOfLastNewView.viewChangeMsgs);

		ld.maxSeqNumTransferredFromPrevViews = descriptorOfLastNewView.maxSeqNumTransferredFromPrevViews;
	}
	else {
		return InconsistentErr;
	}

	ld.viewsManager = viewsManager;
	return Succ;
}

ReplicaLoader::ErrorCode loadReplicaData(
	PersistentStorage* p,
	LoadedReplicaData& ld)
{
	Assert(p != nullptr);

	ReplicaLoader::ErrorCode stat = loadConfig(p, ld);

	Verify((stat == Succ), stat);

	stat = loadViewInfo(p, ld);

	Verify((stat == Succ), stat);

	ld.primaryLastUsedSeqNum = p->getPrimaryLastUsedSeqNum();
	ld.lastStableSeqNum = p->getLastStableSeqNum();
	ld.lastExecutedSeqNum = p->getLastExecutedSeqNum();
	ld.strictLowerBoundOfSeqNums = p->getStrictLowerBoundOfSeqNums();
	

	Verify((ld.primaryLastUsedSeqNum >= 0), InconsistentErr);
	Verify((ld.primaryLastUsedSeqNum <= ld.lastStableSeqNum + kWorkWindowSize), InconsistentErr);
	Verify((ld.lastStableSeqNum >= 0), InconsistentErr);
	Verify((ld.lastExecutedSeqNum >= ld.lastStableSeqNum), InconsistentErr);
	Verify((ld.lastExecutedSeqNum < ld.lastStableSeqNum + kWorkWindowSize), InconsistentErr);
	Verify((ld.strictLowerBoundOfSeqNums >= 0), InconsistentErr);
	Verify((ld.strictLowerBoundOfSeqNums < ld.lastStableSeqNum + kWorkWindowSize), InconsistentErr);
	
	const ViewNum lastView = ld.viewsManager->latestActiveView();
	const bool isInView = ld.viewsManager->viewIsActive(lastView);

	ld.lastViewThatTransferredSeqNumbersFullyExecuted = p->getLastViewThatTransferredSeqNumbersFullyExecuted();

	Verify((ld.lastViewThatTransferredSeqNumbersFullyExecuted >= 0), InconsistentErr);
	Verify((ld.lastViewThatTransferredSeqNumbersFullyExecuted <= lastView), InconsistentErr);

	Assert(ld.maxSeqNumTransferredFromPrevViews != 0 || lastView == 0);
	Verify((ld.maxSeqNumTransferredFromPrevViews >= 0), InconsistentErr);
	Verify((ld.maxSeqNumTransferredFromPrevViews <= ld.lastStableSeqNum + kWorkWindowSize), InconsistentErr);

	if (isInView) {
		SeqNum curSeqNum = ld.lastStableSeqNum;
		for (size_t i = 0; i < sizeof(ld.seqNumWinArr) / sizeof(LoadedReplicaData::SeqNumWinData); i++)
		{
			curSeqNum++;
			LoadedReplicaData::SeqNumWinData& e = ld.seqNumWinArr[i];
			e.prePrepareMsg = p->getAndAllocatePrePrepareMsgInSeqNumWindow(curSeqNum);
			if (e.prePrepareMsg != nullptr) {
				e.slowStarted = p->getSlowStartedInSeqNumWindow(curSeqNum);
				e.fullCommitProofMsg = p->getAndAllocateFullCommitProofMsgInSeqNumWindow(curSeqNum);
				e.forceCompleted = p->getForceCompletedInSeqNumWindow(curSeqNum);
				e.prepareFullMsg = p->getAndAllocatePrepareFullMsgInSeqNumWindow(curSeqNum);
				e.commitFullMsg = p->getAndAllocateCommitFullMsgInSeqNumWindow(curSeqNum);

				Verify((e.prePrepareMsg->seqNumber() == curSeqNum), InconsistentErr);
				Verify((e.prePrepareMsg->viewNumber() == lastView), InconsistentErr);

				// TODO(GG): consider to check digests + signatures

				if (e.fullCommitProofMsg != nullptr)
				{
					Verify((e.fullCommitProofMsg->seqNumber() == curSeqNum), InconsistentErr);
					Verify((e.fullCommitProofMsg->viewNumber() == lastView), InconsistentErr);
				}
				if (e.prepareFullMsg != nullptr)
				{
					Verify((e.prepareFullMsg->seqNumber() == curSeqNum), InconsistentErr);
					Verify((e.prepareFullMsg->viewNumber() == lastView), InconsistentErr);
				}
				if (e.commitFullMsg != nullptr)
				{
					Verify((e.commitFullMsg->seqNumber() == curSeqNum), InconsistentErr);
					Verify((e.commitFullMsg->viewNumber() == lastView), InconsistentErr);
				}
			}	
		}
	}

	SeqNum seqNum = ld.lastStableSeqNum;
	for (size_t i = 0; i < sizeof(ld.checkWinArr) / sizeof(LoadedReplicaData::CheckWinData); i++)
	{
		LoadedReplicaData::CheckWinData& e = ld.checkWinArr[i];
		e.checkpointMsg = p->getAndAllocateCheckpointMsgInCheckWindow(seqNum);
		e.completedMark = p->getCompletedMarkInCheckWindow(seqNum);

		if (seqNum > 0 && seqNum <= ld.lastExecutedSeqNum)
		{
			Verify((e.checkpointMsg != nullptr), InconsistentErr);
			Verify((e.checkpointMsg->seqNumber() == seqNum), InconsistentErr);
			Verify((e.checkpointMsg->senderId() == ld.repConfig.replicaId), InconsistentErr);
			Verify((seqNum > ld.lastStableSeqNum || e.checkpointMsg->isStableState()), InconsistentErr);
		}
		else
		{
			Verify((e.checkpointMsg == nullptr), InconsistentErr);
		}

		seqNum = seqNum + checkpointWindowSize;
	}

	if (p->hasDescriptorOfLastExecution()) {
		DescriptorOfLastExecution d = p->getDescriptorOfLastExecution();
		if (d.executedSeqNum > ld.lastExecutedSeqNum) { 
			Verify((d.executedSeqNum == ld.lastExecutedSeqNum + 1), InconsistentErr);
			Verify(isInView, InconsistentErr);
			Verify(d.executedSeqNum >  ld.lastStableSeqNum, InconsistentErr);
			Verify(d.executedSeqNum <= ld.lastStableSeqNum + kWorkWindowSize, InconsistentErr);

			uint64_t idx = d.executedSeqNum - ld.lastStableSeqNum;
			Assert(idx < kWorkWindowSize);

			const LoadedReplicaData::SeqNumWinData& e = ld.seqNumWinArr[idx];

			Verify(e.prePrepareMsg != nullptr, InconsistentErr);
			Verify(e.prePrepareMsg->seqNumber() == ld.lastExecutedSeqNum + 1, InconsistentErr);
			Verify(e.prePrepareMsg->viewNumber() == ld.viewsManager->latestActiveView(), InconsistentErr);
			Verify(e.prePrepareMsg->numberOfRequests() > 0, InconsistentErr);

			uint32_t numOfReqs = 0;
			for (uint32_t i = 0; i < d.validRequests.numOfBits(); i++) {
				if (d.validRequests.get(i) == true) numOfReqs++;
			}

			Verify(numOfReqs <= e.prePrepareMsg->numberOfRequests(), InconsistentErr);

			ld.isExecuting = true;
			ld.validRequestsThatAreBeingExecuted = d.validRequests;																				
		}
	}

	return Succ;		 
}

void freeReplicaData(LoadedReplicaData& ld)
{
	for (size_t i = 0; i < sizeof(ld.checkWinArr) / sizeof(ld.checkWinArr[0]); i++)	{
		delete ld.checkWinArr[i].checkpointMsg;
	}

	for (size_t i = 0; i < sizeof(ld.seqNumWinArr) / sizeof(ld.seqNumWinArr[0]); i++) {
		delete ld.seqNumWinArr[i].prePrepareMsg;
		delete ld.seqNumWinArr[i].fullCommitProofMsg;
		delete ld.seqNumWinArr[i].prepareFullMsg;
		delete ld.seqNumWinArr[i].commitFullMsg;				
	}

	delete ld.viewsManager;
	delete ld.repsInfo;
	delete ld.sigManager;
}





}



LoadedReplicaData ReplicaLoader::loadReplica(PersistentStorage* p, ReplicaLoader::ErrorCode& outErrCode)
{
	Assert(p != nullptr);
	LoadedReplicaData ld;
	outErrCode = loadReplicaData(p, ld);

	if(outErrCode != Succ) {
	  freeReplicaData(ld);
	}

	return ld;
}







}  // namespace impl
}  // namespace bftEngine
