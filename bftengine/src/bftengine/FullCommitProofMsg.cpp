//Concord
//
//Copyright (c) 2018 VMware, Inc. All Rights Reserved.
//
//This product is licensed to you under the Apache 2.0 license (the "License").  You may not use this product except in compliance with the Apache 2.0 License. 
//
//This product may include a number of subcomponents with separate copyright notices and license terms. Your use of these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE file.

#include <string.h>
#include "FullCommitProofMsg.hpp"
#include "assertUtils.hpp"

namespace bftEngine
{
	namespace impl
	{

		FullCommitProofMsg::FullCommitProofMsg(ReplicaId senderId, ViewNum v, SeqNum s, const char* commitProofSig, uint16_t commitProofSigLength)
			: MessageBase(senderId, MsgCode::FullCommitProof, sizeof(FullCommitProofMsgHeader) + commitProofSigLength)
		{
			b()->viewNum = v;
			b()->seqNum = s;
			b()->thresholSignatureLength = commitProofSigLength;
			memcpy(body() + sizeof(FullCommitProofMsgHeader), commitProofSig, commitProofSigLength);
		}

		bool FullCommitProofMsg::ToActualMsgType(const ReplicasInfo& repInfo, MessageBase* inMsg, FullCommitProofMsg*& outMsg)
		{
			Assert(inMsg->type() == MsgCode::FullCommitProof);
			if (inMsg->size() < sizeof(FullCommitProofMsgHeader)) return false;

			FullCommitProofMsg* t = (FullCommitProofMsg*)inMsg;

			if (t->senderId() == repInfo.myId()) return false;

			if (!repInfo.isIdOfReplica(t->senderId())) return false;

			uint16_t thresholSignatureLength = t->thresholSignatureLength();
			if (t->size() < (sizeof(FullCommitProofMsgHeader) + thresholSignatureLength)) return false;

			// TODO(GG): TBD - check something about the collectors identity (and in other similar messages)

			outMsg = (FullCommitProofMsg*)t;

			return true;
		}
	}
}
