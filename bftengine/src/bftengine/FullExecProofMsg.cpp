//Concord
//
//Copyright (c) 2018 VMware, Inc. All Rights Reserved.
//
//This product is licensed to you under the Apache 2.0 license (the "License").  You may not use this product except in compliance with the Apache 2.0 License. 
//
//This product may include a number of subcomponents with separate copyright notices and license terms. Your use of these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE file.

#include <string.h>
#include "FullExecProofMsg.hpp"
#include "assertUtils.hpp"

namespace bftEngine
{
	namespace impl
	{

		FullExecProofMsg::FullExecProofMsg(ReplicaId senderId, NodeIdType clientId, ReqId requestId, uint16_t sigLength,
			const char* root, uint16_t rootLength,
			const char* executionProof, uint16_t proofLength) :
			MessageBase(senderId, MsgCode::FullExecProof, sizeof(FullExecProofMsgHeader) + sigLength + rootLength + proofLength)
		{
			b()->isNotReady = 1; // message is not ready  
			b()->idOfClient = clientId;
			b()->requestId = requestId;
			b()->signatureLength = sigLength;
			b()->merkleRootLength = rootLength;
			b()->executionProofLength = proofLength;

			memcpy(body() + sizeof(FullExecProofMsgHeader), root, rootLength);
			memcpy(body() + sizeof(FullExecProofMsgHeader) + rootLength, executionProof, proofLength);
		}

		FullExecProofMsg::FullExecProofMsg(ReplicaId senderId, NodeIdType clientId,
			const char* sig, uint16_t sigLength,
			const char* root, uint16_t rootLength,
			const char* readProof, uint16_t proofLength) :
			MessageBase(senderId, MsgCode::FullExecProof, sizeof(FullExecProofMsgHeader) + sigLength + rootLength + proofLength)
		{
			b()->isNotReady = 0; // message is ready 
			b()->idOfClient = clientId;
			b()->requestId = 0;
			b()->signatureLength = sigLength;
			b()->merkleRootLength = rootLength;
			b()->executionProofLength = proofLength;

			memcpy(body() + sizeof(FullExecProofMsgHeader), root, rootLength);
			memcpy(body() + sizeof(FullExecProofMsgHeader) + rootLength, readProof, proofLength);
			memcpy(body() + sizeof(FullExecProofMsgHeader) + rootLength + proofLength, sig, sigLength);
		}

		void FullExecProofMsg::setSignature(const char* sig, uint16_t sigLength)
		{
			Assert(b()->signatureLength >= sigLength);

			memcpy(body() + sizeof(FullExecProofMsgHeader) + b()->merkleRootLength + b()->executionProofLength, sig, sigLength);

			b()->signatureLength = sigLength;

			b()->isNotReady = 0; // message is ready now
		}

		bool FullExecProofMsg::ToActualMsgType(NodeIdType myId, MessageBase* inMsg, FullExecProofMsg*& outMsg)
		{
			Assert(inMsg->type() == MsgCode::FullExecProof);
			if (inMsg->size() < sizeof(FullExecProofMsgHeader)) return false;

			// TODO(GG)

			outMsg = (FullExecProofMsg*)inMsg;

			return true;
		}

	}
}
