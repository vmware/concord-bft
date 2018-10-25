//Concord
//
//Copyright (c) 2018 VMware, Inc. All Rights Reserved.
//
//This product is licensed to you under the Apache 2.0 license (the "License").  You may not use this product except in compliance with the Apache 2.0 License. 
//
//This product may include a number of subcomponents with separate copyright notices and license terms. Your use of these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE file.

#pragma once

#include "MessageBase.hpp"

namespace bftEngine
{
	namespace impl
	{

		// TODO(GG): change class name (also used for read-only operations)
		class FullExecProofMsg : public MessageBase
		{

		public:
			FullExecProofMsg(ReplicaId senderId, NodeIdType clientId, ReqId requestId,
				uint16_t sigLength,
				const char* root, uint16_t rootLength,
				const char* executionProof, uint16_t proofLength);

			FullExecProofMsg(ReplicaId senderId, NodeIdType clientId,
				const char* sig, uint16_t sigLength,
				const char* root, uint16_t rootLength,
				const char* readProof, uint16_t proofLength);

			bool isReady() const { return (b()->isNotReady == 0); }

			bool isForReadOnly() const { return (b()->requestId == 0); }

			NodeIdType clientId() const { return b()->idOfClient; }

			ReqId requestId() const { return b()->requestId; }

			void setSignature(const char* sig, uint16_t sigLength);

			uint16_t signatureLength() { return b()->signatureLength; }
			const char* signature() { return body() + sizeof(FullExecProofMsgHeader) + b()->merkleRootLength + b()->executionProofLength; }

			uint16_t rootLength() { return b()->merkleRootLength; }
			const char* root() { return body() + sizeof(FullExecProofMsgHeader); }

			uint16_t executionProofLength() { return b()->executionProofLength; }
			const char* executionProof() { return body() + sizeof(FullExecProofMsgHeader) + b()->merkleRootLength; }

			static bool ToActualMsgType(NodeIdType myId, MessageBase* inMsg, FullExecProofMsg*& outMsg);

		protected:
#pragma pack(push, 1)
			struct FullExecProofMsgHeader
			{
				MessageBase::Header header;
				NodeIdType idOfClient;
				ReqId requestId;      // requestId==0, for read-only operations
				uint16_t isNotReady;  // TODO(GG): really needed ....?
				uint16_t signatureLength;
				uint16_t merkleRootLength;
				uint16_t executionProofLength;
				// followed by: 
				// 1. the merkle root (merkleRootLength bytes) 
				// 2. the merkle proof (executionProofLength bytes)
				// 3. signature (signatureLength bytes) 
			};
#pragma pack(pop)
			static_assert(sizeof(FullExecProofMsgHeader) == (2 + 2 + 8 + 2 + 2 + 2 + 2), "FullExecProofMsgHeader is 20B");

			FullExecProofMsgHeader* b() const
			{
				return (FullExecProofMsgHeader*)msgBody_;
			}
		};

	}
}
