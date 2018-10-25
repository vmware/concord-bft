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

		// TODO(GG): use SignedShareBase
		class FullCommitProofMsg : public MessageBase
		{

		public:
			FullCommitProofMsg(ReplicaId senderId, ViewNum v, SeqNum s, const char* commitProofSig, uint16_t commitProofSigLength);

			ViewNum viewNumber() const { return b()->viewNum; }

			SeqNum seqNumber() const { return b()->seqNum; }

			uint16_t thresholSignatureLength() const { return b()->thresholSignatureLength; }

			const char* thresholSignature() { return body() + sizeof(FullCommitProofMsgHeader); }

			static bool ToActualMsgType(const ReplicasInfo& repInfo, MessageBase* inMsg, FullCommitProofMsg*& outMsg);

		protected:
#pragma pack(push,1)
			struct FullCommitProofMsgHeader
			{
				MessageBase::Header header;
				ViewNum viewNum;
				SeqNum seqNum;
				uint16_t thresholSignatureLength;
			};
#pragma pack(pop)
			static_assert(sizeof(FullCommitProofMsgHeader) == (2 + 8 + 8 + 2), "FullCommitProofMsgHeader is 20B");

			FullCommitProofMsgHeader* b() const { return (FullCommitProofMsgHeader*)msgBody_; }
		};

	}
}
