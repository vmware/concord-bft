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

		class ReqMissingDataMsg : public MessageBase {

		public:
			ReqMissingDataMsg(ReplicaId senderId, ViewNum v, SeqNum s);

			ViewNum viewNumber() const { return b()->viewNum; }

			SeqNum seqNumber() const { return b()->seqNum; }

			bool getPrePrepareIsMissing() const { return (b()->missingFlags & 0x2) != 0; }
			bool getPartialProofIsMissing() const { return (b()->missingFlags & 0x4) != 0; }
			bool getPartialPrepareIsMissing() const { return (b()->missingFlags & 0x8) != 0; }
			bool getPartialCommitIsMissing() const { return (b()->missingFlags & 0x10) != 0; }
			bool getFullCommitProofIsMissing() const { return (b()->missingFlags & 0x20) != 0; }
			bool getFullPrepareIsMissing() const { return (b()->missingFlags & 0x40) != 0; }
			bool getFullCommitIsMissing() const { return (b()->missingFlags & 0x80) != 0; }

			uint16_t getFlags() const { return b()->missingFlags; }

			void resetFlags();

			void setPrePrepareIsMissing() { b()->missingFlags |= 0x2; }
			void setPartialProofIsMissing() { b()->missingFlags |= 0x4; }
			void setPartialPrepareIsMissing() { b()->missingFlags |= 0x8; }
			void setPartialCommitIsMissing() { b()->missingFlags |= 0x10; }
			void setFullCommitProofIsMissing() { b()->missingFlags |= 0x20; }
			void setFullPrepareIsMissing() { b()->missingFlags |= 0x40; }
			void setFullCommitIsMissing() { b()->missingFlags |= 0x80; }

			static bool ToActualMsgType(const ReplicasInfo& repInfo, MessageBase* inMsg, ReqMissingDataMsg*& outMsg);

		protected:
#pragma pack(push,1)
			struct ReqMissingDataMsgHeader : public MessageBase::Header {
				ViewNum viewNum;
				SeqNum seqNum;

				uint16_t missingFlags;
				// bit 0 : reserved
				// bit 1 : prePrepareIsMissing
				// bit 2 : partialProofIsMissing
				// bit 3 : partialPrepareIsMissing
				// bit 4 : partialCommitIsMissing
				// bit 5 : fullCommitProofIsMissing
				// bit 6 : fullPrepareIsMissing
				// bit 7 : fullCommitIsMissing
			};
#pragma pack(pop)
			static_assert(sizeof(ReqMissingDataMsgHeader) == (2 + 8 + 8 + 2), "ReqMissingDataMsgHeader is 58B");

			ReqMissingDataMsgHeader* b() const {
				return (ReqMissingDataMsgHeader*)msgBody_;
			}
		};
	}
}
