//Concord
//
//Copyright (c) 2018 VMware, Inc. All Rights Reserved.
//
//This product is licensed to you under the Apache 2.0 license (the "License").  You may not use this product except in compliance with the Apache 2.0 License. 
//
//This product may include a number of subcomponents with separate copyright notices and license terms. Your use of these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE file.


#include "SignedShareMsgs.hpp"
#include "Crypto.hpp"
#include "assertUtils.hpp"

namespace bftEngine
{
	namespace impl
	{

		///////////////////////////////////////////////////////////////////////////////
		// SignedShareBase
		///////////////////////////////////////////////////////////////////////////////


		SignedShareBase::SignedShareBase(ReplicaId sender, int16_t type, size_t msgSize) :
			MessageBase(sender, type, msgSize)
		{
		}


		SignedShareBase* SignedShareBase::create(int16_t type, ViewNum v, SeqNum s, ReplicaId senderId, Digest& digest, IThresholdSigner* thresholdSigner)
		{
			const size_t sigLen = thresholdSigner->requiredLengthForSignedData();
			size_t size = sizeof(SignedShareBaseHeader) + sigLen;

			SignedShareBase* m = new SignedShareBase(senderId, type, size);

			m->b()->seqNumber = s;
			m->b()->viewNumber = v;
			m->b()->thresSigLength = (uint16_t)sigLen;

			Digest tmpDigest;
			Digest::calcCombination(digest, v, s, tmpDigest);

			thresholdSigner->signData((const char*)(&(tmpDigest)), sizeof(Digest),
				m->body() + sizeof(SignedShareBaseHeader), sigLen);

			return m;
		}

		SignedShareBase* SignedShareBase::create(int16_t type, ViewNum v, SeqNum s, ReplicaId senderId, const char* sig, uint16_t sigLen)
		{
			size_t size = sizeof(SignedShareBaseHeader) + sigLen;

			SignedShareBase* m = new SignedShareBase(senderId, type, size);

			m->b()->seqNumber = s;
			m->b()->viewNumber = v;
			m->b()->thresSigLength = sigLen;

			memcpy(m->body() + sizeof(SignedShareBaseHeader), sig, sigLen);

			return m;
		}


		bool SignedShareBase::ToActualMsgType(const ReplicasInfo& repInfo, int16_t type, MessageBase* inMsg, SignedShareBase*& outMsg)
		{
			Assert(inMsg->type() == type);
			if (inMsg->size() < sizeof(SignedShareBaseHeader)) return false;

			SignedShareBase* t = (SignedShareBase*)inMsg;

			// size
			if (t->size() < sizeof(SignedShareBaseHeader) + t->signatureLen())	return false;

			// sent from another replica 
			if (t->senderId() == repInfo.myId()) return false;

			if (!repInfo.isIdOfReplica(t->senderId())) return false;

			outMsg = t;
			return true;
		}


		///////////////////////////////////////////////////////////////////////////////
		// PreparePartialMsg
		///////////////////////////////////////////////////////////////////////////////


		PreparePartialMsg* PreparePartialMsg::create(ViewNum v, SeqNum s, ReplicaId senderId, Digest &ppDigest, IThresholdSigner* thresholdSigner)
		{
			return (PreparePartialMsg*)SignedShareBase::create(MsgCode::PreparePartial, v, s, senderId, ppDigest, thresholdSigner);
		}



		bool PreparePartialMsg::ToActualMsgType(const ReplicasInfo& repInfo, MessageBase* inMsg, PreparePartialMsg*& outMsg)
		{
			SignedShareBase* pOutMsg = nullptr;
			bool r = SignedShareBase::ToActualMsgType(repInfo, MsgCode::PreparePartial, inMsg, pOutMsg);
			if (!r) return false;

			if (repInfo.myId() != repInfo.primaryOfView(pOutMsg->viewNumber())) return false; // (the primary is the collector of PreparePartialMsg)

			outMsg = (PreparePartialMsg*)pOutMsg;
			
			return true;
		}


		///////////////////////////////////////////////////////////////////////////////
		// PrepareFullMsg
		///////////////////////////////////////////////////////////////////////////////

		PrepareFullMsg* PrepareFullMsg::create(ViewNum v, SeqNum s, ReplicaId senderId, const char* sig, uint16_t sigLen)
		{
			return (PrepareFullMsg*)SignedShareBase::create(MsgCode::PrepareFull, v, s, senderId, sig, sigLen);
		}

		bool PrepareFullMsg::ToActualMsgType(const ReplicasInfo& repInfo, MessageBase* inMsg, PrepareFullMsg*& outMsg)
		{
			SignedShareBase* pOutMsg = nullptr;
			bool r = SignedShareBase::ToActualMsgType(repInfo, MsgCode::PrepareFull, inMsg, pOutMsg);
			if (r) outMsg = (PrepareFullMsg*)pOutMsg;
			return r;
		}


		///////////////////////////////////////////////////////////////////////////////
		// CommitPartialMsg
		///////////////////////////////////////////////////////////////////////////////

		CommitPartialMsg* CommitPartialMsg::create(ViewNum v, SeqNum s, ReplicaId senderId, Digest &ppDoubleDigest, IThresholdSigner* thresholdSigner)
		{
			return (CommitPartialMsg*)SignedShareBase::create(MsgCode::CommitPartial, v, s, senderId, ppDoubleDigest, thresholdSigner);
		}

		bool CommitPartialMsg::ToActualMsgType(const ReplicasInfo& repInfo, MessageBase* inMsg, CommitPartialMsg*& outMsg)
		{
			SignedShareBase* pOutMsg = nullptr;
			bool r = SignedShareBase::ToActualMsgType(repInfo, MsgCode::CommitPartial, inMsg, pOutMsg);
			if (!r) return false;

			if (repInfo.myId() != repInfo.primaryOfView(pOutMsg->viewNumber())) return false; // (the primary is the collector of CommitPartialMsg)

			outMsg = (CommitPartialMsg*)pOutMsg;

			return true;
		}

		///////////////////////////////////////////////////////////////////////////////
		// CommitFullMsg
		///////////////////////////////////////////////////////////////////////////////

		CommitFullMsg* CommitFullMsg::create(ViewNum v, SeqNum s, int16_t senderId, const char* sig, uint16_t sigLen)
		{
			return (CommitFullMsg*)SignedShareBase::create(MsgCode::CommitFull, v, s, senderId, sig, sigLen);
		}

		bool CommitFullMsg::ToActualMsgType(const ReplicasInfo& repInfo, MessageBase* inMsg, CommitFullMsg*& outMsg)
		{
			SignedShareBase* pOutMsg = nullptr;
			bool r = SignedShareBase::ToActualMsgType(repInfo, MsgCode::CommitFull, inMsg, pOutMsg);
			if (r) outMsg = (CommitFullMsg*)pOutMsg;
			return r;
		}

	}
}
