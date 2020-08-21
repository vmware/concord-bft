//Concord
//
//Copyright (c) 2018 VMware, Inc. All Rights Reserved.
//
//This product is licensed to you under the Apache 2.0 license (the "License").  You may not use this product except in compliance with the Apache 2.0 License. 
//
//This product may include a number of subcomponents with separate copyright notices and license terms. Your use of these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE file.

#include <cstring>
#include "ClientRequestMsg.hpp"
#include "assertUtils.hpp"

namespace bftEngine
{
	namespace impl
	{

		// local helper functions

		static uint16_t getSender(const ClientRequestMsgHeader* r)
		{
			return r->idOfClientProxy;
		}

		static int32_t compRequestMsgSize(const ClientRequestMsgHeader* r)
		{
			return (sizeof(ClientRequestMsgHeader) + r->totalSize);
		}

		uint32_t getRequestSizeTemp(const char* request) // TODO(GG): change - TBD
		{
			const ClientRequestMsgHeader* r = (ClientRequestMsgHeader*)request;
			return compRequestMsgSize(r);
		}



		// class ClientRequestMsg

		ClientRequestMsg::ClientRequestMsg(NodeIdType sender, bool isReadOnly, uint64_t reqSeqNum, uint32_t requestLength, const char* request, bool withTimeStamp)
			: MessageBase(sender, MsgCode::Request, withTimeStamp? maxExternalMessageSize: (sizeof(ClientRequestMsgHeader) + requestLength))
		{
			// TODO(GG): asserts

			b()->idOfClientProxy = sender;
			b()->flags = 0;
			if (isReadOnly) b()->flags |= 0x1;
			b()->reqSeqNum = reqSeqNum;
			b()->requestLength = requestLength;
			b()->totalSize = requestLength;
			memcpy(body() + sizeof(ClientRequestMsgHeader), request, requestLength);


		}

		ClientRequestMsg::ClientRequestMsg(NodeIdType sender)
			: MessageBase(sender, MsgCode::Request, maxExternalMessageSize)
		{
			b()->idOfClientProxy = sender;
			b()->reqSeqNum = 0;
			b()->requestLength = 0;
			b()->flags = 0;
			b()->totalSize = 0;
			setMsgSize(sizeof(ClientRequestMsgHeader));
		}

		ClientRequestMsg::ClientRequestMsg(ClientRequestMsgHeader* body)
			: MessageBase(getSender(body), (MessageBase::Header*)body, compRequestMsgSize(body), false)
		{
		}

		ClientRequestMsg::ClientRequestMsg(ClientRequestMsg* msg)
			: MessageBase(msg->senderId(), MsgCode::Request, sizeof(ClientRequestMsgHeader) + msg->totalSize())
		{
			memcpy(body(), msg->body(), size());
		}

		void ClientRequestMsg::set(ReqId reqSeqNum, uint32_t requestLength, bool isReadOnly)
		{
			Assert(requestLength > 0);
			Assert(requestLength <= (internalStorageSize() - sizeof(ClientRequestMsgHeader)));

			b()->reqSeqNum = reqSeqNum;
			b()->flags = 0;
			if (isReadOnly) b()->flags |= 0x1;
			b()->requestLength = requestLength;

			setMsgSize(sizeof(ClientRequestMsgHeader) + requestLength);
			b()->totalSize = requestLength;
		}

		void ClientRequestMsg::setAsReadWrite()
		{
			const uint8_t m = ~((uint8_t)0x1);
			b()->flags &= m;
		}

		void ClientRequestMsg::setAsReadyOnly()
		{
			b()->flags |= 0x1;
		}

		void ClientRequestMsg::setCombinedTimestamp(CombinedTimeStampMsg* msg)
		{
			memcpy(body() + sizeof(ClientRequestMsgHeader) + b()->requestLength, msg->body(), msg->endLocationOfLastVerifiedTimeStamp());
			if (size() != sizeof(ClientRequestMsgHeader) + b()->requestLength + msg->endLocationOfLastVerifiedTimeStamp()) {
				setMsgSize(sizeof(ClientRequestMsgHeader) + b()->requestLength + msg->endLocationOfLastVerifiedTimeStamp());
				b()->totalSize = b()->requestLength + msg->endLocationOfLastVerifiedTimeStamp();
				shrinkToFit();
			}
		}

        uint64_t ClientRequestMsg::timeStamp() const
		{
			return ((CombinedTimeStampMsg::CombinedTimeStampMsgHeader*)(body() + sizeof(ClientRequestMsgHeader) + b()->requestLength))->timeStamp;
		}

		char* ClientRequestMsg::digest() const
		{
			return body() + sizeof(ClientRequestMsgHeader) + b()->requestLength + sizeof(CombinedTimeStampMsg::CombinedTimeStampMsgHeader);
		}

		bool ClientRequestMsg::ToActualMsgType(const ReplicasInfo& repInfo, MessageBase* inMsg, ClientRequestMsg*& outMsg) {
			Assert(inMsg->type() == MsgCode::Request);
			if (inMsg->size() < sizeof(ClientRequestMsgHeader)) return false;

			ClientRequestMsg* t = (ClientRequestMsg*)inMsg;

			if (t->size() < (sizeof(ClientRequestMsgHeader) + t->totalSize())) return false;

			outMsg = t;

			return true;
		}
	}
}
