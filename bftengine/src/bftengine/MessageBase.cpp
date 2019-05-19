//Concord
//
//Copyright (c) 2018 VMware, Inc. All Rights Reserved.
//
//This product is licensed to you under the Apache 2.0 license (the "License").  You may not use this product except in compliance with the Apache 2.0 License. 
//
//This product may include a number of subcomponents with separate copyright notices and license terms. Your use of these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE file.

#include <cstring>

#include "MessageBase.hpp"
#include "assertUtils.hpp"

#ifdef DEBUG_MEMORY_MSG
#include <set> 

#ifdef USE_TLS
#error DEBUG_MEMORY_MSG is not supported with USE_TLS
#endif

namespace bftEngine
{
	namespace impl
	{

		static std::set<MessageBase*> liveMessagesDebug; // GG: if needed, add debug information

		void MessageBase::printLiveMessages()
		{
			printf("\nDumping all live messages:");
			for (std::set<MessageBase*>::iterator it = liveMessagesDebug.begin(); it != liveMessagesDebug.end(); it++)
			{
				printf("<type=%d, size=%d>", (*it)->type(), (*it)->size());
				printf("%8s", " "); // space
			}
			printf("\n");
		}

	}
}

#endif

namespace bftEngine
{
	namespace impl
	{

		MessageBase::~MessageBase() {
#ifdef DEBUG_MEMORY_MSG
			liveMessagesDebug.erase(this);
#endif
			if (owner_) std::free((char*)msgBody_);
		}

		void MessageBase::shrinkToFit() {
			Assert(owner_);

			// TODO(GG): need to verify more conditions??

			void* p = (void*)msgBody_;
			p = std::realloc(p, msgSize_);
			// always shrinks allocated size, so no bytes should be 0'd

			msgBody_ = (MessageBase::Header*)p;
			storageSize_ = msgSize_;
		}


		MessageBase::MessageBase(NodeIdType sender)
		{
			storageSize_ = 0;
			msgBody_ = nullptr;
			msgSize_ = 0;
			owner_ = false;
			sender_ = sender;

#ifdef DEBUG_MEMORY_MSG
			liveMessagesDebug.insert(this);
#endif
		}

		MessageBase::MessageBase(NodeIdType sender, MsgType type, MsgSize size)
		{
			Assert(size > 0);
			msgBody_ = (MessageBase::Header*)std::malloc(size);
			memset(msgBody_, 0, size);
			storageSize_ = size;
			msgSize_ = size;
			owner_ = true;
			sender_ = sender;
			msgBody_->msgType = type;

#ifdef DEBUG_MEMORY_MSG
			liveMessagesDebug.insert(this);
#endif
		}

		MessageBase::MessageBase(NodeIdType sender, MessageBase::Header* body, MsgSize size, bool ownerOfStorage)
		{
			msgBody_ = body;
			msgSize_ = size;
			storageSize_ = size;
			sender_ = sender;
			owner_ = ownerOfStorage;

#ifdef DEBUG_MEMORY_MSG
			liveMessagesDebug.insert(this);
#endif
		}

		void MessageBase::setMsgSize(MsgSize size) {
			Assert((msgBody_ != nullptr));
			Assert(size <= storageSize_);

			// TODO(GG): do we need to reset memory here?
			if (storageSize_ > size)	memset(body() + size, 0, (storageSize_ - size));

			msgSize_ = size;
		}

		MessageBase* MessageBase::cloneObjAndMsg() const
		{
			Assert(owner_);
			Assert(msgSize_ > 0);

			void* msgBody = std::malloc(msgSize_);
			memcpy(msgBody, msgBody_, msgSize_);

			MessageBase* otherMsg = 
				new MessageBase(sender_, (MessageBase::Header*)msgBody, msgSize_,true);

			return otherMsg;
		}


		void MessageBase::writeObjAndMsgToLocalBuffer(char* buffer, size_t bufferLength, size_t* actualSize) const
		{
			Assert(owner_);
			Assert(msgSize_ > 0);

			const size_t sizeNeeded = sizeof(RawHeaderOfObjAndMsg) + msgSize_;

			Assert(sizeNeeded <= bufferLength);

			RawHeaderOfObjAndMsg* pHeader = (RawHeaderOfObjAndMsg*)buffer;
			pHeader->magicNum = magicNumOfRawFormat;
			pHeader->msgSize = msgSize_;
			pHeader->sender = sender_;

			char* pRawMsg = buffer + sizeof(RawHeaderOfObjAndMsg);
			memcpy(pRawMsg, msgBody_, msgSize_);

			if(actualSize) *actualSize = sizeNeeded;
		}

		size_t MessageBase::sizeNeededForObjAndMsgInLocalBuffer() const
		{
			Assert(owner_);
			Assert(msgSize_ > 0);

			const size_t sizeNeeded = sizeof(RawHeaderOfObjAndMsg) + msgSize_;

			return sizeNeeded;
		}

		MessageBase* MessageBase::createObjAndMsgFromLocalBuffer(char* buffer, size_t bufferLength, size_t* actualSize)
		{
			if(actualSize) *actualSize = 0;

			if (bufferLength <= sizeof(RawHeaderOfObjAndMsg)) return nullptr;

			RawHeaderOfObjAndMsg* pHeader = (RawHeaderOfObjAndMsg*)buffer;
			if (pHeader->magicNum != magicNumOfRawFormat) return nullptr;
			if (pHeader->msgSize == 0) return nullptr;
			if (pHeader->msgSize > maxExternalMessageSize) return nullptr;
			if (pHeader->msgSize + sizeof(RawHeaderOfObjAndMsg) > bufferLength) return nullptr;
			
			char* pBodyInBuffer = buffer + sizeof(RawHeaderOfObjAndMsg);

			void* msgBody = std::malloc(pHeader->msgSize);
			memcpy(msgBody, pBodyInBuffer, pHeader->msgSize);

			MessageBase* msgObj =
				new MessageBase(pHeader->sender, (MessageBase::Header*)msgBody, pHeader->msgSize, true);

			if (actualSize) *actualSize = (pHeader->msgSize + sizeof(RawHeaderOfObjAndMsg));

			return msgObj;
		}


	}
}
