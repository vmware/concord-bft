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

	}
}
