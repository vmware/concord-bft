//Concord
//
//Copyright (c) 2018 VMware, Inc. All Rights Reserved.
//
//This product is licensed to you under the Apache 2.0 license (the "License").  You may not use this product except in compliance with the Apache 2.0 License. 
//
//This product may include a number of subcomponents with separate copyright notices and license terms. Your use of these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE file.

#pragma once

#include <queue>
#include <thread>
#include <mutex>
#include <condition_variable>
#include "TimeUtils.hpp"

namespace bftEngine
{
	namespace impl
	{

		class MessageBase;
		class InternalMessage;

		using std::queue;

		class IncomingMsgsStorage
		{
		public:

			const uint64_t minTimeBetweenOverflowWarningsMilli = 5 * 1000; // 5 seconds

			IncomingMsgsStorage(uint16_t maxNumOfPendingExternalMsgs);
			~IncomingMsgsStorage();

			void pushExternalOrderingMsg(MessageBase* m); // can be called by any thread    
			void pushExternalMsg(MessageBase* m); // can be called by any thread
			void pushInternalMsg(InternalMessage* m); // can be called by any thread

			bool pop(void*& item, bool& external, std::chrono::milliseconds timeout); // should only be called by the main thread
			bool empty(); // should only be called by the main thread. 

		protected:

			bool popThreadLocal(void*& item, bool& external);

			const uint16_t maxNumberOfPendingExternalMsgs;

			std::mutex lock;
			std::condition_variable condVar;

			// new messages are pushed to ptrProtectedQueue.... ; Protected by lock 
			queue<MessageBase*>* ptrProtectedQueueForExternalMessages;
			queue<InternalMessage*>* ptrProtectedQueueForInternalMessages;

			// time of last queue overflow  Protected by lock 
			Time lastOverflowWarning;

			// messages are fetched from ptrThreadLocalQueue... ; should only be accessed by the main thread
			queue<MessageBase*>* ptrThreadLocalQueueForExternalMessages;
			queue<InternalMessage*>* ptrThreadLocalQueueForInternalMessages;
		};

	}
}
