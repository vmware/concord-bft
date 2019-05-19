//Concord
//
//Copyright (c) 2018 VMware, Inc. All Rights Reserved.
//
//This product is licensed to you under the Apache 2.0 license (the "License").  You may not use this product except in compliance with the Apache 2.0 License. 
//
//This product may include a number of subcomponents with separate copyright notices and license terms. Your use of these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE file.

#pragma once


#include "SysConsts.hpp"
#include "PrimitiveTypes.hpp"
#include "MsgCode.hpp"
#include "ReplicasInfo.hpp" 


namespace bftEngine
{
	namespace impl
	{

		class MessageBase
		{
		public:
#pragma pack(push,1)
			struct Header
			{
				MsgType msgType;
			};
#pragma pack(pop)
			static_assert(sizeof(Header) == 2, "MessageBase::Header is 2B");

			MessageBase(NodeIdType sender);

			MessageBase(NodeIdType sender, MsgType type, MsgSize size);

			MessageBase(NodeIdType sender, Header* body, MsgSize size, bool ownerOfStorage);

			~MessageBase();

			MsgSize size() const { return msgSize_; }

			char* body() const { return (char*)msgBody_; }

			NodeIdType senderId() const { return sender_; }

			MsgType type() const { return msgBody_->msgType; }

			MessageBase* cloneObjAndMsg() const;

			void writeObjAndMsgToLocalBuffer(char* buffer, size_t bufferLength, size_t* actualSize) const;
			size_t sizeNeededForObjAndMsgInLocalBuffer() const;
			static MessageBase* createObjAndMsgFromLocalBuffer(char* buffer, size_t bufferLength, size_t* actualSize);

#ifdef DEBUG_MEMORY_MSG
			static void printLiveMessages();
#endif

		protected:

			void shrinkToFit();

			void setMsgSize(MsgSize size);

			MsgSize internalStorageSize() const { return storageSize_; }

		protected:
			Header* msgBody_ = nullptr;
			MsgSize msgSize_ = 0;
			MsgSize storageSize_ = 0;
			NodeIdType sender_;
			bool owner_ = true; // true IFF this instance is not responsible for deallocating the body

#pragma pack(push,1)
			struct RawHeaderOfObjAndMsg
			{
				uint32_t magicNum;
				MsgSize msgSize;
				NodeIdType sender;
				// TODO(GG): consider to add checksum
			};
#pragma pack(pop)
			static const uint32_t magicNumOfRawFormat = 0x5555897BU;
		};


		class InternalMessage // TODO(GG): move class to another file
		{
		public:
			virtual ~InternalMessage() {}
			virtual void handle() = 0;
		};

	}
}
