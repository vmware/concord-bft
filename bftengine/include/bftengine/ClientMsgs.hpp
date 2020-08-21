//Concord
//
//Copyright (c) 2018 VMware, Inc. All Rights Reserved.
//
//This product is licensed to you under the Apache 2.0 license (the "License").  You may not use this product except in compliance with the Apache 2.0 License. 
//
//This product may include a number of subcomponents with separate copyright notices and license terms. Your use of these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE file.

#pragma once

#include <stdint.h>

#define REQUEST_MSG_TYPE (700)
#define REPLY_MSG_TYPE   (800)

namespace bftEngine
{
#pragma pack(push,1)
	struct ClientRequestMsgHeader
	{
		uint16_t msgType; // always == REQUEST_MSG_TYPE
		uint16_t idOfClientProxy;
		uint8_t  flags; // bit 0 == isReadOnly ; bits 1-7 are reserved
		uint32_t totalSize;
		uint64_t reqSeqNum;
		uint32_t requestLength;
		// followed by the request (security information, such as signatures, should be part of the request)

		// TODO(GG): idOfClientProxy is not needed here
		// TODO(GG): add information about "suggested repliers" 
	};

	struct ClientReplyMsgHeader
	{
		uint16_t msgType; // always == REPLY_MSG_TYPE
		uint16_t currentPrimaryId;
		uint64_t reqSeqNum;
		uint32_t replyLength;
	};
#pragma pack(pop)
}
