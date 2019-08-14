// Concord
//
// Copyright (c) 2018 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#pragma once

#include "KVBCInterfaces.h"
#include "SimpleClient.hpp"

using namespace bftEngine;

namespace SimpleKVBC {

	class ClientImp : public IClient
	{
	public:
		// IClient methods
		virtual Status start() ;
		virtual Status stop() ;

		virtual bool isRunning() ;

		virtual Status invokeCommandSynch(const Sliver command, bool isReadOnly, Sliver& outReply) ;

		virtual Status release(Sliver& slice) ; // release memory allocated by invokeCommandSynch  


	protected:
	
		ClientImp() {};
		~ClientImp() {};

		ClientConfig config_;
		char* replyBuf_ = nullptr;
		SeqNumberGeneratorForClientRequests* seqGen_ = nullptr;
		ICommunication* comm_ = nullptr;

		SimpleClient* bftClient_ = nullptr;

		friend IClient* createClient(const ClientConfig& conf, ICommunication* comm);
	};
}
