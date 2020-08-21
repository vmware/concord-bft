//Concord
//
//Copyright (c) 2018 VMware, Inc. All Rights Reserved.
//
//This product is licensed to you under the Apache 2.0 license (the "License").  You may not use this product except in compliance with the Apache 2.0 License. 
//
//This product may include a number of subcomponents with separate copyright notices and license terms. Your use of these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE file.

#pragma once

#include <stdint.h>

namespace bftEngine
{
	namespace impl
	{

		class MsgCode
		{
		public:
			enum : uint16_t
			{
				None = 0,

				Checkpoint = 100,
				CommitPartial,
				CommitFull,
				FullCommitProof,
				FullExecProof,
				NewView,
				PartialCommitProof,
				PartialExecProof,
				PreparePartial,
				PrepareFull,
				ReqMissingData,
				SimpleAckMsg,
				StartSlowCommit,
				ViewChange,
				ReplicaStatus,
				StateTransfer,
				PrePrepare,

				CombinedTimeStamp,
                ClientGetTimeStamp,
				ClientSignedTimeStamp,

				CollectStablePoint,
				LocalCommitSet,

				Request = 700,
				Reply = 800,


			};
		};

	}
}
