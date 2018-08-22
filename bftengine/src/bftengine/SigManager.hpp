//Concord
//
//Copyright (c) 2018 VMware, Inc. All Rights Reserved.
//
//This product is licensed to you under the Apache 2.0 license (the "License").  You may not use this product except in compliance with the Apache 2.0 License. 
//
//This product may include a number of subcomponents with separate copyright notices and license terms. Your use of these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE file.

#pragma once
#include "PrimitiveTypes.hpp"

#include <utility>
#include <set>
#include <map>

namespace bftEngine
{
	namespace impl
	{

		class RSASigner;
		class RSAVerifier;

		class SigManager
		{
		public:

			typedef std::pair<ReplicaId, const std::string> PublicKeyDesc;
			typedef std::string PrivateKeyDesc;

			SigManager(ReplicaId myId,
				int16_t numberOfReplicasAndClients,
				PrivateKeyDesc mySigPrivateKey, std::set<PublicKeyDesc> replicasSigPublicKeys);

			~SigManager();

			uint16_t getSigLength(ReplicaId replicaId) const;
			bool verifySig(ReplicaId replicaId, const char* data, size_t dataLength, const char* sig, uint16_t sigLength)  const;

			void sign(const char* data, size_t dataLength, char* outSig, uint16_t outSigLength)  const;
			uint16_t getMySigLength()  const;



		protected:

			const ReplicaId _myId;

			RSASigner* _mySigner;
			std::map<ReplicaId, RSAVerifier*> _replicasVerifiers;

		};

	}
}
