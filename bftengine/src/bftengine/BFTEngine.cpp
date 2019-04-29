//Concord
//
//Copyright (c) 2018 VMware, Inc. All Rights Reserved.
//
//This product is licensed to you under the Apache 2.0 license (the "License").  You may not use this product except in compliance with the Apache 2.0 License. 
//
//This product may include a number of subcomponents with separate copyright notices and license terms. Your use of these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE file.

#include "Replica.hpp"
#include "ReplicaImp.hpp"

namespace bftEngine
{
	namespace impl
	{
		struct ReplicaInternal : public Replica
		{
			virtual ~ReplicaInternal();

			virtual bool isRunning() const override;

			uint64_t getLastExecutedSequenceNum() const override;

			virtual void start() override;

			virtual void stop() override;

                        virtual void SetAggregator(std::shared_ptr<concordMetrics::Aggregator> a) override;

			ReplicaImp* rep;
		};


		ReplicaInternal::~ReplicaInternal()
		{
			delete rep;
		}

		bool ReplicaInternal::isRunning() const
		{
			return rep->isRunning();
		}

		uint64_t ReplicaInternal::getLastExecutedSequenceNum() const
		{
			return static_cast<uint64_t>(rep->getLastExecutedSequenceNum());
		}


		void ReplicaInternal::start()
		{
			return rep->start();
		}


		void ReplicaInternal::stop()
		{
			return rep->stop();
		}

                void ReplicaInternal::SetAggregator(std::shared_ptr<concordMetrics::Aggregator> a)
                {
                        return rep->SetAggregator(a);
                }

	}
}



bool cryptoInitialized = false;

namespace bftEngine
{
	Replica* Replica::createNewReplica(ReplicaConfig* replicaConfig, RequestsHandler* requestsHandler,
		IStateTransfer* stateTransfer, ICommunication* communication, MetadataStorage* metadataStorage)
	{
		if (!cryptoInitialized)
		{
			cryptoInitialized = true;
			CryptographyWrapper::init();
		}

		ReplicaInternal* retVal = new ReplicaInternal();
		retVal->rep = new ReplicaImp(*replicaConfig, requestsHandler, stateTransfer, communication);

		return retVal;
	}

	Replica* Replica::loadExistingReplica(RequestsHandler* requestsHandler, IStateTransfer* stateTransfer,
		ICommunication* communication, MetadataStorage* metadataStorage)
	{
		Assert(false); // TODO(GG): implement

		return 0;
	}

	Replica::~Replica()
	{
	}
}
