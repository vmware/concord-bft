//Concord
//
//Copyright (c) 2018 VMware, Inc. All Rights Reserved.
//
//This product is licensed to you under the Apache 2.0 license (the "License").  You may not use this product except in compliance with the Apache 2.0 License. 
//
//This product may include a number of subcomponents with separate copyright notices and license terms. Your use of these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE file.

#include "NullStateTransfer.hpp"
#include "assertUtils.hpp"
#include "Digest.hpp"
#include "Logger.hpp"
#include "Crypto.hpp"
 
namespace bftEngine
{
	namespace impl
	{
		void NullStateTransfer::init(uint64_t maxNumOfRequiredStoredCheckpoints, uint32_t numberOfRequiredReservedPages, uint32_t sizeOfReservedPage) 
		{
			Assert(!isInitialized());

			numOfReservedPages = numberOfRequiredReservedPages;
			sizeOfPage = sizeOfReservedPage;

			reservedPages = (char*)std::malloc(numOfReservedPages * sizeOfPage);

			running = false;

			Assert(reservedPages != nullptr);
		}

		void NullStateTransfer::startRunning(IReplicaForStateTransfer* r)
		{
			Assert(isInitialized());
			Assert(!running);

			running = true;
			repApi = r;
		}

		void NullStateTransfer::stopRunning()
		{
			Assert(isInitialized());
			Assert(running);

			running = false;

		}

		bool NullStateTransfer::isRunning() const
		{
			return running;
		}



		void NullStateTransfer::createCheckpointOfCurrentState(uint64_t checkpointNumber)
		{
		}

		void NullStateTransfer::markCheckpointAsStable(uint64_t checkpointNumber)
		{
		}

		void NullStateTransfer::getDigestOfCheckpoint(uint64_t checkpointNumber, uint16_t sizeOfDigestBuffer, char* outDigestBuffer)
		{
			Assert(sizeOfDigestBuffer >= sizeof(Digest));
			LOG_WARN_F(GL, "State digest is only based on sequence number (becuase state transfer module has not been loaded)");

			memset(outDigestBuffer, 0, sizeOfDigestBuffer);

			Digest d;

			DigestUtil::compute((char*)&checkpointNumber, sizeof(checkpointNumber), (char*)&d, sizeof(d));

			memcpy(outDigestBuffer, &d, sizeof(d));
		}


		void NullStateTransfer::startCollectingState()
		{
			if (errorReport) return;
				
			LOG_ERROR_F(GL, "Replica is not able to collect state from the other replicas (becuase state transfer module has not been loaded)");
			errorReport = true;
		}

		bool NullStateTransfer::isCollectingState() const
		{
			return false;
		}


		uint32_t NullStateTransfer::numberOfReservedPages() const
		{
			return numOfReservedPages;
		}

		uint32_t NullStateTransfer::sizeOfReservedPage() const
		{
			return sizeOfPage;
		}

		bool NullStateTransfer::loadReservedPage(uint32_t reservedPageId, uint32_t copyLength, char* outReservedPage) const
		{
			Assert(copyLength <= sizeOfPage);

			char* page = reservedPages + (reservedPageId * sizeOfPage);

			memcpy(outReservedPage, page, copyLength);

			return true;
		}

		void NullStateTransfer::saveReservedPage(uint32_t reservedPageId, uint32_t copyLength, const char* inReservedPage)
		{
			Assert(copyLength <= sizeOfPage);

			char* page = reservedPages + (reservedPageId * sizeOfPage);

			memcpy(page, inReservedPage, copyLength);
		}

		void NullStateTransfer::zeroReservedPage(uint32_t reservedPageId)
		{
			char* page = reservedPages + (reservedPageId * sizeOfPage);


			memset(page, 0, sizeOfPage);
		}



		void NullStateTransfer::onTimer()
		{

		}

		void NullStateTransfer::handleStateTransferMessage(char* msg, uint32_t msgLen, uint16_t senderId)
		{
			repApi->freeStateTransferMsg(msg);
		}


		NullStateTransfer::~NullStateTransfer()
		{
			std::free(reservedPages);
		}
	}
}
