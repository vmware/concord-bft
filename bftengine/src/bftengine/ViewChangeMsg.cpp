//Concord
//
//Copyright (c) 2018 VMware, Inc. All Rights Reserved.
//
//This product is licensed to you under the Apache 2.0 license (the "License").  You may not use this product except in compliance with the Apache 2.0 License. 
//
//This product may include a number of subcomponents with separate copyright notices and license terms. Your use of these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE file.


#include <string.h>
  
#include "assertUtils.hpp"
#include "ViewChangeMsg.hpp"
#include "Crypto.hpp"

namespace bftEngine
{
	namespace impl
	{

		ViewChangeMsg::ViewChangeMsg(ReplicaId srcReplicaId, ViewNum newView, SeqNum lastStableSeq) : MessageBase(srcReplicaId, MsgCode::ViewChange, maxExternalMessageSize)
		{
			b()->genReplicaId = srcReplicaId;
			b()->newView = newView;
			b()->lastStable = lastStableSeq;
			b()->numberOfElements = 0;
			b()->locationAfterLast = 0;
		}

		void ViewChangeMsg::setNewViewNumber(ViewNum newView)
		{
			Assert(newView >= b()->newView);
			b()->newView = newView;
		}

		void ViewChangeMsg::getMsgDigest(Digest& outDigest) const
		{
			size_t bodySize = b()->locationAfterLast;
			if (bodySize == 0) bodySize = sizeof(ViewChangeMsgHeader);
			DigestUtil::compute(body(), bodySize, (char*)outDigest.content(), sizeof(Digest));
		}

		void ViewChangeMsg::addElement(const ReplicasInfo& repInfo, SeqNum  seqNum, const Digest& prePrepreDigest, ViewNum   originView,
			bool   hasPreparedCertificate, ViewNum     certificateView,
			uint16_t certificateSigLength, const char* certificateSig)
		{
			Assert(b()->numberOfElements < kWorkWindowSize);
			Assert(b()->numberOfElements > 0 || b()->locationAfterLast == 0);
			Assert(seqNum > b()->lastStable);
			Assert(seqNum <= b()->lastStable + kWorkWindowSize);

			if (b()->locationAfterLast == 0) // if this is the first element
			{
				Assert(b()->numberOfElements == 0);
				b()->locationAfterLast = sizeof(ViewChangeMsgHeader);
			}

			uint16_t requiredSpace = b()->locationAfterLast + sizeof(Element);
			if (hasPreparedCertificate)
				requiredSpace += (sizeof(PreparedCertificate) + certificateSigLength);

			// TODO(GG): we should make sure that this assert will never be violated (by calculating the maximum  size of a ViewChangeMsg message required for the actual configuration)
			Assert((size_t)(requiredSpace + repInfo.mySigManager().getMySigLength()) <= (size_t)internalStorageSize());

			Element* pElement = (Element*)(body() + b()->locationAfterLast);
			pElement->seqNum = seqNum;
			pElement->prePrepreDigest = prePrepreDigest;
			pElement->originView = originView;
			pElement->hasPreparedCertificate = hasPreparedCertificate;

			if (hasPreparedCertificate)
			{
				PreparedCertificate* pCert =
					(PreparedCertificate*)(body() + b()->locationAfterLast + sizeof(Element));

				pCert->certificateView = certificateView;
				pCert->certificateSigLength = certificateSigLength;

				char* pSig = (char*)(body() + b()->locationAfterLast + sizeof(Element) + sizeof(PreparedCertificate));
				memcpy(pSig, certificateSig, certificateSigLength);
			}

			b()->locationAfterLast = requiredSpace;
			b()->numberOfElements++;
		}

		void ViewChangeMsg::finalizeMessage(const ReplicasInfo& repInfo)
		{
			size_t bodySize = b()->locationAfterLast;
			if (bodySize == 0) bodySize = sizeof(ViewChangeMsgHeader);

			uint16_t sigSize = repInfo.mySigManager().getMySigLength();

			setMsgSize(bodySize + sigSize);
			shrinkToFit();

			repInfo.mySigManager().sign(body(), bodySize, body() + bodySize, sigSize);

			bool b = checkElements((uint16_t)sigSize);

			Assert(b);
		}

		bool ViewChangeMsg::ToActualMsgType(const ReplicasInfo& repInfo, MessageBase* inMsg, ViewChangeMsg*& outMsg)
		{
			Assert(inMsg->type() == MsgCode::ViewChange);
			if (inMsg->size() < sizeof(ViewChangeMsgHeader)) return false;

			ViewChangeMsg* t = (ViewChangeMsg*)inMsg;

			// simple validations
			if (!repInfo.isIdOfReplica(t->idOfGeneratedReplica())) return false;

			// size
			uint16_t dataLength = t->b()->locationAfterLast;
			uint16_t sigLen = repInfo.mySigManager().getSigLength(t->idOfGeneratedReplica());
			if (dataLength < sizeof(ViewChangeMsgHeader)) dataLength = sizeof(ViewChangeMsgHeader);
			if (((uint16_t)t->size()) < (uint16_t)(dataLength + sigLen)) return false;

			// id
			if (t->idOfGeneratedReplica() == repInfo.myId()) return false; 


			// check signature
			bool sigOkay = repInfo.mySigManager().verifySig(t->idOfGeneratedReplica(), t->body(), dataLength, t->body() + dataLength, sigLen);
			if (!sigOkay) return false;

				// check elements in message
			bool elementsOkay = t->checkElements(sigLen);
			if (!elementsOkay) return false;

			// TODO(GG): TBD -  more?

			outMsg = (ViewChangeMsg*)inMsg;
			return true;
		}


		bool ViewChangeMsg::checkElements(uint16_t sigSize) const
		{
			SeqNum lastSeqNumInMsg = lastStable();
			uint16_t numOfActualElements = 0;
			uint16_t remainingBytes = size() - sigSize - sizeof(ViewChangeMsgHeader);
			char* currLoc = body() + sizeof(ViewChangeMsgHeader);

			while ((remainingBytes >= sizeof(Element)) && (numOfActualElements < numberOfElements()))
			{
				numOfActualElements++;
				Element* pElement = (Element*)currLoc;
				if (pElement->seqNum <= lastSeqNumInMsg) return false; // elements should be sorted by seq number
				lastSeqNumInMsg = pElement->seqNum;

				if (lastSeqNumInMsg > lastStable() + kWorkWindowSize) return false;

				if (pElement->originView >= newView()) return false;

				remainingBytes -= sizeof(Element);
				currLoc += sizeof(Element);

				if (pElement->hasPreparedCertificate)
				{
					if (remainingBytes < sizeof(PreparedCertificate)) return false;

					PreparedCertificate* pCert = (PreparedCertificate*)currLoc;

					if (pCert->certificateView > pElement->originView) return false;

					if (pCert->certificateSigLength == 0) return false;

					const int16_t s = sizeof(PreparedCertificate) + pCert->certificateSigLength;

					if (remainingBytes < s) return false;

					// we don't check the certificate here (becuase in many cases it is not needed to make progress)

					remainingBytes -= s;
					currLoc += s;
				}
			}

			if (numOfActualElements != numberOfElements()) return false;

			if (numOfActualElements > 0)
			{
				const int16_t locationAfterLastElement = size() - sigSize - remainingBytes;
				if (this->b()->locationAfterLast != locationAfterLastElement) return false;
			}
			else
			{
				if (this->b()->locationAfterLast != 0) return false;
			}

			return true;
		}

		ViewChangeMsg::ElementsIterator::ElementsIterator(const ViewChangeMsg* const  m) : msg{ m }
		{
			if (m == nullptr || m->numberOfElements() == 0) {
				endLoc = 0;
				currLoc = 0;
				nextElementNum = 1;
			}
			else {
				endLoc = m->b()->locationAfterLast;
				currLoc = sizeof(ViewChangeMsgHeader);
				Assert(endLoc > currLoc);
				nextElementNum = 1;
			}
		}

		bool ViewChangeMsg::ElementsIterator::end()
		{
			if (currLoc >= endLoc)
			{
				Assert(msg == nullptr || ((nextElementNum - 1) == msg->numberOfElements()));
				return true;
			}

			return false;
		}

		bool ViewChangeMsg::ElementsIterator::getCurrent(Element*& pElement)
		{
			pElement = nullptr;

			if (end()) return false;

			const uint16_t remainingbytes = (endLoc - currLoc);

			Assert(remainingbytes >= sizeof(Element));

			pElement = (Element*)(msg->body() + currLoc);

			if (pElement->hasPreparedCertificate)
			{
				Assert(remainingbytes >= sizeof(Element) + sizeof(PreparedCertificate));
				PreparedCertificate* pCert = (PreparedCertificate*)(msg->body() + currLoc + sizeof(Element));
				Assert(remainingbytes >= sizeof(Element) + sizeof(PreparedCertificate) + pCert->certificateSigLength);
			}

			return true;
		}

		// TODO(GG): consider to optimize the methods of ViewChangeMsg::ElementsIterator

		void ViewChangeMsg::ElementsIterator::gotoNext()
		{
			Element* dummy;
			getAndGoToNext(dummy);
		}

		bool ViewChangeMsg::ElementsIterator::getAndGoToNext(Element*& pElement)
		{
			pElement = nullptr;

			if (end()) return false;

			const uint16_t remainingbytes = (endLoc - currLoc);

			Assert(remainingbytes >= sizeof(Element));

			Element* p = (Element*)(msg->body() + currLoc);

			currLoc += sizeof(Element);

			if (p->hasPreparedCertificate)
			{
				Assert(remainingbytes >= sizeof(Element) + sizeof(PreparedCertificate));

				PreparedCertificate* pCert = (PreparedCertificate*)(msg->body() + currLoc);

				Assert(remainingbytes >= sizeof(Element) + sizeof(PreparedCertificate) + pCert->certificateSigLength);

				currLoc += (sizeof(PreparedCertificate) + pCert->certificateSigLength);
			}

			pElement = p;

			nextElementNum++;

			return true;
		}

		bool ViewChangeMsg::ElementsIterator::goToAtLeast(SeqNum lowerBound)
		{
			Element* currElement = nullptr;
			bool validElement = getCurrent(currElement);

			while (validElement && currElement->seqNum < lowerBound)
			{
				gotoNext();
				validElement = getCurrent(currElement);
			}

			return validElement;
		}
	}
}
