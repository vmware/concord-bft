#pragma once

#include "PrimitiveTypes.hpp"
#include "assertUtils.hpp"
#include "Digest.hpp"
#include "MessageBase.hpp"
#include "ClientRequestMsg.hpp"

namespace bftEngine
{
	namespace impl
	{
#pragma pack(push,1)
	struct CollectStablePointItem {
		uint16_t clientId;
        uint64_t seqNumber;
		CollectStablePointItem(uint16_t c, uint64_t s): clientId(c), seqNumber(s) {}
	};
#pragma pack(pop)

	class CollectStablePointMsg : public MessageBase {
    public:
#pragma pack(push,1)
			struct CollectStablePointMsgHeader
			{
				MessageBase::Header header;

				uint64_t prevStablePoint;
				uint64_t nextStablePoint;
				uint16_t numberOfRequests;
				uint32_t endLocationOfLastRequest;
			};
#pragma pack(pop)
			static_assert(sizeof(CollectStablePointMsgHeader) == (2 + 8 + 8 + 2 + 4), "CollectStablePointMsgHeader is 24B");

	public:
			static bool ToActualMsgType(const ReplicasInfo& repInfo, MessageBase* inMsg, CollectStablePointMsg*& outMsg);

			CollectStablePointMsg(ReplicaId senderId, uint64_t prevStablePoint, uint64_t nextStablePoint);

			CollectStablePointMsg(ReplicaId senderId, CollectStablePointMsgHeader* body);

			uint64_t prevStablePoint() const { return b()->prevStablePoint; }

			uint64_t nextStablePoint() const { return b()->nextStablePoint; }

			uint16_t numberOfRequests() const { return b()->numberOfRequests; }

            uint32_t endLocationOfLastRequest() const { return b()->endLocationOfLastRequest; }

            void addRequest(ClientRequestMsg* m);

            uint32_t remainSize() const { return internalStorageSize() / 4 - b()->endLocationOfLastRequest; }

			void finishAddRequest() {
				setMsgSize(b()->endLocationOfLastRequest);
				shrinkToFit();
			}

			CollectStablePointItem* requests() const { return (CollectStablePointItem*)(body() + sizeof(CollectStablePointMsgHeader)); }

	protected:
			CollectStablePointMsgHeader* b() const
			{
				return ((CollectStablePointMsgHeader*)msgBody_);
			}
	};

    class LocalCommitSetMsg : public MessageBase {
    public:
#pragma pack(push,1)
			struct LocalCommitSetMsgHeader
			{
				MessageBase::Header header;

				uint64_t prevStablePoint;
				uint64_t nextStablePoint;
				uint16_t numberOfRequests;
				uint32_t endLocationOfLastRequest;
			};
#pragma pack(pop)
			static_assert(sizeof(LocalCommitSetMsgHeader) == (2 + 8 + 8 + 2 + 4), "LocalCommitSetMsgHeader is 24B");

	public:
			static bool ToActualMsgType(const ReplicasInfo& repInfo, MessageBase* inMsg, LocalCommitSetMsg*& outMsg);

			LocalCommitSetMsg(ReplicaId senderId, uint64_t prevStablePoint, uint64_t nextStablePoint);

			uint64_t prevStablePoint() const { return b()->prevStablePoint; }

			uint64_t nextStablePoint() const { return b()->nextStablePoint; }

			uint16_t numberOfRequests() const { return b()->numberOfRequests; }

			void addRequest(ClientRequestMsg* m);

			uint32_t remainSize() const { return internalStorageSize() - b()->endLocationOfLastRequest; }

            void finishAddRequest() {
				setMsgSize(b()->endLocationOfLastRequest);
				shrinkToFit();
			}

			char* requests() const { return body() + sizeof(LocalCommitSetMsgHeader); }

	protected:
			LocalCommitSetMsgHeader* b() const
			{
				return ((LocalCommitSetMsgHeader*)msgBody_);
			}
	};
    }
}