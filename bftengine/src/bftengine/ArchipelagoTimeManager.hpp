#pragma once

#include "Crypto.hpp"
#include "PrimitiveTypes.hpp"
#include "assertUtils.hpp"
#include "Digest.hpp"
#include "MessageBase.hpp"

namespace bftEngine
{
	namespace impl
	{
    class ClientGetTimeStampMsg : public MessageBase
		{
		protected:
#pragma pack(push,1)
			struct ClientGetTimeStampMsgHeader
			{
				MessageBase::Header header;
				Digest  digestOfRequests;
			};
#pragma pack(pop)
			static_assert(sizeof(ClientGetTimeStampMsgHeader) == (2 + DIGEST_SIZE), "ClientGetTimeStampMsgHeader is 34B");

		public:

			static bool ToActualMsgType(const ReplicasInfo& repInfo, MessageBase* inMsg, ClientGetTimeStampMsg*& outMsg);

			ClientGetTimeStampMsg(ReplicaId sender, const Digest& d);

			ClientGetTimeStampMsg(ReplicaId sender);
			
			ClientGetTimeStampMsg();

			Digest& digestOfRequests() const { return b()->digestOfRequests; }

		protected:

			ClientGetTimeStampMsgHeader* b() const
			{
				return (ClientGetTimeStampMsgHeader*)msgBody_;
			}
		};

    class ClientSignedTimeStampMsg : public MessageBase
    {
    public:
#pragma pack(push,1)
			struct ClientSignedTimeStampMsgHeader
			{
				MessageBase::Header header;

                uint64_t timeStamp;
                uint16_t sigLength;
			};
#pragma pack(pop)
			static_assert(sizeof(ClientSignedTimeStampMsgHeader) == (2 + 8 + 2), "ClientSignedTimeStampMsgHeader is 12B");

		public:

			static bool ToActualMsgType(NodeIdType myId, MessageBase* inMsg, ClientSignedTimeStampMsg*& outMsg);

            static ClientSignedTimeStampMsg *create(ReplicaId senderId, IThresholdSigner* signer, const Digest& d, uint64_t t);

			ClientSignedTimeStampMsg(ReplicaId sender, uint64_t t, uint64_t l);

            uint64_t timeStamp() const { return b()->timeStamp; }

            uint16_t signatureLen() const { return b()->sigLength; }

			char* signatureBody() const { return body() + sizeof(ClientSignedTimeStampMsgHeader); }

		protected:

			ClientSignedTimeStampMsgHeader* b() const
			{
				return (ClientSignedTimeStampMsgHeader*)msgBody_;
			}
    };

#pragma pack(push,1)
	struct SignedTimeStampItem {
		uint16_t replicaId;
        uint64_t timeStamp;
		uint16_t sigLength;
		SignedTimeStampItem(uint16_t r, uint64_t t, uint16_t s): replicaId(r), timeStamp(t), sigLength(s) {}
	};
#pragma pack(pop)

	class CombinedTimeStampMsg : public MessageBase
    {
    public:
#pragma pack(push,1)
			struct CombinedTimeStampMsgHeader
			{
				MessageBase::Header header;
				uint16_t sigLength;

				uint64_t timeStamp;	
				uint16_t numOfVerifiedTimeStamps;
				uint32_t endLocationOfLastVerifiedTimeStamp;
			};
#pragma pack(pop)
			static_assert(sizeof(CombinedTimeStampMsgHeader) == (2 + 2 + 8 + 2 + 4), "CombinedTimeStampMsgHeader is 18B");
	
	public:
			static bool ToActualMsgType(const ReplicasInfo& repInfo, MessageBase* inMsg, CombinedTimeStampMsg*& outMsg);

			static CombinedTimeStampMsg *create(ReplicaId senderId, const char* sig, uint16_t sigLen);

			CombinedTimeStampMsg(ReplicaId senderId, const char* sig, uint16_t sigLen);

            CombinedTimeStampMsg(ReplicaId senderId, CombinedTimeStampMsgHeader* body);

			uint64_t timeStamp() const { return b()->timeStamp; }

			void setTimeStamp(uint64_t t) { b()->timeStamp = t; }

			uint16_t numOfVerifiedTimeStamps() const { return b()->numOfVerifiedTimeStamps; }
			
			uint16_t signatureLen() const { return b()->sigLength; }

			char* signatureBody() const { return body() + sizeof(CombinedTimeStampMsgHeader); }

            void addPartialMsg(ClientSignedTimeStampMsg* part);

            void computeMeanTimeStamp();

            bool checkTimeStamps(IThresholdVerifier* verifier);

			bool checkTimeStamps(SigManager* verifiers);

			uint32_t endLocationOfLastVerifiedTimeStamp() const { return b()->endLocationOfLastVerifiedTimeStamp; }

	protected:
      
			CombinedTimeStampMsgHeader* b() const
			{
				return (CombinedTimeStampMsgHeader*)msgBody_;
			}
	};

    class TimeManager
	{
	public:
            IThresholdSigner* signer;

            TimeManager(IThresholdSigner* signer);

            ~TimeManager();

			ClientSignedTimeStampMsg* GetSignedTimeStamp(ReplicaId r, const Digest& d) const;

			ClientSignedTimeStampMsg* GetSignedTimeStamp(ReplicaId r, const Digest& d, SigManager* s) const;
    };
	}
}
