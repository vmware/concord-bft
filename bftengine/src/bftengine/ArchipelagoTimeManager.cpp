#include "ArchipelagoTimeManager.hpp"
#include "SysConsts.hpp"
#include "TimeUtils.hpp"
#include "Logger.hpp"
#include "threshsign/bls/relic/BlsThresholdVerifier.h"
#include <vector>

namespace bftEngine
{
  namespace impl
  {
    bool ClientGetTimeStampMsg::ToActualMsgType(const ReplicasInfo& repInfo, MessageBase* inMsg, ClientGetTimeStampMsg*& outMsg)
    {
      Assert(inMsg->type() == MsgCode::ClientGetTimeStamp);
      if (inMsg->size() < sizeof(ClientGetTimeStampMsgHeader)) return false;

      if (inMsg->senderId() == repInfo.myId()) return false;

      outMsg = (ClientGetTimeStampMsg*)inMsg;
      return true;
    }

    ClientGetTimeStampMsg::ClientGetTimeStampMsg(ReplicaId sender, const Digest& d) :
      MessageBase(sender, MsgCode::ClientGetTimeStamp, sizeof(ClientGetTimeStampMsgHeader))
    {
      b()->digestOfRequests = d;
    }

    ClientGetTimeStampMsg::ClientGetTimeStampMsg(ReplicaId sender) :
      MessageBase(sender, MsgCode::ClientGetTimeStamp, sizeof(ClientGetTimeStampMsgHeader))
    {
    }

    ClientGetTimeStampMsg::ClientGetTimeStampMsg() :
      MessageBase(0, MsgCode::ClientGetTimeStamp, sizeof(ClientGetTimeStampMsgHeader))
    {
    }

    bool ClientSignedTimeStampMsg::ToActualMsgType(NodeIdType myId, MessageBase* inMsg, ClientSignedTimeStampMsg*& outMsg)
    {
      Assert(inMsg->type() == MsgCode::ClientSignedTimeStamp);
      if (inMsg->size() < sizeof(ClientSignedTimeStampMsgHeader)) return false;

      ClientSignedTimeStampMsg* t = (ClientSignedTimeStampMsg*)inMsg;
      if (t->size() < sizeof(ClientSignedTimeStampMsgHeader) + t->signatureLen()) return false;

      outMsg = t;
      return true;
    }

    ClientSignedTimeStampMsg* ClientSignedTimeStampMsg::create(ReplicaId senderId, IThresholdSigner* signer, const Digest& d, uint64_t t)
    {
      const size_t sigLen = signer->requiredLengthForSignedData();
      ClientSignedTimeStampMsg* m = new ClientSignedTimeStampMsg(senderId, t, sigLen);

      Digest timeDigest;
      Digest::calcCombination(d, t, timeDigest);

      signer->signData((const char*)(&(timeDigest)), sizeof(Digest),
        m->body() + sizeof(ClientSignedTimeStampMsgHeader), sigLen);
      return m;
    }

    ClientSignedTimeStampMsg::ClientSignedTimeStampMsg(ReplicaId sender, uint64_t t, uint64_t l):
      MessageBase(sender, MsgCode::ClientSignedTimeStamp, sizeof(ClientSignedTimeStampMsgHeader) + l)
    {
      b()->timeStamp = t;
      b()->sigLength = l; 
    }

    bool CombinedTimeStampMsg::ToActualMsgType(const ReplicasInfo& repInfo, MessageBase* inMsg, CombinedTimeStampMsg*& outMsg)
    {
      Assert(inMsg->type() == MsgCode::CombinedTimeStamp);
      if (inMsg->size() < sizeof(CombinedTimeStampMsgHeader)) return false;

      CombinedTimeStampMsg* t = (CombinedTimeStampMsg*)inMsg;

      // size
      if (t->b()->endLocationOfLastVerifiedTimeStamp > t->size()) return false;

      // requests
      //if (!t->checkTimeStamps(nullptr)) return false;

      // sent from another replica
      if (t->senderId() == repInfo.myId()) return false;
      if (!repInfo.isIdOfReplica(t->senderId())) return false;

      outMsg = t;
      return true;
    }

    CombinedTimeStampMsg* CombinedTimeStampMsg::create(ReplicaId senderId, const char* sig, uint16_t sigLen)
    {
      return new CombinedTimeStampMsg(senderId, sig, sigLen);
    }

    CombinedTimeStampMsg::CombinedTimeStampMsg(ReplicaId senderId, const char* sig, uint16_t sigLen):
    MessageBase(senderId, MsgCode::CombinedTimeStamp, maxExternalMessageSize / 2)
    {
      b()->sigLength = sigLen;
      b()->timeStamp = 0;
      b()->numOfVerifiedTimeStamps = 0;
      b()->endLocationOfLastVerifiedTimeStamp = sizeof(CombinedTimeStampMsgHeader) + sigLen;
   
      memcpy(body() + sizeof(CombinedTimeStampMsgHeader), sig, sigLen);
    }

    CombinedTimeStampMsg::CombinedTimeStampMsg(ReplicaId senderId, CombinedTimeStampMsgHeader* body):
    MessageBase(senderId, (MessageBase::Header*)body, body->endLocationOfLastVerifiedTimeStamp, false)
    {
    }

    bool CombinedTimeStampMsg::checkTimeStamps(IThresholdVerifier* verifier)
    {
      uint16_t remains = b()->numOfVerifiedTimeStamps;

      if (remains == 0)
        return (b()->endLocationOfLastVerifiedTimeStamp == sizeof(CombinedTimeStampMsgHeader) + b()->sigLength);

      uint32_t i = sizeof(CombinedTimeStampMsgHeader) + b()->sigLength;
      
      Digest c;
      BLS::Relic::G1T h, g;
      while (remains > 0)
      {
        if (i + sizeof(SignedTimeStampItem) >= b()->endLocationOfLastVerifiedTimeStamp) return false;

        const SignedTimeStampItem* t = (const SignedTimeStampItem*)(body() + i);
        const uint32_t currSize = sizeof(SignedTimeStampItem) + t->sigLength;
  
        if (verifier) {
          Digest::calcCombination(signatureBody(), t->timeStamp, c);
          g1_map(h, reinterpret_cast<const unsigned char *>(&c), sizeof(Digest));

          const unsigned char* sigBuf = (const unsigned char *)t + sizeof(SignedTimeStampItem);
          g.fromBytes(sigBuf + sizeof(int), t->sigLength - sizeof(int));

         // BLS::Relic::BNT idNum(reinterpret_cast<const unsigned char*>(sigBuf), sizeof(int));
          //int rid = static_cast<int>(idNum.toDigit());

          bool succ = ((BLS::Relic::BlsThresholdVerifier*)verifier)->verify(h, g, 
            ((BLS::Relic::BlsPublicKey&)(verifier->getShareVerificationKey(t->replicaId + 1))).getPoint());
          if (!succ) LOG_INFO_F(GL, "Verify Error!");
        }

        remains--;
        i += currSize;
      }
      if (i != b()->endLocationOfLastVerifiedTimeStamp) return false;
      return true;
    }
    
    bool CombinedTimeStampMsg::checkTimeStamps(SigManager* verifiers)
    {
      uint16_t remains = b()->numOfVerifiedTimeStamps;

      if (remains == 0)
        return (b()->endLocationOfLastVerifiedTimeStamp == sizeof(CombinedTimeStampMsgHeader) + b()->sigLength);

      uint32_t i = sizeof(CombinedTimeStampMsgHeader) + b()->sigLength;
      
      Digest c;
      while (remains > 0)
      {
        if (i + sizeof(SignedTimeStampItem) >= b()->endLocationOfLastVerifiedTimeStamp) return false;

        const SignedTimeStampItem* t = (const SignedTimeStampItem*)(body() + i);
        const uint32_t currSize = sizeof(SignedTimeStampItem) + t->sigLength;
  
        if (verifiers) {
          Digest::calcCombination(signatureBody(), t->timeStamp, c);
          bool succ = verifiers->verifySig(t->replicaId, (const char*)&c, sizeof(Digest), (const char *)t + sizeof(SignedTimeStampItem), t->sigLength);
          if (!succ) LOG_INFO_F(GL, "Verify Error!");
        }

        remains--;
        i += currSize;
      }
      if (i != b()->endLocationOfLastVerifiedTimeStamp) return false;
      return true;
    }

    void CombinedTimeStampMsg::addPartialMsg(ClientSignedTimeStampMsg* part)
    {
      if (internalStorageSize() - b()->endLocationOfLastVerifiedTimeStamp < sizeof(SignedTimeStampItem) + part->signatureLen()) {
        LOG_INFO_F(GL, "Cannot add SignedTimeStampMsg any more!");
        return;
      }
      char* insertPtr = body() + b()->endLocationOfLastVerifiedTimeStamp;

      SignedTimeStampItem t(part->senderId(), part->timeStamp(), part->signatureLen());
      memcpy(insertPtr, &t, sizeof(SignedTimeStampItem));
      memcpy(insertPtr + sizeof(SignedTimeStampItem), part->signatureBody(), part->signatureLen());

      b()->endLocationOfLastVerifiedTimeStamp = b()->endLocationOfLastVerifiedTimeStamp + sizeof(SignedTimeStampItem) + part->signatureLen();
      b()->numOfVerifiedTimeStamps = b()->numOfVerifiedTimeStamps + 1;
    }

    void CombinedTimeStampMsg::computeMeanTimeStamp()
    {
      if (b()->timeStamp > 0) return;

      std::vector<uint64_t> times(b()->numOfVerifiedTimeStamps, 0);
      char* ptr = body() + sizeof(CombinedTimeStampMsgHeader) + b()->sigLength;
      for (int i = 0; i < b()->numOfVerifiedTimeStamps; i++) {
        SignedTimeStampItem* item = (SignedTimeStampItem*)ptr;
        times[i] = item->timeStamp;
        ptr += sizeof(SignedTimeStampItem) + item->sigLength;
      }
      std::sort(times.begin(), times.end());
      b()->timeStamp = times[b()->numOfVerifiedTimeStamps / 2 + 1];
    }

    TimeManager::TimeManager(IThresholdSigner* signer): signer(signer)
    {

    }

    TimeManager::~TimeManager()
    {
    }

    ClientSignedTimeStampMsg* TimeManager::GetSignedTimeStamp(ReplicaId r, const Digest& d) const
    {
      return ClientSignedTimeStampMsg::create(r, signer, d, getMonotonicTime());
    }

    ClientSignedTimeStampMsg* TimeManager::GetSignedTimeStamp(ReplicaId r, const Digest& d, SigManager* s) const
    {
      const size_t sigLen = s->getMySigLength();
      const uint64_t t = getMonotonicTime();
      ClientSignedTimeStampMsg* m = new ClientSignedTimeStampMsg(r, t, sigLen);

      Digest timeDigest;
      Digest::calcCombination(d, t, timeDigest);

      s->sign((const char*)&timeDigest, sizeof(Digest),
        m->body() + sizeof(ClientSignedTimeStampMsg::ClientSignedTimeStampMsgHeader), sigLen);
      return m;
    }
  }
}
