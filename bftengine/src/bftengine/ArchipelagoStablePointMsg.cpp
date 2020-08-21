#include "ArchipelagoStablePointMsg.hpp"
#include "SysConsts.hpp"
#include "Logger.hpp"

namespace bftEngine
{
  namespace impl
  {
    bool CollectStablePointMsg::ToActualMsgType(const ReplicasInfo& repInfo, MessageBase* inMsg, CollectStablePointMsg*& outMsg)
    {
      Assert(inMsg->type() == MsgCode::CollectStablePoint);
      if (inMsg->size() < sizeof(CollectStablePointMsgHeader)) return false;

      CollectStablePointMsg* t = (CollectStablePointMsg*)inMsg;

      // size
      if (t->b()->endLocationOfLastRequest > t->size())  return false;

      // sent from another replica
      if (t->senderId() == repInfo.myId()) return false;
      if (!repInfo.isIdOfReplica(t->senderId())) return false;

      outMsg = t;
      return true;
    }

    CollectStablePointMsg::CollectStablePointMsg(ReplicaId senderId, uint64_t prevStablePoint, uint64_t nextStablePoint) :
      MessageBase(senderId, MsgCode::CollectStablePoint, maxExternalMessageSize)
    {
      b()->prevStablePoint = prevStablePoint;
      b()->nextStablePoint = nextStablePoint;
      b()->numberOfRequests = 0;
      b()->endLocationOfLastRequest = sizeof(CollectStablePointMsgHeader);
    }

    CollectStablePointMsg::CollectStablePointMsg(ReplicaId senderId, CollectStablePointMsgHeader* body) :
      MessageBase(senderId, (MessageBase::Header*)body, body->endLocationOfLastRequest, false)
    {
    }

    void CollectStablePointMsg::addRequest(ClientRequestMsg* m)
    {
      char* insertPtr = body() + b()->endLocationOfLastRequest;

      CollectStablePointItem t(m->clientProxyId(), m->requestSeqNum());
      memcpy(insertPtr, &t, sizeof(CollectStablePointItem));

      b()->endLocationOfLastRequest += sizeof(CollectStablePointItem);
      b()->numberOfRequests += 1;
    }

     bool LocalCommitSetMsg::ToActualMsgType(const ReplicasInfo& repInfo, MessageBase* inMsg, LocalCommitSetMsg*& outMsg)
    {
      Assert(inMsg->type() == MsgCode::LocalCommitSet);
      if (inMsg->size() < sizeof(LocalCommitSetMsgHeader)) return false;

      LocalCommitSetMsg* t = (LocalCommitSetMsg*)inMsg;

      // size
      if (t->b()->endLocationOfLastRequest > t->size())  return false;

      // sent from another replica
      if (t->senderId() == repInfo.myId()) return false;
      if (!repInfo.isIdOfReplica(t->senderId())) return false;

      outMsg = t;
      return true;
    }

    LocalCommitSetMsg::LocalCommitSetMsg(ReplicaId senderId, uint64_t prevStablePoint, uint64_t nextStablePoint):
      MessageBase(senderId, MsgCode::LocalCommitSet, maxExternalMessageSize)
    {
      b()->prevStablePoint = prevStablePoint;
      b()->nextStablePoint = nextStablePoint;
      b()->numberOfRequests = 0;
      b()->endLocationOfLastRequest = sizeof(LocalCommitSetMsgHeader);
    }

    void LocalCommitSetMsg::addRequest(ClientRequestMsg* m)
    {
      char* insertPtr = body() + b()->endLocationOfLastRequest;

      memcpy(insertPtr, m->body(), m->size());

      b()->endLocationOfLastRequest = b()->endLocationOfLastRequest + m->size();
      b()->numberOfRequests = b()->numberOfRequests + 1;
    }
  }
}