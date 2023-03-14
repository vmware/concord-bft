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

#include <stdint.h>

#include <cstring>
#include <unordered_map>
#include <forward_list>
#include <atomic>

namespace bftEngine {
namespace bcst {
namespace impl {
template <typename T,
          bool SelfTrust,       // = true,
          bool SelfIsRequired,  // = false,
          bool KeepAllMsgs,     // = true,
          typename ExternalFunc>
class MsgsCertificate {
 public:
  MsgsCertificate(void* const context,
                  const uint16_t numOfReplicas,
                  const uint16_t maxFailures,
                  const uint16_t numOfRequired,
                  const uint16_t selfReplicaId);
  MsgsCertificate(const MsgsCertificate&) = delete;
  MsgsCertificate& operator=(const MsgsCertificate&) = delete;
  ~MsgsCertificate();

  bool addMsg(T* msg, uint16_t replicaId);

  bool isComplete() const;

  bool isInconsistent() const;

  T* selfMsg() const;

  T* bestCorrectMsg() const;

  void tryToMarkComplete();

  bool hasMsgFromReplica(uint16_t replicaId) const;

  T* getMsgFromReplica(uint16_t replicaId) const;

  void resetAndFree();

  bool isEmpty() const;

  // for debug

  std::forward_list<uint16_t> includedReplicas() const;

 protected:
  void* const externalContext;

  void addPeerMsg(T* msg, uint16_t replicaId);
  void addSelfMsg(T* msg);

  static const uint16_t NULL_CLASS = UINT16_MAX;

  const uint16_t numOfReps;
  const uint16_t maxFails;
  const uint16_t required;
  const uint16_t selfId;

  std::unordered_map<uint16_t, std::atomic<T*>> msgsFromReplicas;

  struct MsgClassInfo {
    uint16_t size;
    uint16_t representativeReplica;
  };

  MsgClassInfo* msgClasses = nullptr;
  uint16_t numOfClasses = 0;

  bool complete = false;

  uint16_t bestClass = NULL_CLASS;
  uint16_t sizeOfBestClass = 0;

  bool hasTrustedSelfClass = false;
};

template <typename T, bool SelfTrust, bool SelfIsRequired, bool KeepAllMsgs, typename ExternalFunc>
MsgsCertificate<T, SelfTrust, SelfIsRequired, KeepAllMsgs, ExternalFunc>::MsgsCertificate(void* const context,
                                                                                          const uint16_t numOfReplicas,
                                                                                          const uint16_t maxFailures,
                                                                                          const uint16_t numOfRequired,
                                                                                          const uint16_t selfReplicaId)
    : externalContext{context},
      numOfReps{numOfReplicas},
      maxFails{maxFailures},
      required{numOfRequired},
      selfId(selfReplicaId) {
  static_assert(KeepAllMsgs, "KeepAllMsgs==false is not supported yet");
  static_assert(!SelfIsRequired || SelfTrust, "SelfIsRequired=true requires SelfTrust=true");

  // TODO(GG): more asserts

  msgClasses = new MsgClassInfo[numOfReps];
  memset(msgClasses, 0, numOfReps * sizeof(MsgClassInfo));
}

template <typename T, bool SelfTrust, bool SelfIsRequired, bool KeepAllMsgs, typename ExternalFunc>
MsgsCertificate<T, SelfTrust, SelfIsRequired, KeepAllMsgs, ExternalFunc>::~MsgsCertificate() {
  resetAndFree();
  delete[] msgClasses;
}

template <typename T, bool SelfTrust, bool SelfIsRequired, bool KeepAllMsgs, typename ExternalFunc>
bool MsgsCertificate<T, SelfTrust, SelfIsRequired, KeepAllMsgs, ExternalFunc>::isEmpty() const {
  return (msgsFromReplicas.empty());
}

template <typename T, bool SelfTrust, bool SelfIsRequired, bool KeepAllMsgs, typename ExternalFunc>
void MsgsCertificate<T, SelfTrust, SelfIsRequired, KeepAllMsgs, ExternalFunc>::resetAndFree() {
  complete = false;

  if (msgsFromReplicas.empty()) return;  // nothing to do

  for (auto&& m : msgsFromReplicas) ExternalFunc::free(externalContext, m.second);

  msgsFromReplicas.clear();

  if (numOfClasses > 0) memset(msgClasses, 0, numOfClasses * sizeof(MsgClassInfo));

  numOfClasses = 0;

  bestClass = NULL_CLASS;
  sizeOfBestClass = 0;
  hasTrustedSelfClass = false;
}

template <typename T, bool SelfTrust, bool SelfIsRequired, bool KeepAllMsgs, typename ExternalFunc>
bool MsgsCertificate<T, SelfTrust, SelfIsRequired, KeepAllMsgs, ExternalFunc>::addMsg(T* msg, uint16_t replicaId) {
  if (msgsFromReplicas.count(replicaId) > 0) {
    ExternalFunc::free(externalContext, msg);
    return false;
  }

  msgsFromReplicas[replicaId] = msg;

  if (!complete) {
    if (SelfTrust) {
      if (replicaId == selfId)
        addSelfMsg(msg);
      else
        addPeerMsg(msg, replicaId);
    } else {
      addPeerMsg(msg, replicaId);
    }
  }

  return true;
}

template <typename T, bool SelfTrust, bool SelfIsRequired, bool KeepAllMsgs, typename ExternalFunc>
void MsgsCertificate<T, SelfTrust, SelfIsRequired, KeepAllMsgs, ExternalFunc>::addPeerMsg(T* msg, uint16_t replicaId) {
  uint16_t relevantClass = NULL_CLASS;

  if (hasTrustedSelfClass) {
    MsgClassInfo& cls = msgClasses[0];  // in this case, we have a single class

    auto pos = msgsFromReplicas.find(cls.representativeReplica);
    // TODO(GG) ConcordAssert(pos is okay)
    T* representativeMsg = pos->second;

    if (!ExternalFunc::equivalent(representativeMsg, cls.representativeReplica, msg, replicaId))
      return;  // msg should be ignored

    relevantClass = 0;
    cls.size = cls.size + 1;
  } else {
    // looking for a class
    for (uint16_t i = 0; i < numOfClasses; i++) {
      MsgClassInfo& cls = msgClasses[i];

      auto pos = msgsFromReplicas.find(cls.representativeReplica);
      // TODO(GG) ConcordAssert(pos is okay)
      T* representativeMsg = pos->second;

      if (ExternalFunc::equivalent(representativeMsg, cls.representativeReplica, msg, replicaId)) {
        cls.size = cls.size + 1;
        relevantClass = i;
        break;
      }
    }
  }

  if (relevantClass == NULL_CLASS) {
    // we should create a new class

    if (numOfClasses >= numOfReps) {
      // We probably have an internal error
      // TODO(GG): print error
      return;  // get out
    }

    relevantClass = numOfClasses;
    numOfClasses++;

    MsgClassInfo& cls = msgClasses[relevantClass];
    cls.size = 1;
    cls.representativeReplica = replicaId;

    if (bestClass == NULL_CLASS) {
      bestClass = relevantClass;
      sizeOfBestClass = 1;
    }
  } else {
    MsgClassInfo& cls = msgClasses[relevantClass];

    if (cls.size > sizeOfBestClass) {
      bestClass = relevantClass;
      sizeOfBestClass = cls.size;
    }

    if ((relevantClass == bestClass) && (sizeOfBestClass >= required)) {
      tryToMarkComplete();
    }
  }
}

template <typename T, bool SelfTrust, bool SelfIsRequired, bool KeepAllMsgs, typename ExternalFunc>
void MsgsCertificate<T, SelfTrust, SelfIsRequired, KeepAllMsgs, ExternalFunc>::addSelfMsg(T* msg) {
  //  static_assert(SelfTrust == true, "Invalid invocation");  //TODO(GG)

  uint16_t relevantClass = NULL_CLASS;

  // looking for a class
  for (uint16_t i = 0; i < numOfClasses; i++) {
    MsgClassInfo& cls = msgClasses[i];

    auto pos = msgsFromReplicas.find(cls.representativeReplica);
    // TODO(GG) ConcordAssert(pos is okay)
    T* representativeMsg = pos->second;

    if (ExternalFunc::equivalent(representativeMsg, cls.representativeReplica, msg, selfId)) {
      cls.size = cls.size + 1;
      relevantClass = i;
      break;
    }
  }

  MsgClassInfo classInfo;
  classInfo.representativeReplica = selfId;
  if (relevantClass == NULL_CLASS)
    classInfo.size = 1;
  else
    classInfo.size = msgClasses[relevantClass].size + 1;

  // reset msgClasses & numOfClasses
  memset(msgClasses, 0, numOfClasses * sizeof(MsgClassInfo));
  numOfClasses = 1;

  msgClasses[0] = classInfo;

  bestClass = 0;
  sizeOfBestClass = classInfo.size;

  hasTrustedSelfClass = true;

  if (classInfo.size >= required) {
    tryToMarkComplete();
  }
}

template <typename T, bool SelfTrust, bool SelfIsRequired, bool KeepAllMsgs, typename ExternalFunc>
bool MsgsCertificate<T, SelfTrust, SelfIsRequired, KeepAllMsgs, ExternalFunc>::isComplete() const {
  return complete;
}

template <typename T, bool SelfTrust, bool SelfIsRequired, bool KeepAllMsgs, typename ExternalFunc>
bool MsgsCertificate<T, SelfTrust, SelfIsRequired, KeepAllMsgs, ExternalFunc>::isInconsistent() const {
  return (numOfClasses >= maxFails + 1);
}

template <typename T, bool SelfTrust, bool SelfIsRequired, bool KeepAllMsgs, typename ExternalFunc>
T* MsgsCertificate<T, SelfTrust, SelfIsRequired, KeepAllMsgs, ExternalFunc>::selfMsg() const {
  T* retVal = nullptr;
  auto pos = msgsFromReplicas.find(selfId);

  if (pos != msgsFromReplicas.end()) retVal = pos->second;

  return retVal;
}

template <typename T, bool SelfTrust, bool SelfIsRequired, bool KeepAllMsgs, typename ExternalFunc>
T* MsgsCertificate<T, SelfTrust, SelfIsRequired, KeepAllMsgs, ExternalFunc>::bestCorrectMsg() const {
  T* retVal = nullptr;
  if (hasTrustedSelfClass || (sizeOfBestClass >= maxFails + 1)) {
    const MsgClassInfo& mci = msgClasses[bestClass];

    auto pos = msgsFromReplicas.find(mci.representativeReplica);

    // TODO(GG): Assert pos is okay

    retVal = pos->second;
  }

  return retVal;
}

template <typename T, bool SelfTrust, bool SelfIsRequired, bool KeepAllMsgs, typename ExternalFunc>
void MsgsCertificate<T, SelfTrust, SelfIsRequired, KeepAllMsgs, ExternalFunc>::tryToMarkComplete() {
  if (!SelfIsRequired || hasMsgFromReplica(selfId)) {
    complete = true;
  }
}

template <typename T, bool SelfTrust, bool SelfIsRequired, bool KeepAllMsgs, typename ExternalFunc>
bool MsgsCertificate<T, SelfTrust, SelfIsRequired, KeepAllMsgs, ExternalFunc>::hasMsgFromReplica(
    uint16_t replicaId) const {
  bool retVal = (msgsFromReplicas.count(replicaId) > 0);
  return retVal;
}

template <typename T, bool SelfTrust, bool SelfIsRequired, bool KeepAllMsgs, typename ExternalFunc>
T* MsgsCertificate<T, SelfTrust, SelfIsRequired, KeepAllMsgs, ExternalFunc>::getMsgFromReplica(
    uint16_t replicaId) const {
  auto p = msgsFromReplicas.find(replicaId);
  if (p == msgsFromReplicas.end())
    return nullptr;
  else
    return p->second;
}

template <typename T, bool SelfTrust, bool SelfIsRequired, bool KeepAllMsgs, typename ExternalFunc>
std::forward_list<uint16_t> MsgsCertificate<T, SelfTrust, SelfIsRequired, KeepAllMsgs, ExternalFunc>::includedReplicas()
    const {  // for debug
  std::forward_list<uint16_t> r;

  auto lastPos = r.end();

  for (auto i : msgsFromReplicas) {
    if (lastPos == r.end()) {
      r.push_front(i->first);
      lastPos = r.begin();
    } else {
      lastPos = r.insert_after(lastPos, i->first);
    }
  }

  return r;
}

}  // namespace impl
}  // namespace bcst
}  // namespace bftEngine
