// Concord
//
// Copyright (c) 2018 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").  You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include "ViewChangeSafetyLogic.hpp"
#include "threshsign/IThresholdVerifier.h"
#include "assertUtils.hpp"
#include "Logger.hpp"
#include "CryptoManager.hpp"
#include <set>
#include <unordered_map>

namespace bftEngine {
namespace impl {

///////////////////////////////////////////////////////////////////////////////
// Internal methods and types  used by the implementation of ViewChangeSafetyLogic
///////////////////////////////////////////////////////////////////////////////

static int compareSeqNumbers(const void* a, const void* b)  // used to sort sequence numbers
{
  SeqNum x = *((SeqNum*)a);
  SeqNum y = *((SeqNum*)b);

  if (x > y)
    return (-1);
  else if (x < y)
    return 1;
  else
    return 0;
}

struct FastElem {
  ViewChangeMsg::Element* e;

  bool isNull() const { return (e == nullptr); }

  Digest& prePrepreDigest() const { return e->prePrepareDigest; }
};

bool operator==(const FastElem& lhs, const FastElem& rhs)  // TODO(GG): make sure that this method is used correctly
                                                           // (should be called from the unordered_map)
{
  return lhs.prePrepreDigest() == rhs.prePrepreDigest();
}

struct FastElemHash {
  size_t operator()(FastElem const& s) const noexcept { return s.e->prePrepareDigest.hash(); }
};

struct SlowElem {
  ViewChangeMsg::Element* e;

  bool isNull() const { return (e == nullptr); }

  ViewChangeMsg::PreparedCertificate* c() const {
    ConcordAssert(e->hasPreparedCertificate);
    char* p = (char*)e;
    p = p + sizeof(ViewChangeMsg::Element);
    return (ViewChangeMsg::PreparedCertificate*)p;
  }

  SeqNum seqNum() const { return e->seqNum; }
  Digest& prePrepreDigest() const { return e->prePrepareDigest; }

  ViewNum certificateView() const { return c()->certificateView; }
  uint16_t certificateSigLength() const { return c()->certificateSigLength; }

  const char* certificateSig() const {
    ViewChangeMsg::PreparedCertificate* cert = c();
    ConcordAssert(cert->certificateSigLength > 0);
    char* p = (char*)cert;
    p = p + sizeof(ViewChangeMsg::PreparedCertificate);
    return p;
  }
};

struct SlowElemCompare {
  bool operator()(const SlowElem& lhs, const SlowElem& rhs) const {
    const ViewNum leftCV = lhs.certificateView();
    const ViewNum rightCV = rhs.certificateView();

    if (leftCV != rightCV) {
      return (leftCV > rightCV);
    } else {
      // we have 2 different PreparedCertificate for the same view number.
      // This means one of the following:
      // (1) we have an invalid PreparedCertificate (will be ignored...)
      // (2) we have 2 different valid PreparedCertificate (this means that more than f replicas are malicious and/or
      // the crypto is broken)
      const Digest& leftPPDigest = lhs.prePrepreDigest();
      const Digest& rightPPDigest = rhs.prePrepreDigest();

      return ((memcmp(&leftPPDigest, &rightPPDigest, sizeof(Digest))) < 0);
    }
  }
};

static bool checkSlowPathCertificates(std::set<SlowElem, SlowElemCompare>& slowPathCertificates)  // for debug only
{
  SlowElem lastElement{nullptr};
  for (SlowElem e : slowPathCertificates) {
    if (!lastElement.isNull())  // if this is not the first element
    {
      if (e.certificateView() > lastElement.certificateView()) return false;

      if (e.certificateView() == lastElement.certificateView()) {
        ConcordAssert(e.seqNum() == lastElement.seqNum());
        LOG_WARN(GL, "Found two conflicting prepared certificate for SeqNum " << e.seqNum());
      }
    }

    lastElement = e;
  }

  return true;
}

///////////////////////////////////////////////////////////////////////////////
// ViewChangeSafetyLogic
///////////////////////////////////////////////////////////////////////////////

ViewChangeSafetyLogic::ViewChangeSafetyLogic(const uint16_t n,
                                             const uint16_t f,
                                             const uint16_t c,
                                             const Digest& digestOfNull)
    : N(n), F(f), C(c), nullDigest(digestOfNull) {
  ConcordAssert(N == (3 * F + 2 * C + 1));
}

// TODO(GG): consider to optimize this method
SeqNum ViewChangeSafetyLogic::calcLBStableForView(ViewChangeMsg** const viewChangeMsgsOfPendingView) const {
  const uint16_t INC_IN_VC = (2 * F + 2 * C + 1);

  std::vector<SeqNum> stableNumbers(INC_IN_VC);
  ViewNum v = 0;

  uint16_t n = 0;
  for (uint16_t i = 0; i < N; i++) {
    const ViewChangeMsg* vc = viewChangeMsgsOfPendingView[i];

    if (vc == nullptr) continue;

    if (n == 0) {  // if this is the first message
      v = vc->newView();
    } else {
      ConcordAssert(v == vc->newView())  // all VC messages should refer to the same view
    }

    stableNumbers[n] = vc->lastStable();
    n++;
    ConcordAssert(n <= INC_IN_VC);
  }
  ConcordAssert(n == INC_IN_VC);

  qsort(stableNumbers.data(), INC_IN_VC, sizeof(SeqNum), compareSeqNumbers);

  ConcordAssert(stableNumbers[0] >= stableNumbers[n - 1]);

  const SeqNum lowerBoundOfLastStable = stableNumbers[(F + 1) - 1];

  return lowerBoundOfLastStable;
}

void ViewChangeSafetyLogic::computeRestrictions(ViewChangeMsg** const inViewChangeMsgsOfCurrentView,
                                                const SeqNum inLBStableForView,
                                                SeqNum& outMinRestrictedSeqNum,
                                                SeqNum& outMaxRestrictedSeqNum,
                                                Restriction* outSafetyRestrictionsArray) const {
  const SeqNum lowerBound = inLBStableForView + 1;
  const SeqNum upperBound = inLBStableForView + kWorkWindowSize;

  SeqNum lastRestcitionNum = 0;

  // TODO(GG): optimize the restricted range (e.g., add lastPrepared to each VC - the max of all VC msgs can be used as
  // a better upper bound)

  vector<ViewChangeMsg::ElementsIterator*> VCIterators;

  // create iterators, and add to vector
  for (uint16_t i = 0; i < N; i++) {
    const ViewChangeMsg* vc = inViewChangeMsgsOfCurrentView[i];

    if (vc == nullptr) continue;  // no message

    if (vc->numberOfElements() == 0) continue;  // message is not needed

    ViewChangeMsg::ElementsIterator* iter = new ViewChangeMsg::ElementsIterator(vc);

    iter->goToAtLeast(lowerBound);
    if (iter->end())  // no relevant elements in message
      delete iter;    // TODO(GG): avoid this deletion (by adding new ctor to ViewChangeMsg::ElementsIterator)
    else
      VCIterators.push_back(iter);
  }

  const bool noElements =
      VCIterators.empty();  // (useful when we don't have requests, and we still need to change view)

  // look for safety restrictions
  SeqNum currSeqNum = lowerBound;
  for (; currSeqNum <= upperBound && !VCIterators.empty(); currSeqNum++) {
    Restriction& r = outSafetyRestrictionsArray[currSeqNum - lowerBound];

    bool hasRest = computeRestrictionsForSeqNum(currSeqNum, VCIterators, upperBound, r.digest);

    if (hasRest && (r.digest != nullDigest)) {
      lastRestcitionNum = currSeqNum;
      r.isNull = false;
    } else
      r.isNull = true;
  }

  // reset the remaining elements
  for (; currSeqNum <= upperBound; currSeqNum++) {
    Restriction& r = outSafetyRestrictionsArray[currSeqNum - lowerBound];
    r.isNull = true;
  }

  if (noElements) {
    outMinRestrictedSeqNum = 0;
    outMaxRestrictedSeqNum = 0;
  } else {
    outMinRestrictedSeqNum = lowerBound;
    outMaxRestrictedSeqNum = upperBound;

    // TODO(GG): patch (we should fix the "stable point bug" (and this patch will not be needed)). TODO(GG): for
    // simplicity, the patch assumes that: kWorkWindowSize == 2 * checkpointWindowSize
    if (lastRestcitionNum > 0 && lastRestcitionNum <= (upperBound - checkpointWindowSize)) {
      outMaxRestrictedSeqNum = upperBound - checkpointWindowSize;

      LOG_DEBUG(GL, "\"VC stable\" patch was used");
    }
  }

  // delete remaining iterators
  for (ViewChangeMsg::ElementsIterator* iter : VCIterators) delete iter;
}

bool ViewChangeSafetyLogic::computeRestrictionsForSeqNum(SeqNum s,
                                                         vector<ViewChangeMsg::ElementsIterator*>& VCIterators,
                                                         const SeqNum upperBound,
                                                         Digest& outRestrictedDigest) const {
  ConcordAssert(!VCIterators.empty());
  ConcordAssert(s <= upperBound);

  std::set<SlowElem, SlowElemCompare> slowPathCertificates;

  size_t IdxOfMaxLiveIterator = VCIterators.size() - 1;

  // collect slow certificates
  for (size_t idx = 0; idx <= IdxOfMaxLiveIterator; idx++) {
    ViewChangeMsg::ElementsIterator* currIter = VCIterators[idx];
    ViewChangeMsg::Element* elem = nullptr;
    bool validElem = currIter->getCurrent(elem);

    ConcordAssert(validElem && elem->seqNum >= s);

    if ((elem->seqNum == s) && (elem->hasPreparedCertificate)) {
      SlowElem slow{elem};
      slowPathCertificates.insert(slow);
    }
  }

  ConcordAssert(checkSlowPathCertificates(slowPathCertificates));  // for debug

  // Select prepared certificate

  SlowElem selectedSlow{nullptr};

  for (SlowElem slow : slowPathCertificates) {
    ConcordAssert(s == slow.seqNum());
    Digest d;
    slow.prePrepreDigest().calcCombination(slow.certificateView(), slow.seqNum(), d);

    bool valid = CryptoManager::instance().thresholdVerifierForSlowPathCommit(s)->verify(
        d.content(), DIGEST_SIZE, slow.certificateSig(), slow.certificateSigLength());

    if (valid) {
      selectedSlow = slow;
      break;  // we want the highest valid certificate
    } else {
      LOG_WARN(GL, "An invalid prepared certificate for SeqNum " << s << " is ignored");
    }
  }

  ViewNum minRelevantFastView = 0;
  if (!selectedSlow.isNull()) minRelevantFastView = selectedSlow.certificateView() + 1;

  // Collect vote of each replicas
  std::unordered_map<FastElem, size_t, FastElemHash> fastPathCounters;

  FastElem selectedFastElement{nullptr};

  for (size_t idx = 0; idx <= IdxOfMaxLiveIterator; idx++) {
    ViewChangeMsg::ElementsIterator* currIter = VCIterators[idx];
    ViewChangeMsg::Element* elem = nullptr;
    bool validElem = currIter->getCurrent(elem);

    ConcordAssert(validElem && elem->seqNum >= s);

    if ((elem->seqNum == s) && (elem->originView >= minRelevantFastView)) {
      FastElem fast{elem};

      if (fastPathCounters.count(fast) == 0) {
        fastPathCounters[fast] = 1;
      } else {
        size_t& counter = fastPathCounters[fast];
        counter++;
        if (counter == (uint16_t)(F + C + 1)) {
          selectedFastElement = fast;
          break;
        }
      }
    }
  }

  // Advance iterators + delete irrelevant iterators from VCIterators
  size_t currentIdx = 0;
  while ((!VCIterators.empty()) && (currentIdx <= IdxOfMaxLiveIterator)) {
    ViewChangeMsg::ElementsIterator* currIter = VCIterators[currentIdx];

    ViewChangeMsg::Element* elem;

    bool end = !currIter->getCurrent(elem);

    ConcordAssert(!end);

    if (elem->seqNum > s) {
      currentIdx++;
      continue;
    }  // no need to advance iterator

    ConcordAssert(elem->seqNum == s);

    currIter->gotoNext();

    end = !currIter->getCurrent(elem);

    if (end || elem->seqNum > upperBound)  // if currIter is no longer needed
    {
      // delete currIter, and remove from VCIterators
      delete currIter;
      VCIterators[currentIdx] = VCIterators[IdxOfMaxLiveIterator];
      IdxOfMaxLiveIterator--;
      VCIterators.pop_back();

      ConcordAssert(VCIterators.size() == (IdxOfMaxLiveIterator + 1));  // TODO(GG): delete
    } else {
      currentIdx++;
    }
  }

  if (!selectedFastElement.isNull()) {
    outRestrictedDigest = selectedFastElement.prePrepreDigest();
    return true;
  } else if (!selectedSlow.isNull()) {
    outRestrictedDigest = selectedSlow.prePrepreDigest();
    return true;
  } else {
    outRestrictedDigest.makeZero();
    return false;
  }
}

}  // namespace impl
}  // namespace bftEngine
