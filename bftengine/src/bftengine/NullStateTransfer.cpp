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

#include "NullStateTransfer.hpp"
#include "assertUtils.hpp"
#include "Digest.hpp"
#include "Logger.hpp"

using concord::util::digest::Digest;
using concord::util::digest::DigestUtil;

namespace bftEngine {
namespace impl {
void NullStateTransfer::init(uint64_t maxNumOfRequiredStoredCheckpoints,
                             uint32_t numberOfRequiredReservedPages,
                             uint32_t sizeOfReservedPage) {
  ConcordAssert(!isInitialized());

  numOfReservedPages = numberOfRequiredReservedPages;
  sizeOfPage = sizeOfReservedPage;

  reservedPages = (char*)std::malloc(numOfReservedPages * sizeOfPage);

  running = false;

  ConcordAssert(reservedPages != nullptr);
}

void NullStateTransfer::startRunning(IReplicaForStateTransfer* r) {
  ConcordAssert(isInitialized());
  ConcordAssert(!running);

  running = true;
  repApi = r;
}

void NullStateTransfer::stopRunning() {
  ConcordAssert(isInitialized());
  ConcordAssert(running);

  running = false;
}

bool NullStateTransfer::isRunning() const { return running; }

void NullStateTransfer::createCheckpointOfCurrentState(uint64_t checkpointNumber) {}

void NullStateTransfer::getDigestOfCheckpoint(uint64_t checkpointNumber,
                                              uint16_t sizeOfDigestBuffer,
                                              uint64_t& outBlockId,
                                              char* outStateDigest,
                                              char* outResPagesDigest,
                                              char* outRBVDataDigest) {
  ConcordAssert(sizeOfDigestBuffer >= sizeof(Digest));
  LOG_WARN(GL, "State digest is only based on sequence number (because state transfer module has not been loaded)");

  Digest d;
  DigestUtil::compute((char*)&checkpointNumber, sizeof(checkpointNumber), (char*)&d, sizeof(d));

  memset(outStateDigest, 0, sizeOfDigestBuffer);
  memset(outResPagesDigest, 0, sizeOfDigestBuffer);
  memset(outRBVDataDigest, 0, sizeOfDigestBuffer);
  memcpy(outStateDigest, d.content(), sizeof(d));
  memcpy(outResPagesDigest, d.content(), sizeof(d));
  memcpy(outRBVDataDigest, d.content(), sizeof(d));
}

void NullStateTransfer::startCollectingState() {
  if (errorReport) return;

  LOG_ERROR(GL,
            "Replica is not able to collect state from the other replicas (because state transfer module has not "
            "been loaded)");
  errorReport = true;
}

bool NullStateTransfer::isCollectingState() const { return false; }

uint32_t NullStateTransfer::numberOfReservedPages() const { return numOfReservedPages; }

uint32_t NullStateTransfer::sizeOfReservedPage() const { return sizeOfPage; }

bool NullStateTransfer::loadReservedPage(uint32_t reservedPageId, uint32_t copyLength, char* outReservedPage) const {
  ConcordAssert(copyLength <= sizeOfPage);

  char* page = reservedPages + (reservedPageId * sizeOfPage);

  memcpy(outReservedPage, page, copyLength);

  return true;
}

void NullStateTransfer::saveReservedPage(uint32_t reservedPageId, uint32_t copyLength, const char* inReservedPage) {
  ConcordAssert(copyLength <= sizeOfPage);

  char* page = reservedPages + (reservedPageId * sizeOfPage);

  memcpy(page, inReservedPage, copyLength);
}

void NullStateTransfer::zeroReservedPage(uint32_t reservedPageId) {
  char* page = reservedPages + (reservedPageId * sizeOfPage);

  memset(page, 0, sizeOfPage);
}

void NullStateTransfer::onTimer() {}

void NullStateTransfer::handleStateTransferMessage(char* msg, uint32_t msgLen, uint16_t senderId) {
  repApi->freeStateTransferMsg(msg);
}

NullStateTransfer::~NullStateTransfer() { std::free(reservedPages); }
}  // namespace impl
}  // namespace bftEngine
