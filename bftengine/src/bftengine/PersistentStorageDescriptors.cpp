// Concord
//
// Copyright (c) 2019 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the sub-component's license, as noted in the LICENSE
// file.

#include "PersistentStorageDescriptors.hpp"

using namespace std;

namespace bftEngine {

/***** DescriptorOfLastExitFromView *****/

bool DescriptorOfLastExitFromView::registered_ = false;

void DescriptorOfLastExitFromView::registerClass() {
  if (!registered_) {
    classNameToObjectMap_["DescriptorOfLastExitFromView"] =
        UniquePtrToClass(new DescriptorOfLastExitFromView);
    registered_ = true;
  }
}

DescriptorOfLastExitFromView& DescriptorOfLastExitFromView::operator=(
    const DescriptorOfLastExitFromView &other) {
  view = other.view;
  lastStable = other.lastStable;
  lastExecuted = other.lastExecuted;
  elements = other.elements;
  return *this;
}

bool DescriptorOfLastExitFromView::operator==(
    const DescriptorOfLastExitFromView &other) const {
  return (other.view == view &&
      other.lastStable == lastStable &&
      other.lastExecuted == lastExecuted &&
      other.elements == elements);
}

void DescriptorOfLastExitFromView::serializeDataMembers(
    ostream &outStream) const {
  // Serialize view
  outStream.write((char *) &view, sizeof(view));

  // Serialize lastStable
  outStream.write((char *) &lastStable, sizeof(lastStable));

  // Serialize lastExecuted
  outStream.write((char *) &lastExecuted, sizeof(lastExecuted));

  // Serialize elements
  uint32_t numOfElements = elements.size();
  outStream.write((char *) &numOfElements, sizeof(numOfElements));

  // TBD
}

UniquePtrToClass DescriptorOfLastExitFromView::create(istream &inStream) {
  return UniquePtrToClass(); // TBD
}

/***** DescriptorOfLastNewView *****/

bool DescriptorOfLastNewView::registered_ = false;

void DescriptorOfLastNewView::registerClass() {
  if (!registered_) {
    classNameToObjectMap_["DescriptorOfLastNewView"] =
        UniquePtrToClass(new DescriptorOfLastNewView);
    registered_ = true;
  }
}

DescriptorOfLastNewView& DescriptorOfLastNewView::operator=(
    const DescriptorOfLastNewView &other) {
  view = other.view;
  *other.newViewMsg = *newViewMsg;
  viewChangeMsgs = other.viewChangeMsgs;
  maxSeqNumTransferredFromPrevViews = other.maxSeqNumTransferredFromPrevViews;
  return *this;
}

bool DescriptorOfLastNewView::operator==(
    const DescriptorOfLastNewView &other) const {
  return (other.view == view &&
      *other.newViewMsg == *newViewMsg &&
      other.viewChangeMsgs == viewChangeMsgs &&
      other.maxSeqNumTransferredFromPrevViews ==
          maxSeqNumTransferredFromPrevViews);
}

void DescriptorOfLastNewView::serializeDataMembers(ostream &outStream) const {
  // TBD
}

UniquePtrToClass DescriptorOfLastNewView::create(istream &inStream) {
  return UniquePtrToClass(); // TBD
}

/***** DescriptorOfLastExecution *****/

bool DescriptorOfLastExecution::registered_ = false;

void DescriptorOfLastExecution::registerClass() {
  if (!registered_) {
    classNameToObjectMap_["DescriptorOfLastExecution"] =
        UniquePtrToClass(new DescriptorOfLastExecution);
    registered_ = true;
  }
}

DescriptorOfLastExecution& DescriptorOfLastExecution::operator=(
    const DescriptorOfLastExecution &other) {
  executedSeqNum = other.executedSeqNum;
  validRequests = other.validRequests;
  return *this;
}

bool DescriptorOfLastExecution::operator==(
    const DescriptorOfLastExecution &other) const {
  return (other.executedSeqNum == executedSeqNum &&
      other.validRequests == validRequests);
}

void DescriptorOfLastExecution::serializeDataMembers(ostream &outStream) const {
  // TBD
}

UniquePtrToClass DescriptorOfLastExecution::create(istream &inStream) {
  return UniquePtrToClass(); // TBD
}

}  // namespace bftEngine
