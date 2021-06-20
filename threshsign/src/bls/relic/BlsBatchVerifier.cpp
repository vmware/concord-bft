// Concord
//
// Copyright (c) 2018 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the
// LICENSE file.

#include "BlsBatchVerifier.h"

#include "threshsign/bls/relic/BlsPublicParameters.h"
#include "threshsign/bls/relic/BlsThresholdVerifier.h"
#include "threshsign/bls/relic/BlsPublicKey.h"

#include "XAssert.h"

namespace BLS {
namespace Relic {

BlsBatchVerifier::BlsBatchVerifier(const BlsThresholdVerifier& ver, int maxShares)
    : ver(ver),
      fieldOrder(ver.getParams().getGroupOrder()),
      aggTree(maxShares),  // the max # of leafs in the aggregation tree
      isAggregated(false) {}

BlsBatchVerifier::~BlsBatchVerifier() {}

void BlsBatchVerifier::addShare(ShareID id, const G1T& sigShare) {
  const G2T& vk = checked_ref_cast<const BlsPublicKey&>(ver.getShareVerificationKey(id)).getPoint();

  // MAYDO: We could start aggregating incrementally after a leaf is appended
  // (powers of two + finish it off in batchVerify()). Make sure aggregate() still works incrementally.

  // We just insert in the tree directly as a leaf
  aggTree.appendLeaf(Share(id, sigShare, vk));
}

void BlsBatchVerifier::aggregateSigsAndVerifKeys() { aggTree.aggregate(); }

bool BlsBatchVerifier::batchVerify(const G1T& msg, bool wantBadShares, std::vector<ShareID>& shares, bool checkRoot) {
  // WARNING: If we ever multi-thread this, then 'shares' needs to be thread-safe. Right now is not, cause we
  // push_back().
  assertEqual(shares.size(), 0);
  assertLessThanOrEqual(aggTree.getNumLeaves(), ver.getNumTotalShares());

  // TODO: use trick of not checking right root if left root is valid but their parent is invalid
  this->msg = msg;

  if (isAggregated == false) {
    aggregateSigsAndVerifKeys();
    isAggregated = true;
  } else {
    // LOG_WARN(BLS_LOG, "You seem to be verifying the same batch of shares twice. Hopefully you are testing?");
  }

  int rootIdx = aggTree.getRoot();
  if (checkRoot == false) {
    // Handle special case where the tree has size 1 so the root is a leaf
    if (aggTree.getNumLeaves() == 1) {
      if (wantBadShares) shares.push_back(aggTree.getNode(rootIdx).id);
      return false;
    }

    // Forget root (we know it's invalid). Look in left and right child.
    int left = aggTree.getLeft(rootIdx);
    int right = aggTree.getRight(rootIdx);

    // When the tree has size 2 there has to be a left and right child
    batchVerifyRecursive(left, wantBadShares, shares);
    batchVerifyRecursive(right, wantBadShares, shares);

    // Again, when checkRoot is false, we know there's a bad share
    return false;
  } else {
    // Start verifying at the root.
    return batchVerifyRecursive(rootIdx, wantBadShares, shares);
  }
}

bool BlsBatchVerifier::batchVerifyRecursive(int node, bool wantBadShares, std::vector<ShareID>& shares) {
  const Share& share = aggTree.getNode(node);
  assertProperty(node, share.isAggregated);
  bool verified = ver.verify(msg, share.sig, share.vk);

  if (verified) {
    LOG_TRACE(BLS_LOG, "Successfully verified node " << node);

    // Get all leaves below this valid node and push_back() the share IDs in them
    if (wantBadShares == false) {
      // Find the first leaf in this subtree
      int maxLeavesBelow = aggTree.getNumLeavesBelow(node);
      int leaf = node;
      while (!aggTree.isLeaf(leaf)) leaf = aggTree.getLeft(leaf);

      int count = 0;
      while (count < maxLeavesBelow && aggTree.isAppendedLeaf(leaf)) {
        shares.push_back(aggTree.getNode(leaf).id);
        count++;
        leaf++;
      }
    }

    return true;
  } else {
    bool isLeaf = aggTree.isLeaf(node);
    LOG_TRACE(BLS_LOG, "Failed verifying node " << node);

    if (isLeaf) {
      LOG_TRACE(BLS_LOG, "Found bad share " << share.id);
      if (wantBadShares) shares.push_back(share.id);
    } else {
      int left = aggTree.getLeft(node);
      bool hasLeft = aggTree.isAggregatedNode(left);

      int right = aggTree.getRight(node);
      bool hasRight = aggTree.isAggregatedNode(right);

      // If we've found a bad aggregated signature in an internal tree node,
      // it has to be the case that we can go down the tree and identify the
      // bad share(s), or we implemented incorrectly.
      assertTrue(hasLeft || hasRight);

      if (hasLeft) batchVerifyRecursive(left, wantBadShares, shares);

      if (hasRight) batchVerifyRecursive(right, wantBadShares, shares);
    }

    return false;
  }
}

} /* namespace Relic */
} /* namespace BLS */
