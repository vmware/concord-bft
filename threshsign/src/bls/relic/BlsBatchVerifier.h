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

#pragma once

#include "threshsign/ThresholdSignaturesTypes.h"

#include "threshsign/bls/relic/BlsNumTypes.h"

#include "Utils.h"
#include "Logger.hpp"

#include <vector>

namespace BLS {
namespace Relic {

class BlsPublicParameters;
class BlsThresholdVerifier;

/**
 * Binary trees that have their left side full but their bottom right side incomplete.
 * e.g., imagine a history tree (Crosby & Wallach) that grows by having leaves added
 * from left to right. As more leaves are added the tree grows in height so as to
 * "encompass" the new leaves. However, the right side of the tree could be "incomplete"
 * (i.e., the number of leaves is not a power of 2)
 *
 * Example: A tree of height 4
 *
 *            (0)               level 1
 *          /     \
 *        /         \
 *      (1)          (2)        level 2
 *     /   \       /   \
 *    (3)  (4)   (5)    (6)     level 3
 *   /\     /\   /
 *  7  8   9 10 11              level 4
 *  a  b   c  d  e
 */
template <class T, class Agg>
class AlmostCompleteBinaryTree {
 protected:
  // The maximum number of leaves that will ever be inserted in the tree
  int maxLeaves;
  // Rounded to the nearest power-of-two greater than or equal to maxLeaves
  int maxLeavesRounded;
  // e.g., a binary tree with 2^5 leaves will have (2^5 * 2) - 1 nodes overall
  int maxNumNodes;
  // The index of the first leaf
  int firstLeafIndex;
  // The actual number of leaves (i.e., shares) in the tree
  int numLeaves;
  // The maximum height of this tree (i.e., when all maxLeaves are appended)
  int maxTreeHeight;

  std::vector<T> nodes;

 public:
  /**
   * @param maxLeaves    the max # of leaves that will ever be inserted in the tree
   */
  AlmostCompleteBinaryTree(int maxLeaves)
      : maxLeaves(maxLeaves),
        maxLeavesRounded(threshsign::Utils::nearestPowerOfTwo(maxLeaves)),
        maxNumNodes(2 * maxLeavesRounded - 1),
        firstLeafIndex(maxLeavesRounded - 1),
        numLeaves(0),
        maxTreeHeight(getLevel(firstLeafIndex)),
        nodes(static_cast<size_t>(maxNumNodes)) {}

 public:
  void aggregate() {
    int node = firstLeafIndex;
    int levelSize = node + 1;

    // We walk the tree level by level from the bottom (leaves) to the top (root)
    // aggregating the sigs and VKs.
    int rootIdx = getRoot();
    while (node != rootIdx) {
      int parentNode = getParent(node);

      for (int i = 0; i < levelSize; i += 2) {
        int left = node;
        int right = left + 1;
        const T& leftNode = getNode(left);

        // If we've reached the first left node that is "not aggregated" on this level
        // that means we're done and we can move up to the next level.
        if (leftNode.isAggregated == false) {
          assertTrue(getNode(right).isAggregated == false);
          break;
        } else {
          const T& rightNode = getNode(right);
          T& parent = const_cast<T&>(getNode(getParent(left)));

          // Since we have data in the left node and maybe even in
          // its right sibling, then compute their aggregated parent node.
          Agg()(leftNode, rightNode, parent);

          // The parent should be marked as "aggregated" after this
          assertTrue(parent.isAggregated);
        }

        // Move to the next pair of (left, right) nodes on this level
        node += 2;
      }

      // Move up one level in the tree, eventually reaching the root
      levelSize /= 2;
      node = parentNode;
    }

    assertTrue(checkAggregation(rootIdx));
  }

  void appendLeaf(const T& leaf) {
    assertLessThanOrEqual(numLeaves, maxLeaves);

    // TODO: PERF: Copy constructor is called. Move semantics would be faster.
    nodes[static_cast<size_t>(firstLeafIndex + numLeaves)] = leaf;
    numLeaves++;
  }

  int getNumLeaves() const { return numLeaves; }

  const T& getNode(int node) const {
    // Range check
    if (node < 0 || node > maxNumNodes - 1) {
      LOG_ERROR(BLS_LOG, "Accessed node " << node << " is not in [0, " << maxNumNodes << ") range");
      throw std::logic_error("Accessing tree node out-of-bounds");
    }

    size_t nodeIdx = static_cast<size_t>(node);
    return nodes[nodeIdx];
  }

  bool isLeaf(int node) const { return node >= firstLeafIndex; }

  bool isAppendedLeaf(int node) const { return node >= firstLeafIndex && node < firstLeafIndex + numLeaves; }

  int getLevel(int node) const {
    if (node == 0)
      return 1;
    else
      return threshsign::Utils::numBits(node + 1);
  }

  /**
   * Returns the number of leaves below that node, even if leaves not added yet.
   * (e.g., returns maxLeaves when called with root node 0)
   * If the node is a leaf itself, returns 1.
   */
  int getNumLeavesBelow(int node) const {
    // NOTE: This works even if the root is not at index 0
    int level = getLevel(node);

    return threshsign::Utils::pow2(maxTreeHeight - level);
  }

  /**
   * Sometimes there might be less than 'maxLeaves' leaves in the tree. In that
   * case the root will no longer be at index 0, it'll be somewhere on the leftmost
   * path, so we need to compute its index.
   */
  int getRoot() const {
    assertLessThanOrEqual(numLeaves, maxLeaves);
    // MAYDO: Can determine tree height as we add leaves in appendLeaf()
    int treeHeight = getLevel(threshsign::Utils::nearestPowerOfTwo(numLeaves) - 1);

    assertLessThanOrEqual(treeHeight, maxTreeHeight);

    int root = 1;
    root <<= (maxTreeHeight - treeHeight);

    assertStrictlyGreaterThan(root, 0);

    return root - 1;
  }

  int getLeft(int node) const { return 2 * node + 1; }

  int getRight(int node) const { return 2 * node + 2; }

  int getParent(int node) const { return (node - 1) / 2; }

  /**
   * Returns the index of the last leaf.
   */
  int getLastLeaf() const {
    assertStrictlyGreaterThan(numLeaves, 0);
    return (maxLeavesRounded - 1) + (numLeaves - 1);
  }

  /**
   * Returns true if this node is one of the currently aggregated
   * nodes or leaf node in the tree.
   */
  bool isAggregatedNode(int node) const {
    if (isLeaf(node)) {
      return node <= getLastLeaf();
    } else {
      return getNode(node).isAggregated;
    }
  }

 protected:
  bool checkAggregation(int node) {
    if (isLeaf(node) == true) {
      return true;
    } else {
      T& parent = const_cast<T&>(getNode(node));
      if (parent.isAggregated) {
        int left = getLeft(node);
        int right = getRight(node);
        const T& leftNode = getNode(left);
        const T& rightNode = getNode(right);

        T expectedParent;

        Agg()(leftNode, rightNode, expectedParent);
        if (expectedParent != parent) {
          return false;
        }

        return checkAggregation(left) && checkAggregation(right);
      } else {
        return true;
      }
    }
  }
};

/**
 * From "Finding Invalid Signatures in Pairing-Based Batches" by "Law, Laurie; Matt, Brian"
 *
 * "They found that Simple Binary Search is more efficient than naively testing
 * each signature individually if there are fewer than N/8 invalid signatures
 * in the batch."
 */
class BlsBatchVerifier {
 protected:
  class Share {
   public:
    Share() : isAggregated(false), id(0) {}

    Share(ShareID id, const G1T& sig, const G2T& vk) : isAggregated(true), id(id), sig(sig), vk(vk) {}

   public:
    bool isAggregated;
    ShareID id;  // for internal nodes, this is set to zero
    G1T sig;     // possibly, the aggregated signature
    G2T vk;      // possibly, the aggregated VK

   public:
    bool operator!=(const Share& s) {
      return isAggregated != s.isAggregated || id != s.id || sig != s.sig || vk != s.vk;
    }
  };

  class ShareAgg {
   public:
    void operator()(const Share& left, const Share& right, Share& parent) {
      if (left.isAggregated) {
        parent.sig = left.sig;
        parent.vk = left.vk;
        parent.isAggregated = true;

        if (right.isAggregated) {
          parent.sig.Add(right.sig);
          parent.vk.Add(right.vk);
        }
      } else {
        // If no aggregated left child, there should be no aggregated right child either.
        assertFalse(right.isAggregated);
      }
    }
  };

 protected:
  const BlsThresholdVerifier& ver;
  G1T msg;
  const BNT& fieldOrder;

  AlmostCompleteBinaryTree<Share, ShareAgg> aggTree;

  bool isAggregated;

 public:
  /**
   * @param	ver 	the BlsThresholdVerifier object needed for verifying shares
   */
  BlsBatchVerifier(const BlsThresholdVerifier& ver, int maxShares);
  virtual ~BlsBatchVerifier();

 public:
  void addShare(ShareID id, const G1T& sigShare);

  int getNumShares() const { return aggTree.getNumLeaves(); }

  bool empty() const { return aggTree.getNumLeaves() == 0; }

  void aggregateSigsAndVerifKeys();

  /**
   * Verifies all the shares added so far using batch verification!
   *
   * If the batch verification fails, recurses down, identifies all bad shares
   * and adds them to the badShares vector.
   */

  /**
   * @param   msg             the message to be verified
   * @param   wantBadShares   if true, store the bad shares' IDs in 'shares', if false, stores the good shares' IDs
   * @param   shares      good share or bad share IDs, depending on 'wantBadShares'
   * @param   checkRoot   if we already know the threshold sig failed to construct, then
   *                      we know the root aggregated signature is invalid so we can skip it.
   */
  bool batchVerify(const G1T& msg, bool wantBadShares, std::vector<ShareID>& shares, bool checkRoot);

  bool batchVerifyRecursive(int node, bool wantBadShares, std::vector<ShareID>& shares);
};

} /* namespace Relic */
} /* namespace BLS */
