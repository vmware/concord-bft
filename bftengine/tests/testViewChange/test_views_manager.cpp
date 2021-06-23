// Concord
//
// Copyright (c) 2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the
// LICENSE file.

#include "gtest/gtest.h"
#include "helper.hpp"
#include "Logger.hpp"
#include "ReplicaConfig.hpp"
#include "SigManager.hpp"
#include "ViewsManager.hpp"

namespace {

using namespace bftEngine;
using namespace bftEngine::impl;

constexpr bool dynamicCollectorForPartialProofs = true, dynamicCollectorForExecutionProofs = false;

class ViewsManagerTest : public ::testing::Test {
 protected:
  ReplicaConfig& rc;
  ReplicasInfo replicasInfo;
  std::unique_ptr<SigManager> sigManager;
  std::unique_ptr<ViewsManager> viewsManager;

  ViewsManagerTest()
      : rc(createReplicaConfig()),
        replicasInfo(rc, dynamicCollectorForPartialProofs, dynamicCollectorForExecutionProofs),
        sigManager(createSigManager(rc.replicaId,
                                    rc.replicaPrivateKey,
                                    KeyFormat::HexaDecimalStrippedFormat,
                                    rc.publicKeysOfReplicas,
                                    replicasInfo)),
        viewsManager(std::make_unique<ViewsManager>(&replicasInfo)) {}
};

TEST_F(ViewsManagerTest, moving_to_higher_view) {
  const auto currentView = viewsManager->getCurrentView();
  LOG_INFO(GL, KVLOG(currentView));
  viewsManager->setHigherView(currentView + 1);
  LOG_INFO(GL, KVLOG(viewsManager->getCurrentView()));
  ASSERT_LT(currentView, viewsManager->getCurrentView());
}

TEST_F(ViewsManagerTest, form_a_quorum_of_complaints) {
  constexpr int initialView = 0;

  for (int replicaId = 0; replicaId < rc.numReplicas; ++replicaId) {
    if (replicaId == rc.replicaId) continue;

    std::unique_ptr<ReplicaAsksToLeaveViewMsg> complaint(ReplicaAsksToLeaveViewMsg::create(
        replicaId, initialView, ReplicaAsksToLeaveViewMsg::Reason::ClientRequestTimeout));

    viewsManager->storeComplaint(std::move(complaint));
  }

  ASSERT_EQ(viewsManager->hasQuorumToLeaveView(), true);
}

}  // namespace

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}