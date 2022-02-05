// Copyright 2018 VMware, all rights reserved
/**
 * The following test suite tests ordering of KeyValuePairs
 */

#include "gtest/gtest.h"
#include "ReplicaResources.h"
#include <thread>

namespace {

using namespace concord::performance;

TEST(replica_resources_test, pruning_utilization) {
  ReplicaResourceEntity rre;
  auto nulled_measurement = rre.getMeasurement(ISystemResourceEntity::type::pruning_utilization);
  ASSERT_EQ(nulled_measurement, 0);

  rre.addMeasurement({ISystemResourceEntity::type::pruning_utilization, 0, 100, 140});
  {
    auto m = rre.getMeasurement(ISystemResourceEntity::type::pruning_utilization);
    ASSERT_EQ(m, 100);
  }
  rre.addMeasurement({ISystemResourceEntity::type::pruning_utilization, 0, 220, 380});
  rre.addMeasurement({ISystemResourceEntity::type::pruning_utilization, 0, 400, 1380});
  rre.addMeasurement({ISystemResourceEntity::type::pruning_utilization, 0, 2000, 2200});
  // total time = 2200 - 100 = 2100
  // op time = 1380
  // util = 65
  {
    auto m = rre.getMeasurement(ISystemResourceEntity::type::pruning_utilization);
    ASSERT_EQ(m, 65);
  }
}

TEST(replica_resources_test, post_execution_utilization_n_avg) {
  ReplicaResourceEntity rre;
  auto nulled_measurements = rre.getMeasurement(ISystemResourceEntity::type::post_execution_utilization);
  ASSERT_EQ(nulled_measurements, 0);

  rre.addMeasurement({ISystemResourceEntity::type::post_execution_utilization, 0, 100, 140});
  {
    auto m = rre.getMeasurement(ISystemResourceEntity::type::post_execution_utilization);
    ASSERT_EQ(m, 100);
  }
  rre.addMeasurement({ISystemResourceEntity::type::post_execution_utilization, 0, 220, 380});
  rre.addMeasurement({ISystemResourceEntity::type::post_execution_utilization, 0, 400, 1380});
  rre.addMeasurement({ISystemResourceEntity::type::post_execution_utilization, 0, 2000, 2200});
  // total time = 2200 - 100 = 2100
  // op time = 1380
  // util = 65
  // avg -
  {
    auto m = rre.getMeasurement(ISystemResourceEntity::type::post_execution_utilization);
    ASSERT_EQ(m, 65);
    m = rre.getMeasurement(ISystemResourceEntity::type::post_execution_avg_time_micro);
    ASSERT_EQ(m, 345);
  }
}

TEST(replica_resources_test, pruning_avg) {
  ReplicaResourceEntity rre;
  auto nulled_measurements = rre.getMeasurement(ISystemResourceEntity::type::pruning_avg_time_micro);
  ASSERT_EQ(nulled_measurements, 0);

  rre.addMeasurement({ISystemResourceEntity::type::pruning_avg_time_micro, 0, 100, 140});
  {
    auto m = rre.getMeasurement(ISystemResourceEntity::type::pruning_avg_time_micro);
    ASSERT_EQ(m, 40);
  }
  rre.addMeasurement({ISystemResourceEntity::type::pruning_avg_time_micro, 0, 220, 380});
  rre.addMeasurement({ISystemResourceEntity::type::pruning_avg_time_micro, 0, 400, 1383});
  rre.addMeasurement({ISystemResourceEntity::type::pruning_avg_time_micro, 0, 2000, 2200});
  // num blocks = 4
  // op time = 1383
  // avg - 345
  {
    auto m = rre.getMeasurement(ISystemResourceEntity::type::pruning_avg_time_micro);
    ASSERT_EQ(m, 345);
  }
}

TEST(replica_resources_test, reset) {
  ReplicaResourceEntity rre;

  rre.addMeasurement({ISystemResourceEntity::type::pruning_avg_time_micro, 0, 100, 140});
  rre.addMeasurement({ISystemResourceEntity::type::pruning_avg_time_micro, 0, 100, 120});

  rre.addMeasurement({ISystemResourceEntity::type::post_execution_utilization, 0, 100, 140});
  rre.addMeasurement({ISystemResourceEntity::type::post_execution_utilization, 0, 180, 200});

  rre.addMeasurement({ISystemResourceEntity::type::pruning_utilization, 0, 100, 140});

  auto m = rre.getMeasurement(ISystemResourceEntity::type::pruning_avg_time_micro);
  ASSERT_EQ(m, 30);
  m = rre.getMeasurement(ISystemResourceEntity::type::post_execution_utilization);
  ASSERT_EQ(m, 60);
  m = rre.getMeasurement(ISystemResourceEntity::type::pruning_utilization);
  ASSERT_EQ(m, 100);

  rre.reset();

  m = rre.getMeasurement(ISystemResourceEntity::type::pruning_avg_time_micro);
  ASSERT_EQ(m, 0);
  m = rre.getMeasurement(ISystemResourceEntity::type::post_execution_utilization);
  ASSERT_EQ(m, 0);
  m = rre.getMeasurement(ISystemResourceEntity::type::pruning_utilization);
  ASSERT_EQ(m, 0);
}

TEST(replica_resources_test, stop_start) {
  ReplicaResourceEntity rre;

  rre.addMeasurement({ISystemResourceEntity::type::pruning_avg_time_micro, 0, 100, 140});
  rre.addMeasurement({ISystemResourceEntity::type::pruning_avg_time_micro, 0, 100, 120});

  rre.addMeasurement({ISystemResourceEntity::type::post_execution_utilization, 0, 100, 140});
  rre.addMeasurement({ISystemResourceEntity::type::post_execution_utilization, 0, 180, 200});

  rre.addMeasurement({ISystemResourceEntity::type::pruning_utilization, 0, 100, 140});

  auto m = rre.getMeasurement(ISystemResourceEntity::type::pruning_avg_time_micro);
  ASSERT_EQ(m, 30);
  m = rre.getMeasurement(ISystemResourceEntity::type::post_execution_utilization);
  ASSERT_EQ(m, 60);
  m = rre.getMeasurement(ISystemResourceEntity::type::pruning_utilization);
  ASSERT_EQ(m, 100);

  rre.stop();

  rre.addMeasurement({ISystemResourceEntity::type::pruning_avg_time_micro, 0, 100, 140});
  rre.addMeasurement({ISystemResourceEntity::type::pruning_avg_time_micro, 0, 100, 120});
  rre.addMeasurement({ISystemResourceEntity::type::post_execution_utilization, 0, 100, 140});
  rre.addMeasurement({ISystemResourceEntity::type::post_execution_utilization, 0, 180, 200});
  rre.addMeasurement({ISystemResourceEntity::type::pruning_utilization, 0, 100, 140});

  // no change
  m = rre.getMeasurement(ISystemResourceEntity::type::pruning_avg_time_micro);
  ASSERT_EQ(m, 30);
  m = rre.getMeasurement(ISystemResourceEntity::type::post_execution_utilization);
  ASSERT_EQ(m, 60);
  m = rre.getMeasurement(ISystemResourceEntity::type::pruning_utilization);
  ASSERT_EQ(m, 100);

  rre.start();

  m = rre.getMeasurement(ISystemResourceEntity::type::pruning_avg_time_micro);
  ASSERT_EQ(m, 30);
  m = rre.getMeasurement(ISystemResourceEntity::type::post_execution_utilization);
  ASSERT_EQ(m, 60);
  m = rre.getMeasurement(ISystemResourceEntity::type::pruning_utilization);
  ASSERT_EQ(m, 100);

  rre.addMeasurement({ISystemResourceEntity::type::pruning_avg_time_micro, 0, 100, 300});
  m = rre.getMeasurement(ISystemResourceEntity::type::pruning_avg_time_micro);
  ASSERT_GT(m, 30);
}

TEST(replica_resources_test, scoped_dur) {
  using namespace std::chrono_literals;
  ReplicaResourceEntity rre;
  auto m = rre.getMeasurement(ISystemResourceEntity::type::pruning_utilization);
  ASSERT_EQ(m, 0);
  {
    ISystemResourceEntity::scopedDurMeasurment mes(rre, ISystemResourceEntity::type::pruning_utilization);
    std::this_thread::sleep_for(1ms);
  }
  m = rre.getMeasurement(ISystemResourceEntity::type::pruning_utilization);
  ASSERT_GT(m, 0);
  {
    ISystemResourceEntity::scopedDurMeasurment mes(rre, ISystemResourceEntity::type::post_execution_utilization);
    std::this_thread::sleep_for(1ms);
  }
  {
    std::this_thread::sleep_for(1ms);
    ISystemResourceEntity::scopedDurMeasurment mes(rre, ISystemResourceEntity::type::post_execution_utilization);
    std::this_thread::sleep_for(1ms);
  }
  m = rre.getMeasurement(ISystemResourceEntity::type::post_execution_utilization);
  ASSERT_GT(m, 0);
  ASSERT_LT(m, 100);
}

TEST(replica_resources_test, scoped_dur_no_commit) {
  using namespace std::chrono_literals;
  ReplicaResourceEntity rre;
  {
    ISystemResourceEntity::scopedDurMeasurment mes(rre, ISystemResourceEntity::type::post_execution_utilization, false);
    std::this_thread::sleep_for(1ms);
  }
  {
    ISystemResourceEntity::scopedDurMeasurment mes(rre, ISystemResourceEntity::type::post_execution_utilization, false);
    std::this_thread::sleep_for(1ms);
  }
  auto m = rre.getMeasurement(ISystemResourceEntity::type::post_execution_utilization);
  ASSERT_EQ(m, 0);
}

}  // end namespace

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}