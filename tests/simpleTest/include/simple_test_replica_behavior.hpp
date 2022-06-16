#pragma once

#include <cstdint>
#include <chrono>
#include "../../../tests/config/test_parameters.hpp"
#include "../../../tests/simpleTest/include/simple_test_replica.hpp"

struct PersistencyTestInfo {
  uint32_t sleepBetweenRestartsMillis;
  uint32_t downTimeMillis;
  double sleepBetweenRestartsMultiplier;
};

class ISimpleTestReplicaBehavior {
 public:
  virtual bool to_be_restarted() = 0;
  virtual uint32_t get_down_time_millis() = 0;
  virtual void on_restarted() = 0;
  virtual ~ISimpleTestReplicaBehavior() = default;
  explicit ISimpleTestReplicaBehavior(ReplicaParams &rp) : replicaParams{rp}, pti{0, 0, 0} {}
  ISimpleTestReplicaBehavior(ReplicaParams &rp, PersistencyTestInfo &pt) : replicaParams{rp}, pti{pt} {}
  ISimpleTestReplicaBehavior() : pti{0, 0, 0} {};
  uint32_t get_initial_sleep_between_restarts_ms() const { return pti.sleepBetweenRestartsMillis; }

 protected:
  ReplicaParams replicaParams;
  PersistencyTestInfo pti;

  static uint64_t get_epoch_seconds() {
    using namespace std::chrono;
    auto tp = system_clock::now();
    auto dtn = tp.time_since_epoch();
    return dtn.count() * system_clock::period::num / system_clock::period::den;
  }

  static void init_rand() { srand(time(nullptr)); }
};

class DefaultReplicaBehavior : public ISimpleTestReplicaBehavior {
 public:
  bool to_be_restarted() override { return false; }

  uint32_t get_down_time_millis() override { return 0; }

  void on_restarted() override{};

  explicit DefaultReplicaBehavior(ReplicaParams &rp) : ISimpleTestReplicaBehavior{rp} {}

  DefaultReplicaBehavior() = delete;
};

class OneTimePrimaryDownVC : public ISimpleTestReplicaBehavior {
 public:
  bool to_be_restarted() override { return !restarted && replicaParams.replicaId == 0; }

  uint32_t get_down_time_millis() override { return pti.downTimeMillis; }

  void on_restarted() override { restarted = true; }

  explicit OneTimePrimaryDownVC(ReplicaParams &rp) : ISimpleTestReplicaBehavior{rp} {
    pti = PersistencyTestInfo();
    pti.downTimeMillis = 110000;
    pti.sleepBetweenRestartsMillis = 11000;
  }

  OneTimePrimaryDownVC(ReplicaParams &rp, PersistencyTestInfo &pti) : ISimpleTestReplicaBehavior{rp, pti} {}
  OneTimePrimaryDownVC() = delete;

 private:
  bool restarted = false;
};

class Replica2RestartNoVC : public ISimpleTestReplicaBehavior {
 public:
  bool to_be_restarted() override { return replicaParams.replicaId == 2 && get_epoch_seconds() >= nextDownTime; }

  uint32_t get_down_time_millis() override { return (rand() % 10) * 1000 + pti.downTimeMillis; }

  void on_restarted() override { update_next_down_time(); }

  Replica2RestartNoVC(ReplicaParams &rp, PersistencyTestInfo &pti) : ISimpleTestReplicaBehavior{rp, pti} {
    init_rand();
    update_next_down_time();
    pti.sleepBetweenRestartsMultiplier = 3;
  }

  Replica2RestartNoVC(ReplicaParams &rp, PersistencyTestInfo &&pti) : Replica2RestartNoVC(rp, pti) {}

  explicit Replica2RestartNoVC(ReplicaParams &rp) : Replica2RestartNoVC(rp, PersistencyTestInfo{7000, 2000, 1}) {}

  Replica2RestartNoVC() = delete;

 private:
  double nextDownTime = 0;

  void update_next_down_time() {
    nextDownTime = get_epoch_seconds() + pti.sleepBetweenRestartsMillis * pti.sleepBetweenRestartsMultiplier / 1000;
  }
};

class AllReplicasRestartNoVC : public ISimpleTestReplicaBehavior {
 public:
  bool to_be_restarted() override { return get_epoch_seconds() >= nextDownTime; }

  uint32_t get_down_time_millis() override { return (rand() % 20 + 3) * 1000; }

  void on_restarted() override {
    pti.sleepBetweenRestartsMillis *= (uint32_t)pti.sleepBetweenRestartsMultiplier;
    update_next_down_time();
  }

  AllReplicasRestartNoVC(ReplicaParams &rp, PersistencyTestInfo &pti) : ISimpleTestReplicaBehavior(rp, pti) {
    init_rand();
    update_next_down_time();
  }

  AllReplicasRestartNoVC(ReplicaParams &rp, PersistencyTestInfo &&pti) : AllReplicasRestartNoVC(rp, pti) {}

  explicit AllReplicasRestartNoVC(ReplicaParams &rp)
      : AllReplicasRestartNoVC(rp, PersistencyTestInfo{9000, 5000, 1.4}) {}

  AllReplicasRestartNoVC() = delete;

 private:
  double nextDownTime = 0;

  void update_next_down_time() {
    nextDownTime = get_epoch_seconds() +
                   (double)pti.sleepBetweenRestartsMillis / 1000 * pti.sleepBetweenRestartsMultiplier +
                   (replicaParams.replicaId * replicaParams.replicaId);
  }
};

class AllReplicasRestartVC : public AllReplicasRestartNoVC {
 public:
  bool to_be_restarted() override { return get_epoch_seconds() >= nextDownTime; }

  uint32_t get_down_time_millis() override { return (rand() % 20 + 80) * 1000; }

  void on_restarted() override { update_next_down_time(); }

  AllReplicasRestartVC(ReplicaParams &rp, PersistencyTestInfo &pti) : AllReplicasRestartNoVC(rp, pti) {
    init_rand();
    update_next_down_time();
  }

  AllReplicasRestartVC(ReplicaParams &rp, PersistencyTestInfo &&pti) : AllReplicasRestartVC(rp, pti) {}

  explicit AllReplicasRestartVC(ReplicaParams &rp) : AllReplicasRestartVC(rp, PersistencyTestInfo{14000, 0, 1.2}) {}

  AllReplicasRestartVC() = delete;

 private:
  uint64_t nextDownTime = 0;

  void update_next_down_time() {
    if (nextDownTime == 0) {
      nextDownTime = get_epoch_seconds() + (replicaParams.replicaId * replicaParams.viewChangeTimeout / 1000 +
                                            pti.sleepBetweenRestartsMillis / 1000 + replicaParams.replicaId);
    } else {
      nextDownTime =
          get_epoch_seconds() + 3 * (replicaParams.viewChangeTimeout / 1000 + pti.sleepBetweenRestartsMillis / 1000);
    }
  }
};

inline ISimpleTestReplicaBehavior *create_replica_behavior(ReplicaBehavior b, ReplicaParams rp) {
  switch (b) {
    case ReplicaBehavior::Default:
      return new DefaultReplicaBehavior(rp);
    case ReplicaBehavior::Replica0OneTimeRestartVC:
      return new OneTimePrimaryDownVC(rp);
    case ReplicaBehavior::Replica2PeriodicRestartNoVC:
      return new Replica2RestartNoVC(rp);
    case ReplicaBehavior::AllReplicasRandomRestartNoVC:
      return new AllReplicasRestartNoVC(rp);
    case ReplicaBehavior::AllReplicasRandomRestartVC:
      return new AllReplicasRestartVC(rp);
    default:
      throw std::logic_error("None supported behavior");
  }
}
