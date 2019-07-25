#ifndef CONCORD_BFT_SIMPLE_TEST_REPLICA_BEHAVIOR_HPP
#define CONCORD_BFT_SIMPLE_TEST_REPLICA_BEHAVIOR_HPP

#include <cstdint>
#include "simple_test_replica.hpp"
#include "test_parameters.hpp"
#include <chrono>

struct PersistencyTestInfo {
  uint32_t initialSleepBetweenRestartsMillis;
  uint32_t restartDelay;
  double sleepBetweenRestartsMultipler;
};

class ISimpleTestReplicaBehavior {
public:
  virtual bool should_be_down() = 0;
  virtual uint32_t get_down_time_millis() = 0;
  virtual void on_restarted() = 0;
  virtual ~ISimpleTestReplicaBehavior() {};
  explicit ISimpleTestReplicaBehavior(ReplicaParams &rp) : replicaParams{rp} {}
  ISimpleTestReplicaBehavior(ReplicaParams &rp, PersistencyTestInfo &pt) 
    : replicaParams{rp}, pti{pt} {}
  ISimpleTestReplicaBehavior() {}
protected:
  ReplicaParams replicaParams;
  PersistencyTestInfo pti;

  uint64_t get_epoch_seconds() {
    using namespace std::chrono;
    auto tp = system_clock::now();
    auto dtn = tp.time_since_epoch();
    return dtn.count() * system_clock::period::num / system_clock::period::den;
  }

  void init_rand() {
    srand(time(NULL));
  }
};

class DefaultReplicaBehavior : public ISimpleTestReplicaBehavior {
public:
  virtual bool should_be_down() override {
    return false;
  } 

  virtual uint32_t get_down_time_millis() override {
    return 0;
  }

  virtual void on_restarted(){};

  explicit DefaultReplicaBehavior(ReplicaParams &rp) 
  : ISimpleTestReplicaBehavior{rp} {}

  DefaultReplicaBehavior() = delete;
};

class OneTimePrimaryDownVC : public ISimpleTestReplicaBehavior {
public:
   virtual bool should_be_down() override {
    return !restarted && replicaParams.replicaId == 0;
  } 

  virtual uint32_t get_down_time_millis() override {
    return pti.restartDelay;
  }
  
  virtual void on_restarted() override {
    restarted= true;
  }

  explicit OneTimePrimaryDownVC(ReplicaParams &rp) 
  : ISimpleTestReplicaBehavior{rp} {
    pti = PersistencyTestInfo();
    pti.restartDelay = 90000;
    pti.initialSleepBetweenRestartsMillis = 8000;
  }

  OneTimePrimaryDownVC(ReplicaParams &rp, PersistencyTestInfo &pti) 
  : ISimpleTestReplicaBehavior{rp, pti} {}

  OneTimePrimaryDownVC() = delete;

  private:
    bool restarted = false;
};

class Replica2RestartNoVC : public ISimpleTestReplicaBehavior {
public:
   virtual bool should_be_down() override {
    return replicaParams.replicaId == 2;
  } 

  virtual uint32_t get_down_time_millis() override {
    return (rand() % 10) * 1000 + pti.restartDelay;
  }
  
  virtual void on_restarted() override {
    nextDownTime = get_epoch_seconds() +
      pti.initialSleepBetweenRestartsMillis * pti.sleepBetweenRestartsMultipler;
  }

  Replica2RestartNoVC(ReplicaParams &rp, PersistencyTestInfo &pti) 
  : ISimpleTestReplicaBehavior{rp, pti} {
    init_rand();    
  }

  Replica2RestartNoVC(ReplicaParams &rp, PersistencyTestInfo &&pti) 
    : Replica2RestartNoVC(rp, pti) {
  
  }

  explicit Replica2RestartNoVC(ReplicaParams &rp) : 
    Replica2RestartNoVC(rp, PersistencyTestInfo{5000, 8000, 1.1}) {
  }
  
  Replica2RestartNoVC() = delete;
private:
  uint64_t nextDownTime;
};

class AllReplicasRestartNoVC : public ISimpleTestReplicaBehavior {
public:
   virtual bool should_be_down() override {
      return get_epoch_seconds() >= nextDownTime;
  } 

  virtual uint32_t get_down_time_millis() override {
    return (rand() % 20 + 3) * 1000;
  }
  
  virtual void on_restarted() override {
    pti.initialSleepBetweenRestartsMillis *= pti.sleepBetweenRestartsMultipler;
    update_next_down_time();
  }

  AllReplicasRestartNoVC(ReplicaParams &rp, PersistencyTestInfo &pti)
    : ISimpleTestReplicaBehavior(rp, pti) {
      init_rand();
      update_next_down_time();
    }
  
  AllReplicasRestartNoVC(ReplicaParams &rp, PersistencyTestInfo &&pti)
   : AllReplicasRestartNoVC(rp, pti) {

   }

  explicit AllReplicasRestartNoVC(ReplicaParams &rp) 
  : AllReplicasRestartNoVC(rp, PersistencyTestInfo{5000, 10000, 1.4}) {

  }

  AllReplicasRestartNoVC() = delete;

private:
  uint64_t nextDownTime;

  void update_next_down_time() {
    nextDownTime =
     get_epoch_seconds() + pti.initialSleepBetweenRestartsMillis / 1000 *
      pti.sleepBetweenRestartsMultipler +
       (replicaParams.replicaId * replicaParams.replicaId);
  }
};

class AllReplicasRestartVC : public AllReplicasRestartNoVC {
public:
   virtual bool should_be_down() override {
      return get_epoch_seconds() >= nextDownTime;
  } 

  virtual uint32_t get_down_time_millis() override {
    return (rand() % 20 + 80) * 1000;
  }

  virtual void on_restarted() {
    update_next_down_time();
  }

  AllReplicasRestartVC(ReplicaParams &rp, PersistencyTestInfo &pti)
    : AllReplicasRestartNoVC(rp, pti) {
      init_rand();
      update_next_down_time();
    }
  
  AllReplicasRestartVC(ReplicaParams &rp, PersistencyTestInfo &&pti)
   : AllReplicasRestartVC(rp, pti) {

   }

  explicit AllReplicasRestartVC(ReplicaParams &rp) 
  : AllReplicasRestartVC(rp, PersistencyTestInfo{20000, 0, 1.2}) {

  }

  AllReplicasRestartVC() = delete;

private:
  uint64_t nextDownTime = 0;
  
  void update_next_down_time() {
    if(nextDownTime == 0) {
      nextDownTime = 
        get_epoch_seconds() + 
          (replicaParams.replicaId * replicaParams.viewChangeTimeout / 1000
            + pti.initialSleepBetweenRestartsMillis 
              / 1000 + replicaParams.replicaId);
    } else {
      nextDownTime = 
        get_epoch_seconds()
          + 3 * (replicaParams.viewChangeTimeout / 1000 
            + pti.initialSleepBetweenRestartsMillis / 1000);
    }
  }
};

ISimpleTestReplicaBehavior *create_replica_behavior(
  ReplicaBehavior b, ReplicaParams rp){
  switch(b) {
    case ReplicaBehavior::Default:
      return new DefaultReplicaBehavior(rp);
    case ReplicaBehavior::Replica0OneTimeRestartVC:
      assert(rp.viewChangeEnabled);
      return new OneTimePrimaryDownVC(rp);
    case ReplicaBehavior::Replica2PeriodicRestartNoVC:
      return new Replica2RestartNoVC(rp);
    case ReplicaBehavior::AllReplicasRandomRestartNoVC:
      return new AllReplicasRestartNoVC(rp);
    case ReplicaBehavior::AllReplicasRandomRestartVC:
      assert(rp.viewChangeEnabled);
      return new AllReplicasRestartVC(rp);
    default:
      throw std::logic_error("None supported behavior");
  }
}

#endif // CONCORD_BFT_SIMPLE_TEST_REPLICA_BEHAVIOR_HPP