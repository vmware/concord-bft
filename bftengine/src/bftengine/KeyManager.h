// Concord
//
// Copyright (c) 2020 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License"). You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the sub-component's license, as noted in the LICENSE
// file.

#pragma once

#include "InternalBFTClient.hpp"
#include "KeyStore.h"
#include "bftengine/IPathDetector.hpp"
#include "bftengine/IKeyExchanger.hpp"
#include "Timers.hpp"
#include "Metrics.hpp"
#include "SequenceWithActiveWindow.hpp"
#include "SeqNumInfo.hpp"

namespace bftEngine::impl {
class KeyManager {
 public:
  // Generates and publish key to consensus
  void sendKeyExchange();
  // Generates and publish the first key
  void sendInitialKey();
  // The execution handler implementation that is called when a key exchange msg has passed consensus.
  std::string onKeyExchange(KeyExchangeMsg& kemsg, const uint64_t& sn);
  // A callback that is called each checkpoint and can trigger rotation of keys.
  void onCheckpoint(const int& num);
  // Register a IKeyExchanger to notification whehn keys are rotated.
  void registerForNotification(IKeyExchanger* ke);
  KeyExchangeMsg getReplicaPublicKey(const uint16_t& repID) const;
  void loadKeysFromReservedPages();

  // loads the crypto system from a serialized key view.
  std::atomic_bool keysExchanged{false};
  std::future<void> futureRet;

  struct InitData {
    std::shared_ptr<IInternalBFTClient> cl;
    int id{};
    uint32_t clusterSize{};
    IReservedPages* reservedPages{nullptr};
    uint32_t sizeOfReservedPage{};
    std::shared_ptr<IPathDetector> pathDetect;
    concordUtil::Timers* timers{nullptr};
    std::shared_ptr<concordMetrics::Aggregator> a;
    std::chrono::seconds interval;
  };

  static KeyManager& get(InitData* id = nullptr) {
    static KeyManager km{id};
    return km;
  }

  static void start(InitData* id) {
    get(id);
    get().initMetrics(id->a, id->interval);
  }

 private:
  KeyManager(InitData* id);

  uint16_t repID_{};
  uint32_t clusterSize_{};
  std::string generateCid();
  // Raw pointer is ok, since this class does not manage this resource.
  std::shared_ptr<IInternalBFTClient> client_;

  std::vector<IKeyExchanger*> registryToExchange_;
  ClusterKeyStore keyStore_;

  std::shared_ptr<IPathDetector> pathDetector_;
  void onInitialKeyExchange(KeyExchangeMsg& kemsg, const uint64_t& sn);
  void notifyRegistry();

  // Samples periodically how many connections the replica has with other replicas.
  // returns when num of connections is (clusterSize - 1) i.e. full communication.
  void waitForFullCommunication();

  //////////////////////////////////////////////////
  // METRICS
  struct Metrics {
    std::chrono::seconds lastMetricsDumpTime;
    std::chrono::seconds metricsDumpIntervalInSec;
    std::shared_ptr<concordMetrics::Aggregator> aggregator;
    concordMetrics::Component component;
    concordMetrics::CounterHandle keyExchangedCounter;
    concordMetrics::CounterHandle keyExchangedOnStartCounter;
    concordMetrics::CounterHandle DroppedMsgsCounter;
    concordMetrics::CounterHandle publicKeyRotated;
    void setAggregator(std::shared_ptr<concordMetrics::Aggregator> a) {
      aggregator = a;
      component.SetAggregator(aggregator);
    }
    Metrics(std::shared_ptr<concordMetrics::Aggregator> a, std::chrono::seconds interval)
        : lastMetricsDumpTime{0},
          metricsDumpIntervalInSec{interval},
          aggregator(a),
          component{"KeyManager", aggregator},
          keyExchangedCounter{component.RegisterCounter("KeyExchangedCounter")},
          keyExchangedOnStartCounter{component.RegisterCounter("KeyExchangedOnStartCounter")},
          DroppedMsgsCounter{component.RegisterCounter("DroppedMsgsCounter")},
          publicKeyRotated{component.RegisterCounter("publicKeyRotated")} {}
  };

  std::unique_ptr<Metrics> metrics_;
  void initMetrics(std::shared_ptr<concordMetrics::Aggregator> a, std::chrono::seconds interval);
  ///////////////////////////////////////////////////
  // Timers
  concordUtil::Timers::Handle metricsTimer_;
  concordUtil::Timers& timers_;
  // deleted
  KeyManager(const KeyManager&) = delete;
  KeyManager(const KeyManager&&) = delete;
  KeyManager& operator=(const KeyManager&) = delete;
  KeyManager& operator=(const KeyManager&&) = delete;

  friend class TestKeyManager;
};

}  // namespace bftEngine::impl