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
#include "bftengine/IKeyExchanger.hpp"
#include "Timers.hpp"
#include "Metrics.hpp"
#include "SequenceWithActiveWindow.hpp"
#include "SeqNumInfo.hpp"
#include "bftengine/ISecureStore.hpp"
#include "PersistentStorage.hpp"
#include "secrets_manager_impl.h"

namespace bftEngine::impl {
class KeyExchangeManager {
 public:
  // Generates and publish key to consensus
  void sendKeyExchange();
  // Generates and publish the first key
  void sendInitialKey();
  // The execution handler implementation that is called when a key exchange msg has passed consensus.
  std::string onKeyExchange(KeyExchangeMsg& kemsg, const uint64_t& sn);
  // A callback that is called each checkpoint and can trigger rotation of keys.
  void onCheckpoint(const int& num);
  // Register a IKeyExchanger to notification when keys are rotated.
  void registerForNotification(IKeyExchanger* ke);
  KeyExchangeMsg getReplicaPublicKey(const uint16_t& repID) const;
  std::string getPrivateKey() { return keysView_.data.privateKey; }
  void loadKeysFromReservedPages();
  std::string getStatus();

  // loads the crypto system from a serialized key view.
  static void loadCryptoFromKeyView(std::shared_ptr<ISecureStore> sec,
                                    const uint16_t repID,
                                    const uint16_t numReplicas);

  std::atomic_bool keysExchanged{false};

  struct FileSecureStore : public ISecureStore {
    FileSecureStore(const std::string& path, uint16_t id, const std::string& postfix = "") {
      fileName = path + "/" + fileName + "_" + std::to_string(id) + postfix;
      LOG_INFO(KEY_EX_LOG, "Key view file is " << fileName);
    }
    std::string fileName{"genSec"};
    void save(const std::string& str);
    std::string load();
    static uint32_t maxSize() { return 4096; }
  };

  class EncryptedFileSecureStore : public ISecureStore {
    FileSecureStore store;
    const std::shared_ptr<concord::secretsmanager::ISecretsManagerImpl> secretsMgr;

   public:
    EncryptedFileSecureStore(const std::shared_ptr<concord::secretsmanager::ISecretsManagerImpl>& sm,
                             const std::string& path,
                             uint16_t id,
                             const std::string& postfix = "")
        : store{path, id, postfix}, secretsMgr{sm} {}
    void save(const std::string& str);
    std::string load();
  };

  // A private key has three states
  // 1 - published to consensus.
  // 2 - after consesnsus i.e. oustanding
  // 3 - after desired check point i.e. the private key of the replica
  // all three fields may be populated simulatanously

  struct KeysViewData : public concord::serialize::SerializableFactory<KeysViewData> {
    KeysViewData(){};
    std::string generatedPrivateKey;
    std::string outstandingPrivateKey;
    std::string privateKey;
    std::vector<std::string> publicKeys;

   protected:
    const std::string getVersion() const;
    void serializeDataMembers(std::ostream& outStream) const;
    void deserializeDataMembers(std::istream& inStream);
  };

  struct KeysView {
   public:
    KeysView(std::shared_ptr<ISecureStore> sec, std::shared_ptr<ISecureStore> backSec, uint32_t clusterSize);
    void save();
    void backup();
    bool load();
    void rotate(std::string& dst, std::string& src);
    KeysViewData& keys() { return data; }
    bool hasGeneratedKeys() {  // if at least one key exists we have generated key in the past
      return (data.outstandingPrivateKey.size() > 0) || (data.generatedPrivateKey.size() > 0) ||
             (data.privateKey.size() > 0);
    }
    KeysViewData data;
    std::shared_ptr<ISecureStore> secStore{nullptr};
    std::shared_ptr<ISecureStore> backupSecStore{nullptr};

   private:
    void save(std::shared_ptr<ISecureStore>& secureStore);
  };

  std::future<void> futureRet;

  struct InitData {
    std::shared_ptr<IInternalBFTClient> cl;
    int id{};
    uint32_t clusterSize{};
    IReservedPages* reservedPages{nullptr};
    uint32_t sizeOfReservedPage{};
    IMultiSigKeyGenerator* kg{nullptr};
    IKeyExchanger* ke{nullptr};
    std::shared_ptr<ISecureStore> sec;
    std::shared_ptr<ISecureStore> backupSec;
    concordUtil::Timers* timers{nullptr};
    std::shared_ptr<concordMetrics::Aggregator> a;
    std::chrono::seconds interval;
    bool keyExchangeOnStart{false};
  };

  static KeyExchangeManager& instance(InitData* id = nullptr) {
    static KeyExchangeManager km{id};
    return km;
  }

  static void start(InitData* id) {
    instance(id);
    instance().initMetrics(id->a, id->interval);
  }

 private:
  KeyExchangeManager(InitData* id);

  uint16_t repID_{};
  uint32_t clusterSize_{};
  std::string generateCid();
  // Raw pointer is ok, since this class does not manage this resource.
  std::shared_ptr<IInternalBFTClient> client_;

  std::vector<IKeyExchanger*> registryToExchange_;
  ClusterKeyStore keyStore_;

  IMultiSigKeyGenerator* multiSigKeyHdlr_{nullptr};
  KeysView keysView_;
  bool keyExchangeOnStart_{};

  void onInitialKeyExchange(KeyExchangeMsg& kemsg, const uint64_t& sn);
  void notifyRegistry(bool save);

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
          publicKeyRotated{component.RegisterCounter("publicKeyRotated")} {}
  };

  std::unique_ptr<Metrics> metrics_;
  void initMetrics(std::shared_ptr<concordMetrics::Aggregator> a, std::chrono::seconds interval);
  ///////////////////////////////////////////////////
  // Timers
  concordUtil::Timers::Handle metricsTimer_;
  concordUtil::Timers& timers_;
  // deleted
  KeyExchangeManager(const KeyExchangeManager&) = delete;
  KeyExchangeManager(const KeyExchangeManager&&) = delete;
  KeyExchangeManager& operator=(const KeyExchangeManager&) = delete;
  KeyExchangeManager& operator=(const KeyExchangeManager&&) = delete;

  friend class TestKeyManager;
};

}  // namespace bftEngine::impl
