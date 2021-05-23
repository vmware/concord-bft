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

#include "ReplicaConfig.hpp"
#include "KeyStore.h"
#include "IKeyExchanger.hpp"
#include "Timers.hpp"
#include "Metrics.hpp"
#include "secrets_manager_impl.h"

namespace bftEngine::impl {

class IInternalBFTClient;

typedef int64_t SeqNum;  // TODO [TK] redefinition

class KeyExchangeManager {
 public:
  // Generates and publish key to consensus
  void sendKeyExchange(const SeqNum&);
  // Generates and publish the first key
  void sendInitialKey();
  // The execution handler implementation that is called when a key exchange msg has passed consensus.
  std::string onKeyExchange(const KeyExchangeMsg& kemsg, const SeqNum& sn, const std::string& cid);
  // Register a IKeyExchanger to notification when keys are rotated.
  void registerForNotification(IKeyExchanger* ke) { registryToExchange_.push_back(ke); }
  // Called at the end of state transfer
  void loadPublicKeys();
  // whether initial key exchange has occurred
  bool exchanged() const {
    return ReplicaConfig::instance().getkeyExchangeOnStart() ? (publicKeys_.numOfExchangedReplicas() == clusterSize_)
                                                             : true;
  }
  std::string getStatus() const;
  /*
   * Persistent private keys store.
   * Stores seqnum to private key mappings as well as generated key before exchange.
   * Uses ISecretsManagerImpl for secure persistence.
   */
  class PrivateKeys {
   public:
    // internal persistent private keys impl
    struct KeyData : public concord::serialize::SerializableFactory<KeyData> {
      struct {
        // generated private key
        std::string priv;
        // generated public key
        // is stored here for consistency check in case of key exchange process interruption
        std::string pub;
        // cid of key exchange request
        std::string cid;
        // seqnum of key exchange request
        SeqNum sn;
        void clear() {
          priv.clear();
          pub.clear();
          cid.clear();
          sn = 0;
        }
      } generated;
      // seqnum -> private key
      std::map<SeqNum, std::string> keys;

     protected:
      void serializeDataMembers(std::ostream& outStream) const override {
        serialize(outStream, generated.priv);
        serialize(outStream, generated.pub);
        serialize(outStream, generated.cid);
        serialize(outStream, generated.sn);
        serialize(outStream, keys);
      }
      void deserializeDataMembers(std::istream& inStream) override {
        deserialize(inStream, generated.priv);
        deserialize(inStream, generated.pub);
        deserialize(inStream, generated.cid);
        deserialize(inStream, generated.sn);
        deserialize(inStream, keys);
      }
    };

    PrivateKeys(std::shared_ptr<concord::secretsmanager::ISecretsManagerImpl> secretsMgr)
        : secretsMgr_{secretsMgr},
          secrets_file_{ReplicaConfig::instance().getkeyViewFilePath() + std::string("/gen-sec.") +
                        std::to_string(ReplicaConfig::instance().getreplicaId())} {
      load();
    }
    // save to secure store
    void save() {
      LOG_INFO(KEY_EX_LOG, "");
      std::stringstream ss;
      concord::serialize::Serializable::serialize(ss, data_);
      secretsMgr_->encryptFile(secrets_file_, ss.str());
    }
    // load from secure store
    bool load();
    // move from generated to exchanged
    void onKeyExchange(const std::string& cid, const SeqNum& sn) {
      LOG_INFO(KEY_EX_LOG, KVLOG(sn, cid));
      // ConcordAssertEQ(cid, data_.generated_cid); // TODO [TK] uncomment when batch cid issue fixed
      auto res = data_.keys.insert(std::make_pair(sn, data_.generated.priv));
      ConcordAssert(res.second);
      data_.generated.clear();
      save();
    }

    KeyData& key_data() { return data_; }
    bool hasGeneratedKeys() {  // if at least one key exists we have generated key in the past
      return (data_.generated.sn > 0 || data_.keys.size() > 0);
    }
    SeqNum lastGeneratedSeqnum() const {
      if (data_.generated.sn > 0) return data_.generated.sn;
      if (data_.keys.size()) return data_.keys.crbegin()->first;
      return 0;
    }

   private:
    KeyData data_;
    std::shared_ptr<concord::secretsmanager::ISecretsManagerImpl> secretsMgr_;
    std::string secrets_file_;
  };

  struct InitData {
    std::shared_ptr<IInternalBFTClient> cl;
    IReservedPages* reservedPages{nullptr};
    IMultiSigKeyGenerator* kg{nullptr};
    IKeyExchanger* ke{nullptr};
    std::shared_ptr<concord::secretsmanager::ISecretsManagerImpl> secretsMgr;
    concordUtil::Timers* timers{nullptr};
    std::shared_ptr<concordMetrics::Aggregator> a;
  };

  static KeyExchangeManager& instance(InitData* id = nullptr) {
    static KeyExchangeManager km{id};
    return km;
  }

  static void start(InitData* id) {
    instance(id);
    instance().initMetrics(id->a, std::chrono::seconds(ReplicaConfig::instance().getmetricsDumpIntervalSeconds()));
  }

 private:  // methods
  KeyExchangeManager(InitData* id);
  std::string generateCid();
  // build cryptosystem
  void notifyRegistry();
  /**
   * Samples periodically how many connections the replica has with other replicas.
   * returns when num of connections is (clusterSize - 1) i.e. full communication.
   */
  void waitForFullCommunication();
  void initMetrics(std::shared_ptr<concordMetrics::Aggregator> a, std::chrono::seconds interval);
  // deleted
  KeyExchangeManager(const KeyExchangeManager&) = delete;
  KeyExchangeManager(const KeyExchangeManager&&) = delete;
  KeyExchangeManager& operator=(const KeyExchangeManager&) = delete;
  KeyExchangeManager& operator=(const KeyExchangeManager&&) = delete;

 private:  // members
  uint16_t repID_{};
  uint32_t clusterSize_{};
  ClusterKeyStore publicKeys_;
  PrivateKeys private_keys_;
  // Raw pointer is ok, since this class does not manage this resource.
  std::shared_ptr<IInternalBFTClient> client_;
  std::vector<IKeyExchanger*> registryToExchange_;
  IMultiSigKeyGenerator* multiSigKeyHdlr_{nullptr};

  struct Metrics {
    std::chrono::seconds lastMetricsDumpTime;
    std::chrono::seconds metricsDumpIntervalInSec;
    std::shared_ptr<concordMetrics::Aggregator> aggregator;
    concordMetrics::Component component;
    concordMetrics::StatusHandle sent_key_exchange_on_start_status;
    concordMetrics::CounterHandle sent_key_exchange_counter;
    concordMetrics::CounterHandle self_key_exchange_counter;
    concordMetrics::CounterHandle public_key_exchange_for_peer_counter;

    void setAggregator(std::shared_ptr<concordMetrics::Aggregator> a) {
      aggregator = a;
      component.SetAggregator(aggregator);
    }
    Metrics(std::shared_ptr<concordMetrics::Aggregator> a, std::chrono::seconds interval)
        : lastMetricsDumpTime{0},
          metricsDumpIntervalInSec{interval},
          aggregator(a),
          component{"KeyExchangeManager", aggregator},
          sent_key_exchange_on_start_status{component.RegisterStatus("sent_key_exchange_on_start", "False")},
          sent_key_exchange_counter{component.RegisterCounter("sent_key_exchange")},
          self_key_exchange_counter{component.RegisterCounter("self_key_exchange")},
          public_key_exchange_for_peer_counter{component.RegisterCounter("public_key_exchange_for_peer")} {}
  };

  std::unique_ptr<Metrics> metrics_;
  concordUtil::Timers::Handle metricsTimer_;
  concordUtil::Timers& timers_;

  friend class TestKeyManager;
};

}  // namespace bftEngine::impl
