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
#include "util/Timers.hpp"
#include "util/Metrics.hpp"
#include "secrets/secrets_manager_impl.h"
#include "SysConsts.hpp"
#include "crypto/crypto.hpp"
#include <future>
#include "util/filesystem.hpp"

namespace bftEngine::impl {

class IInternalBFTClient;
class ReplicaImp;
typedef int64_t SeqNum;  // TODO [TK] redefinition

class KeyExchangeManager {
 public:
  void exchangeTlsKeys(const SeqNum& bft_sn);
  // Generates and publish key to consensus
  // TODO: persist the candidate
  void generateConsensusKeyAndSendInternalClientMsg(const SeqNum& sn);
  // Send the current main public key of the replica to consensus
  void sendMainPublicKey();

  void waitForQuorum(const ReplicaImp* repImpInstance);

  // Waits for a quorum and calls generateConsensusKeyAndSendInternalClientMsg
  void waitForQuorumAndTriggerConsensusExchange(const ReplicaImp* repImpInstance, const SeqNum s = 0);
  // The execution handler implementation that is called after a key exchange msg passed consensus.
  // The new key pair will be used from two checkpoints after kemsg.generated_sn
  std::string onKeyExchange(const KeyExchangeMsg& kemsg, const SeqNum& req_sn, const std::string& cid);
  /**
   * Updates new key pair in both volatile (CryptoManager) and persistent (reserved pages) memory
   * @param repID - The id of the replica which uses pubkey
   * @param keyGenerationSn - The sequence number in which the key was generated
   * @param pubkey - The public key of replica repID
   * @param cid
   */
  void registerNewKeyPair(uint16_t repID, SeqNum keyGenerationSn, const std::string& pubkey, const std::string& cid);
  // Register a IKeyExchanger to notification when keys are rotated.
  void registerForNotification(IKeyExchanger* ke) { registryToExchange_.push_back(ke); }
  // Called at the end of state transfer
  void loadPublicKeys();
  void loadClientPublicKeys();

  // True if all of the replicas have completed a key rotation on startup.
  // The startup key rotation is only executed if ReplicaConfig::instance().getkeyExchangeOnStart() is true.
  bool isInitialConsensusExchangeComplete() const;
  const std::string kInitialKeyExchangeCid = "KEY-EXCHANGE-";
  const std::string kInitialClientsKeysCid = "CLIENTS-PUB-KEYS-";
  ///////// Clients public keys interface///////////////
  // whether clients keys were published
  bool clientKeysPublished() const { return clientsPublicKeys_.published(); }
  void saveClientsPublicKeys(const std::string& keys) {
    metrics_->clients_keys_published_status.Get().Set("True");
    clientsPublicKeys_.save(keys);
  }
  // Publish the public keys of the clients
  void sendInitialClientsKeys(const std::string&);
  void onPublishClientsKeys(const std::string& keys, std::optional<std::string> bootstrap_keys);
  // called on a new client key
  void onClientPublicKeyExchange(const std::string& key, concord::crypto::KeyFormat, NodeIdType clientId);
  // called when client keys are loaded
  void loadClientPublicKey(const std::string& key,
                           concord::crypto::KeyFormat,
                           NodeIdType clientId,
                           bool saveToReservedPages);

  void persistCandidates(const std::set<SeqNum>& candidatesToPersist);

  ///////// end - Clients public keys interface///////////////

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
      struct GeneratedKeyPairInfo {
        // generated private key
        std::string priv;
        // generated public key
        // is stored here for consistency check in case of key exchange process interruption
        std::string pub;
        // cid of key exchange request
        std::string cid;
        // seqnum of key exchange request
        SeqNum sn;
        // Key algorithm
        concord::crypto::SignatureAlgorithm algorithm;

        void clear() {
          priv.clear();
          pub.clear();
          cid.clear();
          sn = 0;
          algorithm = concord::crypto::SignatureAlgorithm::Uninitialized;
        }
      } generated;
      // TODO: A map containing all generated candidates should replace this single candidate.

      // seqnum -> private key
      std::map<SeqNum, std::string> keys;

      std::optional<SeqNum> getGenerationSequenceByPrivateKey(const std::string& hexPrivateKeyToSearch) {
        for (auto& [generationSeq, privateKeyHex] : keys) {
          if (hexPrivateKeyToSearch == privateKeyHex) {
            return generationSeq;
          }
        }
        return std::nullopt;
      }

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
          secrets_file_{ReplicaConfig::instance().getkeyViewFilePath() + std::string("/" + secFilePrefix + ".") +
                        std::to_string(ReplicaConfig::instance().getreplicaId())} {
      load();
    }
    // save to secure store
    void save() {
      LOG_INFO(KEY_EX_LOG, "Persisting keys in file: " << fs::absolute(secrets_file_));
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
    IMultiSigKeyGenerator* kg{nullptr};
    IKeyExchanger* ke{nullptr};
    std::shared_ptr<concord::secretsmanager::ISecretsManagerImpl> secretsMgr;
    IClientPublicKeyStore* cpks;
    concordUtil::Timers* timers{nullptr};
  };

  void setAggregator(std::shared_ptr<concordMetrics::Aggregator> a) {
    initMetrics(a, std::chrono::seconds(ReplicaConfig::instance().getmetricsDumpIntervalSeconds()));
  }

  static KeyExchangeManager& instance(InitData* id = nullptr) {
    static KeyExchangeManager km{id};
    return km;
  }

  std::map<SeqNum, std::pair<std::string, std::string>> getCandidates() const;

 private:  // methods
  KeyExchangeManager(InitData* id);
  std::string generateCid(std::string);
  // build cryptosystem
  void notifyRegistry();
  void exchangeTlsKeys(const std::string& type, const SeqNum& bft_sn);
  /**
   * Samples periodically how many connections the replica has with other replicas.
   * returns when num of connections is (clusterSize - 1) i.e. full communication.
   */
  void waitForLiveQuorum(const ReplicaImp* repImpInstance);
  void waitForFullCommunication();
  void initMetrics(std::shared_ptr<concordMetrics::Aggregator> a, std::chrono::seconds interval);
  // True if the replica's own consensus keys were exchanged
  bool exchangedSelfConsensusKeys() const;
  // deleted
  KeyExchangeManager(const KeyExchangeManager&) = delete;
  KeyExchangeManager(const KeyExchangeManager&&) = delete;
  KeyExchangeManager& operator=(const KeyExchangeManager&) = delete;
  KeyExchangeManager& operator=(const KeyExchangeManager&&) = delete;

 private:  // members
  uint16_t repID_{};
  uint32_t clusterSize_{};
  uint32_t quorumSize_{};
  ClusterKeyStore publicKeys_;
  PrivateKeys private_keys_;
  ClientKeyStore clientsPublicKeys_;
  // A flag to prevent race on the replica's internal client.
  std::atomic_bool initial_exchange_;
  // Raw pointer is ok, since this class does not manage this resource.
  std::shared_ptr<IInternalBFTClient> client_;
  std::vector<IKeyExchanger*> registryToExchange_;
  IMultiSigKeyGenerator* multiSigKeyHdlr_{nullptr};
  IClientPublicKeyStore* clientPublicKeyStore_{nullptr};
  bool publishedMasterKey = false;
  std::mutex startup_mutex_;
  // map to store seqNum and its candidate key
  std::map<SeqNum, PrivateKeys::KeyData::GeneratedKeyPairInfo> seq_candidate_map_;

  struct Metrics {
    std::chrono::seconds lastMetricsDumpTime;
    std::chrono::seconds metricsDumpIntervalInSec;
    std::shared_ptr<concordMetrics::Aggregator> aggregator;
    concordMetrics::Component component;
    concordMetrics::StatusHandle sent_key_exchange_on_start_status;
    concordMetrics::StatusHandle clients_keys_published_status;
    concordMetrics::CounterHandle sent_key_exchange_counter;
    concordMetrics::CounterHandle self_key_exchange_counter;
    concordMetrics::CounterHandle public_key_exchange_for_peer_counter;
    concordMetrics::CounterHandle tls_key_exchange_requests_;

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
          clients_keys_published_status{component.RegisterStatus("clients_keys_published", "False")},
          sent_key_exchange_counter{component.RegisterCounter("sent_key_exchange")},
          self_key_exchange_counter{component.RegisterCounter("self_key_exchange")},
          public_key_exchange_for_peer_counter{component.RegisterCounter("public_key_exchange_for_peer")},
          tls_key_exchange_requests_{component.RegisterCounter("tls_key_exchange_requests")} {}
  };

  std::unique_ptr<Metrics> metrics_;
  concordUtil::Timers::Handle metricsTimer_;
  concordUtil::Timers& timers_;
  std::shared_ptr<concord::secretsmanager::ISecretsManagerImpl> secretsMgr_;
  friend class TestKeyManager;
};

}  // namespace bftEngine::impl
