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

#include "bftengine/KeyExchangeManager.hpp"
#include "Replica.hpp"
#include "InternalBFTClient.hpp"
#include "util/kvstream.h"
#include "util/json_output.hpp"
#include "SigManager.hpp"
#include "secrets/secrets_manager_plain.h"
#include "concord.cmf.hpp"
#include "bftengine/EpochManager.hpp"
#include "concord.cmf.hpp"
#include "communication/StateControl.hpp"
#include "ReplicaImp.hpp"
#include "crypto/factory.hpp"
#include "crypto/crypto.hpp"
#include "crypto/openssl/certificates.hpp"

namespace bftEngine::impl {

using concord::crypto::KeyFormat;
using concord::crypto::SignatureAlgorithm;
using concord::crypto::EdDSAHexToPem;
using concord::crypto::generateSelfSignedCert;

KeyExchangeManager::KeyExchangeManager(InitData* id)
    : repID_{ReplicaConfig::instance().getreplicaId()},
      clusterSize_{ReplicaConfig::instance().getnumReplicas()},
      quorumSize_{static_cast<uint32_t>(2 * ReplicaConfig::instance().fVal + ReplicaConfig::instance().cVal)},
      publicKeys_{clusterSize_},
      private_keys_(id->secretsMgr),
      clientsPublicKeys_(ReplicaConfig::instance().get("concord.bft.keyExchage.clientKeysEnabled", true)),
      client_(id->cl),
      multiSigKeyHdlr_(id->kg),
      clientPublicKeyStore_{id->cpks},
      timers_(*(id->timers)),
      secretsMgr_(id->secretsMgr) {
  registerForNotification(id->ke);
  notifyRegistry();
  if (!ReplicaConfig::instance().getkeyExchangeOnStart()) initial_exchange_ = true;
}

void KeyExchangeManager::initMetrics(std::shared_ptr<concordMetrics::Aggregator> a, std::chrono::seconds interval) {
  metrics_.reset(new Metrics(a, interval));
  metrics_->component.Register();
  metricsTimer_ = timers_.add(
      std::chrono::milliseconds(100), concordUtil::Timers::Timer::RECURRING, [this](concordUtil::Timers::Handle h) {
        metrics_->component.UpdateAggregator();
        auto currTime =
            std::chrono::duration_cast<std::chrono::seconds>(std::chrono::steady_clock::now().time_since_epoch());
        if (currTime - metrics_->lastMetricsDumpTime >= metrics_->metricsDumpIntervalInSec) {
          metrics_->lastMetricsDumpTime = currTime;
          LOG_DEBUG(KEY_EX_LOG, "-- KeyExchangeManager metrics dump--" + metrics_->component.ToJson());
        }
      });
}

std::string KeyExchangeManager::generateCid(std::string cid) {
  auto now = getMonotonicTime().time_since_epoch();
  auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(now);
  auto sn = now_ms.count();
  cid += std::to_string(repID_) + "-" + std::to_string(sn);
  return cid;
}

void KeyExchangeManager::registerNewKeyPair(uint16_t repID,
                                            SeqNum keyGenerationSn,
                                            const std::string& pubkey,
                                            const std::string& cid) {
  const bool isSelfKeyExchange = repID == repID_;
  publicKeys_.push(repID, pubkey, keyGenerationSn);
  if (!isSelfKeyExchange) {
    for (auto* e : registryToExchange_) e->onPublicKeyExchange(pubkey, repID, keyGenerationSn);
    metrics_->public_key_exchange_for_peer_counter++;
    return;
  }

  ConcordAssertNE(seq_candidate_map_.find(keyGenerationSn), seq_candidate_map_.end());
  private_keys_.key_data().generated = seq_candidate_map_[keyGenerationSn];
  seq_candidate_map_.erase(keyGenerationSn);
  // TODO: Figure out when old candidates can be removed
  ConcordAssert(private_keys_.key_data().generated.pub == pubkey);
  private_keys_.onKeyExchange(cid, keyGenerationSn);
  for (auto e : registryToExchange_)
    e->onPrivateKeyExchange(private_keys_.key_data().keys[keyGenerationSn], pubkey, keyGenerationSn);
  metrics_->self_key_exchange_counter++;
}

std::string KeyExchangeManager::onKeyExchange(const KeyExchangeMsg& kemsg,
                                              const SeqNum& req_sn,
                                              const std::string& cid) {
  const bool isSelfKeyExchange = kemsg.repID == repID_;
  SCOPED_MDC_SEQ_NUM(std::to_string(kemsg.generated_sn));
  // The key will be used from this sequence forwards
  // new keys are used two checkpoints after reaching consensus
  const SeqNum sn = kemsg.generated_sn;
  LOG_INFO(KEY_EX_LOG, KVLOG(kemsg.toString(), sn, cid, isInitialConsensusExchangeComplete()));
  // client query
  if (kemsg.op == KeyExchangeMsg::HAS_KEYS) {
    LOG_INFO(KEY_EX_LOG, "Has keys: " << std::boolalpha << isInitialConsensusExchangeComplete() << std::noboolalpha);
    if (!isInitialConsensusExchangeComplete()) return std::string(KeyExchangeMsg::hasKeysFalseReply);
    return std::string(KeyExchangeMsg::hasKeysTrueReply);
  }
  uint64_t my_epoch = EpochManager::instance().getSelfEpochNumber();
  if (kemsg.epoch != my_epoch) {
    LOG_WARN(KEY_EX_LOG, "Got KeyExchangeMsg of a different epoch, ignore..." << KVLOG(kemsg.epoch, my_epoch));
    return "invalid epoch";
  }
  // reject key exchange message if generated seq_num is outside working window
  if (sn < ((req_sn / kWorkWindowSize) * kWorkWindowSize)) {
    LOG_WARN(KEY_EX_LOG, "Generated sequence number is outside working window, ignore..." << KVLOG(sn, req_sn));
    return "gen_seq_num_ouside_workingwindow";
  }
  if (publicKeys_.keyExists(kemsg.repID, sn)) return "ok";

  if (isSelfKeyExchange && seq_candidate_map_.find(sn) == seq_candidate_map_.end()) {
    return "missing_candidate";
  }

  registerNewKeyPair(kemsg.repID, sn, kemsg.pubkey, cid);

  auto liveQuorumSize = ReplicaConfig::instance().waitForFullCommOnStartup ? clusterSize_ : quorumSize_;
  if (ReplicaConfig::instance().getkeyExchangeOnStart() && (publicKeys_.numOfExchangedReplicas() <= liveQuorumSize))
    LOG_INFO(KEY_EX_LOG,
             "Exchanged [" << publicKeys_.numOfExchangedReplicas() << "] out of [" << liveQuorumSize << "]"
                           << KVLOG(kemsg.repID, initial_exchange_, isInitialConsensusExchangeComplete()));
  if (!initial_exchange_ && isInitialConsensusExchangeComplete()) {
    initial_exchange_ = true;
    if (ReplicaConfig::instance().getkeyExchangeOnStart() &&
        ReplicaConfig::instance().publishReplicasMasterKeyOnStartup) {
      sendMainPublicKey();
      return "ok";
    }
  }

  // The key was exchanged successfully, write a block containing the new key to the chain
  // TODO: need to persist key state and new block atomically
  // (Cannot be done with current reserved pages implementation)
  if (ReplicaConfig::instance().singleSignatureScheme && isSelfKeyExchange) {
    sendMainPublicKey();
  }
  return "ok";
}

void KeyExchangeManager::notifyRegistry() {
  // sort public keys by sequence number in order to "replay" ordered key exchange operations for proper initialization
  // sequence number may be not unique due to batching - therefore a multimap
  // seqnum -> [repid, pubkey]
  std::multimap<SeqNum, std::pair<uint16_t, std::string>> ordered_public_keys;
  for (uint32_t i = 0; i < clusterSize_; i++) {
    if (!publicKeys_.keyExists(i)) continue;
    for (auto [sn, pk] : publicKeys_.keys(i).keys) ordered_public_keys.insert({sn, std::make_pair(i, pk)});
  }

  for (auto [sn, pk_info] : ordered_public_keys)
    for (auto ke : registryToExchange_) ke->onPublicKeyExchange(pk_info.second, pk_info.first, sn);

  for (auto ke : registryToExchange_)
    for (auto [sn, pk] : private_keys_.key_data().keys)
      ke->onPrivateKeyExchange(pk, publicKeys_.getKey(repID_, sn), sn);
}

void KeyExchangeManager::loadPublicKeys() {
  // after State Transfer public keys for all replicas are expected to exist
  auto num_loaded = publicKeys_.loadAllReplicasKeyStoresFromReservedPages();
  uint32_t liveQuorumSize = ReplicaConfig::instance().waitForFullCommOnStartup ? clusterSize_ : quorumSize_;
  if (ReplicaConfig::instance().getkeyExchangeOnStart() && isInitialConsensusExchangeComplete()) {
    ConcordAssertGE(num_loaded, liveQuorumSize);
  }
  LOG_INFO(KEY_EX_LOG, "rebuilding crypto system after state transfer" << KVLOG(num_loaded));
  notifyRegistry();
}

void KeyExchangeManager::loadClientPublicKeys() {
  // after State Transfer public keys for all clients are expected to exist
  clientsPublicKeys_.checkAndSetState();
}

void KeyExchangeManager::exchangeTlsKeys(const std::string& type, const SeqNum& bft_sn) {
  auto keys = concord::crypto::generateEdDSAKeyPair(concord::crypto::KeyFormat::PemFormat);
  bool use_unified_certs = bftEngine::ReplicaConfig::instance().useUnifiedCertificates;
  const std::string base_path =
      bftEngine::ReplicaConfig::instance().certificatesRootPath + "/" + std::to_string(repID_);
  std::string root_path = (use_unified_certs) ? base_path : base_path + "/" + type;
  std::string cert_path = (use_unified_certs) ? root_path + "/node.cert" : root_path + "/" + type + ".cert";

  std::string prev_key_pem;
  if (ReplicaConfig::instance().replicaMsgSigningAlgo == SignatureAlgorithm::EdDSA) {
    prev_key_pem = EdDSAHexToPem(std::make_pair(SigManager::instance()->getSelfPrivKey(), "")).first;
  }

  auto cert =
      generateSelfSignedCert(cert_path, keys.second, prev_key_pem, ReplicaConfig::instance().replicaMsgSigningAlgo);
  // Now that we have generated new key pair and certificate, lets do the actual exchange on this replica
  std::string pk_path = root_path + "/pk.pem";
  std::fstream nec_f(pk_path);
  std::fstream ec_f(pk_path + ".enc");

  // 1. exchange private key
  if (nec_f.good()) {
    secretsMgr_->encryptFile(pk_path, keys.first);
    nec_f.close();
  }
  if (ec_f.good()) {
    secretsMgr_->encryptFile(pk_path + ".enc", keys.first);
    ec_f.close();
  }

  concord::secretsmanager::SecretsManagerPlain psm;
  // 2. exchange the certificate itself
  psm.encryptFile(cert_path, cert);

  if (type == "client") return;
  auto repId = bftEngine::ReplicaConfig::instance().replicaId;
  concord::messages::ReconfigurationRequest req;
  req.sender = repId;
  req.command = concord::messages::ReplicaTlsExchangeKey{repId, cert};
  // Mark this request as an internal one
  std::vector<uint8_t> data_vec;
  concord::messages::serialize(data_vec, req);
  std::string sig(SigManager::instance()->getMySigLength(), '\0');
  SigManager::instance()->sign(SigManager::instance()->getReplicaLastExecutedSeq(),
                               reinterpret_cast<char*>(data_vec.data()),
                               data_vec.size(),
                               sig.data());
  req.signature = std::vector<uint8_t>(sig.begin(), sig.end());
  data_vec.clear();
  concord::messages::serialize(data_vec, req);
  std::string strMsg(data_vec.begin(), data_vec.end());
  std::string cid = "replicaTlsKeyExchange_" + std::to_string(bft_sn) + "_" + std::to_string(repId);
  client_->sendRequest(RECONFIG_FLAG, strMsg.size(), strMsg.c_str(), cid);
}

void KeyExchangeManager::exchangeTlsKeys(const SeqNum& bft_sn) {
  exchangeTlsKeys("server", bft_sn);
  exchangeTlsKeys("client", bft_sn);
  metrics_->tls_key_exchange_requests_++;
  LOG_INFO(KEY_EX_LOG, "Replica has generated a new tls keys");
  bft::communication::StateControl::instance().restartComm(repID_);
  LOG_INFO(KEY_EX_LOG, "Replica communication restarted after tls exchange");
}
void KeyExchangeManager::sendMainPublicKey() {
  constexpr const uint64_t defaultGenerationSeq = 0;
  uint64_t generationSeq = defaultGenerationSeq;
  auto [latestPrivateKey, latestPublicKey] = SigManager::instance()->getMyLatestKeyPair();

  if (ReplicaConfig::instance().singleSignatureScheme && exchangedSelfConsensusKeys()) {
    generationSeq =
        private_keys_.key_data().getGenerationSequenceByPrivateKey(latestPrivateKey).value_or(defaultGenerationSeq);
  }

  concord::messages::ReconfigurationRequest req;
  req.sender = repID_;
  req.command =
      concord::messages::ReplicaMainKeyUpdate{repID_,
                                              latestPublicKey,
                                              "hex",
                                              static_cast<uint8_t>(SigManager::instance()->getMainKeyAlgorithm()),
                                              generationSeq};
  // Mark this request as an internal one
  std::vector<uint8_t> data_vec;
  concord::messages::serialize(data_vec, req);
  req.signature.resize(SigManager::instance()->getMySigLength());
  SigManager::instance()->sign(
      SigManager::instance()->getReplicaLastExecutedSeq(), data_vec.data(), data_vec.size(), req.signature.data());
  data_vec.clear();
  concord::messages::serialize(data_vec, req);
  std::string cid = "ReplicaMainKeyUpdate_" + std::to_string(repID_);
  client_->sendRequest(RECONFIG_FLAG, data_vec.size(), reinterpret_cast<char*>(data_vec.data()), cid);
  LOG_INFO(KEY_EX_LOG, cid + " sent to reconfiguration engine" << KVLOG(generationSeq, latestPublicKey));
}

void KeyExchangeManager::generateConsensusKeyAndSendInternalClientMsg(const SeqNum& sn) {
  if (private_keys_.lastGeneratedSeqnum() &&  // if not initial
      (sn - private_keys_.lastGeneratedSeqnum()) / checkpointWindowSize < 2) {
    LOG_INFO(KEY_EX_LOG,
             "ignore request - already exchanged consensus keys for seqnum: " << private_keys_.lastGeneratedSeqnum());
    return;
  }

  bool generateNewPair = seq_candidate_map_.find(sn) == seq_candidate_map_.end();

  if (generateNewPair) {
    seq_candidate_map_[sn] = {};
    auto& candidate = seq_candidate_map_[sn];
    auto cid = generateCid(kInitialKeyExchangeCid);
    auto [prv, pub, algorithm] = multiSigKeyHdlr_->generateMultisigKeyPair();
    candidate.algorithm = algorithm;
    candidate.priv = prv;
    candidate.pub = pub;
    candidate.cid = cid;
    candidate.sn = sn;
    LOG_INFO(KEY_EX_LOG, "Added new candidate" << KVLOG(candidate.cid, sn));
  } else {
    LOG_INFO(KEY_EX_LOG,
             "we already have a candidate for this sequence number, trying to send it again"
                 << KVLOG(seq_candidate_map_.at(sn).cid, sn));
  }

  auto& candidate = seq_candidate_map_.at(sn);
  KeyExchangeMsg msg;
  msg.repID = repID_;
  msg.generated_sn = candidate.sn;
  msg.epoch = EpochManager::instance().getSelfEpochNumber();
  msg.pubkey = candidate.pub;
  msg.algorithm = candidate.algorithm;
  LOG_INFO(KEY_EX_LOG,
           "Sending consensus key exchange message:" << KVLOG(
               candidate.sn, candidate.cid, msg.pubkey, msg.algorithm, generateNewPair));
  client_->sendRequest(bftEngine::KEY_EXCHANGE_FLAG, msg, candidate.cid);
  metrics_->sent_key_exchange_counter++;
}

// sends the clients public keys via the internal client, if keys weren't published or outdated.
void KeyExchangeManager::sendInitialClientsKeys(const std::string& keys) {
  if (clientsPublicKeys_.published()) {
    LOG_INFO(KEY_EX_LOG, "Clients public keys were already published");
    return;
  }
  auto ret = std::async(std::launch::async, [this, keys]() {
    while (!initial_exchange_) {
      std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    auto cid = generateCid(kInitialClientsKeysCid);
    LOG_INFO(KEY_EX_LOG, "Sends clients keys, cid: " << cid << " payload size " << keys.size());
    client_->sendRequest(
        bftEngine::CLIENTS_PUB_KEYS_FLAG | bftEngine::PUBLISH_ON_CHAIN_OBJECT_FLAG, keys.size(), keys.c_str(), cid);
  });
}

void KeyExchangeManager::onPublishClientsKeys(const std::string& keys, std::optional<std::string> bootstrap_keys) {
  LOG_INFO(KEY_EX_LOG, "");
  auto save = true;
  if (bootstrap_keys) {
    if (keys != *bootstrap_keys) {
      LOG_FATAL(KEY_EX_LOG, "Initial Published Clients keys and replica client keys do not match");
      ConcordAssertEQ(keys, *bootstrap_keys);
    }
    if (clientKeysPublished()) save = false;
  }
  if (save) saveClientsPublicKeys(keys);
}

void KeyExchangeManager::onClientPublicKeyExchange(const std::string& key, KeyFormat fmt, NodeIdType clientId) {
  LOG_INFO(KEY_EX_LOG, "key: " << key << " fmt: " << (uint16_t)fmt << " client: " << clientId);
  // persist a new key
  clientPublicKeyStore_->setClientPublicKey(clientId, key, fmt);
  // load a new key
  loadClientPublicKey(key, fmt, clientId, true);
}

void KeyExchangeManager::loadClientPublicKey(const std::string& key,
                                             KeyFormat fmt,
                                             NodeIdType clientId,
                                             bool saveToReservedPages) {
  LOG_INFO(KEY_EX_LOG, "key: " << key << " fmt: " << (uint16_t)fmt << " client: " << clientId);
  SigManager::instance()->setClientPublicKey(key, clientId, fmt);
  if (saveToReservedPages) saveClientsPublicKeys(SigManager::instance()->getClientsPublicKeys());
}

void KeyExchangeManager::waitForQuorum(const ReplicaImp* repImpInstance) {
  bool partialQuorum = !ReplicaConfig::instance().waitForFullCommOnStartup;
  LOG_INFO(KEY_EX_LOG, "Waiting for quorum" << KVLOG(partialQuorum));
  if (partialQuorum) {
    waitForLiveQuorum(repImpInstance);
  } else {
    waitForFullCommunication();
  }
}

void KeyExchangeManager::waitForQuorumAndTriggerConsensusExchange(const ReplicaImp* repImpInstance, const SeqNum s) {
  std::unique_lock<std::mutex> lock(startup_mutex_);
  SCOPED_MDC(MDC_REPLICA_ID_KEY, std::to_string(ReplicaConfig::instance().replicaId));
  waitForQuorum(repImpInstance);

  generateConsensusKeyAndSendInternalClientMsg(s);
  metrics_->sent_key_exchange_on_start_status.Get().Set("True");
}

void KeyExchangeManager::waitForLiveQuorum(const ReplicaImp* repImpInstance) {
  // If transport is UDP, we can't know the connection status, and we are in Apollo context therefore giving 2sec grace.
  if (client_->isUdp()) {
    LOG_INFO(KEY_EX_LOG, "UDP communication");
    std::this_thread::sleep_for(std::chrono::seconds(2));
    return;
  }
  /*
   * Basically, we can start once we have n-f live replicas. However, to avoid unnecessary view change,
   * we wait for the first primary for a pre-defined amount of time.
   */
  auto primary = repImpInstance->currentPrimary();
  if (repID_ != primary) {
    auto start = getMonotonicTime();
    auto timeoutForPrim = bftEngine::ReplicaConfig::instance().timeoutForPrimaryOnStartupSeconds;
    LOG_INFO(KEY_EX_LOG, "waiting for at most " << timeoutForPrim << " for primary (" << primary << ") to be ready");
    while (std::chrono::duration<double, std::milli>(getMonotonicTime() - start).count() / 1000 < timeoutForPrim) {
      if (client_->isReplicaConnected(primary)) {
        LOG_INFO(KEY_EX_LOG, "primary (" << primary << ") is connected");
        break;
      }
      std::this_thread::sleep_for(std::chrono::seconds(1));
      primary = repImpInstance->currentPrimary();
    }
    if (!client_->isReplicaConnected(primary)) {
      LOG_INFO(KEY_EX_LOG,
               "replica was not able to connect to primary ("
                   << primary << ")  after " << timeoutForPrim
                   << " seconds. This will wait for live quorum(this may cause a view change)");
    }
  }
  auto avlble = client_->numOfConnectedReplicas(clusterSize_);
  LOG_INFO(KEY_EX_LOG, "Consensus engine: " << avlble << " replicas are connected");
  uint32_t liveQuorum = 2 * ReplicaConfig::instance().fVal + ReplicaConfig::instance().cVal + 1;
  // Num of connections should be: (liveQuorum - 1) excluding the current replica
  while (avlble < liveQuorum - 1) {
    LOG_INFO(KEY_EX_LOG,
             "Consensus engine not available, " << avlble << " replicas are connected, we need at least " << liveQuorum
                                                << " to start");
    std::this_thread::sleep_for(std::chrono::seconds(1));
    avlble = client_->numOfConnectedReplicas(clusterSize_);
  }
  LOG_INFO(KEY_EX_LOG, "Consensus engine available, " << avlble << " replicas are connected");
}

void KeyExchangeManager::waitForFullCommunication() {
  auto avlble = client_->numOfConnectedReplicas(clusterSize_);
  LOG_INFO(KEY_EX_LOG, "Consensus engine: " << avlble << " replicas are connected");
  // Num of connections should be: (clusterSize - 1)
  while (avlble < clusterSize_ - 1) {
    LOG_INFO(KEY_EX_LOG, "Consensus engine not available, " << avlble << " replicas are connected");
    std::this_thread::sleep_for(std::chrono::seconds(1));
    avlble = client_->numOfConnectedReplicas(clusterSize_);
  }
  // If transport is UDP, we can't know the connection status, and we are in Apollo context therefore giving 2sec grace.
  if (client_->isUdp()) {
    LOG_INFO(KEY_EX_LOG, "UDP communication");
    std::this_thread::sleep_for(std::chrono::seconds(2));
  }
  LOG_INFO(KEY_EX_LOG, "Consensus engine available, " << avlble << " replicas are connected");
}

std::string KeyExchangeManager::getStatus() const {
  using concordUtils::toPair;
  std::ostringstream oss;
  std::unordered_map<std::string, std::string> result;
  result.insert(toPair("exchanged", isInitialConsensusExchangeComplete()));
  result.insert(toPair("sent_key_exchange_on_start", metrics_->sent_key_exchange_on_start_status.Get().Get()));
  result.insert(toPair("sent_key_exchange", metrics_->sent_key_exchange_counter.Get().Get()));
  result.insert(toPair("self_key_exchange", metrics_->self_key_exchange_counter.Get().Get()));
  result.insert(toPair("public_key_exchange_for_peer", metrics_->public_key_exchange_for_peer_counter.Get().Get()));

  oss << concordUtils::kContainerToJson(result);
  return oss.str();
}

bool KeyExchangeManager::PrivateKeys::load() {
  LOG_INFO(KEY_EX_LOG, "Load private keys");
  auto secrets = secretsMgr_->decryptFile(secrets_file_);
  if (!secrets.has_value()) {
    LOG_WARN(KEY_EX_LOG, "Got empty string from loader. This is expected on first startup with an empty database");
    return false;
  }
  std::stringstream ss;
  std::string str = secrets.value();
  ss.write(str.c_str(), std::streamsize(str.size()));
  concord::serialize::Serializable::deserialize(ss, data_);
  if (data_.generated.sn)
    LOG_INFO(
        KEY_EX_LOG,
        "loaded generated private key for: " << KVLOG(data_.generated.sn, data_.generated.cid, data_.generated.pub));
  for (const auto& it : data_.keys) LOG_INFO(KEY_EX_LOG, "loaded private key for sn: " << it.first << KVLOG(it.second));
  return true;
}

bool KeyExchangeManager::exchangedSelfConsensusKeys() const {
  return publicKeys_.keyExists(ReplicaConfig::instance().replicaId);
}

bool KeyExchangeManager::isInitialConsensusExchangeComplete() const {
  uint32_t liveClusterSize = ReplicaConfig::instance().waitForFullCommOnStartup ? clusterSize_ : quorumSize_;
  bool exchange_self_keys = exchangedSelfConsensusKeys();
  LOG_DEBUG(KEY_EX_LOG,
            KVLOG(ReplicaConfig::instance().waitForFullCommOnStartup,
                  clusterSize_,
                  quorumSize_,
                  exchange_self_keys,
                  ReplicaConfig::instance().getkeyExchangeOnStart(),
                  publicKeys_.numOfExchangedReplicas()));
  return ReplicaConfig::instance().getkeyExchangeOnStart()
             ? (publicKeys_.numOfExchangedReplicas() + 1 >= liveClusterSize) && exchange_self_keys
             : true;
}

std::map<SeqNum, std::pair<std::string, std::string>> KeyExchangeManager::getCandidates() const {
  std::map<SeqNum, std::pair<std::string, std::string>> result;
  for (auto& [seq, generatedPairInfo] : seq_candidate_map_) {
    result[seq] = {generatedPairInfo.priv, generatedPairInfo.pub};
  }
  return result;
}

void KeyExchangeManager::persistCandidates(const std::set<SeqNum>& candidatesToPersist) {
  for (auto keyGenerationSn : candidatesToPersist) {
    auto& keyPairInfo = seq_candidate_map_[keyGenerationSn];
    LOG_INFO(KEY_EX_LOG, "Persisting candidate's private key" << KVLOG(keyGenerationSn, keyPairInfo.pub));
    registerNewKeyPair(repID_, keyPairInfo.sn, keyPairInfo.pub, keyPairInfo.cid);
  }
}

}  // namespace bftEngine::impl
