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
#include "kvstream.h"
#include "json_output.hpp"
#include "SigManager.hpp"
#include "secrets_manager_plain.h"
#include "concord.cmf.hpp"
#include "bftengine/EpochManager.hpp"
#include "concord.cmf.hpp"
#include "communication/StateControl.hpp"

namespace bftEngine::impl {

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

std::string KeyExchangeManager::onKeyExchange(const KeyExchangeMsg& kemsg,
                                              const SeqNum& req_sn,
                                              const std::string& cid) {
  const auto& sn = kemsg.generated_sn;
  SCOPED_MDC_SEQ_NUM(std::to_string(sn));
  LOG_INFO(KEY_EX_LOG, kemsg.toString() << KVLOG(sn, cid, exchanged()));
  // client query
  if (kemsg.op == KeyExchangeMsg::HAS_KEYS) {
    LOG_INFO(KEY_EX_LOG, "Has keys: " << std::boolalpha << exchanged() << std::noboolalpha);
    if (!exchanged()) return std::string(KeyExchangeMsg::hasKeysFalseReply);
    return std::string(KeyExchangeMsg::hasKeysTrueReply);
  }
  uint64_t my_epoch = EpochManager::instance().getSelfEpochNumber();
  if (kemsg.epoch != my_epoch) {
    LOG_WARN(KEY_EX_LOG, "Got KeyExchangeMsg of a different epoch, ignore..." << KVLOG(kemsg.epoch, my_epoch));
    return "invalid epoch";
  }
  // reject key exchange message if generated seq_num is outside working window
  if (sn < ((static_cast<uint64_t>(req_sn) / kWorkWindowSize) * kWorkWindowSize)) {
    LOG_WARN(KEY_EX_LOG, "Generated sequence number is outside working window, ignore..." << KVLOG(sn, req_sn));
    return "gen_seq_num_ouside_workingwindow";
  }
  if (publicKeys_.keyExists(kemsg.repID, sn)) return "ok";
  publicKeys_.push(kemsg, sn);
  if (kemsg.repID == repID_) {  // initiated by me
    private_keys_.key_data().generated = candidate_private_keys_.generated;
    candidate_private_keys_.generated.clear();
    ConcordAssert(private_keys_.key_data().generated.pub == kemsg.pubkey);
    private_keys_.onKeyExchange(cid, sn);
    for (auto e : registryToExchange_) e->onPrivateKeyExchange(private_keys_.key_data().keys[sn], kemsg.pubkey, sn);
    metrics_->self_key_exchange_counter++;
  } else {  // initiated by others
    for (auto e : registryToExchange_) e->onPublicKeyExchange(kemsg.pubkey, kemsg.repID, sn);
    metrics_->public_key_exchange_for_peer_counter++;
  }
  auto liveQuorumSize = ReplicaConfig::instance().waitForFullCommOnStartup ? clusterSize_ : quorumSize_;
  if (ReplicaConfig::instance().getkeyExchangeOnStart() && (publicKeys_.numOfExchangedReplicas() <= liveQuorumSize))
    LOG_INFO(KEY_EX_LOG,
             "Exchanged [" << publicKeys_.numOfExchangedReplicas() << "] out of [" << liveQuorumSize << "]");
  if (!initial_exchange_ && exchanged()) {
    initial_exchange_ = true;
    if (ReplicaConfig::instance().getkeyExchangeOnStart() &&
        ReplicaConfig::instance().publishReplicasMasterKeyOnStartup)
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
  if (ReplicaConfig::instance().getkeyExchangeOnStart()) {
    ConcordAssertGE(num_loaded, liveQuorumSize);
  }
  LOG_INFO(KEY_EX_LOG, "building crypto system after state transfer");
  notifyRegistry();
}

void KeyExchangeManager::loadClientPublicKeys() {
  // after State Transfer public keys for all clients are expected to exist
  clientsPublicKeys_.checkAndSetState();
}

void KeyExchangeManager::exchangeTlsKeys(const std::string& type, const SeqNum& bft_sn) {
  auto keys = concord::util::crypto::Crypto::instance().generateECDSAKeyPair(
      concord::util::crypto::KeyFormat::PemFormat, concord::util::crypto::CurveType::secp384r1);
  bool use_unified_certs = bftEngine::ReplicaConfig::instance().useUnifiedCertificates;
  const std::string base_path =
      bftEngine::ReplicaConfig::instance().certificatesRootPath + "/" + std::to_string(repID_);
  std::string root_path = (use_unified_certs) ? base_path : base_path + "/" + type;

  std::string cert_path = (use_unified_certs) ? root_path + "/node.cert" : root_path + "/" + type + ".cert";
  std::string prev_key_pem = concord::util::crypto::Crypto::instance()
                                 .RsaHexToPem(std::make_pair(SigManager::instance()->getSelfPrivKey(), ""))
                                 .first;
  auto cert = concord::util::crypto::CertificateUtils::generateSelfSignedCert(cert_path, keys.second, prev_key_pem);
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
  uint16_t sig_length{0};
  SigManager::instance()->sign(reinterpret_cast<char*>(data_vec.data()), data_vec.size(), sig.data(), sig_length);
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
  bft::communication::StateControl::instance().restartComm(repID_);
  LOG_INFO(KEY_EX_LOG, "Replica has generated a new tls keys");
}
void KeyExchangeManager::sendMainPublicKey() {
  concord::messages::ReconfigurationRequest req;
  req.sender = repID_;
  req.command =
      concord::messages::ReplicaMainKeyUpdate{repID_, SigManager::instance()->getPublicKeyOfVerifier(repID_), "hex"};
  // Mark this request as an internal one
  std::vector<uint8_t> data_vec;
  concord::messages::serialize(data_vec, req);
  std::string sig(SigManager::instance()->getMySigLength(), '\0');
  uint16_t sig_length{0};
  SigManager::instance()->sign(reinterpret_cast<char*>(data_vec.data()), data_vec.size(), sig.data(), sig_length);
  req.signature = std::vector<uint8_t>(sig.begin(), sig.end());
  data_vec.clear();
  concord::messages::serialize(data_vec, req);
  std::string strMsg(data_vec.begin(), data_vec.end());
  std::string cid = "ReplicaMainKeyUpdate_" + std::to_string(repID_);
  client_->sendRequest(RECONFIG_FLAG, strMsg.size(), strMsg.c_str(), cid);
  LOG_INFO(KEY_EX_LOG, "Replica has published its main public key to the consensus");
}

void KeyExchangeManager::sendKeyExchange(const SeqNum& sn) {
  if (private_keys_.lastGeneratedSeqnum() &&  // if not initial
      (sn - private_keys_.lastGeneratedSeqnum()) / checkpointWindowSize < 2) {
    LOG_INFO(KEY_EX_LOG, "ignore request - already exchanged keys for seqnum: " << private_keys_.lastGeneratedSeqnum());
    return;
  }
  KeyExchangeMsg msg;
  if (sn == candidate_private_keys_.generated.sn && !candidate_private_keys_.generated.cid.empty()) {
    LOG_INFO(KEY_EX_LOG, "we already have a candidate for this sequence number, trying to send it again");
    msg.pubkey = candidate_private_keys_.generated.pub;
    msg.repID = repID_;
    msg.generated_sn = sn;
    msg.epoch = EpochManager::instance().getSelfEpochNumber();
    std::stringstream ss;
    concord::serialize::Serializable::serialize(ss, msg);
    auto strMsg = ss.str();
    client_->sendRequest(
        bftEngine::KEY_EXCHANGE_FLAG, strMsg.size(), strMsg.c_str(), candidate_private_keys_.generated.cid);
    metrics_->sent_key_exchange_counter++;
    return;
  }

  auto cid = generateCid(kInitialKeyExchangeCid);
  auto [prv, pub] = multiSigKeyHdlr_->generateMultisigKeyPair();
  candidate_private_keys_.generated.priv = prv;
  candidate_private_keys_.generated.pub = pub;
  candidate_private_keys_.generated.cid = cid;
  candidate_private_keys_.generated.sn = sn;

  LOG_INFO(KEY_EX_LOG, "Sending key exchange :" << KVLOG(cid, pub));
  msg.pubkey = pub;
  msg.repID = repID_;
  msg.generated_sn = sn;
  msg.epoch = EpochManager::instance().getSelfEpochNumber();
  std::stringstream ss;
  concord::serialize::Serializable::serialize(ss, msg);
  auto strMsg = ss.str();
  client_->sendRequest(bftEngine::KEY_EXCHANGE_FLAG, strMsg.size(), strMsg.c_str(), cid);
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

void KeyExchangeManager::onClientPublicKeyExchange(const std::string& key,
                                                   concord::util::crypto::KeyFormat fmt,
                                                   NodeIdType clientId) {
  LOG_INFO(KEY_EX_LOG, "key: " << key << " fmt: " << (uint16_t)fmt << " client: " << clientId);
  // persist a new key
  clientPublicKeyStore_->setClientPublicKey(clientId, key, fmt);
  // load a new key
  loadClientPublicKey(key, fmt, clientId, true);
}

void KeyExchangeManager::loadClientPublicKey(const std::string& key,
                                             concord::util::crypto::KeyFormat fmt,
                                             NodeIdType clientId,
                                             bool saveToReservedPages) {
  LOG_INFO(KEY_EX_LOG, "key: " << key << " fmt: " << (uint16_t)fmt << " client: " << clientId);
  SigManager::instance()->setClientPublicKey(key, clientId, fmt);
  if (saveToReservedPages) saveClientsPublicKeys(SigManager::instance()->getClientsPublicKeys());
}

void KeyExchangeManager::sendInitialKey(uint32_t prim, const SeqNum& s) {
  std::unique_lock<std::mutex> lock(startup_mutex_);
  SCOPED_MDC(MDC_REPLICA_ID_KEY, std::to_string(ReplicaConfig::instance().replicaId));
  if (!ReplicaConfig::instance().waitForFullCommOnStartup) {
    waitForLiveQuorum(prim);
  } else {
    waitForFullCommunication();
  }
  sendKeyExchange(s);
  metrics_->sent_key_exchange_on_start_status.Get().Set("True");
}

void KeyExchangeManager::waitForLiveQuorum(uint32_t prim) {
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
  if (repID_ != prim) {
    auto start = getMonotonicTime();
    auto timeoutForPrim = bftEngine::ReplicaConfig::instance().timeoutForPrimaryOnStartupSeconds;
    LOG_INFO(KEY_EX_LOG, "waiting for at most " << timeoutForPrim << " for primary (" << prim << ") to be ready");
    while (std::chrono::duration<double, std::milli>(getMonotonicTime() - start).count() / 1000 < timeoutForPrim) {
      if (client_->isReplicaConnected(prim)) {
        LOG_INFO(KEY_EX_LOG, "primary (" << prim << ") is connected");
        break;
      }
      std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    if (!client_->isReplicaConnected(prim)) {
      LOG_INFO(KEY_EX_LOG,
               "replica was not able to connect to primary ("
                   << prim << ")  after " << timeoutForPrim
                   << " seconds. The will wait for live quorum and starts (this may cause to a view change");
    }
  }
  auto avlble = client_->numOfConnectedReplicas(clusterSize_);
  LOG_INFO(KEY_EX_LOG, "Consensus engine: " << avlble << " replicas are connected");
  uint32_t liveQuorum = 2 * ReplicaConfig::instance().fVal + ReplicaConfig::instance().cVal + 1;
  // Num of connections should be: (liveQuorum - 1) excluding the current replica
  while (avlble < liveQuorum - 1) {
    LOG_INFO(KEY_EX_LOG,
             "Consensus engine not available, " << avlble << " replicas are connected, we need at lease " << liveQuorum
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
  result.insert(toPair("exchanged", exchanged()));
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
  for (const auto& it : data_.keys) LOG_INFO(KEY_EX_LOG, "loaded private key for sn: " << it.first);
  return true;
}

}  // namespace bftEngine::impl
