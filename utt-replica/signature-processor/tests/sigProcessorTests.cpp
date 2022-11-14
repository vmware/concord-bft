#include "sigProcessor.hpp"
#include "communication/ICommunication.hpp"
#include "MsgHandlersRegistrator.hpp"
#include "IncomingMsgsStorageImp.hpp"
#include "MsgsCommunicator.hpp"
#include "bftengine/messages/ClientRequestMsg.hpp"

#include "UTTParams.hpp"
#include "coinsSigner.hpp"
#include "client.hpp"
#include "registrator.hpp"
#include "common.hpp"
#include "coin.hpp"
#include "mint.hpp"
#include "burn.hpp"
#include "transaction.hpp"
#include "budget.hpp"
#include "Logger.hpp"
#undef UNUSED
#include "../../../utt/libutt/src/api/include/params.impl.hpp"
#include <utt/RegAuth.h>
#include <utt/RandSigDKG.h>
#include <utt/Serialization.h>
#include <utt/IBE.h>
#include <utt/Serialization.h>
#include <utt/DataUtils.hpp>
#include <chrono>
#include <map>
#include <queue>
#include <memory>
#include <mutex>
#include <condition_variable>
#include <thread>

#include "gtest/gtest.h"
namespace {
using namespace bft::communication;
using namespace libutt;
using namespace libutt::api;
using namespace libutt::api::operations;

logging::Logger logger = logging::getLogger("sigProcessorTests");

class TestComm : public ICommunication {
  std::vector<NodeNum> isolated;
  std::mutex isolated_lock_;

 public:
  void isolateNode(NodeNum node) {
    std::unique_lock lk(isolated_lock_);
    isolated.push_back(node);
  }
  void deisolatedNode(NodeNum node) {
    std::unique_lock lk(isolated_lock_);
    for (auto iter = isolated.begin(); iter != isolated.end(); iter++) {
      if (*iter == node) {
        isolated.erase(iter);
        return;
      }
    }
  }
  ~TestComm() {}
  int getMaxMessageSize() override { return 10 * 1024 * 1024; /* 10 MB */ }
  int start() override { return 0; }
  int stop() override { return 0; }

  bool isRunning() const override { return true; }

  ConnectionStatus getCurrentConnectionStatus(NodeNum node) override { return ConnectionStatus::Connected; }
  int send(NodeNum destNode, std::vector<uint8_t>&& msg, NodeNum endpointNum = MAX_ENDPOINT_NUM) override {
    {
      std::unique_lock lk(isolated_lock_);
      if (std::find(isolated.begin(), isolated.end(), destNode) != isolated.end()) return 0;
    }
    std::unique_lock lk(locks_[destNode]);
    receivers_[destNode]->onNewMessage(0, (const char*)msg.data(), msg.size());
    return 0;
  }

  std::set<NodeNum> send(std::set<NodeNum> dests,
                         std::vector<uint8_t>&& msg,
                         NodeNum srcEndpointNum = MAX_ENDPOINT_NUM) override {
    for (uint64_t node : dests) {
      auto msg_ = msg;
      send((uint16_t)node, std::move(msg_));
    }
    return dests;
  }

  void setReceiver(NodeNum receiverNum, IReceiver* receiver) override { receivers_[receiverNum] = receiver; }

  void restartCommunication(NodeNum i) override {}

 private:
  std::map<NodeNum, IReceiver*> receivers_;
  std::map<NodeNum, std::mutex> locks_;
};

class TestReceiver : public bft::communication::IReceiver {
 public:
  TestReceiver(std::shared_ptr<IncomingMsgsStorage> ims) : ims_{ims} {}
  void onNewMessage(NodeNum sourceNode,
                    const char* const message,
                    size_t messageLength,
                    NodeNum endpointNum = MAX_ENDPOINT_NUM) override {
    if (!ims_->isRunning()) return;
    auto* msgBody = (MessageBase::Header*)std::malloc(messageLength);
    memcpy(msgBody, message, messageLength);

    auto node = sourceNode;

    std::unique_ptr<MessageBase> pMsg(new MessageBase(node, msgBody, messageLength, true));

    ims_->pushExternalMsg(std::move(pMsg));
  }
  void onConnectionStatusChanged(NodeNum, ConnectionStatus) override {}
  ~TestReceiver() {}

 private:
  std::shared_ptr<IncomingMsgsStorage> ims_;
};

struct GpData {
  libutt::CommKey cck;
  libutt::CommKey rck;
};

std::tuple<libutt::api::UTTParams, RandSigDKG, RegAuthSK> init(size_t n, size_t thresh) {
  UTTParams::BaseLibsInitData base_libs_init_data;
  UTTParams::initLibs(base_libs_init_data);
  auto dkg = RandSigDKG(thresh, n, Params::NumMessages);
  auto rc = RegAuthSK::generateKeyAndShares(thresh, n);
  GpData gp_data{dkg.getCK(), rc.ck_reg};
  UTTParams d = UTTParams::create((void*)(&gp_data));
  rc.setIBEParams(d.getImpl()->p.ibe);
  return {d, dkg, rc};
}
std::vector<std::shared_ptr<Registrator>> GenerateRegistrators(size_t n, const RegAuthSK& rsk) {
  std::vector<std::shared_ptr<Registrator>> registrators;
  std::map<uint16_t, std::string> validation_keys;
  for (size_t i = 0; i < n; i++) {
    auto& sk = rsk.shares[i];
    validation_keys[(uint16_t)i] = serialize<RegAuthSharePK>(sk.toPK());
  }
  for (size_t i = 0; i < n; i++) {
    registrators.push_back(std::make_shared<Registrator>(
        (uint16_t)i, serialize<RegAuthShareSK>(rsk.shares[i]), validation_keys, serialize<RegAuthPK>(rsk.toPK())));
  }
  return registrators;
}

std::vector<std::shared_ptr<CoinsSigner>> GenerateCommitters(size_t n, const RandSigDKG& dkg, const RegAuthPK& rvk) {
  std::vector<std::shared_ptr<CoinsSigner>> banks;
  std::map<uint16_t, std::string> share_verification_keys_;
  for (size_t i = 0; i < n; i++) {
    share_verification_keys_[(uint16_t)i] = serialize<RandSigSharePK>(dkg.skShares[i].toPK());
  }
  for (size_t i = 0; i < n; i++) {
    banks.push_back(std::make_shared<CoinsSigner>((uint16_t)i,
                                                  serialize<RandSigShareSK>(dkg.skShares[i]),
                                                  serialize<RandSigPK>(dkg.getPK()),
                                                  share_verification_keys_,
                                                  serialize<RegAuthPK>(rvk)));
  }
  return banks;
}

std::vector<Client> GenerateClients(size_t c, const RandSigPK& bvk, const RegAuthPK& rvk, const RegAuthSK& rsk) {
  std::vector<Client> clients;
  std::string bpk = libutt::serialize<libutt::RandSigPK>(bvk);
  std::string rpk = libutt::serialize<libutt::RegAuthPK>(rvk);

  for (size_t i = 0; i < c; i++) {
    std::string pid = "client_" + std::to_string(i);
    auto sk = rsk.msk.deriveEncSK(rsk.p, pid);
    auto mpk = rsk.toPK().mpk;
    std::string csk = libutt::serialize<libutt::IBE::EncSK>(sk);
    std::string cmpk = libutt::serialize<libutt::IBE::MPK>(mpk);
    clients.push_back(Client(pid, bpk, rpk, csk, cmpk));
  }
  return clients;
}

void registerClients(uint64_t& jid,
                     uint16_t n,
                     const std::function<libutt::api::types::Signature(uint64_t, uint16_t)>& get_full_sig_cb,
                     UTTParams& d,
                     vector<Client>& clients,
                     std::vector<std::shared_ptr<Registrator>>& registrators,
                     std::map<uint16_t, std::shared_ptr<utt::SigProcessor>>& sig_processors) {
  for (size_t i = 0; i < clients.size(); i++) {
    auto& c = clients[i];
    Fr fr_s2 = Fr::random_element();
    types::CurvePoint s2 = fr_s2.to_words();
    auto rcm1 = c.generateInputRCM();
    for (size_t j = 0; j < n; j++) {
      auto r = registrators[j];
      auto res = registrators[j]->signRCM(c.getPidHash(), fr_s2.to_words(), rcm1);
      auto cpidHash = c.getPidHash();
      auto s2 = res.first;
      auto sig = res.second;
      auto vcb = [cpidHash, s2, rcm1, r](uint16_t id, const types::Signature& psig) {
        return r->validatePartialRCMSig(id, cpidHash, s2, rcm1, psig);
      };
      sig_processors[j]->processSignature(jid, sig, vcb);
    }
    auto sig = get_full_sig_cb(jid, 0);
    jid++;
    sig =
        Utils::unblindSignature(d, Commitment::Type::REGISTRATION, {Fr::zero().to_words(), Fr::zero().to_words()}, sig);
    c.setRCMSig(d, s2, sig);
    auto rcm_data = c.rerandomizeRcm(d);
    for (auto& r : registrators) {
      ASSERT_TRUE(r->validateRCM(rcm_data.first, rcm_data.second));
    }
  }
}

void createBudget(uint64_t& jid,
                  uint16_t n,
                  const std::function<libutt::api::types::Signature(uint64_t, uint16_t)>& get_full_sig_cb,
                  UTTParams& d,
                  vector<Client>& clients,
                  std::unordered_map<std::string, libutt::api::Coin>& bcoins,
                  std::vector<std::shared_ptr<CoinsSigner>>& banks,
                  std::map<uint16_t, std::shared_ptr<utt::SigProcessor>>& sig_processors) {
  for (auto& c : clients) {
    std::vector<types::Signature> rsigs;
    auto budget = Budget(d, c, 1000, 123456789);
    for (size_t j = 0; j < n; j++) {
      auto signer = banks[j];
      auto psig = signer->sign(budget).front();
      auto vcb = [budget, signer](uint16_t id, const types::Signature& sig) {
        return signer->validatePartialSignature(id, sig, 0, budget);
      };
      sig_processors[j]->processSignature(jid, psig, vcb);
    }
    auto sig = get_full_sig_cb(jid, 0);
    jid++;
    auto coin = c.claimCoins(budget, d, {sig}).front();
    ASSERT_TRUE(c.validate(coin));
    bcoins.emplace(c.getPid(), coin);
  }
}

void mintCoins(uint64_t& jid,
               uint16_t n,
               const std::function<libutt::api::types::Signature(uint64_t, uint16_t)>& get_full_sig_cb,
               UTTParams& d,
               vector<Client>& clients,
               std::unordered_map<std::string, std::vector<libutt::api::Coin>>& coins,
               std::vector<std::shared_ptr<CoinsSigner>>& banks,
               std::map<uint16_t, std::shared_ptr<utt::SigProcessor>>& sig_processors) {
  for (auto& c : clients) {
    std::string simulatonOfUniqueTxHash = std::to_string(((uint64_t)rand()) * ((uint64_t)rand()) * ((uint64_t)rand()));
    auto mint = Mint(simulatonOfUniqueTxHash, 100, c.getPid());
    for (size_t j = 0; j < n; j++) {
      auto signer = banks[j];
      auto psig = signer->sign(mint).front();
      auto vcb = [mint, signer](uint16_t id_, const types::Signature& sig) {
        return signer->validatePartialSignature(id_, sig, 0, mint);
      };
      sig_processors[j]->processSignature(jid, psig, vcb);
    }
    auto sig = get_full_sig_cb(jid, 0);
    jid++;
    auto coin = c.claimCoins(mint, d, {sig}).front();
    ASSERT_TRUE(c.validate(coin));
    coins[c.getPid()].emplace_back(std::move(coin));
  }
}
class test_utt_instance : public ::testing::Test {
 protected:
  void SetUp() override {
    logging::Logger MSGS = logging::getLogger("concord.bft.msgs");
    logging::Logger GL = logging::getLogger("concord.bft");
    MSGS.setLogLevel(60000);
    GL.setLogLevel(40000);
    auto [p, dkg, rc] = init(n, t);
    d = p;
    registrators = GenerateRegistrators(n, rc);
    banks = GenerateCommitters(n, dkg, rc.toPK());
    clients = GenerateClients(c, dkg.getPK(), rc.toPK(), rc);
    for (size_t i = 0; i < clients.size(); i++) {
      encryptors_[i] = std::make_shared<libutt::IBEEncryptor>(rc.toPK().mpk);
    }
    for (uint16_t i = 0; i < n; i++) {
      std::shared_ptr<MsgHandlersRegistrator> msr = std::make_shared<MsgHandlersRegistrator>();
      std::shared_ptr<IncomingMsgsStorage> ims =
          std::make_shared<IncomingMsgsStorageImp>(msr, std::chrono::milliseconds(1000), i);
      std::shared_ptr<IReceiver> receiver = std::make_shared<TestReceiver>(ims);
      std::shared_ptr<MsgsCommunicator> msc = std::make_shared<MsgsCommunicator>(&comm, ims, receiver);
      message_comms[i] = msc;
      sig_processors[i] = std::make_shared<utt::SigProcessor>(
          i, n, t, 1000, msc, msr, ((IncomingMsgsStorageImp*)(ims.get()))->timers());
      auto sp = sig_processors[i];
      msr->registerMsgHandler(MsgCode::ClientRequest, [&, sp](bftEngine::impl::MessageBase* message) {
        ClientRequestMsg* msg = (ClientRequestMsg*)message;
        uint64_t job_id{0};

        std::vector<uint8_t> cs_buffer(msg->requestLength() - sizeof(uint64_t));
        std::memcpy(&job_id, msg->requestBuf(), sizeof(uint64_t));
        std::memcpy(cs_buffer.data(), msg->requestBuf() + sizeof(uint64_t), cs_buffer.size());
        utt::SigProcessor::CompleteSignatureMsg cs_msg(cs_buffer);
        ASSERT_TRUE(cs_msg.validate());
        auto& fsig = cs_msg.getFullSig();
        {
          std::unique_lock lk(sigs_lock);
          for (size_t j = 0; j < n; j++) {
            if (full_signatures[j].find(job_id) != full_signatures[j].end()) {
              ASSERT_TRUE(fsig == full_signatures[j].at(job_id));
            } else {
              full_signatures[j][job_id] = fsig;
            }
          }
        }
        sp->onReceivingNewValidFullSig(job_id);
        cv.notify_all();
        delete msg;
      });
      msc->startCommunication(i);
      msc->startMsgsProcessing(i);
    }
  }

  void TearDown() override {
    for (uint16_t i = 0; i < n; i++) {
      message_comms[i]->stopMsgsProcessing();
      message_comms[i]->stopCommunication();
    }
  }
  bool wait_cond(uint64_t sigId, uint16_t source) {
    if (full_signatures.find(source) == full_signatures.end()) return false;
    return full_signatures.at(source).find(sigId) != full_signatures.at(source).end();
  }
  libutt::api::types::Signature waitAndGetFullSig(uint64_t sigId, uint16_t source) {
    std::unique_lock lk{sigs_lock};
    cv.wait(lk, [&] { return wait_cond(sigId, source); });
    return full_signatures.at(source).at(sigId);
  }
  std::vector<Client> clients;
  std::vector<std::shared_ptr<CoinsSigner>> banks;
  std::vector<std::shared_ptr<Registrator>> registrators;
  UTTParams d;
  std::map<uint16_t, std::shared_ptr<utt::SigProcessor>> sig_processors;
  std::map<uint16_t, std::shared_ptr<MsgsCommunicator>> message_comms;
  std::unordered_map<size_t, std::shared_ptr<libutt::IBEEncryptor>> encryptors_;
  TestComm comm;
  uint16_t n = 4;
  uint16_t t = 3;
  size_t c = 10;
  std::map<uint16_t, std::map<uint64_t, std::vector<uint8_t>>> full_signatures;
  std::mutex sigs_lock;
  std::condition_variable cv;
  uint64_t job_id{0};
};

class utt_system_include_clients : public test_utt_instance {
 protected:
  void SetUp() override {
    test_utt_instance::SetUp();
    registerClients(
        job_id,
        n,
        [&](uint64_t sigId, uint16_t source) { return waitAndGetFullSig(sigId, source); },
        d,
        clients,
        registrators,
        sig_processors);
    {
      std::unique_lock lk{sigs_lock};
      full_signatures.clear();
    }
  }
};

class utt_system_include_budget : public utt_system_include_clients {
 protected:
  void SetUp() override {
    utt_system_include_clients::SetUp();
    createBudget(
        job_id,
        n,
        [&](uint64_t sigId, uint16_t source) { return waitAndGetFullSig(sigId, source); },
        d,
        clients,
        bcoins,
        banks,
        sig_processors);
    {
      std::unique_lock lk{sigs_lock};
      full_signatures.clear();
    }
  }
  std::unordered_map<std::string, libutt::api::Coin> bcoins;
};

class utt_complete_system : public utt_system_include_budget {
 protected:
  void SetUp() override {
    utt_system_include_budget::SetUp();
    mintCoins(
        job_id,
        n,
        [&](uint64_t sigId, uint16_t source) { return waitAndGetFullSig(sigId, source); },
        d,
        clients,
        coins,
        banks,
        sig_processors);
    {
      std::unique_lock lk{sigs_lock};
      full_signatures.clear();
    }
  }
  std::unordered_map<std::string, std::vector<libutt::api::Coin>> coins;
};
TEST_F(test_utt_instance, test_clients_registration) {
  registerClients(
      job_id,
      n,
      [&](uint64_t sigId, uint16_t source) { return waitAndGetFullSig(sigId, source); },
      d,
      clients,
      registrators,
      sig_processors);
}

TEST_F(utt_system_include_clients, test_budget_creation) {
  std::unordered_map<std::string, libutt::api::Coin> bcoins;
  createBudget(
      job_id,
      n,
      [&](uint64_t sigId, uint16_t source) { return waitAndGetFullSig(sigId, source); },
      d,
      clients,
      bcoins,
      banks,
      sig_processors);
}

TEST_F(utt_system_include_budget, test_mint_creation) {
  std::unordered_map<std::string, std::vector<libutt::api::Coin>> coins;
  mintCoins(
      job_id,
      n,
      [&](uint64_t sigId, uint16_t source) { return waitAndGetFullSig(sigId, source); },
      d,
      clients,
      coins,
      banks,
      sig_processors);
}

TEST_F(utt_complete_system, test_simple_transactions) {
  auto& jid = job_id;
  for (size_t i = 0; i < clients.size(); i++) {
    auto& issuer = clients[i];
    auto& receiver = clients[(i + 1) % clients.size()];
    Transaction tx(d,
                   issuer,
                   {coins[issuer.getPid()].front()},
                   {bcoins[issuer.getPid()]},
                   {{issuer.getPid(), 50}, {receiver.getPid(), 50}},
                   *(encryptors_.at((i + 1) % clients.size())));
    coins[issuer.getPid()].erase(coins[issuer.getPid()].begin());
    bcoins.erase(issuer.getPid());
    auto init_jid = jid;
    for (size_t j = 0; j < n; j++) {
      jid = init_jid;
      auto signer = banks[j];
      auto psigs = signer->sign(tx);
      for (size_t sid = 0; sid < psigs.size(); sid++) {
        auto vcb = [tx, sid, signer](uint16_t id, const types::Signature& sig) {
          return signer->validatePartialSignature(id, sig, sid, tx);
        };
        sig_processors[j]->processSignature(jid, psigs[sid], vcb);
        jid++;
      }
    }
    std::vector<types::Signature> sigs;
    for (uint64_t sid = init_jid; sid < jid; sid++) {
      auto sig = waitAndGetFullSig(sid, 0);
      sigs.push_back(sig);
    }
    auto issuer_coins = issuer.claimCoins(tx, d, sigs);
    for (auto& coin : issuer_coins) {
      if (coin.getType() == api::Coin::Type::Normal) {
        coins[issuer.getPid()].emplace_back(std::move(coin));
      } else {
        ASSERT_TRUE(bcoins.find(issuer.getPid()) == bcoins.end());
        bcoins.emplace(issuer.getPid(), coin);
      }
    }
    auto receiver_coins = receiver.claimCoins(tx, d, sigs);
    for (auto& coin : receiver_coins) {
      if (coin.getType() == api::Coin::Type::Normal) {
        coins[receiver.getPid()].emplace_back(std::move(coin));
      } else {
        ASSERT_TRUE(false);
      }
    }
  }

  for (const auto& c : clients) {
    ASSERT_TRUE(coins[c.getPid()].size() == 2);
    for (const auto& coin : coins[c.getPid()]) ASSERT_TRUE(coin.getVal() == 50U);
    ASSERT_TRUE(bcoins[c.getPid()].getVal() == 950U);
  }
}

TEST_F(utt_complete_system, test_parallel_transactions) {
  auto& jid = job_id;
  std::vector<std::pair<std::vector<uint64_t>, Transaction>> txs;
  for (size_t i = 0; i < clients.size(); i++) {
    auto& issuer = clients[i];
    auto& receiver = clients[(i + 1) % clients.size()];
    std::vector<uint64_t> jids_;
    Transaction tx(d,
                   issuer,
                   {coins[issuer.getPid()].front()},
                   {bcoins[issuer.getPid()]},
                   {{issuer.getPid(), 50}, {receiver.getPid(), 50}},
                   *(encryptors_.at((i + 1) % clients.size())));
    coins[issuer.getPid()].erase(coins[issuer.getPid()].begin());
    bcoins.erase(issuer.getPid());
    for (uint32_t ocoin = 0; ocoin < tx.getNumOfOutputCoins(); ocoin++) jids_.push_back(jid++);
    txs.push_back({jids_, tx});
  }
  std::vector<std::thread> sig_jobs(txs.size());
  for (size_t k = 0; k < txs.size(); k++) {
    sig_jobs[k] = std::thread(
        [&](const std::vector<uint64_t>& ids_, const Transaction& tx_, uint64_t k_) {
          for (size_t j = 0; j < n; j++) {
            auto signer = banks[j];
            auto psigs = signer->sign(tx_);
            for (size_t sid = 0; sid < psigs.size(); sid++) {
              auto vcb = [signer, tx_, sid](uint16_t id, const types::Signature& sig) {
                return signer->validatePartialSignature(id, sig, sid, tx_);
              };
              sig_processors[j]->processSignature(ids_[sid], psigs[sid], vcb);
            }
          }
        },
        txs[k].first,
        txs[k].second,
        k);
  }
  for (auto& t : sig_jobs) t.join();
  std::vector<std::vector<types::Signature>> txs_sigs;
  for (const auto& [ids, tx] : txs) {
    std::vector<types::Signature> sigs;
    for (auto sid : ids) {
      auto sig = waitAndGetFullSig(sid, 0);
      sigs.push_back(sig);
    }
    txs_sigs.push_back(sigs);
  }
  for (size_t i = 0; i < clients.size(); i++) {
    auto& issuer = clients[i];
    auto& receiver = clients[(i + 1) % clients.size()];
    auto& tx = txs[i].second;
    auto& sigs = txs_sigs[i];
    auto issuer_coins = issuer.claimCoins(tx, d, sigs);
    for (auto& coin : issuer_coins) {
      if (coin.getType() == api::Coin::Type::Normal) {
        coins[issuer.getPid()].emplace_back(std::move(coin));
      } else {
        ASSERT_TRUE(bcoins.find(issuer.getPid()) == bcoins.end());
        bcoins.emplace(issuer.getPid(), coin);
      }
    }
    auto receiver_coins = receiver.claimCoins(tx, d, sigs);
    for (auto& coin : receiver_coins) {
      if (coin.getType() == api::Coin::Type::Normal) {
        coins[receiver.getPid()].emplace_back(std::move(coin));
      } else {
        ASSERT_TRUE(false);
      }
    }
  }

  for (const auto& c : clients) {
    ASSERT_TRUE(coins[c.getPid()].size() == 2);
    for (const auto& coin : coins[c.getPid()]) ASSERT_TRUE(coin.getVal() == 50U);
    ASSERT_TRUE(bcoins[c.getPid()].getVal() == 950U);
  }
}

TEST_F(utt_complete_system, test_replica_crash) {
  auto& jid = job_id;
  comm.isolateNode(0);
  for (size_t i = 0; i < clients.size(); i++) {
    auto& issuer = clients[i];
    auto& receiver = clients[(i + 1) % clients.size()];
    Transaction tx(d,
                   issuer,
                   {coins[issuer.getPid()].front()},
                   {bcoins[issuer.getPid()]},
                   {{issuer.getPid(), 50}, {receiver.getPid(), 50}},
                   *(encryptors_.at((i + 1) % clients.size())));
    coins[issuer.getPid()].erase(coins[issuer.getPid()].begin());
    bcoins.erase(issuer.getPid());
    auto init_jid = jid;
    for (size_t j = 1; j < n; j++) {
      jid = init_jid;
      auto signer = banks[j];
      auto psigs = signer->sign(tx);
      for (size_t sid = 0; sid < psigs.size(); sid++) {
        auto vcb = [tx, sid, signer](uint16_t id, const types::Signature& sig) {
          return signer->validatePartialSignature(id, sig, sid, tx);
        };
        sig_processors[j]->processSignature(jid, psigs[sid], vcb);
        jid++;
      }
    }
    std::vector<types::Signature> sigs;
    for (uint64_t sid = init_jid; sid < jid; sid++) {
      auto sig = waitAndGetFullSig(sid, 1);
      sigs.push_back(sig);
    }
    auto issuer_coins = issuer.claimCoins(tx, d, sigs);
    for (auto& coin : issuer_coins) {
      if (coin.getType() == api::Coin::Type::Normal) {
        coins[issuer.getPid()].emplace_back(std::move(coin));
      } else {
        ASSERT_TRUE(bcoins.find(issuer.getPid()) == bcoins.end());
        bcoins.emplace(issuer.getPid(), coin);
      }
    }
    auto receiver_coins = receiver.claimCoins(tx, d, sigs);
    for (auto& coin : receiver_coins) {
      if (coin.getType() == api::Coin::Type::Normal) {
        coins[receiver.getPid()].emplace_back(std::move(coin));
      } else {
        ASSERT_TRUE(false);
      }
    }
  }

  for (const auto& c : clients) {
    ASSERT_TRUE(coins[c.getPid()].size() == 2);
    for (const auto& coin : coins[c.getPid()]) ASSERT_TRUE(coin.getVal() == 50U);
    ASSERT_TRUE(bcoins[c.getPid()].getVal() == 950U);
  }
}

}  // namespace
int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
