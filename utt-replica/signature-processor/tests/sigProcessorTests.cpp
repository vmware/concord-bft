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

#include <utt/RegAuth.h>
#include <utt/RandSigDKG.h>
#include <utt/Serialization.h>
#include <utt/Params.h>
#include <utt/IBE.h>
#include <utt/Serialization.h>
#include <utt/Address.h>
#include <utt/Comm.h>
#include <utt/Coin.h>
#include <utt/MintOp.h>
#include <utt/Coin.h>
#include <utt/BurnOp.h>
#include <utt/Tx.h>

#include <chrono>
#include <map>
#include <queue>
#include <memory>
#include <mutex>
#include <condition_variable>
#include <thread>
using namespace bft::communication;
using namespace libutt;
using namespace libutt::api;
using namespace libutt::api::operations;

std::map<uint16_t, std::map<uint64_t, std::vector<uint8_t>>> full_signatures;
std::mutex sigs_lock;
std::condition_variable cv;
logging::Logger logger = logging::getLogger("sigProcessorTests");
class TestComm : public ICommunication {
  std::vector<NodeNum> isolated;

 public:
  void isolateNode(NodeNum node) { isolated.push_back(node); }
  void deisolatedNode(NodeNum node) {
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
    if (std::find(isolated.begin(), isolated.end(), destNode) != isolated.end()) return 0;
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
    std::unique_ptr<MessageBase> msg;
    msg.reset(
        new MessageBase(sourceNode, reinterpret_cast<MessageBase::Header*>((char*)message), messageLength, false));
    ims_->pushExternalMsg(std::move(msg));
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
  rc.setIBEParams(d.getParams().ibe);
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
bool cond(uint64_t sigId) {
  if (full_signatures.empty()) return false;
  for (const auto& [_, data] : full_signatures) {
    (void)_;
    if (data.find(sigId) == data.end()) return false;
  }
  return true;
}
libutt::api::types::Signature waitAndGetFullSig(uint64_t sigId, uint16_t source) {
  std::unique_lock lk{sigs_lock};
  LOG_INFO(logger, "collecting full signature for sid: " << sigId);
  cv.wait(lk, [&sigId] { return cond(sigId); });
  return full_signatures.at(source).at(sigId);
}
uint16_t n = 4;
uint16_t t = 3;

int main(int argc, char* argv[]) {
  // initiate A UTT instance utt1
  std::srand(0);
  (void)argc;
  (void)argv;
  size_t c = 10;
  auto [d, dkg, rc] = init(n, t);
  auto registrators = GenerateRegistrators(n, rc);
  auto banks = GenerateCommitters(n, dkg, rc.toPK());
  auto clients = GenerateClients(c, dkg.getPK(), rc.toPK(), rc);

  // initiate signature collectors
  TestComm comm;
  std::map<uint16_t, std::shared_ptr<utt::SigProcessor>> sig_processors;
  std::map<uint16_t, std::shared_ptr<MsgsCommunicator>> message_comms;
  for (uint16_t i = 0; i < n; i++) {
    std::shared_ptr<MsgHandlersRegistrator> msr = std::make_shared<MsgHandlersRegistrator>();
    std::shared_ptr<IncomingMsgsStorage> ims =
        std::make_shared<IncomingMsgsStorageImp>(msr, std::chrono::milliseconds(1000), i);
    std::shared_ptr<IReceiver> receiver = std::make_shared<TestReceiver>(ims);
    std::shared_ptr<MsgsCommunicator> msc = std::make_shared<MsgsCommunicator>(&comm, ims, receiver);
    message_comms[i] = msc;
    msr->registerMsgHandler(MsgCode::ClientRequest, [&](bftEngine::impl::MessageBase* message) {
      ClientRequestMsg* msg = (ClientRequestMsg*)message;
      uint64_t job_id{0};
      libutt::api::types::Signature fsig(msg->requestLength() - sizeof(uint64_t));
      std::memcpy(&job_id, msg->requestBuf(), sizeof(uint64_t));
      std::memcpy(fsig.data(), msg->requestBuf() + sizeof(uint64_t), fsig.size());
      {
        std::unique_lock lk(sigs_lock);
        for (size_t j = 0; j < n; j++) full_signatures[j][job_id] = fsig;
      }
      cv.notify_all();
      delete msg;
    });
    sig_processors[i] =
        std::make_shared<utt::SigProcessor>(i, n, t, 1000, msc, msr, ((IncomingMsgsStorageImp*)(ims.get()))->timers());
    msc->startCommunication(i);
    msc->startMsgsProcessing(i);
  }

  LOG_INFO(logger, "register clients...");
  uint64_t jid{0};
  // Now, lets compute the client's rcm using the sigCollectors
  for (size_t i = 0; i < clients.size(); i++) {
    LOG_INFO(logger, "register client " << i);
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
    auto sig = waitAndGetFullSig(jid, 0);
    jid++;
    sig =
        Utils::unblindSignature(d, Commitment::Type::REGISTRATION, {Fr::zero().to_words(), Fr::zero().to_words()}, sig);
    c.setRCMSig(d, s2, sig);
    auto rcm_data = c.rerandomizeRcm(d);
    for (auto& r : registrators) {
      assertTrue(r->validateRCM(rcm_data.first, rcm_data.second));
    }
  }

  std::unordered_map<std::string, std::vector<libutt::api::Coin>> coins;
  std::unordered_map<std::string, libutt::api::Coin> bcoins;
  std::unordered_map<size_t, std::shared_ptr<libutt::IBEEncryptor>> encryptors_;
  for (size_t i = 0; i < clients.size(); i++) {
    encryptors_[i] = std::make_shared<libutt::IBEEncryptor>(rc.toPK().mpk);
  }

  // issue initial budget coins
  LOG_INFO(logger, "issue initial budget coins");
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
    auto sig = waitAndGetFullSig(jid, 0);
    jid++;
    auto coin = c.claimCoins(budget, d, {sig}).front();
    assertTrue(c.validate(coin));
    bcoins.emplace(c.getPid(), coin);
  }

  LOG_INFO(logger, "issue initial UTT coins (mint operation)");
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
    auto sig = waitAndGetFullSig(jid, 0);
    jid++;
    auto coin = c.claimCoins(mint, d, {sig}).front();
    assertTrue(c.validate(coin));
    coins[c.getPid()].emplace_back(std::move(coin));
  }

  // Now, each client transfers a 50$ to its predecessor;
  LOG_INFO(logger, "issue UTT transactions");
  for (size_t i = 0; i < clients.size(); i++) {
    auto& issuer = clients[i];
    auto& receiver = clients[(i + 1) % clients.size()];
    LOG_INFO(logger, "client " << i << " transfers 50$ using one 100$ UTT coin to client " << (i + 1) % clients.size());
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
        assertTrue(bcoins.find(issuer.getPid()) == bcoins.end());
        bcoins.emplace(issuer.getPid(), coin);
      }
    }
    auto receiver_coins = receiver.claimCoins(tx, d, sigs);
    for (auto& coin : receiver_coins) {
      if (coin.getType() == api::Coin::Type::Normal) {
        coins[receiver.getPid()].emplace_back(std::move(coin));
      } else {
        assertTrue(false);
      }
    }
  }

  LOG_INFO(logger, "validate the state is as expected...");
  for (const auto& c : clients) {
    assertTrue(coins[c.getPid()].size() == 2);
    for (const auto& coin : coins[c.getPid()]) assertTrue(coin.getVal() == 50U);
    assertTrue(bcoins[c.getPid()].getVal() == 950U);
  }

  // Now, each client has a budget of 950$ and 2 coins of 50$ each. Lets run some transactions in parallel to make sure
  // the sigCollector is thread safe
  std::vector<std::pair<std::vector<uint64_t>, Transaction>> txs;
  for (size_t i = 0; i < clients.size(); i++) {
    auto& issuer = clients[i];
    auto& receiver = clients[(i + 1) % clients.size()];
    LOG_INFO(logger,
             "client " << i << " created a transaction for transferring 50$ using one 50$ UTT coin to client "
                       << (i + 1) % clients.size());
    std::vector<uint64_t> jids_;
    Transaction tx(d,
                   issuer,
                   {coins[issuer.getPid()].front()},
                   {bcoins[issuer.getPid()]},
                   {{receiver.getPid(), 50}},
                   *(encryptors_.at((i + 1) % clients.size())));
    coins[issuer.getPid()].erase(coins[issuer.getPid()].begin());
    bcoins.erase(issuer.getPid());
    for (uint32_t ocoin = 0; ocoin < tx.getNumOfOutputCoins(); ocoin++) jids_.push_back(jid++);
    txs.push_back({jids_, tx});
  }
  std::vector<std::thread> sig_jobs(txs.size());
  LOG_INFO(logger, "computing signatures in parallel");
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
    // sig_jobs[k].join();
  }
  for (auto& t : sig_jobs) t.join();
  std::vector<std::vector<types::Signature>> txs_sigs;
  LOG_INFO(logger, "collect complete signatures");
  for (const auto& [ids, tx] : txs) {
    std::vector<types::Signature> sigs;
    for (auto sid : ids) {
      auto sig = waitAndGetFullSig(sid, 0);
      sigs.push_back(sig);
    }
    txs_sigs.push_back(sigs);
  }
  LOG_INFO(logger, "claiming coins");
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
        assertTrue(bcoins.find(issuer.getPid()) == bcoins.end());
        bcoins.emplace(issuer.getPid(), coin);
      }
    }
    auto receiver_coins = receiver.claimCoins(tx, d, sigs);
    for (auto& coin : receiver_coins) {
      if (coin.getType() == api::Coin::Type::Normal) {
        coins[receiver.getPid()].emplace_back(std::move(coin));
      } else {
        assertTrue(false);
      }
    }
  }

  LOG_INFO(logger, "validate the state is as expected...");
  for (const auto& c : clients) {
    assertTrue(coins[c.getPid()].size() == 2);
    for (const auto& coin : coins[c.getPid()]) assertTrue(coin.getVal() == 50U);
    assertTrue(bcoins[c.getPid()].getVal() == 900U);
  }

  // Now, we want to take down one of the signature collector (say 0), and make sure we are still able to process the
  // signature that are under its own responsibility
  LOG_INFO(logger, "Disabling collector number 0");
  comm.isolateNode(0);
  for (size_t i = 0; i < clients.size(); i++) {
    auto& issuer = clients[i];
    auto& receiver = clients[(i + 1) % clients.size()];
    LOG_INFO(logger, "client " << i << " transfers 50$ using one 50$ UTT coin to client " << (i + 1) % clients.size());
    std::vector<uint64_t> jids_;
    Transaction tx(d,
                   issuer,
                   {coins[issuer.getPid()].front()},
                   {bcoins[issuer.getPid()]},
                   {{receiver.getPid(), 50}},
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
      auto sig = waitAndGetFullSig(sid, 0);
      sigs.push_back(sig);
    }
    auto issuer_coins = issuer.claimCoins(tx, d, sigs);
    for (auto& coin : issuer_coins) {
      if (coin.getType() == api::Coin::Type::Normal) {
        coins[issuer.getPid()].emplace_back(std::move(coin));
      } else {
        assertTrue(bcoins.find(issuer.getPid()) == bcoins.end());
        bcoins.emplace(issuer.getPid(), coin);
      }
    }
    auto receiver_coins = receiver.claimCoins(tx, d, sigs);
    for (auto& coin : receiver_coins) {
      if (coin.getType() == api::Coin::Type::Normal) {
        coins[receiver.getPid()].emplace_back(std::move(coin));
      } else {
        assertTrue(false);
      }
    }
  }

  LOG_INFO(logger, "validate the state is as expected...");
  for (const auto& c : clients) {
    assertTrue(coins[c.getPid()].size() == 2);
    for (const auto& coin : coins[c.getPid()]) assertTrue(coin.getVal() == 50U);
    assertTrue(bcoins[c.getPid()].getVal() == 850U);
  }

  for (uint16_t i = 0; i < n; i++) {
    message_comms[i]->stopMsgsProcessing();
    message_comms[i]->stopCommunication();
  }
  return 0;
}
