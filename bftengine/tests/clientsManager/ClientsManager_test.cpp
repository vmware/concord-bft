// Concord
//
// Copyright (c) 2020-2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License"). You may not use this product except in
// compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright notices and license terms. Your use of
// these subcomponents is subject to the terms and conditions of the sub-component's license, as noted in the LICENSE
// file.

#include "bftengine/InternalBFTClient.hpp"
#include "bftengine/KeyExchangeManager.hpp"
#include "bftengine/SigManager.hpp"
#include "ClientsManager.hpp"
#include "gtest/gtest.h"
#include "messages/ClientReplyMsg.hpp"
#include "ReservedPagesMock.hpp"
#include "sign_verify_utils.hpp"

using concord::crypto::signature::SignerFactory;
using bftEngine::impl::ClientsManager;
using bftEngine::impl::NodeIdType;
using bftEngine::impl::ReplicasInfo;
using bftEngine::impl::SigManager;
using bftEngine::ReplicaConfig;
using bftEngine::ReservedPagesClientBase;
using bftEngine::test::ReservedPagesMock;
using concord::util::crypto::KeyFormat;
using concord::secretsmanager::ISecretsManagerImpl;
using concordUtil::Timers;
using std::chrono::milliseconds;
using std::ifstream;
using std::map;
using std::nullopt;
using std::optional;
using std::pair;
using std::set;
using std::shared_ptr;
using std::string;
using std::string_view;
using std::this_thread::sleep_for;
using std::unique_ptr;
using std::vector;

using concord::crypto::cryptopp::Crypto;
using concord::crypto::cryptopp::RSA_SIGNATURE_LENGTH;
using concord::crypto::openssl::OpenSSLCryptoImpl;
using concord::crypto::signature::SIGN_VERIFY_ALGO;

// Testing values to be used for certain Concord-BFT configuration that ClientsManager and/or its dependencies may
// reference.
const ReplicaId kReplicaIdForTesting = 0;
const KeyFormat kKeyFormatForTesting = KeyFormat::HexaDecimalStrippedFormat;

const set<pair<PrincipalId, const string>> kPublicKeysOfReplicasForTesting{};
const set<pair<const string, set<uint16_t>>> kInitialPublicKeysOfClientsForTesting;
unique_ptr<ReplicasInfo> sigManagerReplicasInfoForTesting;
const string kArbitraryMessageForTestingKeyAgreement =
    "This is an arbitrary message to sign and verify in order to verify a ClientsManager loaded the correct client key "
    "to a global singleton in cases where the singleton in question offers signing and/or verifying functionalities "
    "using keys loaded to it but does not provide a way of directly retrieving an individual client's key that was "
    "saved to it.";

uint32_t kRSILengthForTesting = 0;

// Some objects ClientsManager objects depend on that are needed for testing them.
concordMetrics::Component metrics{concordMetrics::Component("replica", std::make_shared<concordMetrics::Aggregator>())};
shared_ptr<ReservedPagesMock<ClientsManager>> res_pages_mock_;

// "Mocks" for initializing the KeyExchangeManager singleton (which ClientsManager objects may use). In the interest of
// simplicity and minimizing avoidable dependencies for these unit tests, these mocks intentionally do not actually do
// anything that would be irrelevant to testing ClientsManager(s).
class MockInternalBFTClient : public IInternalBFTClient {
 public:
  virtual ~MockInternalBFTClient() override{};
  virtual NodeIdType getClientId() const override { return 0; }
  virtual uint64_t sendRequest(uint64_t flags,
                               uint32_t requestLength,
                               const char* request,
                               const string& cid) override {
    return 0;
  }
  virtual uint64_t sendRequest(uint64_t flags,
                               uint32_t requestLength,
                               const char* request,
                               const string& cid,
                               IncomingMsgsStorage::Callback onPoppedFromQueue) override {
    return 0;
  }
  virtual uint32_t numOfConnectedReplicas(uint32_t clusterSize) override { return 0; }
  virtual bool isReplicaConnected(uint16_t repId) const override { return false; }
  virtual bool isUdp() const override { return false; }
};

class MockMultiSigKeyGenerator : public IMultiSigKeyGenerator {
 public:
  virtual ~MockMultiSigKeyGenerator() override{};
  virtual pair<string, string> generateMultisigKeyPair() override { return pair("", ""); }
};

class MockKeyExchanger : public IKeyExchanger {
 public:
  virtual ~MockKeyExchanger() override{};
  virtual void onPublicKeyExchange(const string& pub, const uint16_t& signerIndex, const int64_t& seqnum) override {}
  virtual void onPrivateKeyExchange(const string& privKey, const string& pubKey, const int64_t& seqnum) override {}
};

class MockSecretsManagerImpl : public ISecretsManagerImpl {
 public:
  virtual ~MockSecretsManagerImpl() override {}
  virtual bool encryptFile(string_view file_path, const string& input) override { return false; }
  virtual optional<string> encryptString(const string& input) override { return nullopt; }
  virtual optional<string> decryptFile(string_view path) override { return nullopt; }
  virtual optional<string> decryptFile(const ifstream& file) override { return nullopt; }
  virtual optional<string> decryptString(const string& input) override { return nullopt; }
};

class MockClientPublicKeyStore : public IClientPublicKeyStore {
 public:
  virtual ~MockClientPublicKeyStore() override {}
  virtual void setClientPublicKey(uint16_t clientId, const string& key, KeyFormat format) override {}
};

// Objects used by the KeyExchangeManager singleton.
shared_ptr<MockInternalBFTClient> mock_internal_bft_client_for_key_exchange_manager(new MockInternalBFTClient());
unique_ptr<MockMultiSigKeyGenerator> mock_multi_sig_key_generator_for_key_exchange_manager(
    new MockMultiSigKeyGenerator());
unique_ptr<MockKeyExchanger> mock_key_exchanger_for_key_exchange_manager(new MockKeyExchanger());
shared_ptr<MockSecretsManagerImpl> mock_secrets_manager_impl_for_key_exchange_manager(new MockSecretsManagerImpl());
unique_ptr<MockClientPublicKeyStore> mock_client_public_key_store_for_key_exchange_manager(
    new MockClientPublicKeyStore());
unique_ptr<Timers> timers_for_key_exchange_manager(new Timers());
unique_ptr<SigManager> sig_manager_for_key_exchange_manager;

// Helper functions for managing and checking global objects and/or singletons the ClientsManager uses.
static void resetMockReservedPages() {
  res_pages_mock_.reset(new ReservedPagesMock<ClientsManager>());
  ReservedPagesClientBase::setReservedPages(res_pages_mock_.get());
}

static shared_ptr<ReservedPagesMock<ClientsManager>> getMockReservedPages() { return res_pages_mock_; }

static void setMockReservedPages(shared_ptr<ReservedPagesMock<ClientsManager>>& res_pages) {
  res_pages_mock_ = res_pages;
  ReservedPagesClientBase::setReservedPages(res_pages_mock_.get());
}

static void resetSigManager() {
  SigManager::Key kReplicaPrivateKeyForTesting;
  if (ReplicaConfig::instance().replicaMsgSigningAlgo == SIGN_VERIFY_ALGO::RSA) {
    kReplicaPrivateKeyForTesting = Crypto::instance().generateRsaKeyPair(RSA_SIGNATURE_LENGTH).first;
  } else if (ReplicaConfig::instance().replicaMsgSigningAlgo == SIGN_VERIFY_ALGO::EDDSA) {
    kReplicaPrivateKeyForTesting = OpenSSLCryptoImpl::instance().generateEdDSAKeyPair().first;
  }
  sig_manager_for_key_exchange_manager.reset(SigManager::init(kReplicaIdForTesting,
                                                              kReplicaPrivateKeyForTesting,
                                                              kPublicKeysOfReplicasForTesting,
                                                              kKeyFormatForTesting,
                                                              &kInitialPublicKeysOfClientsForTesting,
                                                              kKeyFormatForTesting,
                                                              *sigManagerReplicasInfoForTesting));
}

static void initializeKeyExchangeManagerForClientsManagerTesting() {
  KeyExchangeManager::InitData kem_init_data;

  kem_init_data.cl = mock_internal_bft_client_for_key_exchange_manager;
  kem_init_data.kg = mock_multi_sig_key_generator_for_key_exchange_manager.get();
  kem_init_data.ke = mock_key_exchanger_for_key_exchange_manager.get();
  kem_init_data.secretsMgr = mock_secrets_manager_impl_for_key_exchange_manager;
  kem_init_data.cpks = mock_client_public_key_store_for_key_exchange_manager.get();
  kem_init_data.timers = timers_for_key_exchange_manager.get();

  // Note that KeyExchangeManager passes keys a ClientsManager loads to it through to SigManager. SigManager
  // effectively ignores any client public keys it is given that don't belong to external clients according to the
  // ReplicasInfo it is given a reference to. We therefore set up this ReplicasInfo to present all possible client IDs
  // as valid external client IDs to the SigManager so that we will be able to observe any public key show up there
  // that a ClientsManager loads to the KeyExchangeManager. Though it may be technically incorrect to tell the
  // SigManager every client ID belongs to an external client, SigManager is not known to use this information in any
  // other way relevant to testing ClientsManager and this approach is simpler than any alternatives considered.
  //
  // Also note ReplicasInfo considers external client IDs to be the contiguous range from [numReplicas + numRoReplicas
  // + numOfClientProxies, numReplicas + numRoReplicas + numOfClientProxies + numOfExternalClients - 1].
  ReplicaConfig::instance().setnumOfExternalClients(UINT16_MAX);

  // Note we have to use 1 replica rather than 0 since ReplicasInfo expects the numReplicas = 3F + 2C + 1 variant to
  // hold.
  ReplicaConfig::instance().setnumReplicas(1);

  // We are relying on these defaults.
  ConcordAssert(ReplicaConfig::instance().getfVal() == 0);
  ConcordAssert(ReplicaConfig::instance().getcVal() == 0);
  ConcordAssert(ReplicaConfig::instance().getnumRoReplicas() == 0);
  ConcordAssert(ReplicaConfig::instance().getnumOfClientProxies() == 0);

  sigManagerReplicasInfoForTesting.reset(new ReplicasInfo(ReplicaConfig::instance(), false, false));

  // Avoid leaving the ReplicaConfig singleton in an unexpected state after ReplicasInfo's constructor has read it.
  // Note we have to temporarily alter the ReplicaConfig singleton rather than just constructing one for exclusive use
  // with the ReplicasInfo constructor since ReplicaConfig's own constructor is not public.
  ReplicaConfig::instance().setnumOfExternalClients(0);
  ReplicaConfig::instance().setnumOfExternalClients(0);

  // For some functionalities ClientsManager objects may use, the KeyExchangeManager singleton may itself require the
  // SigManager singleton to be initialized.
  resetSigManager();

  // Note KeyExchangeManager's constructor (which will initialize the KeyExchangeManager singleton on the first call to
  // KeyExchangeManager::instance()) requires the reserved pages system to be initialized.
  resetMockReservedPages();

  KeyExchangeManager::instance(&kem_init_data);
}

static void clearClientPublicKeysLoadedToKEM() {
  // Note KeyExchangeManager passes keys a ClientsManager loads to it through to SigManager. Furthermore, SigManager
  // does not appear to provide a direct means of simply clearing all client public keys it has, so we clear them by
  // resetting the SigManager.
  resetSigManager();
}

// Note due to a technical limitation, verifyClientPublicKeyLoadedToKEM and verifyNoClientPublicKeyLoadedToKEM will not
// be able to verify keys loaded for clients with ID 0. (specifically: KeyExchangeManager passes keys through to
// SigManager, SigManager ignores keys unless a ReplicasInfo object it keeps a pointer to reports the client ID they
// are being loaded for as valid for an external client, and ReplicasInfo cannot be setup to recognize 0 as a valid
// external client ID since it expects the first numReplicas principal IDs to be reserved for replicas, and it
// effectively expects at least 1 replica since it requires numReplicas = 3F + 2C + 1 to hold).
static bool verifyClientPublicKeyLoadedToKEM(NodeIdType client_id, const pair<string, string>& expected_key) {
  // Note KeyExchangeManager passes keys a ClientsManager loads to it through to SigManager. Furthermore, SigManager
  // does not appear to provide a direct accessor for individual client's public keys, so we check that it has the
  // correct public key by actually verifying a signature with it.
  if (!(SigManager::instance()->hasVerifier(client_id))) {
    return false;
  }
  const auto signer = SignerFactory::getReplicaSigner(
      expected_key.first, ReplicaConfig::instance().replicaMsgSigningAlgo, kKeyFormatForTesting);
  string signature = signer->sign(kArbitraryMessageForTestingKeyAgreement);
  return SigManager::instance()->verifySig(client_id,
                                           kArbitraryMessageForTestingKeyAgreement.data(),
                                           kArbitraryMessageForTestingKeyAgreement.length(),
                                           signature.data(),
                                           signature.length());
}
static bool verifyNoClientPublicKeyLoadedToKEM(NodeIdType client_id) {
  // Note KeyExchangeManager passes keys a ClientsManager loads to it through to SigManager.
  return !(SigManager::instance()->hasVerifier(client_id));
}

TEST(ClientsManager, reservedPagesPerClient) {
  uint32_t sizeOfReservedPage = 1024;
  uint32_t maxReplysize = 1000;
  auto numPagesPerCl = bftEngine::impl::ClientsManager::reservedPagesPerClient(sizeOfReservedPage, maxReplysize);
  ASSERT_EQ(numPagesPerCl, 2);
  maxReplysize = 3000;
  numPagesPerCl = bftEngine::impl::ClientsManager::reservedPagesPerClient(sizeOfReservedPage, maxReplysize);
  ASSERT_EQ(numPagesPerCl, 4);
  maxReplysize = 1024;
  numPagesPerCl = bftEngine::impl::ClientsManager::reservedPagesPerClient(sizeOfReservedPage, maxReplysize);
  ASSERT_EQ(numPagesPerCl, 2);
}

TEST(ClientsManager, constructor) {
  resetMockReservedPages();
  unique_ptr<ClientsManager> cm(new ClientsManager({1, 4, 50, 7}, {3, 2, 60}, {}, {5, 11, 55}, metrics));
  for (auto id : {5, 11, 55}) ASSERT_EQ(true, cm->isInternal(id));
  for (auto id : {1, 4, 50, 3, 2, 60}) ASSERT_EQ(false, cm->isInternal(id));

  ASSERT_NO_THROW(cm.reset(new ClientsManager({1}, {}, {}, {}, metrics)))
      << "ClientsManager's constructor failed when given only a single proxy client.";
  EXPECT_TRUE(cm->isValidClient(1) && !(cm->isInternal(1)))
      << "ClientsManager constructed with only a single proxy client did not correctly recognize that client's ID and "
         "type.";
  ASSERT_NO_THROW(cm.reset(new ClientsManager({}, {1}, {}, {}, metrics)))
      << "ClientsManager's constructor failed when given only a single external client.";
  EXPECT_TRUE(cm->isValidClient(1) && !(cm->isInternal(1)))
      << "ClientsManager constructed with only a single external client did not correctly recognize that client's ID "
         "and type.";
  ASSERT_NO_THROW(cm.reset(new ClientsManager({}, {}, {}, {1}, metrics)))
      << "ClientsManager's constructor failed when given only a single internal client.";
  EXPECT_TRUE(cm->isValidClient(1) && cm->isInternal(1))
      << "ClientsManager constructed with only a single internal client did not correctly recognize that client's ID "
         "and type.";

  const set<NodeIdType> proxy_clients{1, 3, 5, 7};
  const set<NodeIdType> external_clients{2, 3, 6, 7};
  const set<NodeIdType> internal_clients{4, 5, 6, 7};
  ASSERT_NO_THROW(cm.reset(new ClientsManager(proxy_clients, external_clients, {}, internal_clients, metrics)))
      << "ClientsManager's constructor failed when given sets of clients including some clients classified as multiple "
         "types.";
  for (NodeIdType i = 1; i <= 7; ++i) {
    ConcordAssert((proxy_clients.count(i) > 0) || (external_clients.count(i) > 0) || (internal_clients.count(i) > 0));
    EXPECT_TRUE(cm->isValidClient(i)) << "ClientsManager constructed with sets of clients including some clients "
                                         "classified as multiple types did not recognize a client ID it was given.";
    EXPECT_EQ(cm->isInternal(i), (internal_clients.count(i) > 0))
        << "ClientsManager constructed with sets of clients including some clients classified as multiple types did "
           "not correctly report the type of one of those clients.";
  }
}

TEST(ClientsManager, numberOfRequiredReservedPagesSucceeds) {
  resetMockReservedPages();
  unique_ptr<ClientsManager> cm(new ClientsManager({1, 2, 3}, {4}, {}, {5, 6}, metrics));

  // Note we do not actually check the values retruned by numberOfRequiredReservedPages (other than expecting them to be
  // above 0), as exactly what value this function returns is considered an implementation detail of ClientsManager
  // subject to change and not something guaranteed by ClientsManager's interface.
  EXPECT_GT(cm->numberOfRequiredReservedPages(), 0)
      << "ClientsManager::numberOfRequiredReservedPages failed to return an integer greater than 0.";

  cm.reset(new ClientsManager({1}, {}, {}, {}, metrics));
  EXPECT_GT(cm->numberOfRequiredReservedPages(), 0) << "ClientsManager::numberOfRequiredReservedPages failed to return "
                                                       "an integer greater than 0 for a single-client ClientsManager.";
}

TEST(ClientsManager, loadInfoFromReservedPagesLoadsCorrectInfo) {
  set<NodeIdType> client_ids{1, 2, 3, 4};
  set<NodeIdType> proxy_client_ids{1, 2, 3};
  set<NodeIdType> external_client_ids{4};
  set<NodeIdType> internal_client_ids{};

  map<NodeIdType, pair<string, string>> client_keys;

  if (ReplicaConfig::instance().replicaMsgSigningAlgo == SIGN_VERIFY_ALGO::RSA) {
    client_keys[2] = Crypto::instance().generateRsaKeyPair(RSA_SIGNATURE_LENGTH);
  } else if (ReplicaConfig::instance().replicaMsgSigningAlgo == SIGN_VERIFY_ALGO::EDDSA) {
    client_keys[2] = OpenSSLCryptoImpl::instance().generateEdDSAKeyPair();
  }

  map<NodeIdType, pair<ReqId, string>> client_replies;
  client_replies[2] = {9, "reply 9 to client 2"};
  client_replies[4] = {12, "reply 12 to client 4"};
  ReqId max_req_id_to_check_for = 16;

  resetMockReservedPages();
  unique_ptr<ClientsManager> cm(
      new ClientsManager(proxy_client_ids, external_client_ids, {}, internal_client_ids, metrics));
  for (const auto& keys : client_keys) {
    cm->setClientPublicKey(keys.first, keys.second.second, kKeyFormatForTesting);
  }
  for (const auto& reply : client_replies) {
    string reply_msg = reply.second.second;
    cm->allocateNewReplyMsgAndWriteToStorage(
        reply.first, reply.second.first, 0, reply_msg.data(), reply_msg.length(), kRSILengthForTesting);
  }

  clearClientPublicKeysLoadedToKEM();
  cm.reset(new ClientsManager(proxy_client_ids, external_client_ids, {}, internal_client_ids, metrics));
  EXPECT_NO_THROW(cm->loadInfoFromReservedPages()) << "ClientsManager::loadInfoFromReservedPages failed.";
  for (const auto& id : client_ids) {
    if (client_keys.count(id) > 0) {
      EXPECT_TRUE(verifyClientPublicKeyLoadedToKEM(id, client_keys[id]))
          << "ClientsManager::loadInfoFromReservedPages failed to load a public key to the KeyExchangeManager from the "
             "reserved pages.";
    } else {
      EXPECT_TRUE(verifyNoClientPublicKeyLoadedToKEM(id))
          << "ClientsManager::loadInfoFromReservedPages loaded a public key to the KeyExchangeManager for a client "
             "which should not have one in the reserved pages.";
    }
    if (client_replies.count(id) > 0) {
      EXPECT_TRUE(cm->hasReply(id, client_replies[id].first))
          << "ClientsManager::loadInfoFromReservedPages failed to load a reply record from the reserved pages.";
    }
    for (ReqId i = 0; i <= max_req_id_to_check_for; ++i) {
      if (client_replies.count(id) <= 0 || i != client_replies[id].first) {
        EXPECT_FALSE(cm->hasReply(id, i)) << "ClientsManager::loadInfoFromReservedPages loaded a reply record which "
                                             "should not have been in the reserved pages.";
      }
    }
  }

  resetMockReservedPages();
  cm.reset(new ClientsManager(proxy_client_ids, external_client_ids, {}, internal_client_ids, metrics));
  for (const auto& keys : client_keys) {
    cm->setClientPublicKey(keys.first, keys.second.second, kKeyFormatForTesting);
  }

  clearClientPublicKeysLoadedToKEM();
  cm.reset(new ClientsManager(proxy_client_ids, external_client_ids, {}, internal_client_ids, metrics));
  EXPECT_NO_THROW(cm->loadInfoFromReservedPages()) << "ClientsManager::loadInfoFromReservedPages failed when the "
                                                      "reserved pages contained only client public key information.";
  for (const auto& id : client_ids) {
    if (client_keys.count(id) > 0) {
      EXPECT_TRUE(verifyClientPublicKeyLoadedToKEM(id, client_keys[id]))
          << "ClientsManager::loadInfoFromReservedPages failed to load a public key to the KeyExchangeManager from the "
             "reserved pages.";
    } else {
      EXPECT_TRUE(verifyNoClientPublicKeyLoadedToKEM(id))
          << "ClientsManager::loadInfoFromReservedPages loaded a public key to the KeyExchangeManager for a client "
             "which should not have one in the reserved pages.";
    }
    for (ReqId i = 0; i <= max_req_id_to_check_for; ++i) {
      EXPECT_FALSE(cm->hasReply(id, i)) << "ClientsManager::loadInfoFromReservedPages loaded a reply record when the "
                                           "reserved pages should have only contained client public key information.";
    }
  }

  resetMockReservedPages();
  cm.reset(new ClientsManager(proxy_client_ids, external_client_ids, {}, internal_client_ids, metrics));
  for (const auto& reply : client_replies) {
    string reply_msg = reply.second.second;
    cm->allocateNewReplyMsgAndWriteToStorage(
        reply.first, reply.second.first, 0, reply_msg.data(), reply_msg.length(), kRSILengthForTesting);
  }

  clearClientPublicKeysLoadedToKEM();
  cm.reset(new ClientsManager(proxy_client_ids, external_client_ids, {}, internal_client_ids, metrics));
  EXPECT_NO_THROW(cm->loadInfoFromReservedPages())
      << "ClientsManager::loadInfoFromReservedPages failed when the reserved pages contained only client reply "
         "information and no client public key information.";
  for (const auto& id : client_ids) {
    EXPECT_TRUE(verifyNoClientPublicKeyLoadedToKEM(id))
        << "ClientsManager::loadInfoFromReservedPages loaded a public key to the KeyExchangeManager for a client when "
           "the reserved pages should have only contained client reply information.";
    if (client_replies.count(id) > 0) {
      EXPECT_TRUE(cm->hasReply(id, client_replies[id].first))
          << "ClientsManager::loadInfoFromReservedPages failed to load a reply record from the reserved pages.";
    }
    for (ReqId i = 0; i <= max_req_id_to_check_for; ++i) {
      if (client_replies.count(id) <= 0 || i != client_replies[id].first) {
        EXPECT_FALSE(cm->hasReply(id, i)) << "ClientsManager::loadInfoFromReservedPages loaded a reply record which "
                                             "should not have been in the reserved pages.";
      }
    }
  }
}

TEST(ClientsManager, loadInfoFromReservedPagesHandlesNoInfoAvailable) {
  resetMockReservedPages();
  clearClientPublicKeysLoadedToKEM();
  unique_ptr<ClientsManager> cm(new ClientsManager({1, 2, 3}, {4}, {}, {5, 6}, metrics));
  EXPECT_NO_THROW(cm->loadInfoFromReservedPages())
      << "ClientsManager::loadInfoFromReservedPages failed when there is no info available in the reserved pages.";
  for (NodeIdType i = 1; i <= 6; ++i) {
    EXPECT_TRUE(verifyNoClientPublicKeyLoadedToKEM(i))
        << "ClientsManager::loadInfoFromReservedPages loaded a public key to the KeyExchangeManaseger in a case with "
           "no info available in the reserved pages.";
    for (ReqId j = 0; j <= 8; ++j) {
      EXPECT_FALSE(cm->hasReply(i, j)) << "ClientsManager::loadInfoFromReservedPages loaded a reply record in a case "
                                          "with no info available in the reserved pages.";
    }
  }
}

TEST(ClientsManager, loadInfoFromReservedPagesHandlesSingleClientClientsManager) {
  pair<string, string> client_key_pair;
  if (ReplicaConfig::instance().replicaMsgSigningAlgo == SIGN_VERIFY_ALGO::RSA) {
    client_key_pair = Crypto::instance().generateRsaKeyPair(RSA_SIGNATURE_LENGTH);
  } else if (ReplicaConfig::instance().replicaMsgSigningAlgo == SIGN_VERIFY_ALGO::EDDSA) {
    client_key_pair = OpenSSLCryptoImpl::instance().generateEdDSAKeyPair();
  }

  string reply_message = "reply 1 to client 2";

  resetMockReservedPages();
  unique_ptr<ClientsManager> cm(new ClientsManager({2}, {}, {}, {}, metrics));
  cm->setClientPublicKey(2, client_key_pair.second, kKeyFormatForTesting);
  cm->allocateNewReplyMsgAndWriteToStorage(2, 1, 0, reply_message.data(), reply_message.length(), kRSILengthForTesting);

  clearClientPublicKeysLoadedToKEM();
  cm.reset(new ClientsManager({2}, {}, {}, {}, metrics));
  EXPECT_NO_THROW(cm->loadInfoFromReservedPages())
      << "ClientsManager::loadInfoFromReservedPages failed when loading info for a single-client ClientsManager.";
  EXPECT_TRUE(verifyClientPublicKeyLoadedToKEM(2, client_key_pair))
      << "ClientsManager::loadInfoFromReservedPages failed to load a public key to the KeyExchangeManager from the "
         "reserved pages for a Single-client ClientsManager.";
  EXPECT_TRUE(cm->hasReply(2, 1)) << "ClientsManager::loadInfoFromReservedPages failed to load a reply record from the "
                                     "reserved pages for a Single-client ClientsManager.";
  for (ReqId i = 2; i <= 8; ++i) {
    EXPECT_FALSE(cm->hasReply(2, i))
        << "ClientsManager::loadInfoFromReservedPages loaded a reply record which should not have been in the "
           "reserved pages for a Single-client ClientsManager.";
  }
}

TEST(ClientsManager, loadInfoFromReservedPagesCorrectlyDeletesOldReplies) {
  set<NodeIdType> client_ids{1, 2, 3, 4};
  set<NodeIdType> proxy_client_ids{1, 2, 3};
  set<NodeIdType> external_client_ids{4};
  set<NodeIdType> internal_client_ids{};

  map<NodeIdType, pair<ReqId, string>> client_replies;
  client_replies[2] = {9, "reply 9 to client 2"};
  client_replies[4] = {12, "reply 12 to client 4"};

  uint16_t client_batch_size = 2;
  map<NodeIdType, vector<pair<ReqId, string>>> older_client_replies;
  older_client_replies[2].emplace_back(1, "reply 1 to client 2");
  older_client_replies[2].emplace_back(3, "reply 3 to client 2");
  older_client_replies[4].emplace_back(2, "reply 2 to client 4");
  older_client_replies[4].emplace_back(8, "reply 8 to client 4");

  resetMockReservedPages();
  unique_ptr<ClientsManager> cm(
      new ClientsManager(proxy_client_ids, external_client_ids, {}, internal_client_ids, metrics));
  for (const auto& reply : client_replies) {
    string reply_msg = reply.second.second;
    cm->allocateNewReplyMsgAndWriteToStorage(
        reply.first, reply.second.first, 0, reply_msg.data(), reply_msg.length(), kRSILengthForTesting);
  }
  auto res_pages_with_newer_replies = getMockReservedPages();

  ReplicaConfig::instance().setclientBatchingEnabled(false);
  resetMockReservedPages();
  cm.reset(new ClientsManager(proxy_client_ids, external_client_ids, {}, internal_client_ids, metrics));
  for (const auto& replies : older_client_replies) {
    ReqId seq_num = replies.second[0].first;
    string reply_msg = replies.second[0].second;
    cm->allocateNewReplyMsgAndWriteToStorage(
        replies.first, seq_num, 0, reply_msg.data(), reply_msg.length(), kRSILengthForTesting);
  }
  setMockReservedPages(res_pages_with_newer_replies);
  EXPECT_NO_THROW(cm->loadInfoFromReservedPages()) << "ClientsManager::loadInfoFromReservedPages failed.";
  for (const auto& reply : client_replies) {
    EXPECT_TRUE(cm->hasReply(reply.first, reply.second.first))
        << "ClientsManager::loadInfoFromReservedPages failed to load a newer reply record from the reserved pages to a "
           "ClientsManager that already had an older reply record for the relevant client.";
    EXPECT_FALSE(cm->hasReply(reply.first, older_client_replies[reply.first][0].first))
        << "ClientsManager::loadInfoFromReservedPages failed to automatically delete the existing reply record for a "
           "client when loading a newer one with client batching disabled.";
  }

  ReplicaConfig::instance().setclientBatchingEnabled(true);
  ReplicaConfig::instance().setclientBatchingMaxMsgsNbr(client_batch_size);
  resetMockReservedPages();
  cm.reset(new ClientsManager(proxy_client_ids, external_client_ids, {}, internal_client_ids, metrics));
  for (const auto& replies : older_client_replies) {
    NodeIdType client_id = replies.first;
    for (const auto& reply : replies.second) {
      ReqId seq_num = reply.first;
      string reply_msg = reply.second;
      cm->allocateNewReplyMsgAndWriteToStorage(
          client_id, seq_num, 0, reply_msg.data(), reply_msg.length(), kRSILengthForTesting);
    }
  }
  setMockReservedPages(res_pages_with_newer_replies);
  EXPECT_NO_THROW(cm->loadInfoFromReservedPages()) << "ClientsManager::loadInfoFromReservedPages failed.";
  for (const auto& reply : client_replies) {
    EXPECT_TRUE(cm->hasReply(reply.first, reply.second.first))
        << "ClientsManager::loadInfoFromReservedPages failed to load a newer reply record from the reserved pages to a "
           "ClientsManager that already had older reply records for the relevant client.";
    EXPECT_FALSE(cm->hasReply(reply.first, older_client_replies[reply.first][0].first))
        << "ClientsManager::loadInfoFromReservedPages failed to automatically delete the oldest existing reply record "
           "for a client when loading a newer one with client batching enabled and a number of existing reply records "
           "already equalling the maximum client batch size.";
    EXPECT_TRUE(cm->hasReply(reply.first, older_client_replies[reply.first][1].first))
        << "ClientsManager::loadInfoFromReservedPages deleted a reply record for a client other than the oldest one "
           "when loading a newer record with client batching enabled and a number of existing reply records already "
           "equalling the maximum client batch size.";
  }

  ReplicaConfig::instance().setclientBatchingEnabled(true);
  ReplicaConfig::instance().setclientBatchingMaxMsgsNbr(client_batch_size);
  resetMockReservedPages();
  cm.reset(new ClientsManager(proxy_client_ids, external_client_ids, {}, internal_client_ids, metrics));
  for (const auto& replies : older_client_replies) {
    ReqId seq_num = replies.second[0].first;
    string reply_msg = replies.second[0].second;
    cm->allocateNewReplyMsgAndWriteToStorage(
        replies.first, seq_num, 0, reply_msg.data(), reply_msg.length(), kRSILengthForTesting);
  }
  setMockReservedPages(res_pages_with_newer_replies);
  EXPECT_NO_THROW(cm->loadInfoFromReservedPages()) << "ClientsManager::loadInfoFromReservedPages failed.";
  for (const auto& reply : client_replies) {
    EXPECT_TRUE(cm->hasReply(reply.first, reply.second.first))
        << "ClientsManager::loadInfoFromReservedPages failed to load a newer reply record from the reserved pages to a "
           "ClientsManager that already had an older reply record for the relevant client.";
    EXPECT_TRUE(cm->hasReply(reply.first, older_client_replies[reply.first][0].first))
        << "ClientsManager::loadInfoFromReservedPages deleted an existing reply record for a client when loading a "
           "newer one with client batching enabled despite having fewer existing reply records than the maximum client "
           "batch size.";
  }
}

TEST(ClientsManager, loadInfoFromReservedPagesCorrectlyDeletesOldRequests) {
  ReplicaConfig::instance().setclientBatchingEnabled(true);
  ReplicaConfig::instance().setclientBatchingMaxMsgsNbr(5);
  resetMockReservedPages();
  unique_ptr<ClientsManager> cm(new ClientsManager({1}, {2, 3}, {}, {4, 5}, metrics));

  string reply_message = "reply 6 to client 3";
  cm->allocateNewReplyMsgAndWriteToStorage(3, 6, 0, reply_message.data(), reply_message.length(), kRSILengthForTesting);

  cm.reset(new ClientsManager({1}, {2, 3}, {}, {4, 5}, metrics));
  for (ReqId i = 5; i <= 7; ++i) {
    cm->addPendingRequest(3, i, "Correlation ID");
  }
  cm->addPendingRequest(2, 5, "Correlation ID");

  EXPECT_NO_THROW(cm->loadInfoFromReservedPages()) << "ClientsManager::loadInfoFromReservedPages failed.";
  EXPECT_FALSE(cm->isClientRequestInProcess(3, 5))
      << "ClientsManager::loadInfoFromReservedPages failed to delete a request record with a sequence number less than "
         "that of a reply record to the same client in the reserved pages.";
  EXPECT_FALSE(cm->isClientRequestInProcess(3, 6))
      << "ClientsManager::loadInfoFromReservedPages failed to delete a request record with a sequence number equal to "
         "that of a reply record to the same client in the reserved pages.";
  EXPECT_TRUE(cm->isClientRequestInProcess(3, 7))
      << "ClientsManager::loadInfoFromReservedPages deleted a request record with a sequence number greater than that "
         "of a reply record to the same client in the reserved pages.";
  EXPECT_TRUE(cm->isClientRequestInProcess(2, 5))
      << "ClientsManager::loadInfoFromReservedPages deleted a request record for a client for which there should have "
         "been no reply records in the reserved pages.";
}

TEST(ClientsManager, loadInfoFromReservedPagesCorrectlyHandles0BatchSize) {
  string reply_1_message = "reply 1 to client 2";
  string reply_2_message = "reply 2 to client 2";
  string reply_message_to_client_3 = "reply 1 to client 3";

  ReplicaConfig::instance().setclientBatchingEnabled(true);
  ReplicaConfig::instance().setclientBatchingMaxMsgsNbr(0);
  resetMockReservedPages();
  unique_ptr<ClientsManager> cm(new ClientsManager({}, {1, 2, 3}, {}, {4}, metrics));
  cm->allocateNewReplyMsgAndWriteToStorage(
      2, 2, 0, reply_2_message.data(), reply_2_message.length(), kRSILengthForTesting);
  auto res_pages_with_new_reply = getMockReservedPages();

  // Note the influence of client batch size on ClientsManager::loadInfoFromReservedPages's behavior is that (when
  // client batching is enabled), if a reply record for a client is found in the reserved pages, and the number of
  // existing reply records the ClientsManager has for that client equals or exceeds the maximum client batch size, then
  // the single oldest reply record for that client is automatically deleted. Therefore, we expect that, if
  // ClientsManager::loadInfoFromReservedPages is called for a ClientsManager with client batching enabled but a batch
  // size of 0, it will delete the oldest existing reply record for any client it finds a reply for in the reserved
  // pages, but it will still successfully load the new reply record from the reserved pages. We also expect it will not
  // delete any reply records for clients for which there are no reply records in the reserved pages.

  resetMockReservedPages();
  cm.reset(new ClientsManager({}, {1, 2, 3}, {}, {4}, metrics));
  cm->allocateNewReplyMsgAndWriteToStorage(
      2, 1, 0, reply_1_message.data(), reply_1_message.length(), kRSILengthForTesting);
  cm->allocateNewReplyMsgAndWriteToStorage(
      3, 1, 0, reply_message_to_client_3.data(), reply_message_to_client_3.length(), kRSILengthForTesting);
  setMockReservedPages(res_pages_with_new_reply);
  EXPECT_NO_THROW(cm->loadInfoFromReservedPages()) << "ClientsManager::loadInfoFromReservedPages for a ClientsManager "
                                                      "configured with client batching enabled and a batch size of 0.";
  EXPECT_TRUE(cm->hasReply(2, 2))
      << "ClientsManager::loadInfoFromReservedPages failed to load a newer reply record from the reserved pages to a "
         "ClientsManager that already had an older reply record for the relevant client while configured with client "
         "batching enabled and a batch size of 0.";
  EXPECT_FALSE(cm->hasReply(2, 1))
      << "ClientsManager::loadInfoFromReservedPages failed to automatically delete the existing reply record for a "
         "client when loading a newer one with client batching enabled and a batch size of 0.";
  EXPECT_TRUE(cm->hasReply(3, 1)) << "ClientsManager::loadInfoFromReservedPages deleted a reply record for a client "
                                     "for which there should have been no reply records in the reserved pages while "
                                     "configured with client batching enabled and a batch size of 0.";
}

TEST(ClientsManager, hasReply) {
  resetMockReservedPages();
  string reply_message = "reply 9 to client 5";
  unique_ptr<ClientsManager> cm(new ClientsManager({1, 2}, {3, 4}, {}, {5, 6}, metrics));
  cm->allocateNewReplyMsgAndWriteToStorage(5, 9, 0, reply_message.data(), reply_message.length(), kRSILengthForTesting);

  EXPECT_TRUE(cm->hasReply(5, 9))
      << "ClientsManager::hasReply returned false for a reply the ClientsManager should have had a record for.";
  EXPECT_FALSE(cm->hasReply(9, 9)) << "ClientsManager::hasReply returned true when given an invalid client ID.";
  EXPECT_FALSE(cm->hasReply(5, 5)) << "ClientsManager::hasReply returned true when given a client ID/reply sequence "
                                      "number combination that it should not have had a reply record for.";

  cm->deleteOldestReply(5);
  EXPECT_FALSE(cm->hasReply(5, 9)) << "ClientsManager::hasReply returned true for a reply record it previously had but "
                                      "which should have been deleted.";
}

TEST(ClientsManager, isValidClient) {
  resetMockReservedPages();
  set<NodeIdType> proxy_client_ids{3, 6, 9};
  set<NodeIdType> external_client_ids{5, 10, 15};
  set<NodeIdType> internal_client_ids{7, 14, 21};
  NodeIdType max_id_to_check = 27;
  unique_ptr<ClientsManager> cm(
      new ClientsManager(proxy_client_ids, external_client_ids, {}, internal_client_ids, metrics));
  for (const auto& id : proxy_client_ids) {
    EXPECT_TRUE(cm->isValidClient(id)) << "ClientsManager::isValidClient returned false for a valid proxy client ID.";
  }
  for (const auto& id : external_client_ids) {
    EXPECT_TRUE(cm->isValidClient(id))
        << "ClientsManager::isValidClient returned false for a valid external client ID.";
  }
  for (const auto& id : internal_client_ids) {
    EXPECT_TRUE(cm->isValidClient(id))
        << "ClientsManager::isValidClient returned false for a valid internal client ID.";
  }
  for (NodeIdType i = 0; i <= max_id_to_check; ++i) {
    if ((proxy_client_ids.count(i) < 1) && (external_client_ids.count(i) < 1) && (internal_client_ids.count(i) < 1)) {
      EXPECT_FALSE(cm->isValidClient(i)) << "ClientsManager::isValidClient returned true for an invalid client ID.";
    }
  }

  proxy_client_ids = {2, 3, 6, 7, 10, 11, 14, 15};
  external_client_ids = {4, 5, 6, 7, 12, 13, 14, 15};
  internal_client_ids = {8, 9, 10, 11, 12, 13, 14, 15};
  max_id_to_check = 24;
  cm.reset(new ClientsManager(proxy_client_ids, external_client_ids, {}, internal_client_ids, metrics));
  for (NodeIdType i = 0; i <= max_id_to_check; ++i) {
    if ((proxy_client_ids.count(i) > 0) || (external_client_ids.count(i) > 0) || (internal_client_ids.count(i) > 0)) {
      EXPECT_TRUE(cm->isValidClient(i))
          << "ClientsManager::isValidClient failed to correctly identify a valid client ID for a ClientsManager with "
             "some clients that are classified as multiple types.";
    } else {
      EXPECT_FALSE(cm->isValidClient(i))
          << "ClientsManager::isValidClient failed to correctly identify an invalid client ID for a ClientsManager "
             "with some clients that are classified as multiple types.";
    }
  }
}

TEST(ClientsManager, allocateNewReplyMsgAndWriteToStorageWorksCorrectlyInTheGeneralCase) {
  resetMockReservedPages();
  const string kExpectedMessageBody = "This is an arbitrary reply message.";
  string message_body = kExpectedMessageBody;
  unique_ptr<ClientsManager> cm(new ClientsManager({}, {1, 2, 4}, {}, {5, 7}, metrics));

  unique_ptr<ClientReplyMsg> message = cm->allocateNewReplyMsgAndWriteToStorage(
      4, 3, 8, message_body.data(), message_body.length(), kRSILengthForTesting);
  EXPECT_TRUE(cm->hasReply(4, 3)) << "ClientsManager::allocateNewReplyMsgAndWriteToStorage failed to add a reply "
                                     "record for the allocated reply to the ClientsManager.";
  ASSERT_TRUE(message)
      << "ClientsManager::allocateNewReplyMsgAndWriteToStorage returned a null pointer when given valid input.";
  EXPECT_EQ(message->reqSeqNum(), 3)
      << "ClientsManager::allocateNewReplyMsgAndWriteToStorage returned an allocated ClientReplyMsg with a reqSeqNum "
         "value not matching the request sequence number given as input.";
  EXPECT_EQ(message->currentPrimaryId(), 8)
      << "ClientsManager::allocateNewReplyMsgAndWriteToStorage returned an allocated ClientReplyMsg with a "
         "currentPrimaryId value not matching the current primary ID given as input.";
  string allocated_reply_body(message->replyBuf(), message->replyLength());
  EXPECT_EQ(allocated_reply_body, kExpectedMessageBody)
      << "ClientsManager::allocateNewReplyMsgAndWriteToStorage returned an allocated ClientReplyMsg with a payload not "
         "matching the reply given as input.";
  EXPECT_EQ(message_body, kExpectedMessageBody)
      << "ClientsManager::allocateNewReplyMsgAndWriteToStorage unexpectedly changed the original reply body in the "
         "buffer it it was passed as input.";

  cm.reset(new ClientsManager({}, {1, 2, 4}, {}, {5, 7}, metrics));
  cm->loadInfoFromReservedPages();
  EXPECT_TRUE(cm->hasReply(4, 3)) << "ClientsManager::allocateNewReplyMsgAndWriteToStorage failed to write a correct "
                                     "reply record to the reserved pages.";
  message = cm->allocateReplyFromSavedOne(4, 3, 8);
  ASSERT_TRUE(message) << "ClientsManager::allocateNewReplyMsgAndWriteToStorage failed to correcty write the message "
                          "it allocated to the reserved pages.";
  allocated_reply_body = string(message->replyBuf(), message->replyLength());
  EXPECT_EQ(allocated_reply_body, kExpectedMessageBody)
      << "ClientsManager::allocateNewReplyMsgAndWriteToStorage failed to correctly write the message it allocated to "
         "the reserved pages.";

  const string kNewExpectedMessageBody = "This is an arbitrary newer revision of the previous message.";
  message_body = kNewExpectedMessageBody;
  message = cm->allocateNewReplyMsgAndWriteToStorage(
      4, 3, 9, message_body.data(), message_body.length(), kRSILengthForTesting);
  ASSERT_TRUE(message) << "ClientsManager::allocateNewReplyMsgAndWriteToStorage returned a null pointer when given a "
                          "newer replacement for an existing message client ID/sequence number combination.";
  EXPECT_EQ(message->currentPrimaryId(), 9)
      << "ClientsManager::allocateNewReplyMsgAndWriteToStorage returned an allocated ClientReplyMsg with a "
         "currentPrimaryId value not matching the new current primary ID given as input for a newer replacement for an "
         "existing message client ID/sequence number combination.";
  allocated_reply_body = string(message->replyBuf(), message->replyLength());
  EXPECT_EQ(allocated_reply_body, kNewExpectedMessageBody)
      << "ClientsManager::allocateNewReplyMsgAndWriteToStorage returned an allocated ClientReplyMsg with a payload not "
         "matching the updated one when given a newer replacement for an existing message client ID/sequence number "
         "combination.";
  EXPECT_EQ(message_body, kNewExpectedMessageBody)
      << "ClientsManager::allocateNewReplyMsgAndWriteToStorage unexpectedly changed the original reply body in the "
         "buffer it was passed as input for a newer replacement for an existing message client ID/sequence number "
         "combination.";

  cm.reset(new ClientsManager({}, {1, 2, 4}, {}, {5, 7}, metrics));
  cm->loadInfoFromReservedPages();
  message = cm->allocateReplyFromSavedOne(4, 3, 9);
  ASSERT_TRUE(message) << "ClientsManager::allocateNewReplyMsgAndWriteToStorage failed to correctly write the message "
                          "it allocated to the reserved pages when given a newer replacement for an existing message "
                          "client ID/sequence number combination.";
  allocated_reply_body = string(message->replyBuf(), message->replyLength());
  EXPECT_EQ(allocated_reply_body, kNewExpectedMessageBody)
      << "ClientsManager::allocateNewReplyMsgAndWriteToStorage failed to correctly write the message it allocated to "
         "the reserved pages when given a newer replacement for an existing message client ID/sequence number "
         "combination.";
}

TEST(ClientsManager, allocateNewReplyMsgAndWriteToStorageCorrectlyDeletesOldReplies) {
  resetMockReservedPages();
  string reply_1_to_5_message = "reply 1 to client 5";
  string reply_4_to_5_message = "reply 4 to client 5";
  string reply_6_to_5_message = "reply 6 to client 5";
  string reply_1_to_10_message = "reply 1 to client 10";

  ReplicaConfig::instance().setclientBatchingEnabled(false);
  unique_ptr<ClientsManager> cm(new ClientsManager({2, 7}, {1, 3, 5}, {}, {10, 11}, metrics));
  cm->allocateNewReplyMsgAndWriteToStorage(
      5, 1, 0, reply_1_to_5_message.data(), reply_1_to_5_message.length(), kRSILengthForTesting);
  cm->allocateNewReplyMsgAndWriteToStorage(
      10, 1, 0, reply_1_to_10_message.data(), reply_1_to_10_message.length(), kRSILengthForTesting);
  cm->allocateNewReplyMsgAndWriteToStorage(
      5, 6, 0, reply_6_to_5_message.data(), reply_6_to_5_message.length(), kRSILengthForTesting);
  EXPECT_TRUE(cm->hasReply(5, 6))
      << "ClientsManager::allocateNewReplyMsgAndWriteToStorage failed to add a new reply record for a client which "
         "already had an existing reply record with client batching disabled.";
  EXPECT_FALSE(cm->hasReply(5, 1))
      << "ClientsManager::allocateNewReplyMsgAndWriteToStorage failed to delete the existing reply record for a client "
         "when adding a new one with client batching disabled.";
  EXPECT_TRUE(cm->hasReply(10, 1)) << "ClientsManager::allocateNewReplyMsgAndWriteToStorage deleted a reply record for "
                                      "an unrelated client when loading a new reply record for a different client.";

  ReplicaConfig::instance().setclientBatchingEnabled(true);
  ReplicaConfig::instance().setclientBatchingMaxMsgsNbr(2);
  cm.reset(new ClientsManager({2, 7}, {1, 3, 5}, {}, {10, 11}, metrics));
  cm->allocateNewReplyMsgAndWriteToStorage(
      5, 1, 0, reply_1_to_5_message.data(), reply_1_to_5_message.length(), kRSILengthForTesting);
  cm->allocateNewReplyMsgAndWriteToStorage(
      5, 4, 0, reply_4_to_5_message.data(), reply_4_to_5_message.length(), kRSILengthForTesting);
  EXPECT_TRUE(cm->hasReply(5, 4)) << "ClientsManager::allocateNewReplyMsgAndWriteToStorage failed to add a new reply "
                                     "record for a client with client batching enabled and a number of existing reply "
                                     "records for that client less than the maximum client batch size.";
  EXPECT_TRUE(cm->hasReply(5, 1))
      << "ClientsManager::allocateNewReplyMsgAndWriteToStorage deleted an existing reply record for a client when "
         "loading a new one with client batching enabled, despite the number of existing reply records for that client "
         "being less than the maximum client batch size.";

  cm->allocateNewReplyMsgAndWriteToStorage(
      5, 6, 0, reply_6_to_5_message.data(), reply_6_to_5_message.length(), kRSILengthForTesting);
  EXPECT_TRUE(cm->hasReply(5, 6)) << "ClientsManager::allocateNewReplyMsgAndWriteToStorage failed to add a new reply "
                                     "record for a client with client batching enabled and a number of existing reply "
                                     "records for that client equal to the maximum client batch size.";
  EXPECT_FALSE(cm->hasReply(5, 1))
      << "ClientsManager::allocateNewReplyMsgAndWriteToStorage failed to delete the oldest existing reply record for a "
         "client when adding a new one with client batching enabled and when the number of existing replies was equal "
         "to the maximum client batch size.";
  EXPECT_TRUE(cm->hasReply(5, 4))
      << "ClientsManager::allocateNewReplyMsgAndWriteToStorage deleted an existing reply record for a client other "
         "than the oldest one when adding a new reply for that client with client batching enabled and a number of "
         "existing reply records equal to the maximum client batch size.";
}

TEST(ClientsManager, allocateNewReplyMsgAndWriteToStorageHandlesApplicableCornerCases) {
  resetMockReservedPages();
  string empty_reply = "";

  unique_ptr<ClientsManager> cm(new ClientsManager({}, {1, 2, 3}, {}, {4}, metrics));
  unique_ptr<ClientReplyMsg> message =
      cm->allocateNewReplyMsgAndWriteToStorage(1, 0, 0, empty_reply.data(), empty_reply.length(), kRSILengthForTesting);
  ASSERT_TRUE(message) << "ClientsManager::allocateNewReplyMsgAndWriteToStorage returned a null pointer when "
                          "attempting to allocate a ClientReplyMsg with an empty payload.";
  string allocated_reply_body = string(message->replyBuf(), message->replyLength());
  EXPECT_EQ(allocated_reply_body, "") << "ClientsManager::allocateNewReplyMsgAndWriteToStorage returned an allocated "
                                         "ClientReplyMsg with a non-empty payload when called with an empty payload.";
}

TEST(ClientsManager, allocateReplyFromSavedOneWorksCorrectlyInTheGeneralCase) {
  resetMockReservedPages();
  ReplicaConfig::instance().setclientBatchingEnabled(false);
  const string kExpectedMessageBody = "reply 3 to client 1";
  string reply = kExpectedMessageBody;

  unique_ptr<ClientsManager> cm(new ClientsManager({8}, {1, 2}, {}, {4}, metrics));
  cm->allocateNewReplyMsgAndWriteToStorage(1, 3, 9, reply.data(), reply.length(), kRSILengthForTesting);

  cm.reset(new ClientsManager({8}, {1, 2}, {}, {4}, metrics));
  unique_ptr<ClientReplyMsg> message = cm->allocateReplyFromSavedOne(1, 3, 12);
  ASSERT_TRUE(message) << "ClientsManager::allocateReplyFromSavedOne returned a null pointer when the requested reply "
                          "message should have been available in the reserved pages.";
  EXPECT_EQ(message->reqSeqNum(), 3)
      << "ClientsManager::allocateReplyFromSavedOne returned an allocated ClientReplyMsg with a reqSeqNum value not "
         "matching the requested sequence number.";
  EXPECT_EQ(message->currentPrimaryId(), 12)
      << "ClientsManager::allocateReplyFromSavedOne returned an allocated ClientReplyMsg with a currentPrimaryId value "
         "not matching the one passed as a parameter to it.";
  string allocated_reply_body = string(message->replyBuf(), message->replyLength());
  EXPECT_EQ(allocated_reply_body, kExpectedMessageBody)
      << "ClientsManager::allocateReplyFromSavedOne returned an allocated ClientReplyMsg with a paylod not matching "
         "the message that should have been in the reserved pages.";
}

TEST(ClientsManager, allocateReplyFromSavedOneCorrectlyHandlesCasesWithClientBatchingEnabled) {
  resetMockReservedPages();
  ReplicaConfig::instance().setclientBatchingEnabled(true);
  ReplicaConfig::instance().setclientBatchingMaxMsgsNbr(2);
  const string kExpectedMessageBody = "reply 3 to client 1";
  string reply = kExpectedMessageBody;

  unique_ptr<ClientsManager> cm(new ClientsManager({8}, {1, 2}, {}, {4}, metrics));
  cm->allocateNewReplyMsgAndWriteToStorage(1, 3, 9, reply.data(), reply.length(), kRSILengthForTesting);

  cm.reset(new ClientsManager({8}, {1, 2}, {}, {4}, metrics));
  unique_ptr<ClientReplyMsg> message = cm->allocateReplyFromSavedOne(1, 3, 12);
  ASSERT_TRUE(message)
      << "ClientsManager::allocateReplyFromSavedOne returned a null pointer when the requested reply message should "
         "have been available in the reserved pages in the case where client batching is enabled.";
  EXPECT_EQ(message->reqSeqNum(), 3)
      << "ClientsManager::allocateReplyFromSavedOne returned an allocated ClientReplyMsg with a reqSeqNum value not "
         "matching the requested sequence number.";
  string allocated_reply_body = string(message->replyBuf(), message->replyLength());
  EXPECT_EQ(allocated_reply_body, kExpectedMessageBody)
      << "ClientsManager::allocateReplyFromSavedOne returned an allocated ClientReplyMsg with a paylod not matching "
         "the message that should have been in the reserved pages.";

  message = cm->allocateReplyFromSavedOne(1, 8, 12);
  EXPECT_FALSE(message)
      << "ClientsManger::allocateReplyFromSavedOne returned a non-null pointer when given a sequence number not "
         "matching the reply that should have been in the reserved pages in the case where client batching is enabled.";
}

TEST(ClientsManager, isClientRequestInProcess) {
  resetMockReservedPages();

  unique_ptr<ClientsManager> cm(new ClientsManager({1, 6}, {2, 8, 14}, {}, {3, 10}, metrics));
  set<NodeIdType> all_client_ids{1, 6, 2, 8, 14, 3, 10};
  set<pair<NodeIdType, ReqId>> requests_in_process{{6, 3}, {6, 6}, {2, 1}, {2, 2}, {14, 8}, {3, 3}, {3, 4}, {3, 5}};
  ReqId max_req_id_to_check = 10;
  for (const auto& request : requests_in_process) {
    cm->addPendingRequest(request.first, request.second, "correlation ID");
  }
  for (const auto& client_id : all_client_ids) {
    for (ReqId i = 0; i <= max_req_id_to_check; ++i) {
      if (requests_in_process.count({client_id, i}) > 0) {
        EXPECT_TRUE(cm->isClientRequestInProcess(client_id, i))
            << "ClientsManager::isClientRequestInProcess returned false for a request the ClientsManager should have "
               "had a record for.";
      } else {
        EXPECT_FALSE(cm->isClientRequestInProcess(client_id, i))
            << "ClientsManager::isClientRequestInProcess returned true for a client ID/request sequence number "
               "combination the ClientsManager should not have had a record for.";
      }
    }
  }

  EXPECT_FALSE(cm->isClientRequestInProcess(12, 3))
      << "ClientsManager::isClientRequestInProcess returned true when given an invalid client ID.";

  pair<NodeIdType, ReqId> request_to_remove = *(requests_in_process.begin());
  cm->removePendingForExecutionRequest(request_to_remove.first, request_to_remove.second);
  EXPECT_FALSE(cm->isClientRequestInProcess(request_to_remove.first, request_to_remove.second))
      << "ClientsManager::isClientRequestInProcess returned true for a request after that request was removed from the "
         "ClientsManager.";
}

TEST(ClientsManager, canBecomePending) {
  resetMockReservedPages();

  ReplicaConfig::instance().setclientBatchingEnabled(true);
  ReplicaConfig::instance().setclientBatchingMaxMsgsNbr(2);
  unique_ptr<ClientsManager> cm(new ClientsManager({}, {2, 3, 5, 7}, {}, {1}, metrics));
  cm->addPendingRequest(3, 2, "correlation ID");
  EXPECT_TRUE(cm->canBecomePending(3, 3))
      << "ClientsManager::canBecomePending returned false for a client ID/request sequence number combination it was "
         "expected to return true for (specifically, a combination for which there should be no existing request "
         "record in the ClientsManager and for a client which should have had fewer existing request records than the "
         "configured maximum client batch size with client batching enabled).";
  EXPECT_TRUE(cm->canBecomePending(2, 3))
      << "ClientsManager::canBecomePending returned false for a client ID/request sequence number combination it was "
         "expected to return true for (specifically, a combination for which there should be no existing request "
         "record in the ClientsManager and for a client which should have had no existing request records).";

  EXPECT_FALSE(cm->canBecomePending(8, 3))
      << "ClientsManager::canBecomePending returned true for an invalid client ID.";
  EXPECT_FALSE(cm->canBecomePending(3, 2)) << "ClientsManager::canBecomePending returned true for a request the "
                                              "ClientsManager should have already had an existing request record for.";

  cm->addPendingRequest(3, 3, "correlation ID");
  EXPECT_FALSE(cm->canBecomePending(3, 4))
      << "ClientsManager::canBecomePending returned true for a client ID for which the ClientsManager should have "
         "already had a number of existing request records equal to the maximum client batch size with client batching "
         "enabled.";

  cm->removePendingForExecutionRequest(3, 3);
  EXPECT_TRUE(cm->canBecomePending(3, 3))
      << "ClientsManager::canBecomePending returned false for a recently removed request record whose removal should "
         "have allowed canBecomePending to return true for it.";

  string reply = "This is an arbitrary reply message.";
  cm->allocateNewReplyMsgAndWriteToStorage(5, 1, 0, reply.data(), reply.length(), kRSILengthForTesting);
  EXPECT_FALSE(cm->canBecomePending(5, 1))
      << "ClientsManager::canBecomePending returned true for a client ID/request sequence number combination for which "
         "the ClientsManager should have had an existing reply record.";

  ReplicaConfig::instance().setclientBatchingEnabled(false);
  cm.reset(new ClientsManager({}, {2, 3, 5, 7}, {}, {1}, metrics));
  cm->addPendingRequest(3, 2, "correlation ID");
  EXPECT_FALSE(cm->canBecomePending(3, 3))
      << "ClientsManager::canBecomePending returned true for a client ID for which the ClientsManager should have "
         "already had an existing request record with client batching disabled.";
}

TEST(ClientsManager, isPending) {
  resetMockReservedPages();

  unique_ptr<ClientsManager> cm(new ClientsManager({6, 8}, {10, 11, 12}, {}, {2, 4}, metrics));
  cm->addPendingRequest(2, 5, "correlation ID");
  EXPECT_TRUE(cm->isPending(2, 5))
      << "ClientsManager::isPending returned false for a request that should have been pending.";

  EXPECT_FALSE(cm->isPending(5, 5)) << "ClientsManager::isPending returned true when given an invalid client ID.";
  EXPECT_FALSE(cm->isPending(2, 6)) << "ClientsManager::isPending returned true for a request for which the "
                                       "ClientsManager should not have had any record.";

  cm->markRequestAsCommitted(2, 5);
  EXPECT_FALSE(cm->isPending(2, 5))
      << "ClientsManager::isPending returned true for a request which was marked as committed.";

  cm->addPendingRequest(2, 6, "correlation ID");
  cm->removePendingForExecutionRequest(2, 6);
  EXPECT_FALSE(cm->isPending(2, 6)) << "ClientsManager::isPending returned true for a request after that request "
                                       "record should have already been removed from the ClientsManager.";
}

TEST(ClientsManager, addPendingRequest) {
  resetMockReservedPages();

  string expected_cid = "expected correlation ID";
  string observed_cid;
  unique_ptr<ClientsManager> cm(new ClientsManager({1, 2, 3, 4}, {}, {}, {}, metrics));
  cm->addPendingRequest(3, 1, expected_cid);
  EXPECT_TRUE(cm->isClientRequestInProcess(3, 1))
      << "ClientsManager::addPendingRequest failed to add a request record to the ClientsManager.";
  EXPECT_TRUE(cm->isPending(3, 1))
      << "ClientsManager::addPendingRequest failed to add a request record initially not marked as committed.";
  Time original_time = cm->infoOfEarliestPendingRequest(observed_cid);
  EXPECT_EQ(expected_cid, observed_cid) << "ClientsManager::addPendingRequest appears to have added a request record "
                                           "with a CID not matching the one it was given as a parameter.";

  EXPECT_NO_THROW(cm->addPendingRequest(3, 1, "This string is not \"" + expected_cid + "\"."))
      << "ClientsManager::addPendingRequest failed when trying to add a request that already existed.";
  EXPECT_TRUE(cm->isClientRequestInProcess(3, 1))
      << "ClientsManager::addPendingRequest appears to have removed the existing request record when trying to add a "
         "request that should have already existed.";
  Time new_time = cm->infoOfEarliestPendingRequest(observed_cid);
  EXPECT_EQ(expected_cid, observed_cid)
      << "ClientsManager::addPendingRequest appears to have altered the CID of the existing request when trying to add "
         "a request that should have already existed.";
  EXPECT_EQ(original_time, new_time)
      << "ClientsManager::addPendingRequest appears to have altered the recorded time for the existing request record "
         "when trying to add a request that should have already existed.";

  cm->markRequestAsCommitted(3, 1);
  cm->addPendingRequest(3, 1, expected_cid);
  EXPECT_FALSE(cm->isPending(3, 1))
      << "ClientsManager::addPendingRequest appears to have un-marked an existing request record marked as committed "
         "when trying to add a rquest that should have already existed.";
}

TEST(ClientsManager, markRequestAsCommitted) {
  resetMockReservedPages();

  unique_ptr<ClientsManager> cm(new ClientsManager({8}, {4, 5}, {}, {9}, metrics));
  cm->addPendingRequest(5, 4, "correlation ID");
  cm->markRequestAsCommitted(5, 4);
  EXPECT_TRUE(cm->isClientRequestInProcess(5, 4))
      << "ClientsManager::markRequestAsCommitted appears to have completely deleted the request record it was called "
         "to mark as committed.";
  EXPECT_FALSE(cm->isPending(5, 4))
      << "ClientsManager::markRequestAsCommitted failed to mark the requested request record as committed.";

  EXPECT_NO_THROW(cm->markRequestAsCommitted(5, 5)) << "ClientsManager::markRequestAsCommitted failed when called with "
                                                       "a request for which no record should have existed.";
  EXPECT_FALSE(cm->isClientRequestInProcess(5, 5))
      << "ClientsManager::markRequestAsCommitted appears to have created a record for the given request for which no "
         "record should have previously existed.";
  EXPECT_NO_THROW(cm->markRequestAsCommitted(5, 4)) << "ClientsManager::markRequestAsCommitted failed when called with "
                                                       "a request that was already marked as committed.";
  EXPECT_TRUE(cm->isClientRequestInProcess(5, 4) && !(cm->isPending(5, 4)))
      << "ClientsManager::markRequestAsCommitted unexpectedly affected the relevant request record when called for a "
         "request that was already marked as committed.";
}

TEST(ClientsManager, removeRequestsOutOfBatchBounds) {
  resetMockReservedPages();

  static_assert(maxNumOfRequestsInBatch > 2,
                "This unit test for ClientsManager::removeRequestsOutOfBatchBounds assumes a minimum value for the "
                "global system constant maxNumOfRequestsInBatch (which influences removeRequestsOutOfBatchBounds's "
                "behavior), but that assumption has not been met.");

  unique_ptr<ClientsManager> cm(new ClientsManager({8, 12}, {4, 5, 7}, {}, {10, 11}, metrics));
  for (uint32_t i = 1; i < maxNumOfRequestsInBatch; ++i) {
    cm->addPendingRequest(8, i, "correlation ID");
  }
  cm->addPendingRequest(8, maxNumOfRequestsInBatch + 1, "correlation ID");

  cm->removeRequestsOutOfBatchBounds(8, maxNumOfRequestsInBatch);
  EXPECT_FALSE(cm->isClientRequestInProcess(8, maxNumOfRequestsInBatch + 1))
      << "ClientsManager::removeRequestOutOfBatchBounds failed to remove the request record with the greatest sequence "
         "number when that sequence number exceeded the one passed to it as a parameter, that sequence number passed "
         "as a parameter did not match any request records the ClientsManager should have had, and when the "
         "ClientsManager should have had maxNumOfRequestsInBatch request records.";
  for (uint32_t i = 1; i < maxNumOfRequestsInBatch; ++i) {
    EXPECT_TRUE(cm->isClientRequestInProcess(8, i))
        << "ClientsManager::removeRequestsOutOfBatchBounds removed a request record other than the one with the "
           "greatest sequence number.";
  }

  cm->addPendingRequest(8, maxNumOfRequestsInBatch, "correlation ID");
  cm->removeRequestsOutOfBatchBounds(8, maxNumOfRequestsInBatch + 1);
  EXPECT_TRUE(cm->isClientRequestInProcess(8, maxNumOfRequestsInBatch))
      << "ClientsManager::removeRequestsOutOfBatchBounds removed the request record with the greatest sequence number "
         "in spite of that sequence number being less than the one passed to it as a parameter.";
  cm->removeRequestsOutOfBatchBounds(8, maxNumOfRequestsInBatch);
  EXPECT_TRUE(cm->isClientRequestInProcess(8, maxNumOfRequestsInBatch))
      << "ClientsManager::removeRequestsOutOfBatchBounds removed the request record with the greatest sequence number "
         "in spite of that sequence number equalling the one passed to it as a parameter.";

  cm->removePendingForExecutionRequest(8, maxNumOfRequestsInBatch - 1);
  cm->removeRequestsOutOfBatchBounds(8, maxNumOfRequestsInBatch - 1);
  EXPECT_TRUE(cm->isClientRequestInProcess(8, maxNumOfRequestsInBatch))
      << "ClientsManager::removeRequestsOutOfBatchBounds removed the request record with the greatest sequence number "
         "when it should have had fewer than maxNumOfRequestsInBatch request records.";

  cm->addPendingRequest(8, maxNumOfRequestsInBatch + 1, "correlation ID");
  cm->removeRequestsOutOfBatchBounds(8, maxNumOfRequestsInBatch);
  EXPECT_TRUE(cm->isClientRequestInProcess(8, maxNumOfRequestsInBatch + 1))
      << "ClientsManager::removeRequestsOutOfBatchBounds removed the request record with the greatest sequence number "
         "when it should have had a request record with sequence number exactly matching the one given as a parameter.";
}

TEST(ClientsManager, removePendingForExecutionRequest) {
  resetMockReservedPages();

  unique_ptr<ClientsManager> cm(new ClientsManager({3, 5}, {2, 4, 6}, {}, {}, metrics));
  cm->addPendingRequest(4, 3, "correlation ID");
  cm->addPendingRequest(4, 6, "correlation ID");
  cm->addPendingRequest(5, 3, "correlation ID");

  cm->removePendingForExecutionRequest(4, 3);
  EXPECT_FALSE(cm->isClientRequestInProcess(4, 3))
      << "ClientsManager::removePendingForExecutionRequest failed to remove the specified request record.";
  EXPECT_TRUE(cm->isClientRequestInProcess(4, 6) && cm->isClientRequestInProcess(5, 3))
      << "ClientsManager::removePendingForExecutionRequest removed a client request record other than the requested "
         "one.";

  EXPECT_NO_THROW(cm->isClientRequestInProcess(4, 12))
      << "ClientsManager::removePendingForExecutionRequest failed when given a request that should not exist.";
  EXPECT_TRUE(cm->isClientRequestInProcess(4, 6) && cm->isClientRequestInProcess(5, 3))
      << "ClientsManager::removePendingForExecutionRequest removed a client request record other than the requested "
         "one when given a request that should not exist.";
  EXPECT_NO_THROW(cm->isClientRequestInProcess(8, 3))
      << "ClientsManager::removePendingForExecutionRequest failed when given an invalid client ID.";
  EXPECT_TRUE(cm->isClientRequestInProcess(4, 6) && cm->isClientRequestInProcess(5, 3))
      << "ClientsManager::removePendingForExecutionRequest removed a client request record other than the requested "
         "one when given an invalid client ID.";
}

TEST(ClientsManager, clearAllPendingRequests) {
  resetMockReservedPages();

  unique_ptr<ClientsManager> cm(new ClientsManager({}, {1, 3, 4, 6}, {}, {8, 9}, metrics));
  set<pair<NodeIdType, ReqId>> requests{{1, 9}, {1, 12}, {1, 14}, {4, 4}, {4, 8}, {6, 5}, {8, 1}, {8, 2}, {9, 3}};
  for (const auto& request : requests) {
    cm->addPendingRequest(request.first, request.second, "correlation ID");
  }

  cm->clearAllPendingRequests();
  for (const auto& request : requests) {
    EXPECT_FALSE(cm->isClientRequestInProcess(request.first, request.second))
        << "ClientsManager::clearAllPendingRequests failed to remove a request record.";
  }

  EXPECT_NO_THROW(cm->clearAllPendingRequests())
      << "ClientsManager::clearAllPendingRequests failed when given a ClientsManager with no request records.";
}

TEST(ClientsManager, infoOfEarliestPendingRequest) {
  resetMockReservedPages();

  unique_ptr<ClientsManager> cm(new ClientsManager({7}, {6, 8, 12}, {}, {9}, metrics));
  cm->addPendingRequest(12, 2, "earliest correlation ID");
  cm->addPendingRequest(12, 3, "middle correlation ID");
  cm->addPendingRequest(9, 5, "latest correlation ID");

  string observed_cid;
  Time earliest_time = cm->infoOfEarliestPendingRequest(observed_cid);
  EXPECT_EQ(observed_cid, "earliest correlation ID")
      << "ClientsManager::infoOfEarliestPendingRequest did not return the correct CID for the earliest request.";

  cm->markRequestAsCommitted(12, 2);
  Time middle_time = cm->infoOfEarliestPendingRequest(observed_cid);
  EXPECT_EQ(observed_cid, "middle correlation ID")
      << "ClientsManager::infoOfEarliestPendingRequests did not return the correct CID for the earliest pending "
         "request in the presence of an earlier request marked as committed (which should no longer considered "
         "pending).";

  cm->removePendingForExecutionRequest(12, 3);
  Time latest_time = cm->infoOfEarliestPendingRequest(observed_cid);
  EXPECT_TRUE((earliest_time <= middle_time) && (middle_time <= latest_time))
      << "ClientManager::infoOfEarliestPendingRequest returned time values for requests which do not appear to be "
         "monotonically increasing with respect to the order the requests were added.";

  cm.reset(new ClientsManager({7}, {6, 8, 12}, {}, {9}, metrics));
  Time time_given_no_request_records = cm->infoOfEarliestPendingRequest(observed_cid);
  EXPECT_EQ(time_given_no_request_records, MaxTime) << "ClientsManager::infoOfEarliestPendingRequest returned a time "
                                                       "other than MaxTime in the absence of any request records.";
  EXPECT_EQ(observed_cid, "") << "ClientsManager::infoOfEarliestPendingRequest returned a CID other than an empty "
                                 "string in the absence of any request records.";

  cm->addPendingRequest(7, 4, "correlation ID");
  cm->markRequestAsCommitted(7, 4);
  cm->addPendingRequest(6, 7, "correlation ID");
  cm->markRequestAsCommitted(6, 7);

  time_given_no_request_records = cm->infoOfEarliestPendingRequest(observed_cid);
  EXPECT_EQ(time_given_no_request_records, MaxTime)
      << "ClientsManager::infoOfEarliestPendingRequest returned a time other than MaxTime for a ClientsManager that "
         "should have only had committed request records.";
  EXPECT_EQ(observed_cid, "") << "ClientsManager::infoOfEarliestPendingRequest returned a CID other than an empty "
                                 "string for a ClientsManager that should have had only committed request records.";
}

TEST(ClientsManager, logAllPendingRequestsExceedingThreshold) {
  resetMockReservedPages();

  unique_ptr<ClientsManager> cm(new ClientsManager({7}, {6, 8, 12}, {}, {9}, metrics));
  int64_t threshold = 50;
  cm->addPendingRequest(6, 2, "correlation ID");
  cm->addPendingRequest(6, 3, "correlation ID");
  cm->addPendingRequest(12, 4, "correlation ID");
  sleep_for(milliseconds(threshold * 2));
  cm->addPendingRequest(12, 8, "correlation ID");
  cm->addPendingRequest(9, 3, "correlation ID");
  Time log_time = getMonotonicTime();

  // Note that we do not actually test what logAllPendingRequestsExceedingThreshold logs for a few reasons, including
  // that the exact contents and format of log messages it generates could be considered an implementation detail and
  // not something formally defined by ClientsManager's interface and that a clean and straightforward way of checking
  // log output in these unit tests has not been identified. However, we do expect that
  // logAllPendingRequestsExceedingThreshold should not crash, throw an exception, or otherwise non-gracefully fail for
  // any input for which its behavior is defined.

  EXPECT_NO_THROW(cm->logAllPendingRequestsExceedingThreshold(threshold, log_time))
      << "ClientsManager::logAllPendingRequestsExceedingThreshold failed.";

  EXPECT_NO_THROW(cm->logAllPendingRequestsExceedingThreshold(0, log_time))
      << "ClientsManager::logAllPendingRequestsExceedingThreshold failed when given a threshold value of 0.";
  EXPECT_NO_THROW(cm->logAllPendingRequestsExceedingThreshold(-1, log_time))
      << "ClientsManager::logAllPendingRequestsExceedingThreshold failed when given a threshold value of -1.";
  EXPECT_NO_THROW(cm->logAllPendingRequestsExceedingThreshold(-2 * threshold, log_time))
      << "ClientsManager::logAllPendingRequestsExceedingThreshold failed when given a threshold value of "
      << (-2 * threshold) << ".";
  EXPECT_NO_THROW(cm->logAllPendingRequestsExceedingThreshold(INT64_MIN, log_time))
      << "ClientsManager::logAllPendingRequestsExceedingThreshold failed when given a threshold value of " << INT64_MIN
      << ".";
  EXPECT_NO_THROW(cm->logAllPendingRequestsExceedingThreshold(INT64_MAX, log_time))
      << "ClientsManager::logAllPendingRequestsExceedingThreshold failed when given a threshold value of " << INT64_MAX
      << ".";
  EXPECT_NO_THROW(cm->logAllPendingRequestsExceedingThreshold(threshold, Time::min()))
      << "ClientsManager::logAllPendingRequestsExceedingThreshold failed when given the minimum possible current time "
         "value.";
  EXPECT_NO_THROW(cm->logAllPendingRequestsExceedingThreshold(threshold, Time::max()))
      << "ClientsManager::logAllPendingRequestsExceedingThreshold failed when given the maximum possible current time "
         "value.";
  EXPECT_NO_THROW(cm->logAllPendingRequestsExceedingThreshold(-2 * threshold, Time::min()))
      << "ClientsManager::logAllPendingRequestsExceedingThreshold failed when given the minimum possible current time "
         "value and a threshold value of "
      << (-2 * threshold) << ".";

  cm->markRequestAsCommitted(6, 2);
  cm->markRequestAsCommitted(6, 3);
  cm->markRequestAsCommitted(12, 4);
  cm->markRequestAsCommitted(12, 8);
  cm->markRequestAsCommitted(9, 3);
  EXPECT_NO_THROW(cm->logAllPendingRequestsExceedingThreshold(threshold, log_time))
      << "ClientsManager::logAllPendingRequestsExceedingThreshold failed for a ClientsManager with only committed "
         "request records.";

  cm->clearAllPendingRequests();
  EXPECT_NO_THROW(cm->logAllPendingRequestsExceedingThreshold(threshold, log_time))
      << "ClientsManager::logAllPendingRequestsExceedingThreshold failed for a ClientsManager with no request records.";
}

TEST(ClientsManager, deleteOldestReply) {
  resetMockReservedPages();
  ReplicaConfig::instance().setclientBatchingEnabled(true);
  ReplicaConfig::instance().setclientBatchingMaxMsgsNbr(3);
  milliseconds brief_delay(5);
  string reply_2 = "repy 2";
  string reply_6 = "repy 6";
  string reply_8 = "repy 8";

  unique_ptr<ClientsManager> cm(new ClientsManager({5, 8}, {6, 7, 9}, {}, {11}, metrics));
  cm->allocateNewReplyMsgAndWriteToStorage(8, 6, 0, reply_6.data(), reply_6.size(), kRSILengthForTesting);
  sleep_for(brief_delay);
  cm->allocateNewReplyMsgAndWriteToStorage(8, 8, 0, reply_8.data(), reply_8.size(), kRSILengthForTesting);
  sleep_for(brief_delay);
  cm->allocateNewReplyMsgAndWriteToStorage(8, 2, 0, reply_2.data(), reply_2.size(), kRSILengthForTesting);

  cm->deleteOldestReply(8);
  EXPECT_FALSE(cm->hasReply(8, 6))
      << "ClientsManager::deleteOldestReply failed to delete the oldest reply record for the given client.";
  EXPECT_TRUE(cm->hasReply(8, 8) && cm->hasReply(8, 2))
      << "ClientsManager::deleteOldestReply deleted a reply record other than the oldest one for the given client.";

  EXPECT_NO_THROW(cm->deleteOldestReply(6))
      << "ClientsManager::deleteOldestReply failed when given a client with no reply records.";
}

TEST(ClientsManager, isInternal) {
  resetMockReservedPages();

  unique_ptr<ClientsManager> cm(new ClientsManager({1, 7}, {6, 13, 14}, {}, {2, 4, 9, 12}, metrics));
  set<NodeIdType> internal_clients{2, 4, 9, 12};
  set<NodeIdType> non_internal_clients{1, 7, 6, 13, 14};
  set<NodeIdType> invalid_client_ids{3, 5, 8, 10, 11, 15};

  for (const auto& id : internal_clients) {
    EXPECT_TRUE(cm->isInternal(id)) << "ClientsManager::isInternal failed to correctly identify an internal client.";
  }
  for (const auto& id : non_internal_clients) {
    EXPECT_FALSE(cm->isInternal(id))
        << "ClientsManager::isInternal incorrectly identified a non-internal client as an internal client.";
  }
  for (const auto& id : invalid_client_ids) {
    EXPECT_FALSE(cm->isInternal(id))
        << "ClientsManager::isInternal incorrectly identified an invalid client ID as an internal client.";
  }

  cm.reset(new ClientsManager({2, 4, 6, 8}, {1, 4, 5, 8}, {}, {1, 2, 3, 8}, metrics));
  internal_clients = {1, 2, 3, 8};
  for (NodeIdType i = 0; i <= 10; ++i) {
    if (internal_clients.count(i) > 0) {
      EXPECT_TRUE(cm->isInternal(i)) << "ClientsManager::isInternal failed to correctly identify an internal client "
                                        "for a ClientsManager including some clients classified as multiple types.";
    } else {
      EXPECT_FALSE(cm->isInternal(i))
          << "ClientsManager::isInternal incorrectly identified a client ID not belonging to any client node as "
             "belonging to one for a ClientsManager including some clients classified as multiple types.";
    }
  }
}

TEST(ClientsManager, setClientPublicKey) {
  resetMockReservedPages();
  map<NodeIdType, pair<string, string>> client_keys;

  pair<string, string> client_2_key;
  pair<string, string> client_7_key;
  if (ReplicaConfig::instance().replicaMsgSigningAlgo == SIGN_VERIFY_ALGO::RSA) {
    client_2_key = Crypto::instance().generateRsaKeyPair(RSA_SIGNATURE_LENGTH);
    client_7_key = Crypto::instance().generateRsaKeyPair(RSA_SIGNATURE_LENGTH);
  } else if (ReplicaConfig::instance().replicaMsgSigningAlgo == SIGN_VERIFY_ALGO::EDDSA) {
    client_2_key = OpenSSLCryptoImpl::instance().generateEdDSAKeyPair();
    client_7_key = OpenSSLCryptoImpl::instance().generateEdDSAKeyPair();
  }

  unique_ptr<ClientsManager> cm(new ClientsManager({}, {4, 5, 7}, {}, {}, metrics));
  cm->setClientPublicKey(7, client_7_key.second, kKeyFormatForTesting);

  clearClientPublicKeysLoadedToKEM();
  cm.reset(new ClientsManager({}, {4, 5, 7}, {}, {}, metrics));
  cm->loadInfoFromReservedPages();

  EXPECT_TRUE(verifyClientPublicKeyLoadedToKEM(7, client_7_key))
      << "ClientsManager::setClientPublicKey failed to correctly write the given public key to the reserved pages.";
  EXPECT_TRUE(verifyNoClientPublicKeyLoadedToKEM(4))
      << "ClientsManager::setClientPublicKey appears to have written a key to the reserved pages for a client other "
         "than the one specified to it.";
  EXPECT_TRUE(verifyNoClientPublicKeyLoadedToKEM(5))
      << "ClientsManager::setClientPublicKey appears to have written a key to the reserved pages for a client other "
         "than the one specified to it.";
}

int main(int argc, char** argv) {
  // Some functionalit(y/ies) of ClientsManager may require the global KeyExchangeManager::instance() to be
  // initialized.
  initializeKeyExchangeManagerForClientsManagerTesting();

  // ClientsManager's automatically considers node IDs [0, numReplicas + numRoReplicas) to be valid client IDs (in
  // order to accommodate replicas acting as clients in certain situations, at a minimum including certain state
  // transfer operations). We keep numReplicas and numRoReplicas set to 0 during these unit tests in order to prevent
  // the ClientsManager from automatically recognizing clients that the test cases do not expect.
  ReplicaConfig::instance().setnumReplicas(0);
  ReplicaConfig::instance().setnumRoReplicas(0);

  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
