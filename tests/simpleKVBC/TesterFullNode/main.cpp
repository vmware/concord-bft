// Concord
//
// Copyright (c) 2018-2019 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include "fnGrpc.hpp"

#include "setup.hpp"
#include "Replica.h"
#include "internalCommandsHandler.hpp"
#include "replica_state_sync_imp.hpp"
#include "block_metadata.hpp"
#include "SimpleBCStateTransfer.hpp"
#include "secrets/secrets_manager_plain.h"
#include "secrets/secrets_manager_enc.h"
#include "bftengine/ControlStateManager.hpp"
#include "messages/ReplicaRestartReadyMsg.hpp"
#include "bftengine/ReconfigurationCmd.hpp"
#include "client/reconfiguration/cre_interfaces.hpp"
#include "bftclient/bft_client.h"
#include "util/assertUtils.hpp"
#include "util/Metrics.hpp"
#include "diagnostics_server.hpp"
#include "communication/CommFactory.hpp"
#include "bftclient/config.h"
#include "bftclient/bft_client.h"
#include "config/test_comm_config.hpp"
#include <variant>
#include <csignal>
#include <thread>

#ifdef USE_ROCKSDB
#include "rocksdb/client.h"
#include "rocksdb/key_comparator.h"
#endif

#include <memory>
#include <unistd.h>

using namespace std;
using namespace bftEngine;
using namespace bft::communication;
using namespace concord::client::reconfiguration;
using std::string;
using bft::client::ClientConfig;
using bft::client::ClientId;
using bft::client::Client;

namespace concord::kvbc::test {

std::shared_ptr<concord::kvbc::Replica> replica;
std::shared_ptr<bft::client::Client> bft_client;
std::shared_ptr<bft::communication::ICommunication> fn_communication;
std::shared_ptr<InternalCommandsHandler> cmdHandler;
std::unique_ptr<RequestServiceImpl> request_service;
std::unique_ptr<grpc::Server> fn_server;

std::atomic_bool timeToExit = false;

auto logger = logging::getLogger("skvbtest.fullnode");

ICommunication* createCommunication(const bft::client::ClientConfig& cc,
                                    const std::string& commFileName,
                                    const std::string& certFolder,
                                    std::shared_ptr<concord::secretsmanager::ISecretsManagerImpl>& sm,
                                    bool& enc) {
  TestCommConfig testCommConfig(logger);
  uint16_t numOfReplicas = cc.all_replicas.size();
  uint16_t clients = cc.id.val;
#ifdef USE_COMM_PLAIN_TCP
  PlainTcpConfig conf = testCommConfig.GetTCPConfig(false, cc.id.val, clients, numOfReplicas, commFileName);
#elif USE_COMM_TLS_TCP
  TlsTcpConfig conf = testCommConfig.GetTlsTCPConfig(
      false, cc.id.val, clients, numOfReplicas, commFileName, cc.use_unified_certs, certFolder);
  if (conf.secretData_.has_value()) {
    sm = std::make_shared<concord::secretsmanager::SecretsManagerEnc>(conf.secretData_.value());
    enc = true;
  } else {
    sm = std::make_shared<concord::secretsmanager::SecretsManagerPlain>();
    enc = false;
  }
#else
  PlainUdpConfig conf = testCommConfig.GetUDPConfig(false, cc.id.val, clients, numOfReplicas, commFileName);
#endif

  return CommFactory::create(conf);
}

bft::client::ClientConfig setupClientConfig() {
  bft::client::ClientConfig client_config;

  client_config.f_val = 0;
  client_config.c_val = 0;
  client_config.id = ClientId{6};

  for (int i = 0; i < 4; i++) {
    client_config.all_replicas.emplace(bft::client::ReplicaId{static_cast<uint16_t>(i)});
  }
  return client_config;
}

void run_fn_grpc_server() {
  while (true) {
    grpc::ServerBuilder builder;
    builder.AddListeningPort("localhost:50051", grpc::InsecureServerCredentials());
    builder.SetMaxReceiveMessageSize(52428800);

    builder.RegisterService(request_service.get());

    fn_server = std::unique_ptr<grpc::Server>(builder.BuildAndStart());

    LOG_INFO(GL, "skvbc_fullnode (fn grpc) running...");
    fn_server->Wait();
    LOG_INFO(GL, "skvbc_fullnode (fn grpc) Waiting End...");
  }
}

void run_fullnode(int argc, char** argv) {
  std::thread grpc_thread;
  std::shared_ptr<concord::secretsmanager::ISecretsManagerImpl> sm_;
  bool enc;

  const auto setup = concord::kvbc::TestSetup::ParseArgs(argc, argv);
  logging::initLogger(setup->getLogPropertiesFile());
  auto logger = setup->GetLogger();
  MDC_PUT(MDC_REPLICA_ID_KEY, std::to_string(setup->GetReplicaConfig().replicaId));
  MDC_PUT(MDC_THREAD_KEY, "main");

  replica = std::make_shared<Replica>(
      setup->GetCommunication(),
      setup->GetReplicaConfig(),
      setup->GetStorageFactory(),
      setup->GetMetricsServer().GetAggregator(),
      setup->GetPerformanceManager(),
      std::map<std::string, categorization::CATEGORY_TYPE>{
          {VERSIONED_KV_CAT_ID, categorization::CATEGORY_TYPE::versioned_kv},
          {categorization::kExecutionEventGroupLatestCategory, categorization::CATEGORY_TYPE::versioned_kv},
          {BLOCK_MERKLE_CAT_ID, categorization::CATEGORY_TYPE::block_merkle}},
      setup->GetSecretManager());

  std::unique_ptr<bft::communication::ICommunication> comm_ptr{
      createCommunication(setupClientConfig(), "", setup->getCertsRootPath(), sm_, enc)};

  bft_client = std::make_shared<bft::client::Client>(std::move(comm_ptr), setupClientConfig());

  auto* blockMetadata = new BlockMetadata(*replica);
  cmdHandler =
      std::make_shared<InternalCommandsHandler>(replica.get(),
                                                replica.get(),
                                                blockMetadata,
                                                logger,
                                                setup->AddAllKeysAsPublic(),
                                                replica->kvBlockchain() ? &replica->kvBlockchain().value() : nullptr);

  request_service = std::make_unique<RequestServiceImpl>(bft_client, cmdHandler, true);

  grpc_thread = std::thread(run_fn_grpc_server);

  while (true) {
    if (timeToExit) {
      LOG_INFO(GL, "skvbc_fullnode time to exit");
      fn_server->Shutdown();
      if (grpc_thread.joinable()) {
        LOG_INFO(GL, "skvbc_fullnode join thread");
        grpc_thread.join();
      }
      LOG_INFO(GL, "skvbc_fullnode exitting");
      break;
    } else {
      std::this_thread::sleep_for(std::chrono::seconds(1));
    }
  }
}
}  // namespace concord::kvbc::test

namespace {
static void signal_handler(int signal_num) {
  LOG_INFO(GL, "Program received signal " << signal_num);
  concord::kvbc::test::timeToExit = true;
}
}  // namespace

int main(int argc, char** argv) {
  struct sigaction sa;
  LOG_INFO(GL, "skvbc_fullnode (concord-bft tester fullnode) starting...");
  memset(&sa, 0, sizeof(sa));
  sa.sa_handler = signal_handler;
  sigfillset(&sa.sa_mask);
  sigaction(SIGINT, &sa, NULL);
  sigaction(SIGTERM, &sa, NULL);

  try {
    concord::kvbc::test::run_fullnode(argc, argv);
  } catch (const std::exception& e) {
    LOG_FATAL(GL, "exception: " << e.what());
  }
  LOG_INFO(GL, "skvbc_fullnode (concord-bft tester fullnode) shutting down...");
  return 0;
}