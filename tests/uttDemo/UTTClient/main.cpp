// Concord
//
// Copyright (c) 2018-2022 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include <iostream>
#include <fstream>
#include <memory>
#include <mutex>
#include <condition_variable>

#include <config_file_parser.hpp>
#include "config/test_comm_config.hpp"
#include "config/test_parameters.hpp"
#include "communication/CommFactory.hpp"
#include "utt_messages.cmf.hpp"

#include "app_state.hpp"
#include "utt_config.hpp"

#include <utt/Client.h>

using namespace bftEngine;
using namespace bft::communication;
using std::string;
using namespace utt::messages;

/////////////////////////////////////////////////////////////////////////////////////////////////////
auto logger = logging::getLogger("uttdemo.wallet");

/////////////////////////////////////////////////////////////////////////////////////////////////////
class WalletCommunicator : public IReceiver {
 public:
  WalletCommunicator(logging::Logger& logger, uint16_t walletId, const std::string& cfgFileName)
      : logger_{logger}, walletId_{walletId} {
    // [TODO-UTT] Support for other communcation modes like TCP/TLS (if needed)
    if (cfgFileName.empty()) throw std::runtime_error("Network config filename empty!");

    concord::util::ConfigFileParser cfgFileParser(logger_, cfgFileName);
    if (!cfgFileParser.Parse()) throw std::runtime_error("Failed to parse configuration file: " + cfgFileName);

    // Load wallet network address
    auto walletAddr = cfgFileParser.GetNthValue("wallets_config", walletId);
    if (walletAddr.empty()) throw std::runtime_error("No wallet address for id " + std::to_string(walletId));

    std::string listenHost;
    uint16_t listenPort = 0;
    {
      std::stringstream ss(std::move(walletAddr));
      std::getline(ss, listenHost, ':');
      ss >> listenPort;

      if (listenHost.empty()) throw std::runtime_error("Empty wallet address!");
      if (listenPort == 0) throw std::runtime_error("Invalid wallet port!");
    }

    std::cout << "Wallet listening addr: " << listenHost << " : " << listenPort << "\n";

    // Map wallet id to payment service id:
    // {1, 2, 3} -> 1
    // {4, 5, 6} -> 2
    // {7, 8, 9} -> 3
    paymentServiceId_ = (walletId_ - 1) / 3 + 1;

    // Load the corresponding payment service network address
    auto paymentServiceAddr = cfgFileParser.GetNthValue("payment_services_config", paymentServiceId_);
    if (paymentServiceAddr.empty())
      throw std::runtime_error("No payment service address for id " + std::to_string(paymentServiceId_));

    std::string paymentServiceHost;
    uint16_t paymentServicePort = 0;
    {
      std::stringstream ss(std::move(paymentServiceAddr));
      std::getline(ss, paymentServiceHost, ':');
      ss >> paymentServicePort;

      if (paymentServiceHost.empty()) throw std::runtime_error("Empty payment service host!");
      if (paymentServicePort == 0) throw std::runtime_error("Invalid payment service port!");
    }

    std::cout << "Using PaymentService #" << paymentServiceId_;
    std::cout << " addr: " << paymentServiceHost << " : " << paymentServicePort << "\n";

    // Each wallet connects only to the payment service node
    // The actual node id for the payment service is always 0 from the point of view
    // of the wallet.
    std::unordered_map<NodeNum, NodeInfo> nodes;
    nodes.emplace(0, NodeInfo{paymentServiceHost, paymentServicePort, false});

    const int32_t msgMaxSize = 128 * 1024;  // 128 kB -- Same as TestCommConfig

    PlainUdpConfig conf(listenHost, listenPort, msgMaxSize, nodes, walletId);

    comm_.reset(CommFactory::create(conf));
    if (!comm_) throw std::runtime_error("Failed to create communication for wallet!");

    comm_->setReceiver(NodeNum(walletId), this);
    comm_->start();
  }

  ~WalletCommunicator() { comm_->stop(); }

  // Invoked when a new message is received
  // Notice that the memory pointed by message may be freed immediately
  // after the execution of this method.
  void onNewMessage(NodeNum sourceNode, const char* const message, size_t messageLength, NodeNum endpointNum) override {
    // This is called from the listening thread of the communication
    LOG_INFO(logger_, "onNewMessage from: " << sourceNode << " msgLen: " << messageLength);

    // [TODO-UTT] Guard against mismatched number of replies compared to requests

    // Deserialize the reply
    auto reply = std::make_unique<BftReply>();
    auto begin = reinterpret_cast<const uint8_t*>(message);
    auto end = begin + messageLength;
    deserialize(begin, end, *reply);

    {
      std::lock_guard<std::mutex> lk{mut_};
      // The comm should always be ready to deliver the reply from the last request.
      // There should never be a message received other than an expected reply.
      ConcordAssert(reply_ == nullptr);
      reply_ = std::move(reply);
    }

    condVar_.notify_one();  // Notify sendSync
  }

  // Invoked when the known status of a connection is changed.
  // For each NodeNum, this method will never be concurrently
  // executed by two different threads.
  void onConnectionStatusChanged(NodeNum node, ConnectionStatus newStatus) override {
    // Not applicable to UDP
    LOG_INFO(logger_, "onConnectionStatusChanged from: " << node << " newStatus: " << (int)newStatus);
  }

  std::unique_ptr<BftReply> sendSync(std::vector<uint8_t>&& msg) {
    int err = comm_->send(NodeNum(0), std::move(msg));
    if (err) throw std::runtime_error("Error sending message: " + std::to_string(err));

    std::unique_ptr<BftReply> reply;

    // Wait for the reply or timeout
    {
      std::unique_lock<std::mutex> lk{mut_};

      condVar_.wait_for(lk, std::chrono::seconds(5), [&]() { return reply_ != nullptr; });

      reply = std::move(reply_);
    }

    return reply;
  }

  uint16_t getPaymentServiceId() const { return paymentServiceId_; }

 private:
  logging::Logger& logger_;
  uint16_t walletId_;
  uint16_t paymentServiceId_;
  std::unique_ptr<ICommunication> comm_;
  std::unique_ptr<BftReply> reply_;
  std::mutex mut_;
  std::condition_variable condVar_;
};

/////////////////////////////////////////////////////////////////////////////////////////////////////
struct ClientApp : public AppState {
  ClientApp(uint16_t walletId) {
    if (walletId == 0) throw std::runtime_error("wallet id must be a positive value!");

    const std::string fileName = "config/utt_client_" + std::to_string(walletId);
    std::ifstream ifs(fileName);
    if (!ifs.is_open()) throw std::runtime_error("Missing config: " + fileName);

    UTTClientConfig cfg;
    ifs >> cfg;

    myPid_ = cfg.wallet_.getUserPid();
    if (myPid_.empty()) throw std::runtime_error("Empty wallet pid!");

    for (auto& pid : cfg.pids_) {
      if (pid == myPid_) continue;
      otherPids_.emplace(std::move(pid));
    }
    if (otherPids_.empty()) throw std::runtime_error("Other pids are empty!");

    std::cout << "Successfully loaded UTT wallet with pid '" << myPid_ << "'\n";

    addAccount(Account{std::move(cfg.wallet_)});
  }

  const Account& getMyAccount() const { return *getAccountById(myPid_); }
  Account& getMyAccount() { return *getAccountById(myPid_); }

  template <typename T>
  std::string fmtCurrency(T val) {
    return "$" + std::to_string(val);
  }

  std::string myPid_;
  std::set<std::string> otherPids_;
};

/////////////////////////////////////////////////////////////////////////////////////////////////////
struct UttPayment {
  UttPayment(std::string receiver, size_t amount) : receiver_(std::move(receiver)), amount_(amount) {}

  std::string receiver_;
  size_t amount_;
};

/////////////////////////////////////////////////////////////////////////////////////////////////////
ClientParams setupClientParams(int argc, char** argv) {
  ClientParams clientParams;
  clientParams.clientId = UINT16_MAX;
  clientParams.numOfFaulty = UINT16_MAX;
  clientParams.numOfSlow = UINT16_MAX;
  clientParams.numOfOperations = UINT16_MAX;
  char argTempBuffer[PATH_MAX + 10];
  int o = 0;
  while ((o = getopt(argc, argv, "i:f:c:p:n:")) != EOF) {
    switch (o) {
      case 'i': {
        strncpy(argTempBuffer, optarg, sizeof(argTempBuffer) - 1);
        argTempBuffer[sizeof(argTempBuffer) - 1] = 0;
        string idStr = argTempBuffer;
        int tempId = std::stoi(idStr);
        if (tempId >= 0 && tempId < UINT16_MAX) clientParams.clientId = (uint16_t)tempId;
      } break;

      case 'f': {
        strncpy(argTempBuffer, optarg, sizeof(argTempBuffer) - 1);
        argTempBuffer[sizeof(argTempBuffer) - 1] = 0;
        string fStr = argTempBuffer;
        int tempfVal = std::stoi(fStr);
        if (tempfVal >= 1 && tempfVal < UINT16_MAX) clientParams.numOfFaulty = (uint16_t)tempfVal;
      } break;

      case 'c': {
        strncpy(argTempBuffer, optarg, sizeof(argTempBuffer) - 1);
        argTempBuffer[sizeof(argTempBuffer) - 1] = 0;
        string cStr = argTempBuffer;
        int tempcVal = std::stoi(cStr);
        if (tempcVal >= 0 && tempcVal < UINT16_MAX) clientParams.numOfSlow = (uint16_t)tempcVal;
      } break;

      case 'p': {
        strncpy(argTempBuffer, optarg, sizeof(argTempBuffer) - 1);
        argTempBuffer[sizeof(argTempBuffer) - 1] = 0;
        string numOfOpsStr = argTempBuffer;
        uint32_t tempPVal = std::stoul(numOfOpsStr);
        if (tempPVal >= 1 && tempPVal < UINT32_MAX) clientParams.numOfOperations = tempPVal;
      } break;

      case 'n': {
        strncpy(argTempBuffer, optarg, sizeof(argTempBuffer) - 1);
        argTempBuffer[sizeof(argTempBuffer) - 1] = 0;
        clientParams.configFileName = argTempBuffer;
      } break;

      default:
        break;
    }
  }
  return clientParams;
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
TxReply sendTxRequest(WalletCommunicator& comm, const Tx& tx) {
  // [TODO-UTT] Debug output
  if (const auto* txUtt = std::get_if<TxUtt>(&tx)) {
    std::cout << "Sending UTT Tx " << txUtt->utt_.getHashHex() << '\n';
  }

  std::stringstream ss;
  ss << tx;

  TxRequest txRequest;
  txRequest.tx = ss.str();

  UTTRequest req;
  req.request = std::move(txRequest);

  std::vector<uint8_t> reqBytes;
  serialize(reqBytes, req);

  auto replyBytes = comm.sendSync(std::move(reqBytes));
  ConcordAssert(replyBytes != nullptr);

  UTTReply reply;
  deserialize(replyBytes->matched_data, reply);

  const auto& txReply = std::get<TxReply>(reply.reply);  // throws if unexpected variant
  std::cout << "Got TxReply, success=" << txReply.success << " last_block_id=" << txReply.last_block_id << '\n';

  return txReply;
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
GetLastBlockReply sendGetLastBlockRequest(WalletCommunicator& comm) {
  UTTRequest req;
  req.request = GetLastBlockRequest();

  std::vector<uint8_t> reqBytes;
  serialize(reqBytes, req);

  auto replyBytes = comm.sendSync(std::move(reqBytes));
  ConcordAssert(replyBytes != nullptr);

  UTTReply reply;
  deserialize(replyBytes->matched_data, reply);

  const auto& lastBlockReply = std::get<GetLastBlockReply>(reply.reply);  // throws if unexpected variant
  std::cout << "Got GetLastBlockReply, last_block_id=" << lastBlockReply.last_block_id << '\n';

  return lastBlockReply;
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
GetBlockDataReply sendGetBlockDataRequest(WalletCommunicator& comm, BlockId blockId, ReplicaSigShares& outSigShares) {
  GetBlockDataRequest blockDataReq;
  blockDataReq.block_id = blockId;

  UTTRequest req;
  req.request = std::move(blockDataReq);

  std::vector<uint8_t> reqBytes;
  serialize(reqBytes, req);

  auto replyBytes = comm.sendSync(std::move(reqBytes));
  ConcordAssert(replyBytes != nullptr);

  UTTReply reply;
  deserialize(replyBytes->matched_data, reply);

  const auto& blockDataReply = std::get<GetBlockDataReply>(reply.reply);  // throws if unexpected variant
  std::cout << "Got GetBlockDataReply, success=" << blockDataReply.success << " block_id=" << blockDataReply.block_id
            << '\n';

  if (!blockDataReply.success) {
    return blockDataReply;
  }

  // Deserialize sign shares
  // [TODO-UTT]: Need to collect F+1 (out of 2F+1) shares that combine to a valid RandSig
  // Here, we assume naively that all replicas are honest

  for (const auto& kvp : replyBytes->rsi) {
    if (outSigShares.signerIds_.size() == 2) break;  // Pick first F+1 signers

    outSigShares.signerIds_.emplace_back(kvp.first);  // ReplicaId

    std::stringstream ss(BytesToStr(kvp.second));
    size_t size = 0;  // The size reflects the number of output coins
    ss >> size;
    ss.ignore(1, '\n');  // skip newline

    ConcordAssert(size <= 3);
    // Add this replica share to the list for i-th coin (the order is defined by signerIds)
    for (size_t i = 0; i < size; ++i) {
      if (outSigShares.sigShares_.size() == i)  // Resize to accommodate up to 3 coins
        outSigShares.sigShares_.emplace_back();

      outSigShares.sigShares_[i].emplace_back(libutt::RandSigShare(ss));
    }
  }

  ConcordAssert(outSigShares.signerIds_.size() == 2);
  for (size_t i = 0; i < outSigShares.sigShares_.size(); ++i)  // Check for each coin we have F+1 signers
    ConcordAssert(outSigShares.signerIds_.size() == outSigShares.sigShares_[i].size());

  return blockDataReply;
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
// Sync state by fetching missing blocks and executing them
void syncState(ClientApp& app, WalletCommunicator& comm) {
  std::cout << "Sync state...\n";

  auto lastBlockReply = sendGetLastBlockRequest(comm);
  app.setLastKnownBlockId(lastBlockReply.last_block_id);

  // Sync missing blocks
  auto missingBlockId = app.executeBlocks();
  while (missingBlockId) {
    // Request missing block
    ReplicaSigShares sigShares;
    auto blockDataReply = sendGetBlockDataRequest(comm, *missingBlockId, sigShares);
    const auto replyBlockId = blockDataReply.block_id;

    if (!blockDataReply.success) throw std::runtime_error("Requested block does not exist!");

    if (replyBlockId != *missingBlockId) throw std::runtime_error("Requested missing block id differs from reply!");

    auto tx = parseTx(blockDataReply.tx);
    if (!tx) throw std::runtime_error("Failed to parse tx for block " + std::to_string(replyBlockId));

    if (auto* txUtt = std::get_if<TxUtt>(&(*tx))) {
      // [TODO-UTT] Debug output
      std::cout << "Received UTT Tx " << txUtt->utt_.getHashHex() << " for block " << replyBlockId << '\n';

      // Add sig shares
      txUtt->sigShares_ = std::move(sigShares);
    }

    app.appendBlock(Block{std::move(*tx)});
    missingBlockId = app.executeBlocks();
  }
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
std::optional<UttPayment> createUttPayment(const std::string& cmd, const ClientApp& app) {
  std::vector<std::string> tokens;
  std::string token;
  std::stringstream ss(cmd);
  while (std::getline(ss, token, ' ')) tokens.emplace_back(std::move(token));

  if (tokens.size() == 3 && tokens[0] == "utt") {
    const auto& receiver = tokens[1];
    if (receiver == app.myPid_) throw std::domain_error("utt explicit self payments are not supported!");

    if (app.otherPids_.count(receiver) == 0) throw std::domain_error("utt payment receiver is unknown!");

    int payment = std::atoi(tokens[2].c_str());
    if (payment <= 0) throw std::domain_error("utt payment amount must be positive!");

    return UttPayment(receiver, payment);
  }

  return std::nullopt;
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
std::optional<Tx> createPublicTx(const std::string& cmd, const ClientApp& app) {
  std::stringstream ss(cmd);
  std::string token;
  std::vector<std::string> tokens;

  while (std::getline(ss, token, ' ')) tokens.emplace_back(std::move(token));

  if (tokens.size() == 2) {
    if (tokens[0] == "deposit")
      return TxPublicDeposit(app.myPid_, std::atoi(tokens[1].c_str()));
    else if (tokens[0] == "withdraw")
      return TxPublicWithdraw(app.myPid_, std::atoi(tokens[1].c_str()));
  } else if (tokens.size() == 3) {
    if (tokens[0] == "transfer")
      return TxPublicTransfer(app.myPid_, std::move(tokens[1]), std::atoi(tokens[2].c_str()));
  }

  return std::nullopt;
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
void printHelp() {
  std::cout << "\nCommands:\n";
  std::cout << "balance\t\t\t-- print account's public and utt balances\n";
  std::cout << "ledger\t\t\t-- print all transactions that happened\n";
  // std::cout << "deposit [amount]\t-- public money deposit to account\n";
  // std::cout << "withdraw [amount]\t-- public money withdraw from account\n";
  std::cout << "transfer [to] [amount]\t-- public money transfer\n";
  std::cout << "utt [to] [amount]\t-- anonymous money transfer\n";
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
void checkBalance(ClientApp& app, WalletCommunicator& comm) {
  syncState(app, comm);
  std::cout << '\n';

  const auto& myAccount = app.getMyAccount();
  std::cout << "Balance:\n";
  std::cout << "  Public: " << app.fmtCurrency(myAccount.getPublicBalance()) << '\n';
  std::cout << "  Anonymous: " << app.fmtCurrency(myAccount.getUttBalance()) << '\n';
  std::cout << "  Remaining anonymous budget: " << app.fmtCurrency(myAccount.getUttBudget()) << '\n';
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
void checkLedger(ClientApp& app, WalletCommunicator& comm) {
  syncState(app, comm);
  std::cout << '\n';
  for (const auto& block : app.GetBlocks()) {
    std::cout << block << '\n';
  }
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
void runUttPayment(const UttPayment& payment, ClientApp& app, WalletCommunicator& comm) {
  std::cout << "Running a UTT payment to '" << payment.receiver_;
  std::cout << "' for " << app.fmtCurrency(payment.amount_) << '\n';

  while (true) {
    // We do a sync before each attempt to do a utt payment since only the client
    // knows if it has the right coins to do it.
    // This also handles the case where we do repeated splits or merges to arrive at a final payment
    // and need to obtain the resulting coins before we proceed.
    syncState(app, comm);

    const auto& myAccount = app.getMyAccount();
    const auto* myWallet = myAccount.getWallet();
    ConcordAssert(myWallet != nullptr);

    // Check that we can still do the payment and this holds:
    // payment <= balance && payment <= budget
    if (myAccount.getUttBalance() < payment.amount_) throw std::domain_error("Insufficient balance for utt payment!");
    if (myAccount.getUttBudget() < payment.amount_)
      throw std::domain_error("Insufficient anonymous budget for utt payment!");

    auto uttTx = libutt::Client::createTxForPayment(*myWallet, payment.receiver_, payment.amount_);

    // We assume that any tx with a budget coin must be an actual payment
    // and not a coin split or merge.
    // We assume that any valid utt payment terminates with a paying transaction.
    const bool isPayment = uttTx.isBudgeted();

    Tx tx = TxUtt(std::move(uttTx));

    auto reply = sendTxRequest(comm, tx);
    if (reply.success) {
      app.setLastKnownBlockId(reply.last_block_id);
      std::cout << "Ok.\n";
    } else {
      std::cout << "Transaction failed: " << reply.err << '\n';
      break;  // Stop payment if we encounter an error
    }

    if (isPayment) {
      std::cout << "Payment completed.\n";
      break;  // Done
    }
  }
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
void sendPublicTx(const Tx& tx, ClientApp& app, WalletCommunicator& comm) {
  auto reply = sendTxRequest(comm, tx);
  if (reply.success) {
    app.setLastKnownBlockId(reply.last_block_id);
    std::cout << "Ok.\n";
    checkBalance(app, comm);
  } else {
    std::cout << "Transaction failed: " << reply.err << '\n';
  }
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
void dbgForceCheckpoint(ClientApp& app, WalletCommunicator& comm) {
  for (int i = 0; i < 150; ++i) {
    auto reply = sendTxRequest(comm, TxPublicDeposit(app.myPid_, 1));
    if (reply.success) {
      app.setLastKnownBlockId(reply.last_block_id);
    } else {
      std::cout << "Checkpoint transaction " << (i + 1) << " failed: " << reply.err << '\n';
      break;
    }
  }
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
int main(int argc, char** argv) {
  logging::initLogger("config/logging.properties");

  ClientParams clientParams = setupClientParams(argc, argv);

  if (clientParams.clientId == UINT16_MAX) {
    std::cout << "Wrong usage! Required parameters: " << argv[0] << "-i <id>";
    exit(-1);
  }

  try {
    AppState::initUTTLibrary();

    ClientApp state(clientParams.clientId);

    WalletCommunicator comm(logger, clientParams.clientId, clientParams.configFileName);

    std::cout << "Wallet initialization done.\n";

    while (true) {
      std::cout << "\nEnter command (type 'h' for commands, 'q' to exit):\n";
      std::cout << state.myPid_ << "> ";
      std::string cmd;
      std::getline(std::cin, cmd);
      try {
        if (std::cin.eof() || cmd == "q") {
          std::cout << "Quit!\n";
          return 0;
        } else if (cmd == "h") {
          printHelp();
        } else if (cmd == "balance") {
          checkBalance(state, comm);
        } else if (cmd == "ledger") {
          checkLedger(state, comm);
        } else if (cmd == "checkpoint") {
          dbgForceCheckpoint(state, comm);
        } else if (auto uttPayment = createUttPayment(cmd, state)) {
          runUttPayment(*uttPayment, state, comm);
          checkBalance(state, comm);
        } else if (auto tx = createPublicTx(cmd, state)) {
          sendPublicTx(*tx, state, comm);
        } else {
          std::cout << "Unknown command '" << cmd << "'\n";
        }
        // [TODO-UTT] Handle timeouts from comm
        // } catch (const bft::client::TimeoutException& e) {
        //   std::cout << "Request timeout: " << e.what() << '\n';
      } catch (const std::domain_error& e) {
        std::cout << "Validation error: " << e.what() << '\n';
      }
    }
  } catch (const std::exception& e) {
    std::cout << "Exception: " << e.what() << '\n';
    exit(-1);
  }
}
