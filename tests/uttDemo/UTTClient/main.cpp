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

#include <SharedTypes.hpp>
#include <config_file_parser.hpp>
#include "config/test_comm_config.hpp"
#include "config/test_parameters.hpp"
#include "communication/CommFactory.hpp"
#include "utt_messages.cmf.hpp"
#include "bftclient/seq_num_generator.h"

#include "utt_blockchain_app.hpp"
#include "utt_config.hpp"

#include "UTTClientApp.hpp"
#include <utt/Client.h>
#include <utt/MintOp.h>
#include <utt/BurnOp.h>

#include "UTTDataViewer.hpp"

using namespace bftEngine;
using namespace bft::communication;
using std::string;
using namespace utt::messages;

using ReplicaSpecificInfo = std::map<uint16_t, std::vector<uint8_t>>;  // [ReplicaId : bytes]

/////////////////////////////////////////////////////////////////////////////////////////////////////
namespace {
const std::string k_CmdQuit = "q";
const std::string k_CmdHelp = "h";
// Query
const std::string k_CmdAccounts = "accounts";
const std::string k_CmdBalance = "balance";
const std::string k_CmdLedger = "ledger";
const std::string k_CmdView = "view";
// UTT
const std::string k_CmdUtt = "utt";
const std::string k_CmdMint = "mint";
const std::string k_CmdBurn = "burn";
// Debug
const std::string k_CmdDbgUttDoubleSpend = "utt-double-spend";
const std::string k_CmdDbgPrimary = "primary";
const std::string k_CmdDbgCheckpoint = "checkpoint";
const std::string k_CmdRandom = "random";
}  // namespace

/////////////////////////////////////////////////////////////////////////////////////////////////////
struct PaymentServiceTimeoutException : std::runtime_error {
  PaymentServiceTimeoutException() : std::runtime_error{"PaymentServiceTimeout"} {}
};

/////////////////////////////////////////////////////////////////////////////////////////////////////
struct BftServiceTimeoutException : std::runtime_error {
  BftServiceTimeoutException() : std::runtime_error{"BftServiceTimeout"} {}
};

/////////////////////////////////////////////////////////////////////////////////////////////////////
auto logger = logging::getLogger("wallet");

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

    LOG_INFO(logger, "Wallet listening addr: " << listenHost << " : " << listenPort);

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

    LOG_INFO(logger,
             "Using PaymentService-" << paymentServiceId_ << " at " << paymentServiceHost << ':' << paymentServicePort);

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

      // Returns false if the predicate still evaluates false when the wait time expires
      if (condVar_.wait_for(lk, std::chrono::seconds(7), [&]() { return reply_ != nullptr; })) {
        if (reply_->result == static_cast<uint32_t>(OperationResult::TIMEOUT)) {
          reply_.reset();
          throw BftServiceTimeoutException{};
        }

        reply = std::move(reply_);
      } else {
        throw PaymentServiceTimeoutException{};
      }
    }

    if (reply->primary) lastKnownPrimaryId_ = *reply->primary;

    return reply;
  }

  uint16_t getPaymentServiceId() const { return paymentServiceId_; }

  std::string getLastKnownPrimary() const {
    return lastKnownPrimaryId_ ? "replica-" + std::to_string(*lastKnownPrimaryId_ + 1) : "N/A";
  }

 private:
  logging::Logger& logger_;
  uint16_t walletId_;
  uint16_t paymentServiceId_;
  std::optional<uint16_t> lastKnownPrimaryId_;
  std::unique_ptr<ICommunication> comm_;
  std::unique_ptr<BftReply> reply_;
  std::mutex mut_;
  std::condition_variable condVar_;
};

/////////////////////////////////////////////////////////////////////////////////////////////////////
struct UttPayment {
  UttPayment(std::string receiver, size_t amount, bool dbgDoubleSpend = false)
      : receiver_(std::move(receiver)), amount_(amount), dbgDoubleSpend_(dbgDoubleSpend) {}

  std::string receiver_;
  size_t amount_;
  bool dbgDoubleSpend_;
};
std::ostream& operator<<(std::ostream& os, const UttPayment& payment) {
  os << "utt " << payment.receiver_ << ' ' << payment.amount_;
  return os;
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
struct ClientAppParams {
  uint16_t clientId_ = 0;
  std::string configFileName_;
  std::string summarizeFileName_;
};

/////////////////////////////////////////////////////////////////////////////////////////////////////
ClientAppParams setupParams(int argc, char** argv) {
  ClientAppParams params;

  char argTempBuffer[PATH_MAX + 10];
  int o = 0;
  while ((o = getopt(argc, argv, "i:n:s:")) != EOF) {
    switch (o) {
      case 'i': {
        strncpy(argTempBuffer, optarg, sizeof(argTempBuffer) - 1);
        argTempBuffer[sizeof(argTempBuffer) - 1] = 0;
        string idStr = argTempBuffer;
        int tempId = std::stoi(idStr);
        if (tempId >= 0 && tempId < UINT16_MAX) params.clientId_ = (uint16_t)tempId;
      } break;

      case 'n': {
        strncpy(argTempBuffer, optarg, sizeof(argTempBuffer) - 1);
        argTempBuffer[sizeof(argTempBuffer) - 1] = 0;
        params.configFileName_ = argTempBuffer;
      } break;

      case 's': {
        strncpy(argTempBuffer, optarg, sizeof(argTempBuffer) - 1);
        argTempBuffer[sizeof(argTempBuffer) - 1] = 0;
        params.summarizeFileName_ = argTempBuffer;
      } break;

      default:
        break;
    }
  }
  return params;
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
TxReply sendTxRequest(WalletCommunicator& comm, const Tx& tx) {
  if (const auto* txUtt = std::get_if<TxUtt>(&tx)) {
    LOG_INFO(logger, "Sending UTT Tx " << txUtt->utt_.getHashHex());
  }

  std::stringstream ss;
  ss << tx;

  TxRequest txRequest;
  txRequest.tx = ss.str();

  UTTRequest req;
  req.request = std::move(txRequest);

  std::vector<uint8_t> reqBytes;
  serialize(reqBytes, req);

  auto bftReply = comm.sendSync(std::move(reqBytes));
  ConcordAssert(bftReply != nullptr);

  UTTReply reply;
  deserialize(bftReply->matched_data, reply);

  auto txReply = std::get<TxReply>(std::move(reply.reply));  // throws if unexpected variant
  LOG_INFO(logger, "Got TxReply, success=" << txReply.success << " last_block_id=" << txReply.last_block_id);

  return txReply;
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
GetLastBlockReply sendGetLastBlockRequest(WalletCommunicator& comm) {
  UTTRequest req;
  req.request = GetLastBlockRequest();

  std::vector<uint8_t> reqBytes;
  serialize(reqBytes, req);

  auto bftReply = comm.sendSync(std::move(reqBytes));
  ConcordAssert(bftReply != nullptr);

  UTTReply reply;
  deserialize(bftReply->matched_data, reply);

  auto lastBlockReply = std::get<GetLastBlockReply>(std::move(reply.reply));  // throws if unexpected variant

  LOG_INFO(logger, "Got GetLastBlockReply, last_block_id=" << lastBlockReply.last_block_id);

  return lastBlockReply;
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
std::pair<GetBlockDataReply, ReplicaSpecificInfo> sendGetBlockDataRequest(WalletCommunicator& comm, BlockId blockId) {
  GetBlockDataRequest blockDataReq;
  blockDataReq.block_id = blockId;

  UTTRequest req;
  req.request = std::move(blockDataReq);

  std::vector<uint8_t> reqBytes;
  serialize(reqBytes, req);

  auto bftReply = comm.sendSync(std::move(reqBytes));
  ConcordAssert(bftReply != nullptr);

  UTTReply reply;
  deserialize(bftReply->matched_data, reply);

  // Extract the GetBlockDataReply and replica specific info
  std::pair<GetBlockDataReply, ReplicaSpecificInfo> result;
  result.first = std::get<GetBlockDataReply>(std::move(reply.reply));  // throws if unexpected variant
  result.second = std::move(bftReply->rsi);

  LOG_INFO(logger, "Got GetBlockDataReply, success=" << result.first.success << " block_id=" << result.first.block_id);

  return result;
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
ReplicaSigShares DeserializeSigShares(size_t numOutCoins, ReplicaSpecificInfo&& rsi, size_t thresh) {
  ReplicaSigShares result;

  // Pick first F+1 signers - we assume no malicious replicas

  for (const auto& kvp : rsi) {
    if (result.signerIds_.size() == thresh) break;

    result.signerIds_.emplace_back(kvp.first);  // ReplicaId

    std::stringstream ss(BytesToStr(kvp.second));
    size_t size = 0;  // The size reflects the number of output coins
    ss >> size;
    ss.ignore(1, '\n');  // skip newline

    ConcordAssert(size == numOutCoins);  // Make sure the rsi contains the same number of coins as the tx
    result.sigShares_.resize(size);

    // Add this replica share to the list for i-th coin (the order is defined by signerIds)
    for (size_t i = 0; i < size; ++i) {
      result.sigShares_[i].emplace_back(libutt::RandSigShare(ss));
    }
  }

  // Sanity checks
  ConcordAssert(result.signerIds_.size() == thresh);
  for (size_t i = 0; i < result.sigShares_.size(); ++i)
    ConcordAssert(result.signerIds_.size() == result.sigShares_[i].size());

  return result;
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
// Sync app by fetching missing blocks and executing them
void syncState(UTTClientApp& app, WalletCommunicator& comm) {
  LOG_INFO(logger, "Sync app.");

  auto lastBlockReply = sendGetLastBlockRequest(comm);
  app.setLastKnownBlockId(lastBlockReply.last_block_id);

  // Sync missing blocks
  auto missingBlockId = app.executeBlocks();
  while (missingBlockId) {
    // Request missing block
    auto result = sendGetBlockDataRequest(comm, *missingBlockId);
    const auto& blockDataReply = result.first;
    const auto replyBlockId = blockDataReply.block_id;

    if (!blockDataReply.success) throw std::runtime_error("Requested block does not exist!");

    if (replyBlockId != *missingBlockId) throw std::runtime_error("Requested missing block id differs from reply!");

    auto tx = parseTx(blockDataReply.tx);
    if (!tx) throw std::runtime_error("Failed to parse tx for block " + std::to_string(replyBlockId));

    if (auto* txUtt = std::get_if<TxUtt>(&(*tx))) {
      LOG_INFO(logger, "Received UTT Tx " << txUtt->utt_.getHashHex() << " for block " << replyBlockId);

      // Deserialize sig shares from replica specific info
      ReplicaSigShares sigShares =
          DeserializeSigShares(txUtt->utt_.outs.size(), std::move(result.second), app.getSigThresh());

      // Add sig shares
      txUtt->sigShares_ = std::move(sigShares);
    } else if (auto* txMint = std::get_if<TxMint>(&(*tx))) {
      LOG_INFO(logger, "Received Mint Tx " << txMint->op_.getHashHex() << " for block " << replyBlockId);

      // Deserialize sig shares from replica specific info
      ReplicaSigShares sigShares =
          DeserializeSigShares(1 /*expected coins*/, std::move(result.second), app.getSigThresh());

      // Add sig shares
      txMint->sigShares_ = std::move(sigShares);
    }

    app.appendBlock(Block{std::move(*tx)});
    missingBlockId = app.executeBlocks();
  }
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
std::optional<UttPayment> createUttPayment(const std::vector<std::string>& tokens, const UTTClientApp& app) {
  if (tokens.size() == 3 && (tokens[0] == k_CmdUtt || tokens[0] == k_CmdDbgUttDoubleSpend)) {
    const auto& receiver = tokens[1];
    if (receiver == app.getMyPid()) throw std::domain_error("utt explicit self payments are not supported!");

    if (app.getOtherPids().count(receiver) == 0) throw std::domain_error("utt payment receiver is unknown!");

    int payment = std::atoi(tokens[2].c_str());
    if (payment <= 0) throw std::domain_error("utt payment amount must be positive!");

    const bool doubleSpend = tokens[0] == k_CmdDbgUttDoubleSpend;

    return UttPayment(receiver, payment, doubleSpend);
  }

  return std::nullopt;
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
std::optional<Tx> createPublicTx(const std::vector<std::string>& tokens, const UTTClientApp& app) {
  if (tokens.size() == 2) {
    if (tokens[0] == "deposit")
      return TxPublicDeposit(app.getMyPid(), std::atoi(tokens[1].c_str()));
    else if (tokens[0] == "withdraw")
      return TxPublicWithdraw(app.getMyPid(), std::atoi(tokens[1].c_str()));
  } else if (tokens.size() == 3) {
    if (tokens[0] == "transfer")
      return TxPublicTransfer(app.getMyPid(), std::move(tokens[1]), std::atoi(tokens[2].c_str()));
  }

  return std::nullopt;
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
void printHelp() {
  std::cout << "\nCommands:\n";
  std::cout << "view\t\t\t\t-- start browsing the application state.\n";

  std::cout << "transfer [account] [amount]\t-- transfer public money to another account.\n";
  std::cout << "utt [account] [amount]\t\t-- transfer money anonymously to another account.\n";
  std::cout << k_CmdMint << " [amount]\t\t\t-- exchanges public money for coin(s) usable in utt payments\n";
  std::cout << k_CmdBurn << " [amount]\t\t\t-- exchanges utt coin(s) for public money.\n";

  std::cout << "\nExtra commands:\n";
  std::cout << "deposit [amount]\t\t-- deposit public money to current account\n";
  std::cout << "withdraw [amount]\t\t-- withdraw public money from current account\n";

  std::cout << "\nDebug:\n";
  std::cout << k_CmdDbgUttDoubleSpend << "\t\t-- each UTT transaction is sent twice. Only the first should succeed.\n";
  std::cout << k_CmdDbgPrimary
            << "\t\t\t\t-- prints the last known primary replica. This value is updated when receiving responses.\n";
  std::cout << k_CmdRandom << " [count] [seed=0]"
            << "\t\t-- do [count] random money transfers including public and utt transactions. Optionally provide "
               "a seed.\n";
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
void printAccounts(UTTClientApp& app) {
  std::cout << "My account: " << app.getMyPid() << '\n';
  for (const auto& pid : app.getOtherPids()) {
    std::cout << pid << '\n';
  }
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
void printCreateTxResult(const UTTClientApp& app, const libutt::Client::CreateTxResult& result) {
  // Describe what the utt transaction intends to do.
  std::cout << "\nCreated UTT tx " << result.tx.getHashHex() << '\n';
  std::cout << "  Type: " << result.txType_ << '\n';
  for (size_t coinValue : result.inputNormalCoinValues_)
    std::cout << "  - '" << app.getMyPid() << "' will spend " << app.fmtCurrency(coinValue) << " normal coin.\n";
  if (result.inputBudgetCoinValue_)
    std::cout << "  - '" << app.getMyPid() << "' will spend " << app.fmtCurrency(*result.inputBudgetCoinValue_)
              << " budget coin.\n";
  for (const auto& [pid, coinValue] : result.recipients_)
    std::cout << "  + '" << pid << "' will receive " << app.fmtCurrency(coinValue) << " normal coin.\n";
  if (result.outputBudgetCoinValue_)
    std::cout << "  + '" << app.getMyPid() << "' will receive " << app.fmtCurrency(*result.outputBudgetCoinValue_)
              << " budget coin.\n";
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
void checkBalance(UTTClientApp& app, WalletCommunicator& comm) {
  syncState(app, comm);

  const auto& myAccount = app.getMyAccount();
  std::cout << "\nAccount summary:\n";
  std::cout << "  Public balance:\t" << app.fmtCurrency(myAccount.getPublicBalance()) << '\n';
  std::cout << "  UTT wallet balance:\t" << app.fmtCurrency(app.getUttBalance()) << '\n';
  std::cout << "  UTT wallet coins:\t[";
  if (!app.getMyUttWallet().coins.empty()) {
    for (int i = 0; i < (int)app.getMyUttWallet().coins.size() - 1; ++i)
      std::cout << app.fmtCurrency(app.getMyUttWallet().coins[i].getValue()) << ", ";
    std::cout << app.fmtCurrency(app.getMyUttWallet().coins.back().getValue());
  }
  std::cout << "]\n";
  std::cout << "  Anonymous budget:\t" << app.fmtCurrency(app.getUttBudget()) << '\n';
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
void checkLedger(UTTClientApp& app, WalletCommunicator& comm) {
  syncState(app, comm);
  std::cout << '\n';
  for (const auto& block : app.getBlocks()) {
    std::cout << block << '\n';
  }
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
void mintCoin(UTTClientApp& app, WalletCommunicator& comm, const std::vector<std::string>& tokens) {
  // Precondition: token[0] == k_CmdMint
  if (tokens.size() != 2) throw std::domain_error(k_CmdMint + " requires an amount value!");
  int value = std::atoi(tokens[1].c_str());
  if (value <= 0) throw std::domain_error(k_CmdMint + " requires a positive amount value!");

  const size_t amount = static_cast<size_t>(value);

  static bft::client::SeqNumberGenerator gen{bft::client::ClientId{0}};  // ClientId used just for logging
  uint64_t nextMintSeqNum = gen.unique();

  auto uniqueHash = uniqueMintHash(app.getMyPid(), nextMintSeqNum);
  libutt::MintOp mintOp(uniqueHash, amount, app.getMyPid());

  std::cout << "\nCreated a Mint tx: " << mintOp.getHashHex() << '\n';
  std::cout << " - '" << app.getMyPid() << "' will spend " << app.fmtCurrency(amount) << " public money.\n";
  std::cout << " + '" << app.getMyPid() << "' will receive " << app.fmtCurrency(amount) << " coin.\n";

  Tx tx = TxMint{app.getMyPid(), nextMintSeqNum, amount, std::move(mintOp)};

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
void burnCoin(UTTClientApp& app, WalletCommunicator& comm, const std::vector<std::string>& tokens) {
  // Precondition: tokens[0] == k_CmdBurn
  if (tokens.size() != 2) throw std::domain_error(k_CmdBurn + " requires an amount value!");
  int value = std::atoi(tokens[1].c_str());
  if (value <= 0) throw std::domain_error(k_CmdBurn + " requires a positive amount value!");

  const size_t amount = static_cast<size_t>(value);

  std::cout << ">>> Running a BURN operation for '" << app.getMyPid() << " of " << app.fmtCurrency(amount) << '\n';

  while (true) {
    syncState(app, comm);
    if (app.getMyUttWallet().coins.empty()) throw std::domain_error("No coins to burn!");
    if (app.getUttBalance() < amount) throw std::domain_error("Insufficient anonymous balance!");

    auto result = libutt::Client::createTxForBurn(app.getMyUttWallet(), amount);

    if (result.burnOp_) {
      std::cout << "\nCreated a BURN tx: " << result.burnOp_->getHashHex() << '\n';
      const size_t burnValue = result.burnOp_->getValue();
      std::cout << " - '" << result.burnOp_->getOwnerPid() << "' will spend a " << app.fmtCurrency(burnValue)
                << " coin.\n";
      std::cout << " + '" << result.burnOp_->getOwnerPid() << "' will receive " << app.fmtCurrency(burnValue)
                << " public money.\n";

      Tx tx = TxBurn(std::move(*result.burnOp_));

      auto reply = sendTxRequest(comm, tx);
      if (reply.success) {
        app.setLastKnownBlockId(reply.last_block_id);
        std::cout << "Ok.\n";
        std::cout << "\n>>> BURN operation completed.\n";
        checkBalance(app, comm);
      } else {
        std::cout << "Transaction failed: " << reply.err << '\n';
      }

      break;  // Done
    } else if (result.selfTx_) {
      printCreateTxResult(app, *result.selfTx_);

      Tx tx = TxUtt(std::move(result.selfTx_->tx));

      auto reply = sendTxRequest(comm, tx);
      if (reply.success) {
        app.setLastKnownBlockId(reply.last_block_id);
        std::cout << "Ok.\n";
      } else {
        std::cout << "Transaction failed: " << reply.err << '\n';
        break;  // Stop if we encounter an error
      }

    } else {
      ConcordAssert(false);  // Ambiguous result
    }
  }
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
void runUttPayment(const UttPayment& payment, UTTClientApp& app, WalletCommunicator& comm) {
  std::cout << "\n>>> Running a UTT payment from '" << app.getMyPid() << "' to '" << payment.receiver_;
  std::cout << "' for " << app.fmtCurrency(payment.amount_) << '\n';

  while (true) {
    // We do a sync before each attempt to do a utt payment since only the client
    // knows if it has the right coins to do it.
    // This also handles the case where we do repeated splits or merges to arrive at a final payment
    // and need to obtain the resulting coins before we proceed.
    syncState(app, comm);

    // Check that we can still do the payment and this holds:
    // payment <= balance && payment <= budget
    if (app.getUttBalance() < payment.amount_) throw std::domain_error("Insufficient balance for utt payment!");
    if (app.getUttBudget() < payment.amount_) throw std::domain_error("Insufficient anonymous budget for utt payment!");

    auto result = libutt::Client::createTxForPayment(app.getMyUttWallet(), payment.receiver_, payment.amount_);

    // Describe what the utt transaction intends to do.
    printCreateTxResult(app, result);

    // We assume that any tx with a budget coin must be an actual payment
    // and not a coin split or merge.
    // We assume that any valid utt payment terminates with a paying transaction.
    const bool isPayment = result.tx.isBudgeted();

    Tx tx = TxUtt(std::move(result.tx));

    auto reply = sendTxRequest(comm, tx);
    if (reply.success) {
      app.setLastKnownBlockId(reply.last_block_id);
      std::cout << "Ok.\n";
    } else {
      std::cout << "Transaction failed: " << reply.err << '\n';
      break;  // Stop payment if we encounter an error
    }

    if (payment.dbgDoubleSpend_) {
      std::cout << "WARNING: Trying to double-spend by sending again!\n";

      auto reply = sendTxRequest(comm, tx);
      if (reply.success) {
        app.setLastKnownBlockId(reply.last_block_id);
        std::cout << "ERROR!!! This tx should have not succeeded!\n";
        break;  // Stop payment
      } else {
        std::cout << "Transaction failed: " << reply.err << '\n';
      }
    }

    if (isPayment) {
      std::cout << "\n>>> Payment completed.\n";
      break;  // Done
    }
  }
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
void sendPublicTx(const Tx& tx, UTTClientApp& app, WalletCommunicator& comm) {
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
void dbgForceCheckpoint(UTTClientApp& app, WalletCommunicator& comm) {
  const int numTx = 150;
  std::cout << "Running " << numTx << " transactions to cause a checkpoint...\n";
  const int depositValue = 1;
  for (int i = 0; i < numTx; ++i) {
    std::cout << (i + 1) << " : Public deposit " << app.fmtCurrency(depositValue) << " to '" << app.getMyPid() << "'\n";
    auto reply = sendTxRequest(comm, TxPublicDeposit(app.getMyPid(), depositValue));
    if (reply.success) {
      app.setLastKnownBlockId(reply.last_block_id);
    } else {
      std::cout << "Checkpoint transaction " << (i + 1) << " failed: " << reply.err << '\n';
      break;
    }
  }
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
void dbgRunRandomTxs(UTTClientApp& app, WalletCommunicator& comm, int count, unsigned int seed = 0) {
  ConcordAssert(count > 0);
  const auto& myAccount = app.getMyAccount();
  const size_t numOtherPids = app.getOtherPids().size();
  ConcordAssert(numOtherPids > 0);

  if (seed == 0) {
    std::random_device rd;
    seed = rd();
  }

  std::mt19937 gen;
  gen.seed(seed);
  std::cout << "random " << count << " using seed " << seed << '\n';

  for (int i = 0; i < count; ++i) {
    // Pick random wallet to transfer to
    auto randWalletIt = app.getOtherPids().begin();
    std::advance(randWalletIt, gen() % numOtherPids);
    ConcordAssert(randWalletIt != app.getOtherPids().end());

    const size_t uttBalance = app.getUttBalance();
    const size_t publicBalance = myAccount.getPublicBalance();

    // Note that we may create impossible transactions due to balance or budget
    // constraints which is OK - they should fail.

    auto randomMint = [&]() {
      const int maxBalance = std::max<int>(publicBalance, 1);
      const int amount = 1 + gen() % maxBalance;  // in [1 .. maxBalance]
      std::cout << "Random mint tx " << (i + 1) << ": " << amount << '\n';
      std::vector<std::string> tokens = {"mint", std::to_string(amount)};
      mintCoin(app, comm, tokens);
    };

    auto randomBurn = [&]() {
      const int maxBalance = std::max<int>(uttBalance, 1);
      const int amount = 1 + gen() % maxBalance;  // in [1 .. maxBalance]
      std::cout << "Random burn tx " << (i + 1) << ": " << amount << '\n';
      std::vector<std::string> tokens = {"burn", std::to_string(amount)};
      try {
        burnCoin(app, comm, tokens);
      } catch (const std::domain_error& e) {
        std::cout << "Validation error: " << e.what() << '\n';
      }
    };

    auto randomPublicTx = [&]() {
      const int maxBalance = std::max<int>(publicBalance, 1);
      const int amount = 1 + gen() % maxBalance;  // in [1 .. maxBalance]
      std::vector<std::string> tokens = {"transfer", *randWalletIt, std::to_string(amount)};
      auto tx = createPublicTx(tokens, app);
      ConcordAssert(tx.has_value());
      std::cout << "Random public tx " << (i + 1) << ": " << *tx << '\n';
      sendPublicTx(*tx, app, comm);
    };

    auto randomUttTx = [&]() {
      const int amount = 1 + gen() % uttBalance;  // in [1 .. balance]
      std::vector<std::string> tokens = {"utt", *randWalletIt, std::to_string(amount)};
      auto uttPayment = createUttPayment(tokens, app);
      ConcordAssert(uttPayment.has_value());
      std::cout << "Random utt tx " << (i + 1) << ": " << *uttPayment << '\n';
      try {
        runUttPayment(*uttPayment, app, comm);
      } catch (const std::domain_error& e) {
        std::cout << "Validation error: " << e.what() << '\n';
      }
    };

    if (gen() % 3 == 0) {
      if (publicBalance == 0)
        randomBurn();
      else
        randomPublicTx();
    } else {
      if (uttBalance == 0) {
        randomMint();
      } else {
        if (gen() % 2 == 0)
          randomUttTx();
        else if (gen() % 2 == 0)
          randomBurn();
        else
          randomMint();
      }
    }
  }
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
void dbgParseRandomCmd(UTTClientApp& app, WalletCommunicator& comm, const std::vector<std::string>& tokens) {
  // Precondition: tokens[0] == "random"
  if (tokens.size() < 2 || tokens.size() > 3)
    throw std::domain_error("random requires a count and optionally a seed argument");

  const int count = std::atoi(tokens[1].c_str());
  if (count <= 0) throw std::domain_error("random requires a positive count argument");

  if (tokens.size() == 3) {
    const long long seed = std::atoll(tokens[2].c_str());
    if (seed <= 0) throw std::domain_error("random requires a positive seed argument");
    if (seed > std::numeric_limits<unsigned int>::max()) throw std::domain_error("random seed outside numeric range");
    dbgRunRandomTxs(app, comm, count, static_cast<unsigned int>(seed));
  } else {
    dbgRunRandomTxs(app, comm, count);
  }
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
int main(int argc, char** argv) {
  logging::initLogger("config/logging.properties");

  ClientAppParams params = setupParams(argc, argv);

  if (params.clientId_ == 0 || params.configFileName_.empty()) {
    std::cout << "Wrong usage! Required parameters: " << argv[0] << "-n <config_file_name> -i <id>";
    exit(-1);
  }

  try {
    UTTBlockchainApp::initUTTLibrary();

    UTTClientApp app(logger, params.clientId_);

    WalletCommunicator comm(logger, params.clientId_, params.configFileName_);

    // Print the following tuple as a summary of the state and quit
    // MyPid,LastKnowBlockId,PublicBalance,UttBalance,UttBudget
    if (!params.summarizeFileName_.empty()) {
      std::ofstream ofs(params.summarizeFileName_);
      if (!ofs.is_open()) throw std::runtime_error("Failed to open file " + params.summarizeFileName_);

      syncState(app, comm);

      const auto& myAccount = app.getMyAccount();
      // Format: WalletPid LastKnownBlockId PublicBalance UttBalance UttBudget";
      ofs << app.getMyPid() << ' ' << app.getLastKnownBlockId() << ' ' << myAccount.getPublicBalance() << ' '
          << app.getUttBalance() << ' ' << app.getUttBudget() << '\n';
      return 0;
    }

    std::cout << "Wallet initialization for '" << app.getMyPid() << "' done.\n";
    std::cout << "Using Payment Service #" << comm.getPaymentServiceId() << "\n\n";

    // Initial check of balance
    try {
      std::cout << "Checking account...\n";
      checkBalance(app, comm);
    } catch (const BftServiceTimeoutException& e) {
      std::cout << "Concord-BFT service did not respond.\n";
    } catch (const PaymentServiceTimeoutException& e) {
      std::cout << "PaymentService did not respond.\n";
    }

    std::optional<UTTDataViewer> viewer;

    while (true) {
      if (viewer) {
        std::cout << "\nYou are navigating the application state (type ':q' to exit):\n";
        std::cout << app.getMyPid() << ' ' << viewer->getCurrentPath() << " > ";
      } else {
        std::cout << "\nEnter command (type 'h' for commands, 'q' to exit):\n";
        std::cout << app.getMyPid() << "> ";
      }

      std::string cmd;
      std::getline(std::cin, cmd);

      if (std::cin.eof()) {
        std::cout << "Quitting...\n";
        return 0;
      }

      // Tokenize command
      std::vector<std::string> tokens;
      {
        std::stringstream ss(cmd);
        std::string t;
        while (std::getline(ss, t, ' ')) tokens.emplace_back(std::move(t));
      }
      if (tokens.empty()) continue;

      try {
        if (viewer) {
          if (tokens[0] == ":q")
            viewer = std::nullopt;  // Reset
          else
            viewer->handleCommand(tokens[0]);
        } else if (tokens[0] == k_CmdQuit) {
          std::cout << "Quitting...\n";
          return 0;
        } else if (tokens[0] == k_CmdHelp) {
          printHelp();
        } else if (tokens[0] == k_CmdDbgPrimary) {
          std::cout << "Last known primary: " << comm.getLastKnownPrimary() << '\n';
        } else if (tokens[0] == k_CmdAccounts) {
          printAccounts(app);
        } else if (tokens[0] == k_CmdBalance) {
          checkBalance(app, comm);
        } else if (tokens[0] == k_CmdLedger) {
          checkLedger(app, comm);
        } else if (tokens[0] == k_CmdView) {
          ConcordAssert(viewer == std::nullopt);
          viewer = UTTDataViewer(app);
          syncState(app, comm);
          if (tokens.size() > 1)
            viewer->handleCommand(tokens[1]);
          else
            viewer->handleCommand("");
        } else if (tokens[0] == k_CmdDbgCheckpoint) {
          dbgForceCheckpoint(app, comm);
        } else if (tokens[0] == k_CmdRandom) {
          dbgParseRandomCmd(app, comm, tokens);
        } else if (tokens[0] == k_CmdMint) {
          mintCoin(app, comm, tokens);
        } else if (tokens[0] == k_CmdBurn) {
          burnCoin(app, comm, tokens);
        } else if (auto uttPayment = createUttPayment(tokens, app)) {
          runUttPayment(*uttPayment, app, comm);
          checkBalance(app, comm);
        } else if (auto tx = createPublicTx(tokens, app)) {
          sendPublicTx(*tx, app, comm);
        } else if (!tokens.empty()) {
          std::cout << "Unknown command '" << cmd << "'\n";
        }
      } catch (const BftServiceTimeoutException& e) {
        std::cout << "Concord-BFT service did not respond.\n";
      } catch (const PaymentServiceTimeoutException& e) {
        std::cout << "PaymentService did not respond.\n";
      } catch (const std::domain_error& e) {
        std::cout << "Validation error: " << e.what() << '\n';
      } catch (const UTTDataViewerError& e) {
        std::cout << "view: " << e.what() << '\n';
      }
    }
  } catch (const std::exception& e) {
    std::cout << "Exception: " << e.what() << '\n';
    exit(-1);
  }
}
