// Concord
//
// Copyright (c) 2018-2021 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#include "UTTCommandsHandler.hpp"

#include <fstream>

#include "kvbc_key_types.hpp"
#include "ReplicaConfig.hpp"

#include <utt/Replica.h>

#include "utt_blockchain_app.hpp"
#include "utt_config.hpp"

using namespace bftEngine;
using namespace utt::messages;

using concord::kvbc::BlockId;
namespace kvbc_cat = concord::kvbc::categorization;

/////////////////////////////////////////////////////////////////////////////////////////////////////
class UTTReplicaApp : public UTTBlockchainApp {
 public:
  UTTReplicaApp(logging::Logger& logger, uint16_t replicaId) : logger_{logger} {
    const auto fileName = "config/utt_replica_" + std::to_string(replicaId);
    std::ifstream ifs(fileName);
    if (!ifs.is_open()) throw std::runtime_error("Failed to open " + fileName);

    ifs >> config_;

    LOG_INFO(logger_, "Loaded config '" << fileName);

    for (const auto& pid : config_.pids_) {
      addAccount(Account{pid, config_.initPublicBalance_});
    }
    if (GetAccounts().empty()) throw std::runtime_error("No accounts loaded!");
  }

  bool canExecuteTx(const Tx& tx, std::string& err) const {
    struct Visitor {
      const UTTReplicaApp& app_;
      Visitor(const UTTReplicaApp& app) : app_{app} {}

      void operator()(const TxPublicDeposit& tx) const {
        if (tx.amount_ <= 0) throw std::domain_error("Public deposit amount must be positive!");
        auto acc = app_.getAccountById(tx.toAccountId_);
        if (!acc) throw std::domain_error("Unknown account for public deposit!");
      }

      void operator()(const TxPublicWithdraw& tx) const {
        if (tx.amount_ <= 0) throw std::domain_error("Public withdraw amount must be positive!");
        auto acc = app_.getAccountById(tx.toAccountId_);
        if (!acc) throw std::domain_error("Unknown account for public withdraw!");
        if (tx.amount_ > acc->getPublicBalance()) throw std::domain_error("Account has insufficient public balance!");
      }

      void operator()(const TxPublicTransfer& tx) const {
        if (tx.amount_ <= 0) throw std::domain_error("Public transfer amount must be positive!");
        auto sender = app_.getAccountById(tx.fromAccountId_);
        if (!sender) throw std::domain_error("Unknown sender account for public transfer!");
        auto receiver = app_.getAccountById(tx.toAccountId_);
        if (!receiver) throw std::domain_error("Unknown receiver account for public transfer!");
        if (sender == receiver) throw std::domain_error("Cannot transfer public money to itself!");
        if (tx.amount_ > sender->getPublicBalance()) throw std::domain_error("Sender has insufficient public balance!");
      }

      void operator()(const TxUtt& tx) const {
        // [TODO-UTT] Validate takes the bank public key, but that's used for quickPay validation
        // which we aren't using in the demo
        if (!tx.utt_.validate(app_.config_.p_, app_.config_.bpk_, app_.config_.rpk_))
          throw std::domain_error("Invalid utt transfer tx!");

        // [TODO-UTT] Does a copy of nullifiers
        for (const auto& n : tx.utt_.getNullifiers()) {
          if (app_.hasNullifier(n)) throw std::domain_error("Input coin already spent!");
        }
      }
    };

    try {
      std::visit(Visitor{*this}, tx);
    } catch (const std::domain_error& e) {
      err = e.what();
      return false;
    }
    return true;
  }

  const std::vector<uint8_t>& GetSigShareRsiForBlock(const libutt::Tx& tx, uint64_t blockId) const {
    // Compute signature shares lazily when the block is requested
    // Precondition: monotonically increasing blocks
    auto& result = sigSharesRsiCache_[blockId];

    if (result.empty()) {
      auto sigShares = libutt::Replica::signShareOutputCoins(tx, config_.bskShare_);
      ConcordAssert(!sigShares.empty());

      LOG_INFO(logger_, "Computed utt sign shares for block " << blockId << " tx: " << tx.getHashHex());

      // Cache the rsi reply
      std::stringstream ssRsi;
      ssRsi << sigShares.size() << '\n';
      for (const auto& sigShare : sigShares) {
        ssRsi << sigShare;
      }
      result = StrToBytes(ssRsi.str());
    }

    return result;
  }

 private:
  logging::Logger& logger_;
  UTTReplicaConfig config_;

  // A cached Resplica Specific Info msg based on
  // the computed utt sig shares for some block
  mutable std::map<size_t, std::vector<uint8_t>> sigSharesRsiCache_;
};

/////////////////////////////////////////////////////////////////////////////////////////////////////
UTTCommandsHandler::UTTCommandsHandler(concord::kvbc::IReader* storage,
                                       concord::kvbc::IBlockAdder* blocksAdder,
                                       logging::Logger& logger,
                                       concord::kvbc::categorization::KeyValueBlockchain* kvbc)
    : storage_(storage), blockAdder_(blocksAdder), logger_(logger), kvbc_{kvbc} {
  ConcordAssertNE(kvbc_, nullptr);

  UTTBlockchainApp::initUTTLibrary();

  const auto replicaId = ReplicaConfig::instance().getreplicaId();
  app_ = std::make_unique<UTTReplicaApp>(logger, replicaId);
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
UTTCommandsHandler::~UTTCommandsHandler() = default;

/////////////////////////////////////////////////////////////////////////////////////////////////////
void UTTCommandsHandler::execute(UTTCommandsHandler::ExecutionRequestsQueue& requests,
                                 std::optional<bftEngine::Timestamp> timestamp,
                                 const std::string& batchCid,
                                 concordUtils::SpanWrapper& parent_span) {
  syncAppState();

  for (auto& req : requests) {
    if (req.outExecutionStatus != static_cast<uint32_t>(OperationResult::UNKNOWN))
      continue;  // Request already executed (internal)

    LOG_INFO(logger_, "UTTCommandsHandler execute");

    UTTRequest uttRequest;
    try {
      auto uttRequestBytes = (const uint8_t*)req.request;
      deserialize(uttRequestBytes, uttRequestBytes + req.requestSize, uttRequest);

      UTTReply reply;
      const auto* reqVariant = &uttRequest.request;

      // Replica specific info is used to provide signature shares of output coins
      vector<uint8_t> rsi;

      if (const auto* txRequest = std::get_if<TxRequest>(reqVariant)) {
        reply.reply = handleRequest(*txRequest);
      } else if (const auto* lastBlockRequest = std::get_if<GetLastBlockRequest>(reqVariant)) {
        reply.reply = handleRequest(*lastBlockRequest);
      } else if (const auto* blockDataReq = std::get_if<GetBlockDataRequest>(reqVariant)) {
        reply.reply = handleRequest(*blockDataReq, rsi);
      } else {
        throw std::runtime_error("Unhandled UTTRquest type!");
      }

      // Serialize reply and rsi (if not empty)
      vector<uint8_t> replyBuffer;
      serialize(replyBuffer, reply);

      size_t totalReplySize = replyBuffer.size() + rsi.size();

      if (req.maxReplySize < totalReplySize) {
        LOG_ERROR(logger_,
                  "replySize is too big: total=" << totalReplySize << " max=" << req.maxReplySize
                                                 << " reply=" << replyBuffer.size() << " rsi=" << rsi.size());
        req.outExecutionStatus = static_cast<uint32_t>(OperationResult::EXEC_DATA_TOO_LARGE);
      } else {
        copy(replyBuffer.begin(), replyBuffer.end(), req.outReply);
        copy(rsi.begin(), rsi.end(), req.outReply + replyBuffer.size());

        req.outActualReplySize = totalReplySize;
        req.outReplicaSpecificInfoSize = rsi.size();

        LOG_INFO(logger_,
                 "UTTRequest successfully executed. totalReplySize=" << totalReplySize << " max=" << req.maxReplySize);
        req.outExecutionStatus = static_cast<uint32_t>(OperationResult::SUCCESS);
      }
    } catch (const std::exception& e) {
      LOG_ERROR(logger_, "Failed to execute UTTRequest: " << e.what());
      strcpy(req.outReply, "Failed to execute UTTRequest");
      req.outActualReplySize = strlen(req.outReply);
      req.outExecutionStatus = static_cast<uint32_t>(OperationResult::INVALID_REQUEST);
    }
  }
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
TxReply UTTCommandsHandler::handleRequest(const TxRequest& txRequest) {
  auto tx = parseTx(txRequest.tx);
  if (!tx) throw std::runtime_error("Failed to parse tx!");

  if (const auto* txUtt = std::get_if<TxUtt>(&(*tx))) {
    LOG_INFO(logger_, "Handling UTT Tx: " << txUtt->utt_.getHashHex());
  } else {
    LOG_INFO(logger_, "Handling Public Tx: " << txRequest.tx);
  }

  TxReply reply;
  std::string err;

  if (app_->canExecuteTx(*tx, err)) {
    // Add a real block to the kv blockchain
    {
      BlockId nextExpectedBlockId = storage_->getLastBlockId() + 1;

      kvbc_cat::VersionedUpdates verUpdates;

      auto copyTx = txRequest.tx;
      verUpdates.addUpdate("tx" + std::to_string(nextExpectedBlockId), std::move(copyTx));

      kvbc_cat::Updates updates;
      updates.add(VERSIONED_KV_CAT_ID, std::move(verUpdates));
      const auto newBlockId = blockAdder_->add(std::move(updates));
      ConcordAssert(newBlockId == nextExpectedBlockId);
    }

    // Execute the added block
    // Note that for utt txs the signatures are computed lazily
    // and cached when the block data is requested

    app_->appendBlock(Block{std::move(*tx)});
    app_->executeBlocks();
    reply.success = true;
  } else {
    LOG_WARN(logger_, "Failed to execute Tx request: " << err);
    reply.err = std::move(err);
    reply.success = false;
  }

  reply.last_block_id = app_->getLastKnownBlockId();

  return reply;
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
GetLastBlockReply UTTCommandsHandler::handleRequest(const GetLastBlockRequest&) {
  LOG_INFO(logger_, "Executing GetLastBlockRequest");

  GetLastBlockReply reply;
  reply.last_block_id = app_->getLastKnownBlockId();

  return reply;
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
GetBlockDataReply UTTCommandsHandler::handleRequest(const GetBlockDataRequest& req, std::vector<uint8_t>& outRsi) {
  LOG_INFO(logger_, "Executing GetBlockDataRequest for block_id=" << req.block_id);

  GetBlockDataReply reply;

  const auto* block = app_->getBlockById(req.block_id);
  if (!block) {
    LOG_WARN(logger_, "Request block " << req.block_id << " does not exist!");
    reply.success = false;
    return reply;
  }

  reply.success = true;
  reply.block_id = block->id_;

  if (block->tx_) {
    std::stringstream ss;
    ss << *block->tx_;
    reply.tx = ss.str();

    if (const auto* txUtt = std::get_if<TxUtt>(&(*block->tx_))) {
      outRsi = app_->GetSigShareRsiForBlock(txUtt->utt_, req.block_id);
    }
  }

  return reply;
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
std::string UTTCommandsHandler::getLatest(const std::string& key) const {
  const auto v = storage_->getLatest(VERSIONED_KV_CAT_ID, key);
  if (!v) {
    return std::string();
  }
  return std::visit([](const auto& v) { return v.data; }, *v);
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
void UTTCommandsHandler::syncAppState() {
  const auto lastBlockId = storage_->getLastBlockId();

  ConcordAssert(lastBlockId >= app_->getLastKnownBlockId());

  app_->setLastKnownBlockId(lastBlockId);

  auto missingBlockId = app_->executeBlocks();
  while (missingBlockId) {
    const auto key = "tx" + std::to_string(*missingBlockId);
    const auto v = getLatest(key);

    LOG_INFO(logger_, "Fetching tx for missing block " << *missingBlockId);

    auto tx = parseTx(v);
    if (!tx) throw std::runtime_error("Failed to parse tx!");

    if (const auto* txUtt = std::get_if<TxUtt>(&(*tx))) {
      LOG_INFO(logger_, "Executing utt tx: " << txUtt->utt_.getHashHex());
    } else {
      LOG_INFO(logger_, "Executing public tx: " << v);
    }

    app_->appendBlock(Block{std::move(*tx)});

    missingBlockId = app_->executeBlocks();
  }
}