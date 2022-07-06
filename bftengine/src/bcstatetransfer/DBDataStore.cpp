#include <algorithm>
#include "DBDataStore.hpp"
#include "storage/db_interface.h"
#include "Serializable.h"

using concord::serialize::Serializable;

namespace bftEngine {
namespace bcst {
namespace impl {

std::ostream& operator<<(std::ostream& os, const DataStore::CheckpointDesc& desc) {
  os << "CheckpointDesc "
     << " checkpointNum: " << desc.checkpointNum << " lastBlock: " << desc.maxBlockId
     << " digestOfLastBlock: " << desc.digestOfMaxBlockId.toString()
     << " digestOfResPagesDescriptor:" << desc.digestOfResPagesDescriptor.toString()
     << " rvbData size:" << desc.rvbData.size();
  return os;
}

std::string toString(const DataStore::CheckpointDesc& desc) {
  std::ostringstream oss;
  oss << desc;
  return oss.str();
}

/** ******************************************************************************************************************/
void DBDataStore::memoryStateToLog() {
  std::ostringstream oss;

  const auto replicas = inmem_->getReplicas();
  const auto numbersOfPendingResPages = inmem_->getNumbersOfPendingResPages();
  oss << "Replicas(): ";
  std::copy(replicas.begin(), replicas.end(), std::ostream_iterator<int>(oss, " "));
  oss << " ,MyReplicaId: " << inmem_->getMyReplicaId();
  oss << " ,FVal: " << inmem_->getFVal();
  oss << " ,MaxNumOfStoredCheckpoints: " << inmem_->getMaxNumOfStoredCheckpoints();
  oss << " ,NumberOfReservedPages: " << inmem_->getNumberOfReservedPages();
  oss << " ,LastStoredCheckpoint: " << inmem_->getLastStoredCheckpoint();
  oss << " ,FirstStoredCheckpoint: " << inmem_->getFirstStoredCheckpoint();
  oss << " ,NumbersOfPendingResPages: ";
  std::copy(numbersOfPendingResPages.begin(), numbersOfPendingResPages.end(), std::ostream_iterator<int>(oss, " "));
  oss << std::boolalpha << " IsFetchingState: " << inmem_->getIsFetchingState();
  if (inmem_->hasCheckpointBeingFetched()) {
    oss << " ,CheckpointBeingFetched: " << inmem_->getCheckpointBeingFetched();
  }
  oss << " ,FirstRequiredBlock: " << inmem_->getFirstRequiredBlock();
  oss << " ,LastRequiredBlock: " << inmem_->getLastRequiredBlock();
  auto prunedDigests = inmem_->getPrunedBlocksDigests();
  if (!prunedDigests.empty()) {
    oss << " ,PrunedBlocksDigests (size=" << prunedDigests.size() << ")";
    for (const auto& p : prunedDigests) {
      oss << "[" << p.first << "," << p.second.toString() << " ] ";
    }
  }
  LOG_INFO(logger(), "Current In Memory State:" << oss.str());
}

void DBDataStore::load(bool loadResPages_) {
  LOG_DEBUG(logger(), "");
  if (!get<bool>(Initialized)) {
    LOG_INFO(logger(), " DBDataStore is not initialized");
    return;
  }
  inmem_->setAsInitialized();
  inmem_->setMyReplicaId(get<uint16_t>(MyReplicaId));
  inmem_->setMaxNumOfStoredCheckpoints(get<uint64_t>(MaxNumOfStoredCheckpoints));
  inmem_->setNumberOfReservedPages(get<uint32_t>(NumberOfReservedPages));
  inmem_->setLastStoredCheckpoint(get<uint64_t>(LastStoredCheckpoint));
  inmem_->setFirstStoredCheckpoint(get<uint64_t>(FirstStoredCheckpoint));
  inmem_->setIsFetchingState(get<bool>(IsFetchingState));
  inmem_->setFVal(get<uint16_t>(fVal));
  inmem_->setFirstRequiredBlock(get<uint64_t>(FirstRequiredBlock));
  inmem_->setLastRequiredBlock(get<uint64_t>(LastRequiredBlock));

  Sliver rep;
  if (get(Replicas, rep)) {
    std::istringstream iss(std::string(reinterpret_cast<const char*>(rep.data()), rep.length()));
    std::set<std::uint16_t> replicas;
    Serializable::deserialize(iss, replicas);
    inmem_->setReplicas(replicas);
  }
  Sliver cpbf;
  if (get(CheckpointBeingFetched, cpbf)) {
    std::istringstream iss(std::string(reinterpret_cast<const char*>(cpbf.data()), cpbf.length()));
    DataStore::CheckpointDesc cpd;
    deserializeCheckpoint(iss, cpd);
    inmem_->setCheckpointBeingFetched(cpd);
  }

  if (loadResPages_) {
    if (inmem_->getLastStoredCheckpoint() > 0) loadResPages();

    loadPendingPages();
  }

  Sliver pbd;
  if (get(PrunedBlocksDigests, pbd)) {
    std::istringstream iss(std::string(reinterpret_cast<const char*>(pbd.data()), pbd.length()));
    std::vector<std::pair<BlockId, Digest>> digests;
    deserializePrunedBlocksDigests(iss, digests);
    inmem_->setPrunedBlocksDigests(digests);
  }
  memoryStateToLog();

  if (get<bool>(EraseDataOnStartup)) {
    try {
      clearDataStoreData();
      LOG_INFO(logger(), "delete state transfer metadata");
    } catch (std::exception& e) {
      LOG_FATAL(logger(), e.what());
      std::terminate();
    }
  }
}
/** ******************************************************************************************************************/
void DBDataStore::setAsInitialized() {
  LOG_DEBUG(logger(), "");
  putInt(Initialized, true);
  inmem_->setAsInitialized();
}
void DBDataStore::setMyReplicaId(uint16_t id) {
  LOG_DEBUG(logger(), "MyReplicaId: " << id);
  putInt(MyReplicaId, id);
  inmem_->setMyReplicaId(id);
}
void DBDataStore::setMaxNumOfStoredCheckpoints(uint64_t num) {
  LOG_DEBUG(logger(), "MaxNumOfStoredCheckpoints: " << num);
  putInt(MaxNumOfStoredCheckpoints, num);
  inmem_->setMaxNumOfStoredCheckpoints(num);
}
void DBDataStore::setNumberOfReservedPages(uint32_t numResPages) {
  LOG_DEBUG(logger(), "NumberOfReservedPages: " << numResPages);
  putInt(NumberOfReservedPages, numResPages);
  inmem_->setNumberOfReservedPages(numResPages);
}
void DBDataStore::setLastStoredCheckpoint(uint64_t c) {
  LOG_DEBUG(logger(), "LastStoredCheckpoint: " << c);
  putInt(LastStoredCheckpoint, c);
  inmem_->setLastStoredCheckpoint(c);
}
void DBDataStore::setFirstStoredCheckpoint(uint64_t c) {
  LOG_DEBUG(logger(), "FirstStoredCheckpoint: " << c);
  putInt(FirstStoredCheckpoint, c);
  inmem_->setFirstStoredCheckpoint(c);
}
void DBDataStore::setIsFetchingState(bool b) {
  LOG_DEBUG(logger(), "IsFetchingState: " << b);
  putInt(IsFetchingState, b);
  inmem_->setIsFetchingState(b);
}
void DBDataStore::setFVal(uint16_t fVal) {
  LOG_DEBUG(logger(), "FVal: " << fVal);
  putInt(GeneralIds::fVal, fVal);
  inmem_->setFVal(fVal);
}
void DBDataStore::setFirstRequiredBlock(uint64_t i) {
  LOG_DEBUG(logger(), "FirstRequiredBlock: " << i);
  putInt(FirstRequiredBlock, i);
  inmem_->setFirstRequiredBlock(i);
}
void DBDataStore::setLastRequiredBlock(uint64_t i) {
  LOG_DEBUG(logger(), "LastRequiredBlock: " << i);
  putInt(LastRequiredBlock, i);
  inmem_->setLastRequiredBlock(i);
}
void DBDataStore::setReplicas(const std::set<std::uint16_t>& replicas) {
  std::ostringstream oss;
  Serializable::serialize(oss, replicas);
  put(Replicas, oss.str());
  inmem_->setReplicas(replicas);
}
/** ******************************************************************************************************************
 *  Checkpoint
 */
void DBDataStore::serializeCheckpoint(std::ostream& os, const CheckpointDesc& desc) const {
  Serializable::serialize(os, desc.checkpointNum);
  Serializable::serialize(os, desc.maxBlockId);
  Serializable::serialize(os, desc.digestOfMaxBlockId.get(), DIGEST_SIZE);
  Serializable::serialize(os, desc.digestOfResPagesDescriptor.get(), DIGEST_SIZE);
  Serializable::serialize(os, desc.rvbData);
}
void DBDataStore::deserializeCheckpoint(std::istream& is, CheckpointDesc& desc) const {
  Serializable::deserialize(is, desc.checkpointNum);
  Serializable::deserialize(is, desc.maxBlockId);
  Serializable::deserialize(is, desc.digestOfMaxBlockId.getForUpdate(), DIGEST_SIZE);
  Serializable::deserialize(is, desc.digestOfResPagesDescriptor.getForUpdate(), DIGEST_SIZE);
  Serializable::deserialize(is, desc.rvbData);
}
void DBDataStore::setCheckpointDesc(uint64_t checkpoint, const CheckpointDesc& desc, const bool checkIfAlreadyExists) {
  LOG_DEBUG(logger(), toString(desc));
  std::ostringstream oss;
  serializeCheckpoint(oss, desc);
  put(chkpDescKey(checkpoint), oss.str());
  inmem_->setCheckpointDesc(checkpoint, desc, checkIfAlreadyExists);
}
bool DBDataStore::hasCheckpointDesc(uint64_t checkpoint) {
  if (inmem_->hasCheckpointDesc(checkpoint)) return true;
  Sliver val;
  if (!get(chkpDescKey(checkpoint), val)) return false;
  std::istringstream iss(std::string(reinterpret_cast<const char*>(val.data()), val.length()));
  DataStore::CheckpointDesc cpd;
  deserializeCheckpoint(iss, cpd);
  inmem_->setCheckpointDesc(checkpoint, cpd);
  return true;
}
DataStore::CheckpointDesc DBDataStore::getCheckpointDesc(uint64_t checkpoint) {
  if (hasCheckpointDesc(checkpoint)) return inmem_->getCheckpointDesc(checkpoint);
  throw std::runtime_error("no description for checkpoint " + std::to_string(checkpoint));
}
void DBDataStore::deleteDescOfSmallerCheckpointsTxn(uint64_t checkpoint, ITransaction* txn) {
  for (auto& p : inmem_->getDescMap()) {
    if (p.first < checkpoint) {
      LOG_DEBUG(logger(),
                "deleting chkp: " << p.first << " smaller than chkp: " << checkpoint << " txn: " << txn->getId());
      txn->del(chkpDescKey(p.first));
    } else
      break;
  }
}

void DBDataStore::deleteAllDesc() {
  auto first_cp = inmem_->getFirstStoredCheckpoint();
  auto last_cp = inmem_->getLastStoredCheckpoint();
  ITransaction::Guard g(dbc_->beginTransaction());
  for (auto cp = first_cp; cp <= last_cp; cp++) {
    LOG_INFO(logger(), "deletes descriptor for " << cp);
    g.txn()->del(chkpDescKey(cp));
  }
}

void DBDataStore::deleteDescOfSmallerCheckpoints(uint64_t checkpoint) {
  if (txn_)
    deleteDescOfSmallerCheckpointsTxn(checkpoint, txn_);
  else {
    ITransaction::Guard g(dbc_->beginTransaction());
    deleteDescOfSmallerCheckpointsTxn(checkpoint, g.txn());
  }
  inmem_->deleteDescOfSmallerCheckpoints(checkpoint);
}
void DBDataStore::setCheckpointBeingFetched(const CheckpointDesc& desc) {
  LOG_DEBUG(logger(), toString(desc));
  std::ostringstream oss;
  serializeCheckpoint(oss, desc);
  put(CheckpointBeingFetched, oss.str());
  inmem_->setCheckpointBeingFetched(desc);
}
void DBDataStore::deleteCheckpointBeingFetched() {
  LOG_DEBUG(logger(), "");
  del(CheckpointBeingFetched);
  inmem_->deleteCheckpointBeingFetched();
}
/**
 * ResPage serialized form: [pageId][checkpoint][PageDigest][PageSize][Page]
 */
void DBDataStore::serializeResPage(
    std::ostream& os, uint32_t inPageId, uint64_t inCheckpoint, const Digest& inPageDigest, const char* inPage) const {
  Serializable::serialize(os, inPageId);
  Serializable::serialize(os, inCheckpoint);
  Serializable::serialize(os, inPageDigest.get(), DIGEST_SIZE);
  Serializable::serialize(os, inmem_->getSizeOfReservedPage());
  Serializable::serialize(os, inPage, inmem_->getSizeOfReservedPage());
}
void DBDataStore::deserializeResPage(
    std::istream& is, uint32_t& outPageId, uint64_t& outCheckpoint, Digest& outPageDigest, char*& outPage) const {
  Serializable::deserialize(is, outPageId);
  Serializable::deserialize(is, outCheckpoint);
  char dgst[DIGEST_SIZE] = {'\0'};
  Serializable::deserialize(is, dgst, DIGEST_SIZE);
  outPageDigest = Digest(dgst);
  std::uint32_t sizeOfReseredPage;
  Serializable::deserialize(is, sizeOfReseredPage);
  ConcordAssert(sizeOfReseredPage == inmem_->getSizeOfReservedPage());
  Serializable::deserialize(is, outPage, sizeOfReseredPage);
}
/**
 * Reserved pages are loaded on startup.
 *
 * [pageId, checkpoint] => <serialized page>
 */
void DBDataStore::loadResPages() {
  auto it(dbc_->getIteratorGuard());
  for (auto keyValue = it->seekAtLeast(dynamicResPageKey(0, 0));  // start of dynamic keys space
       !it->isEnd() && keyValue.first.string_view().find(keymanip_->getReservedPageKeyPrefix().string_view()) == 0;
       keyValue = it->next()) {
    std::istringstream iss(
        std::string(reinterpret_cast<const char*>(keyValue.second.data()), keyValue.second.length()));
    uint32_t pageid;
    uint64_t checkpoint;
    Digest digest;
    char* page = new char[inmem_->getSizeOfReservedPage()];
    deserializeResPage(iss, pageid, checkpoint, digest, page);
    inmem_->setResPage(pageid, checkpoint, digest, page);
    delete[] page;  // is copied
    LOG_DEBUG(logger(), KVLOG(pageid, checkpoint, digest.toString()));
  }
  LOG_DEBUG(logger(), inmem_->getPagesForLog());
}
void DBDataStore::setResPageTxn(
    uint32_t inPageId, uint64_t inCheckpoint, const Digest& inPageDigest, const char* inPage, ITransaction* txn) {
  std::ostringstream oss;
  serializeResPage(oss, inPageId, inCheckpoint, inPageDigest, inPage);
  Sliver dynamic_key = dynamicResPageKey(inPageId, inCheckpoint);
  txn->put(dynamic_key, oss.str());
  LOG_DEBUG(logger(), KVLOG(inPageId, inCheckpoint, inPageDigest.toString(), txn->getId(), dynamic_key));
}

void DBDataStore::setResPage(uint32_t inPageId,
                             uint64_t inCheckpoint,
                             const Digest& inPageDigest,
                             const char* inPage,
                             const bool checkIfAlreadyExists) {
  if (txn_)
    setResPageTxn(inPageId, inCheckpoint, inPageDigest, inPage, txn_);
  else {
    ITransaction::Guard g(dbc_->beginTransaction());
    setResPageTxn(inPageId, inCheckpoint, inPageDigest, inPage, g.txn());
  }
  inmem_->setResPage(inPageId, inCheckpoint, inPageDigest, inPage, checkIfAlreadyExists);
  LOG_TRACE(logger(), inmem_->getPagesForLog());
}
/** ******************************************************************************************************************
 *  Pending Reserved Pages
 */
void DBDataStore::deserializePendingPage(std::istream& is, char*& page, uint32_t& pageLen) const {
  Serializable::deserialize(is, pageLen);
  page = new char[pageLen];
  Serializable::deserialize(is, page, pageLen);
}

void DBDataStore::loadPendingPages() {
  LOG_DEBUG(logger(), "");
  for (uint32_t pageid = 0; pageid < inmem_->getNumberOfReservedPages(); ++pageid) {
    Sliver serializedPendingPage;
    if (!get(pendingPageKey(pageid), serializedPendingPage)) continue;
    std::istringstream iss(
        std::string(reinterpret_cast<const char*>(serializedPendingPage.data()), serializedPendingPage.length()));
    std::uint32_t pageLen = 0;
    char* page = nullptr;
    deserializePendingPage(iss, page, pageLen);
    inmem_->setPendingResPage(pageid, page, pageLen);
    delete[] page;
  }
}

void DBDataStore::setPendingResPage(uint32_t inPageId, const char* inPage, uint32_t inPageLen) {
  LOG_DEBUG(logger(), "page: " << inPageId);
  std::ostringstream oss;
  Serializable::serialize(oss, inPageLen);
  Serializable::serialize(oss, inPage, inPageLen);
  put(pendingPageKey(inPageId), oss.str());
  inmem_->setPendingResPage(inPageId, inPage, inPageLen);
}

void DBDataStore::associatePendingResPageWithCheckpoint(uint32_t inPageId,
                                                        uint64_t inCheckpoint,
                                                        const Digest& inPageDigest) {
  if (txn_)
    associatePendingResPageWithCheckpointTxn(inPageId, inCheckpoint, inPageDigest, txn_);
  else {
    ITransaction::Guard g(dbc_->beginTransaction());
    associatePendingResPageWithCheckpointTxn(inPageId, inCheckpoint, inPageDigest, g.txn());
  }
  inmem_->associatePendingResPageWithCheckpoint(inPageId, inCheckpoint, inPageDigest);
  LOG_DEBUG(logger(), inmem_->getPagesForLog());
}

void DBDataStore::associatePendingResPageWithCheckpointTxn(uint32_t inPageId,
                                                           uint64_t inCheckpoint,
                                                           const Digest& inPageDigest,
                                                           ITransaction* txn) {
  LOG_DEBUG(logger(),
            "page: " << inPageId << " chkp: " << inCheckpoint << " digest: " << inPageDigest.toString()
                     << " txn: " << txn->getId());
  auto& pendingPages = inmem_->getPendingPagesMap();
  auto page = pendingPages.find(inPageId);
  ConcordAssert(page != pendingPages.end());

  setResPageTxn(inPageId, inCheckpoint, inPageDigest, page->second.get(), txn);
  txn->del(pendingPageKey(inPageId));
}
void DBDataStore::deleteAllPendingPagesTxn(ITransaction* txn) {
  std::set<uint32_t> pageIds = getNumbersOfPendingResPages();
  for (auto pid : pageIds) txn->del(pendingPageKey(pid));
}
void DBDataStore::deleteAllPendingPages() {
  if (txn_) {
    deleteAllPendingPagesTxn(txn_);
  } else {
    ITransaction::Guard g(dbc_->beginTransaction());
    deleteAllPendingPagesTxn(g.txn());
  }
  inmem_->deleteAllPendingPages();
}

void DBDataStore::deleteCoveredResPageInSmallerCheckpointsTxn(uint64_t minChkp, ITransaction* txn) {
  LOG_DEBUG(logger(), " min chkp: " << minChkp << " txn: " << txn->getId());
  auto pages = inmem_->getPagesMap();
  auto it = pages.begin();
  if (it == pages.end()) return;
  uint32_t prevItemPageId = it->first.pageId;
  bool prevItemIsInLastRelevantCheckpoint = (it->first.checkpoint <= minChkp);
  std::ostringstream oss("deleted: ");
  it++;
  for (; it != pages.end(); ++it) {
    if (it->first.pageId == prevItemPageId && prevItemIsInLastRelevantCheckpoint) {
      ConcordAssert(it->second.page != nullptr);
      oss << "[" << it->first.pageId << ":" << it->first.checkpoint << "] ";
      LOG_TRACE(logger(),
                "delete: [" << it->first.pageId << ":" << it->first.checkpoint << "] "
                            << dynamicResPageKey(it->first.pageId, it->first.checkpoint));
      txn->del(dynamicResPageKey(it->first.pageId, it->first.checkpoint));
    } else {
      prevItemPageId = it->first.pageId;
      prevItemIsInLastRelevantCheckpoint = (it->first.checkpoint <= minChkp);
    }
  }
  LOG_DEBUG(logger(), oss.str());
}

void DBDataStore::deleteAllResPages() {
  auto pages = inmem_->getPagesMap();
  ITransaction::Guard g(dbc_->beginTransaction());
  for (const auto& it : pages) {
    LOG_INFO(logger(), "deletes resPage " << KVLOG(it.first.pageId, it.first.checkpoint));
    g.txn()->del(dynamicResPageKey(it.first.pageId, it.first.checkpoint));
  }
}

void DBDataStore::deleteCoveredResPageInSmallerCheckpoints(uint64_t minChkp) {
  if (txn_)
    deleteCoveredResPageInSmallerCheckpointsTxn(minChkp, txn_);
  else {
    ITransaction::Guard g(dbc_->beginTransaction());
    deleteCoveredResPageInSmallerCheckpointsTxn(minChkp, g.txn());
  }
  inmem_->deleteCoveredResPageInSmallerCheckpoints(minChkp);
  LOG_DEBUG(logger(), inmem_->getPagesForLog());
}

void DBDataStore::clearDataStoreData() {
  del(Initialized);
  del(MyReplicaId);
  del(MaxNumOfStoredCheckpoints);
  del(NumberOfReservedPages);
  del(LastStoredCheckpoint);
  del(FirstStoredCheckpoint);
  del(IsFetchingState);
  del(fVal);
  del(FirstRequiredBlock);
  del(LastRequiredBlock);
  del(Replicas);
  del(CheckpointBeingFetched);
  del(EraseDataOnStartup);
  del(PrunedBlocksDigests);
  deleteAllPendingPages();
  deleteCheckpointBeingFetched();
  deleteAllResPages();
  deleteAllDesc();

  inmem_.reset(new InMemoryDataStore(inmem_->getSizeOfReservedPage()));
}

/** ******************************************************************************************************************
 *  Range Validation Blocks
 */

void DBDataStore::serializePrunedBlocksDigests(std::ostream& os,
                                               const std::vector<std::pair<BlockId, Digest>>& digests) const {
  Serializable::serialize(os, digests.size());
  for (size_t i{0}; i < digests.size(); ++i) {
    Serializable::serialize(os, digests[i].first);
    Serializable::serialize(os, digests[i].second.get(), DIGEST_SIZE);
  }
}
void DBDataStore::deserializePrunedBlocksDigests(std::istream& is,
                                                 std::vector<std::pair<BlockId, Digest>>& outDigests) const {
  size_t size{};
  Serializable::deserialize(is, size);
  for (size_t i{0}; i < size; ++i) {
    BlockId blockId;
    Digest digest;
    Serializable::deserialize(is, blockId);
    Serializable::deserialize(is, digest.getForUpdate(), DIGEST_SIZE);
    outDigests.emplace_back(std::make_pair(blockId, digest));
  }
}

void DBDataStore::setPrunedBlocksDigests(const std::vector<std::pair<BlockId, Digest>>& digests) {
  std::ostringstream oss;
  serializePrunedBlocksDigests(oss, digests);
  put(PrunedBlocksDigests, oss.str());
  inmem_->setPrunedBlocksDigests(digests);
}

/** ******************************************************************************************************************/
}  // namespace impl
}  // namespace bcst
}  // namespace bftEngine
