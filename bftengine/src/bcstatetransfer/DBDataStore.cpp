#include <algorithm>
#include "DBDataStore.hpp"
#include "storage/db_interface.h"
#include "Serializable.h"

namespace bftEngine {
namespace SimpleBlockchainStateTransfer {
namespace impl  {
using bftEngine::SimpleBlockchainStateTransfer::BLOCK_DIGEST_SIZE;
using concord::serialize::Serializable;
/** ******************************************************************************************************************/
void DBDataStore::load() {
  if (!get<bool>(Initialized)) {
    LOG_INFO(logger(), "Not initialized");
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
  if (get(Replicas, rep)){
    std::istringstream iss(reinterpret_cast<const char*>(rep.data()));
    std::set<std::uint16_t> replicas;
    Serializable::deserialize(iss, replicas);
    inmem_->setReplicas(replicas);
  }
  Sliver cpbf;
  if (get(CheckpointBeingFetched, cpbf)){
    std::istringstream iss(reinterpret_cast<const char*>(cpbf.data()));
    DataStore::CheckpointDesc cpd;
    deserializeCheckpoint(iss, cpd);
    inmem_->setCheckpointBeingFetched(cpd);
  }
  loadResPages();
  loadPendingPages();
}
/** ******************************************************************************************************************/
void DBDataStore::setAsInitialized() {
  putInt(Initialized, true);
  inmem_->setAsInitialized();
}
void DBDataStore::setMyReplicaId(uint16_t id) {
  putInt(MyReplicaId, id);
  inmem_->setMyReplicaId(id);
}
void DBDataStore::setMaxNumOfStoredCheckpoints(uint64_t num){
  putInt(MaxNumOfStoredCheckpoints, num);
  inmem_->setMaxNumOfStoredCheckpoints(num);
}
void DBDataStore::setNumberOfReservedPages(uint32_t numResPages) {
  putInt(NumberOfReservedPages, numResPages);
  inmem_->setNumberOfReservedPages(numResPages);
}
void DBDataStore::setLastStoredCheckpoint(uint64_t c) {
  putInt(LastStoredCheckpoint, c);
  inmem_->setLastStoredCheckpoint(c);
}
void DBDataStore::setFirstStoredCheckpoint(uint64_t c) {
  putInt(FirstStoredCheckpoint, c);
  inmem_->setFirstStoredCheckpoint(c);
}
void DBDataStore::setIsFetchingState(bool b) {
  putInt(IsFetchingState, b);
  inmem_->setIsFetchingState(b);
}
void DBDataStore::setFVal(uint16_t fVal) {
  putInt(GeneralIds::fVal, fVal);
  inmem_->setFVal(fVal);
}
void DBDataStore::setFirstRequiredBlock(uint64_t i) {
  putInt(FirstRequiredBlock, i);
  inmem_->setFirstRequiredBlock(i);
}
void DBDataStore::setLastRequiredBlock(uint64_t i) {
  putInt(LastRequiredBlock, i);
  inmem_->setLastRequiredBlock(i);
}
void DBDataStore::setReplicas(const std::set<std::uint16_t> replicas) {
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
  Serializable::serialize(os, desc.lastBlock);
  Serializable::serialize(os, desc.digestOfLastBlock.get()         , BLOCK_DIGEST_SIZE);
  Serializable::serialize(os, desc.digestOfResPagesDescriptor.get(), BLOCK_DIGEST_SIZE);
}
void DBDataStore::deserializeCheckpoint(std::istream& is, CheckpointDesc& desc) const {
  Serializable::deserialize(is, desc.checkpointNum);
  Serializable::deserialize(is, desc.digestOfLastBlock.getForUpdate()         , BLOCK_DIGEST_SIZE);
  Serializable::deserialize(is, desc.digestOfResPagesDescriptor.getForUpdate(), BLOCK_DIGEST_SIZE);
}
void DBDataStore::setCheckpointDesc(uint64_t checkpoint, const CheckpointDesc& desc) {
  std::ostringstream oss;
  serializeCheckpoint(oss, desc);
  put(chkpDescKey(checkpoint), oss.str());
  inmem_->setCheckpointDesc(checkpoint, desc);
}
DataStore::CheckpointDesc DBDataStore::getCheckpointDesc(uint64_t checkpoint) {
  if(inmem_->hasCheckpointDesc(checkpoint))
    return inmem_->getCheckpointDesc(checkpoint);
  Sliver val;
  if(!get(chkpDescKey(checkpoint), val))
    return {};
  std::istringstream iss(reinterpret_cast<const char*>(val.data()));
  DataStore::CheckpointDesc cpd;
  deserializeCheckpoint(iss, cpd);
  inmem_->setCheckpointDesc(checkpoint, cpd);
  return inmem_->getCheckpointDesc(checkpoint);
}

void DBDataStore::deleteDescOfSmallerCheckpointsTxn(uint64_t checkpoint, ITransaction* txn){
  for (auto & p: inmem_->getDescMap() ){
    if( p.first >=  checkpoint)
      break;
    txn->del(chkpDescKey(checkpoint));
  }
}
void DBDataStore::deleteDescOfSmallerCheckpoints(uint64_t checkpoint) {
  if(txn_)
    deleteDescOfSmallerCheckpointsTxn(checkpoint, txn_.get());
  else{
    ITransaction::Guard g(dbc_->beginTransaction());
    deleteDescOfSmallerCheckpointsTxn(checkpoint, g.txn());
  }
  inmem_->deleteDescOfSmallerCheckpoints(checkpoint);
}
void DBDataStore::setCheckpointBeingFetched(const CheckpointDesc& desc) {
  std::ostringstream oss;
  serializeCheckpoint(oss, desc);
  put(CheckpointBeingFetched, oss.str());
  inmem_->setCheckpointBeingFetched(desc);
}
void DBDataStore::deleteCheckpointBeingFetched() {
  del(CheckpointBeingFetched);
  inmem_->deleteCheckpointBeingFetched();
}
/** *******************************************************************************************************************
 *  Reserved Pages
 *
 *  C = lastStoredCheckpoint
 *  n = maxNumOfStoredCheckpoints
 *  K = numberOfReservedPages
 *  #keys = n x K
 *
 *               |page1        |page2        |       |pageK        |
 *               |1|2|3|4|...|n|1|2|3|4|...|n| ..... |1|2|3|4|...|n|
 *               | | | |     |                       | | | |     |
 *         rp11 -| | | |     |                       | | | |     |-- rpKn
 *         rp12 ---| | |     |                       | | | |         ...
 *         rp13 -----| |     |                       | | | |-------- rpK4
 *         rp14 -------|     |                       | | |---------- rpK3
 *         ...               |                       | |------------ rpK2
 *         rp1n -------------|                       |-------------- rpK18
 *
 * Keys indirection table maps keys in the pages array to the actual (dynamic page key):
 *
 * rp_pid_idx -> page_pid_checkpoint
 *
 * to find right slot having pid and checkpoint:
 * page_pid_checkpoint -> rp_pid_idx , idx = checkpoint mod n
 *
 * ResPage serialized form: [pageId][checkpoint][PageDigest][PageSize][Page]
 */
void DBDataStore::serializeResPage(std::ostream&   os,
                                   uint32_t        inPageId,
                                   uint64_t        inCheckpoint,
                                   const STDigest& inPageDigest,
                                   const char*     inPage) const {
  Serializable::serialize(os, inPageId);
  Serializable::serialize(os, inCheckpoint);
  Serializable::serialize(os, inPageDigest.get(), BLOCK_DIGEST_SIZE);
  Serializable::serialize(os, inmem_->getSizeOfReservedPage());
  Serializable::serialize(os, inPage, inmem_->getSizeOfReservedPage());
}
void DBDataStore::deserializeResPage(std::istream&   is,
                                     uint32_t&       outPageId,
                                     uint64_t&       outCheckpoint,
                                     STDigest&       outPageDigest,
                                     char*           outPage) const {
  Serializable::deserialize(is, outPageId);
  Serializable::deserialize(is, outCheckpoint);
  char dgst[BLOCK_DIGEST_SIZE];
  Serializable::deserialize(is, dgst, BLOCK_DIGEST_SIZE);
  outPageDigest = STDigest(dgst);
  std::uint32_t sizeOfReseredPage;
  Serializable::deserialize(is, sizeOfReseredPage);
  assert(sizeOfReseredPage == inmem_->getSizeOfReservedPage());
  Serializable::deserialize(is, outPage, sizeOfReseredPage);
}
void DBDataStore::loadDynamicKeys() {
  for(uint32_t pageid = 0; pageid < inmem_->getNumberOfReservedPages(); ++pageid)
    for(uint64_t chkp = 0; chkp < inmem_->getMaxNumOfStoredCheckpoints(); ++chkp ) {
      Sliver val;
      get(staticResPageKey(pageid, chkp), val);
      keysofkeys_->operator[]({pageid,chkp}) = val.toString();
    }
}
void DBDataStore::loadResPages() {
  loadDynamicKeys();
  // populate pages map
  for(auto& it: *keysofkeys_.get()){
    //retrieve page
    Sliver serializedPage;
    if(!get(it.second, serializedPage)){
      LOG_TRACE(logger(), "failed to load page for ["
                          << staticResPageKey(it.first.pageId, it.first.checkpoint) << ":" << it.second << "]");
      continue;
    }
    std::istringstream iss(reinterpret_cast<const char*>(serializedPage.data()));
    uint32_t pageid;
    uint64_t checkpoint;
    STDigest digest;
    char* page = new char[inmem_->getSizeOfReservedPage()];
    deserializeResPage(iss, pageid, checkpoint, digest, page);
    inmem_->setResPage(pageid, checkpoint, digest, page);
    delete [] page;// is copied
  }
}
void DBDataStore::setResPageTxn( uint32_t        inPageId,
                                 uint64_t        inCheckpoint,
                                 const STDigest& inPageDigest,
                                 const char*     inPage,
                                 ITransaction*   txn){
  std::ostringstream oss;
  serializeResPage(oss, inPageId, inCheckpoint, inPageDigest, inPage);
  Sliver dynamic_key = dynamicResPageKey(inPageId, inCheckpoint);
  txn->put(dynamic_key, oss.str());
  txn->put(staticResPageKey(inPageId, inCheckpoint), dynamic_key);
  keysofkeys_->operator[]({inPageId, inCheckpoint}) = dynamic_key.toString();
}

void DBDataStore::setResPage( uint32_t        inPageId,
                              uint64_t        inCheckpoint,
                              const STDigest& inPageDigest,
                              const char*     inPage){
  if(txn_)
    setResPageTxn(inPageId, inCheckpoint, inPageDigest, inPage, txn_.get());
  else {
    ITransaction::Guard g(dbc_->beginTransaction());
    setResPageTxn(inPageId, inCheckpoint, inPageDigest, inPage, g.txn());
  }
  inmem_->setResPage(inPageId, inCheckpoint, inPageDigest, inPage);
}
/** ******************************************************************************************************************
 *  Pending Reserved Pages
 */
void DBDataStore::deserializePendingPage(std::istream& is, char* page, uint32_t& pageLen) const {
  Serializable::deserialize(is, pageLen);
  page = new char[pageLen];
  Serializable::deserialize(is, page, pageLen);
}

void DBDataStore::loadPendingPages() {
  for(uint32_t pageid = 0; pageid < inmem_->getNumberOfReservedPages(); ++pageid) {
    Sliver serializedPendingPage;
    if(!get(pendingPageKey(pageid), serializedPendingPage))
      continue;

    std::istringstream iss(reinterpret_cast<const char*>(serializedPendingPage.data()));
    std::uint32_t pageLen = 0;
    char* page = nullptr;
    deserializePendingPage(iss, page, pageLen);
    inmem_->setPendingResPage(pageid, page, pageLen);
    delete [] page;
  }
}

void DBDataStore::setPendingResPage(uint32_t    inPageId,
                                    const char* inPage,
                                    uint32_t    inPageLen){
  std::ostringstream oss;
  Serializable::serialize(oss, inPageLen);
  Serializable::serialize(oss, inPage, inPageLen);
  put(pendingPageKey(inPageId), oss.str());
  inmem_->setPendingResPage(inPageId, inPage, inPageLen);
}

void DBDataStore::associatePendingResPageWithCheckpoint(uint32_t inPageId,
                                                        uint64_t inCheckpoint,
                                                        const STDigest& inPageDigest){
  if(txn_)
    associatePendingResPageWithCheckpointTxn(inPageId, inCheckpoint, inPageDigest, txn_.get());
  else{
    ITransaction::Guard g(dbc_->beginTransaction());
    associatePendingResPageWithCheckpointTxn(inPageId, inCheckpoint, inPageDigest, g.txn());
  }
  inmem_->associatePendingResPageWithCheckpoint(inPageId, inCheckpoint, inPageDigest);
}

void DBDataStore::associatePendingResPageWithCheckpointTxn(uint32_t inPageId,
                                                           uint64_t inCheckpoint,
                                                           const STDigest& inPageDigest,
                                                           ITransaction* txn) {
  Sliver serializedPendingPage;
  if (get(pendingPageKey(inPageId), serializedPendingPage)){
    txn->del(pendingPageKey(inPageId));
    std::istringstream iss(reinterpret_cast<const char*>(serializedPendingPage.data()));
    std::uint32_t pageLen = 0;
    char* page = nullptr;
    deserializePendingPage(iss, page, pageLen);
    setResPageTxn(inPageId, inCheckpoint, inPageDigest, page, txn);
    delete [] page;
    txn->del(pendingPageKey(inPageId));
  }else{
    LOG_WARN(logger(), "pending page: " << inPageId << " not found in db");
  }
}
void DBDataStore::deleteAllPendingPagesTxn(ITransaction* txn) {
  std::set<uint32_t> pageIds = getNumbersOfPendingResPages();
  for(auto pid: pageIds)
    txn->del(pendingPageKey(pid));
}
void DBDataStore::deleteAllPendingPages() {
  if(txn_)
    deleteAllPendingPagesTxn(txn_.get());
  else{
    ITransaction::Guard g(dbc_->beginTransaction());
    deleteAllPendingPagesTxn(g.txn());
  }
  inmem_->deleteAllPendingPages();
}

void DBDataStore::deleteCoveredResPageInSmallerCheckpointsTxn( uint64_t minChkp, ITransaction* txn) {

  auto pages = inmem_->getPagesMap();

  for (auto it = std::find_if(pages.begin(),
                              pages.end(),
                              [minChkp](const std::pair<InMemoryDataStore::ResPageKey,
                                                        InMemoryDataStore::ResPageVal>& elt) {
                                                       return elt.first.checkpoint < minChkp;});
        it != pages.end();
        it++){
    txn->del(staticResPageKey (it->first.pageId, it->first.checkpoint));
    txn->del(dynamicResPageKey(it->first.pageId, it->first.checkpoint));
  }
  inmem_->deleteCoveredResPageInSmallerCheckpoints(minChkp);
}
void DBDataStore::deleteCoveredResPageInSmallerCheckpoints( uint64_t minChkp) {
  if(txn_)
    deleteCoveredResPageInSmallerCheckpointsTxn(minChkp, txn_.get());
   else{
     ITransaction::Guard g(dbc_->beginTransaction());
     deleteCoveredResPageInSmallerCheckpointsTxn(minChkp, g.txn());
   }
  inmem_->deleteCoveredResPageInSmallerCheckpoints(minChkp);
}

/** ******************************************************************************************************************/
}
}
}
