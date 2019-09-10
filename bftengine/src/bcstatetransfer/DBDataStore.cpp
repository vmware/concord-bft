#include "DBDataStore.hpp"
#include "storage/db_interface.h"
#include "Serializable.h"

namespace bftEngine {
namespace SimpleBlockchainStateTransfer {
namespace impl  {
using bftEngine::SimpleBlockchainStateTransfer::BLOCK_DIGEST_SIZE;
using concord::serialize::Serializable;

void     DBDataStore::setAsInitialized(){ putInt("Initialized", 1);}
bool     DBDataStore::initialized()     { return get<bool>("Initialized");}

void     DBDataStore::setMyReplicaId(uint16_t id) { putInt("MyReplicaId", id);}
uint16_t DBDataStore::getMyReplicaId()            { return get<uint16_t>("MyReplicaId");}

void     DBDataStore::setMaxNumOfStoredCheckpoints(uint64_t num){putInt("MaxNumOfStoredCheckpoints", num);}
uint64_t DBDataStore::getMaxNumOfStoredCheckpoints()            {return get<uint64_t>("MaxNumOfStoredCheckpoints");}

void     DBDataStore::setNumberOfReservedPages(uint32_t numResPages){putInt("NumberOfReservedPages", numResPages);}
uint32_t DBDataStore::getNumberOfReservedPages()                    {return get<uint32_t>("NumberOfReservedPages");}

void     DBDataStore::setLastStoredCheckpoint(uint64_t c) {putInt("LastStoredCheckpoint", c);}
uint64_t DBDataStore::getLastStoredCheckpoint()           {return get<uint64_t>("LastStoredCheckpoint");}

void     DBDataStore::setFirstStoredCheckpoint(uint64_t c) {putInt("FirstStoredCheckpoint", c);}
uint64_t DBDataStore::getFirstStoredCheckpoint()           {return get<uint64_t>("FirstStoredCheckpoint");}

void     DBDataStore::setIsFetchingState(bool b){putInt("IsFetchingState", b);}
bool     DBDataStore::getIsFetchingState()      {return get<bool>("IsFetchingState");}

void     DBDataStore::setFVal(uint16_t fVal) { putInt("fVal", fVal);}
uint16_t DBDataStore::getFVal()              {return get<uint16_t>("fVal");}

void     DBDataStore::setFirstRequiredBlock(uint64_t i){putInt("FirstRequiredBlock", i);}
uint64_t DBDataStore::getFirstRequiredBlock()          {return get<uint64_t>("FirstRequiredBlock");}

void     DBDataStore::setLastRequiredBlock(uint64_t i){putInt("LastRequiredBlock", i);}
uint64_t DBDataStore::getLastRequiredBlock()          {return get<uint64_t>("LastRequiredBlock");}


/** ******************************************************************************************************************/

void
DBDataStore::setReplicas(const std::set<std::uint16_t> replicas) {
  std::ostringstream oss;
  Serializable::serialize(oss, replicas);
  put("Replicas", oss.str());
}
std::set<uint16_t>
DBDataStore::getReplicas() {
  concordUtils::Sliver val;
  get("Replicas", val);
  if(val.length() == 0) return {};
  std::istringstream iss(reinterpret_cast<const char*>(val.data()));
  std::set<std::uint16_t> res;
  Serializable::deserialize(iss, res);
  return res;
}
/** ******************************************************************************************************************
 *  Checkpoint descriptions
 */
void
DBDataStore::serializeCheckpoint(std::ostream& os, const CheckpointDesc& desc) {
  Serializable::serialize(os, desc.checkpointNum);
  Serializable::serialize(os, desc.lastBlock);
  Serializable::serialize(os, desc.digestOfLastBlock.get()         , BLOCK_DIGEST_SIZE);
  Serializable::serialize(os, desc.digestOfResPagesDescriptor.get(), BLOCK_DIGEST_SIZE);
}
void
DBDataStore::deserializeCheckpoint(std::istream& is, CheckpointDesc& desc) {

  Serializable::deserialize(is, desc.checkpointNum);
  Serializable::deserialize(is, desc.digestOfLastBlock.getForUpdate()         , BLOCK_DIGEST_SIZE);
  Serializable::deserialize(is, desc.digestOfResPagesDescriptor.getForUpdate(), BLOCK_DIGEST_SIZE);
}
void
DBDataStore::setCheckpointDesc(uint64_t checkpoint,  const CheckpointDesc& desc){
  std::ostringstream oss;
  serializeCheckpoint(oss, desc);
  put("CheckpointDesc" + std::to_string(checkpoint), oss.str());
}
DataStore::CheckpointDesc
DBDataStore::getCheckpointDesc(uint64_t checkpoint) {
  concordUtils::Sliver val;
  get("CheckpointDesc" + std::to_string(checkpoint), val);
  if(val.length() == 0) return {};
  std::istringstream iss(reinterpret_cast<const char*>(val.data()));
  DataStore::CheckpointDesc cpd;
  deserializeCheckpoint(iss, cpd);
  return cpd;
}
bool
DBDataStore::hasCheckpointDesc(uint64_t checkpoint) {
  DataStore::CheckpointDesc cpd = getCheckpointDesc(checkpoint);
  return cpd.checkpointNum != 0;
}
void
DBDataStore::setCheckpointBeingFetched(const CheckpointDesc& desc) {
  std::ostringstream oss;
  serializeCheckpoint(oss, desc);
  put("CheckpointBeingFetched", oss.str());
}
DataStore::CheckpointDesc
DBDataStore::getCheckpointBeingFetched() {
  concordUtils::Sliver val;
  get("CheckpointBeingFetched", val);
  if(val.length() == 0) return {};
  std::istringstream iss(reinterpret_cast<const char*>(val.data()));
  DataStore::CheckpointDesc cpd;
  deserializeCheckpoint(iss, cpd);
  return cpd;
}
void
DBDataStore::deleteCheckpointBeingFetched() {
  del("CheckpointBeingFetched");
}
bool
DBDataStore::hasCheckpointBeingFetched() {
  DataStore::CheckpointDesc cpd = getCheckpointBeingFetched();
  return cpd.checkpointNum != 0;
}
/** *******************************************************************************************************************
 *  Reserved Pages
 *
 * key - @see resPageKey()
 * val - [inPageDigest][sizeOfReservedPage][inPage]
 */
void
DBDataStore::setResPage( uint32_t        inPageId,
                         uint64_t        inCheckpoint,
                         const STDigest& inPageDigest,
                         const char*     inPage){
  std::ostringstream oss;
  Serializable::serialize(oss, inPageDigest.get(), BLOCK_DIGEST_SIZE);
  Serializable::serialize(oss, sizeOfReservedPage_);
  Serializable::serialize(oss, inPage, sizeOfReservedPage_);
  put(resPageKey(inCheckpoint, inPageId), oss.str());
}
void
DBDataStore::getResPage(uint32_t inPageId,
                        uint64_t inCheckpoint,
                        uint64_t* outActualCheckpoint) {
  getResPage(inPageId, inCheckpoint, outActualCheckpoint, nullptr, nullptr, 0);
}
void
DBDataStore::getResPage(uint32_t inPageId,
                        uint64_t inCheckpoint,
                        uint64_t* outActualCheckpoint,
                        char* outPage,
                        uint32_t copylength) {
  getResPage(inPageId, inCheckpoint, outActualCheckpoint, nullptr, outPage, copylength);
}
void
DBDataStore::getResPage(uint32_t inPageId,
                        uint64_t inCheckpoint,
                        uint64_t* outActualCheckpoint,
                        STDigest* outPageDigest,
                        char* outPage,
                        uint32_t copylength) {
  concordUtils::Sliver val;
  bool found = get(resPageKey(inCheckpoint, inPageId), val); // TODO [TK] lower_bound
  if(!found) return;
  //*outActualCheckpoint = ; // TODO [TK]
  std::istringstream iss(reinterpret_cast<const char*>(val.data()));
  if(outPageDigest){
    char digest[BLOCK_DIGEST_SIZE];
    Serializable::deserialize(iss, digest, BLOCK_DIGEST_SIZE);
    *outPageDigest = digest;
  }
  if(outPage) {
    std::uint32_t sizeOfReseredPage;
    Serializable::deserialize(iss, sizeOfReseredPage);
    // assert(sizeOfReseredPage == sizeOfReservedPage_); [TK] is it right ???
    Serializable::deserialize(iss, outPage, sizeOfReseredPage);
  }
}

DataStore::ResPagesDescriptor*
DBDataStore::getResPagesDescriptor(uint64_t inCheckpoint) {return nullptr;}
/** ******************************************************************************************************************
 *  Pending Reserved Pages
 */

void
DBDataStore::getPendingResPage( uint32_t inPageId,
                                char*    outPage,
                                uint32_t pageLen) {
  assert(pageLen <= sizeOfReservedPage_);
  concordUtils::Sliver val;
  bool found = get(pendingPageKey(inPageId), val);
  assert(found);
  memcpy(outPage, val.data(), pageLen);
}
void
DBDataStore::setPendingResPage(uint32_t    inPageId,
                               const char* inPage,
                               uint32_t    inPageLen){
  assert(inPageLen <= sizeOfReservedPage_);


}

bool           DBDataStore::hasPendingResPage(uint32_t inPageId) {return false;}
uint32_t       DBDataStore::numOfAllPendingResPage() {return 0;}
std::set<uint32_t>  DBDataStore::getNumbersOfPendingResPages() {return {};}
void
DBDataStore::associatePendingResPageWithCheckpoint(uint32_t inPageId,
                                                   uint64_t inCheckpoint,
                                                   const STDigest& inPageDigest) {}
void DBDataStore::deleteAllPendingPages() {}
/** ******************************************************************************************************************/

void DBDataStore::deleteDescOfSmallerCheckpoints(uint64_t checkpoint) {}
void DBDataStore::deleteCoveredResPageInSmallerCheckpoints( uint64_t inCheckpoint) {}
void DBDataStore::free(DataStore::ResPagesDescriptor*) {}






}
}
}
