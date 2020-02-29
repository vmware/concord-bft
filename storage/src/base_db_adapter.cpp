// Copyright 2020 VMware, all rights reserved

#include "blockchain/base_db_adapter.h"
#include "blockchain/db_types.h"
#include "kv_types.hpp"
#include "Logger.hpp"

#include <memory>

using concordUtils::Key;
using concordUtils::Sliver;

namespace concord {
namespace storage {
namespace blockchain {

DBAdapterBase::DBAdapterBase(const std::shared_ptr<IDBClient> &db, bool readOnly)
    : logger_{concordlogger::Log::getLogger("concord.storage.BlockchainDBAdapter")}, db_{db} {
  db_->init(readOnly);
}

}  // namespace blockchain
}  // namespace storage
}  // namespace concord
